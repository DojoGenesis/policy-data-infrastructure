package grounding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

// Planner turns a question into an Intent. It is the only place a model is
// allowed to influence what gets looked up, and its output is validated against
// a closed schema before anything runs.
type Planner interface {
	Plan(ctx context.Context, question string, ds *Dataset, retryHint string) (*Intent, error)
	Name() string
}

// Composer turns a Result into prose. Its draft is verified before use, and a
// failing draft is discarded in favour of Result.Facts.
type Composer interface {
	Compose(ctx context.Context, question string, res *Result) (string, error)
	Name() string
}

// ── system prompt ──────────────────────────────────────────────────────

// PlannerPrompt is built per-request so the model sees this deployment's actual
// vocabulary. Handing it the real indicator ids and place names is what makes
// "refuse rather than guess" achievable: it cannot invent an indicator it was
// never shown, and if it does, Validate rejects it.
func PlannerPrompt(ds *Dataset) string {
	var b strings.Builder
	b.WriteString(`You translate a question about Wisconsin census data into a JSON query.

Reply with ONE JSON object and nothing else. No prose, no code fence.

You never answer the question and you never state a number. You only produce the
query. A separate deterministic step runs it against the real data.

Schema:
{
  "operation":  "lookup" | "rank" | "compare" | "aggregate" | "threshold" | "representation" | "time_series",
  "indicator":  one of the indicator ids below (omit for representation),
  "places":     array of county names or tract GEOIDs,
  "level":      "county" | "tract"   (default "county"),
  "limit":      integer, for rank and threshold,
  "direction":  "highest" | "lowest", for rank,
  "aggregate":  "median" | "min" | "max" | "count", for aggregate,
  "threshold":  number, for threshold,
  "comparator": "above" | "below", for threshold
}

Operation guide:
  lookup          one value for one or more named places
  compare         two or more named places, side by side
  rank            the highest or lowest N places
  aggregate       a summary across every place at a level
  threshold       every place above or below a number
  representation  which districts cover a place and who holds the seats
  time_series     how one indicator has changed over time. Include the place name in "places". The system will fetch all available vintages automatically.

Rules:
- "mean" and "average" are NOT available. Averaging medians does not produce a
  median. Use "median".
- Direction is about the VALUE, not about whether it is good. "Worst poverty"
  means direction "highest".
- representation is only available at tract level.
- If the question cannot be expressed in this schema, or asks about an indicator
  that is not listed, reply exactly: {"operation":"unsupported"}
  Refusing is correct and expected. Do not approximate with a different
  indicator.

`)
	b.WriteString("Indicator ids available in this dataset:\n")
	for _, ind := range ds.Indicators {
		b.WriteString("  " + ind.ID + " — " + ind.Label)
		if ind.Unit != "" {
			b.WriteString(" (" + ind.Unit + ")")
		}
		b.WriteString("\n")
	}
	b.WriteString("\nGeography: Wisconsin only. ")
	fmt.Fprintf(&b, "%d counties and %d census tracts, ACS %s.\n",
		ds.Count(LevelCounty), ds.Count(LevelTract), ds.Vintage)
	b.WriteString("Counties may be named with or without the word \"County\". " +
		"Tracts must be given as an 11-digit GEOID.\n")
	return b.String()
}

// ComposerPrompt constrains the writing stage. Even so, its output is verified —
// the prompt is a request, the verifier is the guarantee.
const ComposerPrompt = `You write one short, plain answer from a result set.

Absolute rule: every number in your answer must appear in the result JSON.
Do not round, do not convert units, do not add context figures, do not compare
to any year or place that is not in the result. If you want to say a number and
it is not in the result, leave it out.

Do not speculate about causes. This data does not support causal claims.
Two or three sentences. Name the source at the end.`

// ── OpenRouter / Ollama lane ───────────────────────────────────────────

// ChatModel is one configured model lane. OpenRouter and Ollama both speak the
// OpenAI chat-completions shape, so one client covers both; only the base URL,
// key, and model name differ.
type ChatModel struct {
	BaseURL string
	APIKey  string
	Model   string
	Label   string
	Client  *http.Client
}

// OpenRouterFromEnv builds the quality lane. Returns nil when unconfigured, so
// a deployment without a key degrades to deterministic answers rather than
// failing to start.
func OpenRouterFromEnv() *ChatModel {
	key := os.Getenv("OPENROUTER_API_KEY")
	if key == "" {
		return nil
	}
	model := os.Getenv("OPENROUTER_MODEL")
	if model == "" {
		model = "anthropic/claude-sonnet-4.5"
	}
	return &ChatModel{
		BaseURL: "https://openrouter.ai/api/v1",
		APIKey:  key, Model: model, Label: "openrouter",
		Client: &http.Client{Timeout: 45 * time.Second},
	}
}

// OllamaFromEnv builds the cost lane — a local or self-hosted open model.
func OllamaFromEnv() *ChatModel {
	base := os.Getenv("OLLAMA_BASE_URL")
	if base == "" {
		return nil
	}
	model := os.Getenv("OLLAMA_MODEL")
	if model == "" {
		model = "llama3.1"
	}
	return &ChatModel{
		BaseURL: strings.TrimSuffix(base, "/") + "/v1",
		Model:   model, Label: "ollama",
		Client:  &http.Client{Timeout: 90 * time.Second},
	}
}

func (m *ChatModel) Name() string { return m.Label + ":" + m.Model }

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func (m *ChatModel) complete(ctx context.Context, system, user string, maxTokens int) (string, error) {
	payload := map[string]any{
		"model": m.Model,
		"messages": []chatMessage{
			{Role: "system", Content: system},
			{Role: "user", Content: user},
		},
		"temperature": 0,
		"max_tokens":  maxTokens,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		m.BaseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if m.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+m.APIKey)
	}

	resp, err := m.Client.Do(req)
	if err != nil {
		return "", fmt.Errorf("%s: %w", m.Label, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s: HTTP %d: %s", m.Label, resp.StatusCode,
			strings.TrimSpace(string(raw)))
	}
	var out struct {
		Choices []struct {
			Message chatMessage `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("%s: decode: %w", m.Label, err)
	}
	if len(out.Choices) == 0 {
		return "", fmt.Errorf("%s: no choices returned", m.Label)
	}
	return out.Choices[0].Message.Content, nil
}

// jsonObjectRe salvages the JSON object from a model that wrapped it in a fence
// or a sentence despite being told not to. Being lenient here costs nothing —
// Validate is still the gate.
var jsonObjectRe = regexp.MustCompile(`(?s)\{.*\}`)

// Plan implements Planner.
func (m *ChatModel) Plan(ctx context.Context, question string, ds *Dataset, retryHint string) (*Intent, error) {
	user := question
	if retryHint != "" {
		user = question + "\n\nYour previous query was rejected: " + retryHint +
			"\nProduce a corrected query, or {\"operation\":\"unsupported\"}."
	}
	text, err := m.complete(ctx, PlannerPrompt(ds), user, 300)
	if err != nil {
		return nil, err
	}
	match := jsonObjectRe.FindString(text)
	if match == "" {
		return nil, fmt.Errorf("planner returned no JSON object: %q", truncate(text, 200))
	}
	var in Intent
	if err := json.Unmarshal([]byte(match), &in); err != nil {
		return nil, fmt.Errorf("planner JSON did not parse: %w", err)
	}
	return &in, nil
}

// Compose implements Composer.
func (m *ChatModel) Compose(ctx context.Context, question string, res *Result) (string, error) {
	payload, err := json.Marshal(res)
	if err != nil {
		return "", err
	}
	user := "Question: " + question + "\n\nResult JSON:\n" + string(payload)
	return m.complete(ctx, ComposerPrompt, user, 400)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
