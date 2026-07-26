package gateway

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/grounding"
)

// ChatPlugin serves grounded question-answering over the Atlas bundle.
//
// It is a separate plugin from PolicyPlugin on purpose: it reads the shipped
// static bundle rather than PostGIS, so it answers from exactly the dataset a
// reader sees on the Atlas pages. Two sources of truth for the same numbers is
// how a chat ends up contradicting the map next to it.
type ChatPlugin struct {
	engine *grounding.Engine
}

// NewChatPlugin loads the Atlas bundle from dir and wires whichever model lanes
// are configured. It returns an error only if the bundle itself is unreadable —
// a missing model lane is a degraded mode, not a failure: structured queries
// still work, and questions get an honest "no planner configured".
func NewChatPlugin(dir string) (*ChatPlugin, error) {
	ds, err := grounding.Load(dir)
	if err != nil {
		return nil, err
	}
	e := &grounding.Engine{Dataset: ds, MaxPlanAttempts: 2}

	// OpenRouter for quality, Ollama for cost, per PIP-92. Whichever is
	// configured wins; if both are, OpenRouter plans and Ollama is available as
	// the cheaper composer, since composing is the easier of the two jobs and
	// its output is verified anyway.
	or := grounding.OpenRouterFromEnv()
	ol := grounding.OllamaFromEnv()
	switch {
	case or != nil:
		e.Planner = or
		if ol != nil {
			e.Composer = ol
		} else {
			e.Composer = or
		}
	case ol != nil:
		e.Planner = ol
		e.Composer = ol
	}

	return &ChatPlugin{engine: e}, nil
}

// Name returns the plugin identifier.
func (p *ChatPlugin) Name() string { return "policy-data-grounded-chat" }

// Health reports whether the dataset loaded.
func (p *ChatPlugin) Health() error { return nil }

// RegisterRoutes mounts the chat endpoints under the given group.
func (p *ChatPlugin) RegisterRoutes(group *gin.RouterGroup) {
	group.POST("/chat", p.handleChat)
	group.POST("/chat/query", p.handleStructuredQuery)
	group.GET("/chat/schema", p.handleSchema)
}

type chatRequest struct {
	Query string `json:"query"`
}

// handleChat answers a natural-language question.
//
// Always 200 with an `answered` flag rather than a 4xx for a refusal: "I can't
// answer that from this data" is a successful, correct response, and returning
// it as an error invites callers to retry or paper over it.
func (p *ChatPlugin) handleChat(c *gin.Context) {
	var req chatRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Query) == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "body must be JSON with a non-empty \"query\" field",
		})
		return
	}

	ans, err := p.engine.Answer(c.Request.Context(), req.Query)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, ans)
}

// handleStructuredQuery takes an Intent directly — no model anywhere in the
// path. This is the endpoint an agent or a script should use: same execution,
// same citations, fully deterministic, no key required.
func (p *ChatPlugin) handleStructuredQuery(c *gin.Context) {
	var in grounding.Intent
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "body must be a JSON Intent: " + err.Error()})
		return
	}
	ans, err := p.engine.AnswerIntent(c.Request.Context(), "", &in)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, ans)
}

// handleSchema publishes the query vocabulary — the same thing the planner is
// shown. Exposing it means a caller can build a valid Intent without guessing,
// and an agent can discover the surface instead of probing it.
func (p *ChatPlugin) handleSchema(c *gin.Context) {
	ds := p.engine.Dataset
	c.JSON(http.StatusOK, gin.H{
		"operations": grounding.AllOperations,
		"levels":     []string{string(grounding.LevelCounty), string(grounding.LevelTract)},
		"aggregates": []string{"median", "min", "max", "count"},
		"indicators": ds.Indicators,
		"vintage":    ds.Vintage,
		"counts": gin.H{
			"county": ds.Count(grounding.LevelCounty),
			"tract":  ds.Count(grounding.LevelTract),
		},
		"notes": []string{
			"Mean is unavailable: averaging medians does not produce a median.",
			"Every number in a response comes from executing the query; the model never supplies a figure.",
			"A model-written answer is verified against the result set before it is returned; " +
				"source=model-rejected-fell-back means a draft was discarded.",
		},
	})
}
