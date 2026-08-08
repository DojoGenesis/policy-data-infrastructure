package gateway

import (
	"context"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/grounding"
)

// ChatPlugin serves grounded question-answering.
//
// Two grounding modes share one engine:
//
//   - Store-backed (NewChatPluginFromStore, what `pdi serve` mounts): the
//     dataset is a snapshot of the live database — every indicator with
//     data, both levels, per-indicator vintages — refreshed on a TTL. The
//     original bundle-only design existed so chat could never answer from a
//     different dataset than the map; drawing both from the one database
//     honors that principle without freezing the agent at 11 indicators
//     while the platform grows past 40 (the 2026-08-08 directive).
//   - Bundle-backed (NewChatPlugin, the offline CLI): the shipped atlas
//     bundle, unchanged.
//
// The engine pointer is swapped atomically on refresh; a request in flight
// keeps the snapshot it started with.
type ChatPlugin struct {
	engine atomic.Pointer[grounding.Engine]

	// Store-mode refresh state; src is nil in bundle mode.
	src        grounding.StoreSource
	ttl        time.Duration
	loadedAt   atomic.Int64 // unix seconds of the current snapshot
	refreshing atomic.Bool
}

// buildEngine wires the model lanes around a dataset. OpenRouter for
// quality, Ollama for cost, per PIP-92: whichever is configured wins; if
// both are, OpenRouter plans and Ollama composes, since composing is the
// easier job and its output is verified anyway.
func buildEngine(ds *grounding.Dataset) *grounding.Engine {
	e := &grounding.Engine{Dataset: ds, MaxPlanAttempts: 2}
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
	return e
}

// NewChatPlugin loads the Atlas bundle from dir and wires whichever model
// lanes are configured. It returns an error only if the bundle itself is
// unreadable — a missing model lane is a degraded mode, not a failure:
// structured queries still work, and questions get an honest "no planner
// configured".
func NewChatPlugin(dir string) (*ChatPlugin, error) {
	ds, err := grounding.Load(dir)
	if err != nil {
		return nil, err
	}
	p := &ChatPlugin{}
	p.engine.Store(buildEngine(ds))
	p.loadedAt.Store(time.Now().Unix())
	return p, nil
}

// NewChatPluginFromStore grounds the agent in the live database, refreshing
// its snapshot every ttl (0 means a 5-minute default). Construction fails
// only if the initial snapshot cannot be built — after that, a failed
// refresh keeps serving the previous snapshot and logs, because a stale
// honest dataset beats a dead endpoint.
func NewChatPluginFromStore(ctx context.Context, src grounding.StoreSource, ttl time.Duration) (*ChatPlugin, error) {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	ds, err := grounding.LoadFromStore(ctx, src)
	if err != nil {
		return nil, err
	}
	p := &ChatPlugin{src: src, ttl: ttl}
	p.engine.Store(buildEngine(ds))
	p.loadedAt.Store(time.Now().Unix())
	return p, nil
}

// eng returns the current engine, kicking an async refresh first when the
// store-backed snapshot has aged past its TTL. Requests never wait on a
// rebuild; they use the snapshot that exists.
func (p *ChatPlugin) eng() *grounding.Engine {
	if p.src != nil && time.Now().Unix()-p.loadedAt.Load() > int64(p.ttl.Seconds()) {
		if p.refreshing.CompareAndSwap(false, true) {
			go func() {
				defer p.refreshing.Store(false)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				ds, err := grounding.LoadFromStore(ctx, p.src)
				if err != nil {
					log.Printf("chat: snapshot refresh failed (serving previous): %v", err)
					return
				}
				p.engine.Store(buildEngine(ds))
				p.loadedAt.Store(time.Now().Unix())
			}()
		}
	}
	return p.engine.Load()
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

	ans, err := p.eng().Answer(c.Request.Context(), req.Query)
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
	ans, err := p.eng().AnswerIntent(c.Request.Context(), "", &in)
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
	ds := p.eng().Dataset
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
