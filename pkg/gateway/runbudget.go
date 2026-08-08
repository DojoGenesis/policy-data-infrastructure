package gateway

// Run admission control for POST /analyses (ADR-014 D10): a queued-run
// endpoint on a public surface is a denial-of-service lever with a database
// behind it, so the ceiling ships BEFORE the endpoint, not after.
//
// Modeled on ChatBudget (reserve-then-settle, UTC-daily roll, per-client
// sub-cap, CF-Connecting-IP identity) with one deliberate difference: runs
// are counted, not priced, and a run is consumed at ADMISSION with no refund
// on failure. Refunding failed runs would let a crashing run type farm free
// retries, and a failed run consumed queue slot and compute regardless.
//
// Auth is a separate concern (runAuthMiddleware below): D10 fixes the
// mechanism, ADR-014 OQ4 leaves the policy — open-with-ceiling versus
// token-gated — to the operator, so the token is optional configuration, and
// the budget applies in both modes.

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Environment variables read by RunBudgetConfigFromEnv.
const (
	envRunGlobalDaily = "PDI_RUN_GLOBAL_DAILY"
	envRunClientDaily = "PDI_RUN_CLIENT_DAILY"
	envRunQueueDepth  = "PDI_RUN_QUEUE_DEPTH"
	envRunToken       = "PDI_RUN_TOKEN" //nolint:gosec // env var NAME, not a credential
)

// RunBudgetConfig caps queued analysis runs.
type RunBudgetConfig struct {
	// GlobalDailyRuns is the total number of runs admitted per UTC day.
	GlobalDailyRuns int
	// PerClientDailyRuns is one client's share of the day.
	PerClientDailyRuns int
	// MaxQueueDepth refuses new runs while this many are queued or running,
	// independent of the daily tallies — the DB-backed queue must stay
	// bounded even inside the daily allowance.
	MaxQueueDepth int
}

// DefaultRunBudgetConfig mirrors chatbudget's posture: small, public-safe
// defaults that an operator raises deliberately.
func DefaultRunBudgetConfig() RunBudgetConfig {
	return RunBudgetConfig{
		GlobalDailyRuns:    200,
		PerClientDailyRuns: 20,
		MaxQueueDepth:      32,
	}
}

// RunBudgetConfigFromEnv builds the config from environment variables,
// falling back to defaults (never to "unlimited") on absent or invalid
// values. The warnings list names every value it refused.
func RunBudgetConfigFromEnv() (RunBudgetConfig, []string) {
	cfg := DefaultRunBudgetConfig()
	var warnings []string
	cfg.GlobalDailyRuns = envPositiveInt(envRunGlobalDaily, cfg.GlobalDailyRuns, &warnings)
	cfg.PerClientDailyRuns = envPositiveInt(envRunClientDaily, cfg.PerClientDailyRuns, &warnings)
	cfg.MaxQueueDepth = envPositiveInt(envRunQueueDepth, cfg.MaxQueueDepth, &warnings)
	return cfg, warnings
}

func envPositiveInt(name string, def int, warnings *[]string) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		*warnings = append(*warnings,
			fmt.Sprintf("%s=%q is not a positive integer; using default %d", name, raw, def))
		return def
	}
	return v
}

// RunBudget is an in-memory UTC-daily admission ledger for analysis runs.
// Like ChatBudget it is deliberately not persisted: restarts are
// operator-initiated deploys, so the worst case is one extra day's allowance
// per deploy — bounded and cheap next to putting the ledger on the request
// path's critical dependencies. The queue itself IS persisted (analysis_runs
// table), so MaxQueueDepth is enforced against durable truth, not this
// ledger.
type RunBudget struct {
	cfg RunBudgetConfig

	mu      sync.Mutex
	now     func() time.Time
	day     string // UTC calendar day, "2006-01-02"
	used    int
	clients map[string]int
}

// NewRunBudget builds a ledger rolling at UTC midnight.
func NewRunBudget(cfg RunBudgetConfig) *RunBudget {
	b := &RunBudget{
		cfg:     cfg,
		now:     time.Now,
		clients: make(map[string]int),
	}
	b.day = b.now().UTC().Format("2006-01-02")
	return b
}

// Config returns the immutable configuration.
func (b *RunBudget) Config() RunBudgetConfig { return b.cfg }

// SetClock replaces the time source. Test-only.
func (b *RunBudget) SetClock(fn func() time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.now = fn
	b.day = fn().UTC().Format("2006-01-02")
}

// Admit consumes one run from the day's allowance, or explains the refusal.
// queuedNow is the caller-measured count of queued+running rows, checked
// against MaxQueueDepth here so every limit lives in one place.
func (b *RunBudget) Admit(clientKey string, queuedNow int) *RunDenial {
	if clientKey == "" {
		clientKey = UnknownClientKey
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now().UTC()
	b.rollLocked(now)

	if queuedNow >= b.cfg.MaxQueueDepth {
		return &RunDenial{
			Scope:   "queue",
			Message: fmt.Sprintf("run queue is full (%d deep); retry when current runs finish", queuedNow),
		}
	}
	if b.used >= b.cfg.GlobalDailyRuns {
		return &RunDenial{
			Scope:      "daily",
			RetryAfter: nextUTCMidnight(now).Sub(now),
			Message:    fmt.Sprintf("daily run budget (%d) exhausted; resets at UTC midnight", b.cfg.GlobalDailyRuns),
		}
	}
	if b.clients[clientKey] >= b.cfg.PerClientDailyRuns {
		return &RunDenial{
			Scope:      "client",
			RetryAfter: nextUTCMidnight(now).Sub(now),
			Message:    fmt.Sprintf("per-client run budget (%d) exhausted; resets at UTC midnight", b.cfg.PerClientDailyRuns),
		}
	}

	b.used++
	b.clients[clientKey]++
	return nil
}

// rollLocked resets the ledger when the UTC calendar day changes.
func (b *RunBudget) rollLocked(now time.Time) {
	day := now.UTC().Format("2006-01-02")
	if day == b.day {
		return
	}
	b.day = day
	b.used = 0
	b.clients = make(map[string]int)
}

// RunDenial is a refused admission with an operator-actionable reason.
type RunDenial struct {
	Scope      string // "queue" | "daily" | "client"
	RetryAfter time.Duration
	Message    string
}

// Abort writes the denial as HTTP 429 and stops the handler chain.
func (d *RunDenial) Abort(c *gin.Context) {
	if d.RetryAfter > 0 {
		c.Header("Retry-After", strconv.Itoa(int(d.RetryAfter.Seconds())))
	}
	c.AbortWithStatusJSON(http.StatusTooManyRequests, ErrorResponse{
		Error:  "run budget exceeded",
		Detail: d.Message,
	})
}

// runAuthMiddleware gates a route behind a bearer token when one is
// configured. An empty token means open access — ADR-014 OQ4's
// open-with-a-ceiling posture — and the middleware becomes a no-op rather
// than a lock-out, so the safety default cannot brick the endpoint.
func runAuthMiddleware(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if token == "" {
			c.Next()
			return
		}
		const prefix = "Bearer "
		header := c.GetHeader("Authorization")
		if strings.HasPrefix(header, prefix) {
			presented := strings.TrimSpace(strings.TrimPrefix(header, prefix))
			if subtle.ConstantTimeCompare([]byte(presented), []byte(token)) == 1 {
				c.Next()
				return
			}
		}
		c.Header("WWW-Authenticate", "Bearer")
		c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorResponse{
			Error:  "authentication required",
			Detail: "this deployment gates analysis runs behind a bearer token (" + envRunToken + ")",
		})
	}
}
