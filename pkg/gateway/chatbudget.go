package gateway

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// ── Why this exists ─────────────────────────────────────────────────────────
//
// /v1/chat is public and unauthenticated: anyone with the URL can spend the
// operator's model credit, and every message re-sends a ~6,900-token system
// prompt. Without a cap, the only thing standing between a bored visitor and
// an unbounded bill is politeness.
//
// The design is a two-phase ledger — reserve a conservative estimate before
// forwarding, settle against the Gateway's ACTUAL reported usage afterwards.
// A pre-estimate alone cannot work, because it cannot know how long the answer
// will be; settlement alone cannot work, because by the time you know the cost
// you have already paid it. So: admit on the pessimistic guess, then correct.
//
// The global daily cap is the real guarantee. The per-client cap is a speed
// bump — see ClientKey for exactly how spoofable the key is and why that is
// acceptable.

// ── Measured baseline ───────────────────────────────────────────────────────
//
// These are observations, not policy. They exist so that "how many exchanges
// does the budget buy?" is DERIVED at runtime from the current price and
// budget rather than written down as a literal. A literal is precisely how
// this codebase got burned once already: a pinned model id went stale upstream
// and every request 500'd for an unknown period. A stale "280 exchanges/day"
// comment would rot the same way the moment the model or its price changes.
const (
	// MeasuredPromptTokens is the system prompt that
	// ChatAdapter._buildSystemPrompt() rebuilds and re-sends on EVERY
	// message: 24,817 chars ≈ 6,894 tokens, measured in the browser against
	// production on 2026-07-29. It is ~85% of the cost of an exchange.
	MeasuredPromptTokens = 6894

	// MeasuredCompletionTokens is a typical answer, from the same session
	// (the Gateway reported OutputTokens: 637). Observed range 600–900.
	MeasuredCompletionTokens = 637
)

// ── Defaults ────────────────────────────────────────────────────────────────

const (
	// DefaultDailyBudgetUSD is the operator's stated cap: roughly a dollar a
	// day of model spend for public chat.
	DefaultDailyBudgetUSD = 1.00

	// DefaultPromptUSDPerToken and DefaultCompletionUSDPerToken are
	// OpenRouter's published rates for deepseek/deepseek-v4-pro
	// ($0.435 and $0.87 per 1M tokens), checked 2026-07-29. The model is
	// pinned in cmd/pdi/frontend/lib/chat.js; if that pin moves, move these
	// with it — or better, set the env vars so the binary need not be rebuilt.
	DefaultPromptUSDPerToken     = 0.000000435
	DefaultCompletionUSDPerToken = 0.00000087

	// DefaultClientDailyShare is the fraction of the daily budget any single
	// client may spend. At 5% of $1.00 that is $0.05 ≈ 14 exchanges — enough
	// for one real conversation per person per day, while requiring at least
	// 20 distinct clients to drain the day.
	DefaultClientDailyShare = 0.05

	// DefaultEstimatedOutputTokens is the admission-time guess for answer
	// length. Deliberately the TOP of the observed 600–900 range: over-admit
	// and you overspend, over-reserve and you merely under-serve slightly,
	// and settlement corrects the difference within milliseconds anyway.
	DefaultEstimatedOutputTokens = 900

	// DefaultCharsPerToken converts request bytes to prompt tokens. Derived
	// from the same measurement as MeasuredPromptTokens: 24,817 chars /
	// 6,894 tokens = 3.60 chars per token. Counting BYTES rather than runes
	// is deliberate — multi-byte UTF-8 (the site is bilingual) inflates the
	// count, which errs toward reserving too much.
	DefaultCharsPerToken = 3.6
)

// Environment variables. Only an explicitly-set, parseable, positive value
// takes effect; anything else falls back to the default and prints a warning
// at startup. There is deliberately no "disable" value: an operator who wants
// no cap can set a large number, and a typo can therefore never silently
// remove the cap.
const (
	EnvDailyBudgetUSD        = "PDI_CHAT_DAILY_BUDGET_USD"
	EnvPromptUSDPerToken     = "PDI_CHAT_PROMPT_USD_PER_TOKEN"
	EnvCompletionUSDPerToken = "PDI_CHAT_COMPLETION_USD_PER_TOKEN"
	EnvClientDailyShare      = "PDI_CHAT_CLIENT_DAILY_SHARE"
	EnvEstimatedOutput       = "PDI_CHAT_ESTIMATED_OUTPUT_TOKENS"
)

// UnknownClientKey is the bucket used when no client IP can be resolved at
// all. Everyone who lands here shares one per-client allowance, which is the
// intended pressure: it degrades service for un-attributable traffic rather
// than for everyone.
const UnknownClientKey = "unknown"

// ChatBudgetConfig is the price list and the caps. It is immutable once a
// ChatBudget is built.
type ChatBudgetConfig struct {
	DailyBudgetUSD        float64
	PromptUSDPerToken     float64
	CompletionUSDPerToken float64
	ClientDailyShare      float64
	EstimatedOutputTokens int
	CharsPerToken         float64
}

// DefaultChatBudgetConfig returns the compiled-in defaults.
func DefaultChatBudgetConfig() ChatBudgetConfig {
	return ChatBudgetConfig{
		DailyBudgetUSD:        DefaultDailyBudgetUSD,
		PromptUSDPerToken:     DefaultPromptUSDPerToken,
		CompletionUSDPerToken: DefaultCompletionUSDPerToken,
		ClientDailyShare:      DefaultClientDailyShare,
		EstimatedOutputTokens: DefaultEstimatedOutputTokens,
		CharsPerToken:         DefaultCharsPerToken,
	}
}

// ChatBudgetConfigFromEnv reads the configuration, returning any warnings for
// the caller to print. Bad input never disables the cap — it falls back to the
// default and says so.
func ChatBudgetConfigFromEnv() (ChatBudgetConfig, []string) {
	cfg := DefaultChatBudgetConfig()
	var warnings []string

	cfg.DailyBudgetUSD = envPositiveFloat(EnvDailyBudgetUSD, cfg.DailyBudgetUSD, &warnings)
	cfg.PromptUSDPerToken = envPositiveFloat(EnvPromptUSDPerToken, cfg.PromptUSDPerToken, &warnings)
	cfg.CompletionUSDPerToken = envPositiveFloat(EnvCompletionUSDPerToken, cfg.CompletionUSDPerToken, &warnings)

	share := envPositiveFloat(EnvClientDailyShare, cfg.ClientDailyShare, &warnings)
	if share > 1 {
		warnings = append(warnings, fmt.Sprintf("%s=%g is above 1.0 (a single client could take the whole day); using %g",
			EnvClientDailyShare, share, cfg.ClientDailyShare))
	} else {
		cfg.ClientDailyShare = share
	}

	if v := strings.TrimSpace(os.Getenv(EnvEstimatedOutput)); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			warnings = append(warnings, fmt.Sprintf("%s=%q is not a positive integer; using %d",
				EnvEstimatedOutput, v, cfg.EstimatedOutputTokens))
		} else {
			cfg.EstimatedOutputTokens = n
		}
	}

	return cfg, warnings
}

func envPositiveFloat(name string, def float64, warnings *[]string) float64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 || math.IsInf(f, 0) || math.IsNaN(f) {
		*warnings = append(*warnings, fmt.Sprintf("%s=%q is not a positive number; using %g", name, v, def))
		return def
	}
	return f
}

// CostUSD prices a request from token counts.
func (cfg ChatBudgetConfig) CostUSD(promptTokens, completionTokens int) float64 {
	if promptTokens < 0 {
		promptTokens = 0
	}
	if completionTokens < 0 {
		completionTokens = 0
	}
	return float64(promptTokens)*cfg.PromptUSDPerToken + float64(completionTokens)*cfg.CompletionUSDPerToken
}

// EstimateRequestUSD is the admission-time cost guess for a request body of
// the given size.
//
// It estimates prompt tokens from the raw body length rather than by decoding
// known JSON fields, on purpose: the estimate then stays correct when the
// request shape changes (extra history, a renamed field, a second prompt), and
// cannot be dodged by moving text into a field the estimator does not know
// about. The JSON envelope overhead it also counts is a rounding error against
// a 25 KB system prompt, and it errs in the safe direction.
func (cfg ChatBudgetConfig) EstimateRequestUSD(bodyBytes int) float64 {
	if bodyBytes < 0 {
		bodyBytes = 0
	}
	chars := cfg.CharsPerToken
	if chars <= 0 {
		chars = DefaultCharsPerToken
	}
	promptTokens := int(math.Ceil(float64(bodyBytes) / chars))
	return cfg.CostUSD(promptTokens, cfg.EstimatedOutputTokens)
}

// PerClientDailyUSD is the per-client allowance in dollars.
func (cfg ChatBudgetConfig) PerClientDailyUSD() float64 {
	return cfg.DailyBudgetUSD * cfg.ClientDailyShare
}

// ReferenceExchangeUSD prices one exchange at the measured baseline. This is
// the number that makes the budget legible to a human.
func (cfg ChatBudgetConfig) ReferenceExchangeUSD() float64 {
	return cfg.CostUSD(MeasuredPromptTokens, MeasuredCompletionTokens)
}

// ExchangesPerDay derives how many measured-baseline exchanges the daily
// budget buys. Derived, never hardcoded — change the price or the budget and
// this follows.
func (cfg ChatBudgetConfig) ExchangesPerDay() int {
	c := cfg.ReferenceExchangeUSD()
	if c <= 0 {
		return 0
	}
	return int(cfg.DailyBudgetUSD / c)
}

// ExchangesPerClientPerDay is the same derivation for one client's share.
func (cfg ChatBudgetConfig) ExchangesPerClientPerDay() int {
	c := cfg.ReferenceExchangeUSD()
	if c <= 0 {
		return 0
	}
	return int(cfg.PerClientDailyUSD() / c)
}

// ── The ledger ──────────────────────────────────────────────────────────────

type clientLedger struct {
	spent    float64
	reserved float64
}

// ChatBudget is an in-memory, UTC-daily spend ledger for the public chat proxy.
//
// State is deliberately in-memory and therefore LOST ON RESTART, which means a
// restart resets the day's tally to zero. That is a fail-open choice, made for
// three reasons:
//
//  1. The alternative — persisting to Postgres — puts a database on the
//     critical path of a proxy that currently has none, and a DB outage would
//     then force the same fail-open/fail-closed choice one layer down, except
//     with chat entirely down as the "safe" option.
//  2. Restarts are operator-initiated deploys, not attacker-triggerable. The
//     handler has no input-driven crash path, so nobody can farm restarts to
//     mint fresh budget.
//  3. The blast radius is bounded and small: worst case is
//     budget × restarts-in-a-day. At this deploy cadence that is a few dollars
//     a year, which is cheaper than the operational weight of persistence.
//
// What does NOT fail open is the check itself: a misconfigured env var falls
// back to the default cap rather than disabling it, and a nil budget in
// newChatProxyHandler is replaced with a default one rather than skipped.
type ChatBudget struct {
	cfg ChatBudgetConfig

	mu       sync.Mutex
	now      func() time.Time
	day      string // UTC calendar day, "2006-01-02"
	spent    float64
	reserved float64
	clients  map[string]*clientLedger
}

// NewChatBudget builds a ledger. The reset boundary is UTC midnight: the
// server's own wall clock and timezone are irrelevant to it, so a machine
// whose TZ changes (or a VPS that is not in the operator's timezone, which
// this one is not) cannot shift or duplicate a budget day.
func NewChatBudget(cfg ChatBudgetConfig) *ChatBudget {
	b := &ChatBudget{
		cfg:     cfg,
		now:     time.Now,
		clients: make(map[string]*clientLedger),
	}
	b.day = b.now().UTC().Format("2006-01-02")
	return b
}

// Config returns the immutable configuration.
func (b *ChatBudget) Config() ChatBudgetConfig { return b.cfg }

// SetClock replaces the time source. Test-only.
func (b *ChatBudget) SetClock(fn func() time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.now = fn
	b.day = fn().UTC().Format("2006-01-02")
}

// ChatReservation is a hold placed on the budget for one in-flight request.
// It must be settled exactly once; Settle is idempotent so a defer can act as
// the catch-all without fighting the explicit calls on each exit path.
type ChatReservation struct {
	b       *ChatBudget
	day     string
	key     string
	amount  float64
	settled bool
}

// Reserve admits a request or refuses it. estimateUSD is the conservative
// admission cost; the caller settles the true cost afterwards.
//
// A client with nothing spent yet is always admitted (subject to the global
// cap). Without that rule a single request whose estimate exceeds the
// per-client allowance would lock that client out permanently — and since the
// allowance is a fraction of the budget, a grown system prompt would silently
// lock out everyone. The loophole it opens is bounded: one over-allowance
// request per distinct key, still under the global cap.
func (b *ChatBudget) Reserve(clientKey string, estimateUSD float64) (*ChatReservation, *ChatBudgetDenial) {
	if estimateUSD < 0 {
		estimateUSD = 0
	}
	if clientKey == "" {
		clientKey = UnknownClientKey
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now().UTC()
	b.rollLocked(now)

	if b.spent+b.reserved+estimateUSD > b.cfg.DailyBudgetUSD {
		return nil, b.denialLocked(now, denialScopeDaily, estimateUSD)
	}

	perClient := b.cfg.PerClientDailyUSD()
	var used float64
	if cl := b.clients[clientKey]; cl != nil {
		used = cl.spent + cl.reserved
	}
	if used > 0 && used+estimateUSD > perClient {
		return nil, b.denialLocked(now, denialScopeClient, estimateUSD)
	}

	cl := b.clients[clientKey]
	if cl == nil {
		// Only an ADMITTED request allocates a map entry. Refusals must not,
		// or a spoofed-IP flood would grow the map without bound. Because
		// admissions are themselves capped by the daily budget, the map holds
		// at most ~one entry per admitted request per day.
		cl = &clientLedger{}
		b.clients[clientKey] = cl
	}
	b.reserved += estimateUSD
	cl.reserved += estimateUSD

	return &ChatReservation{b: b, day: b.day, key: clientKey, amount: estimateUSD}, nil
}

// Settle releases the hold and books the true cost. Calling it more than once
// is a no-op after the first.
func (r *ChatReservation) Settle(actualUSD float64) {
	if r == nil || r.settled {
		return
	}
	r.settled = true
	if actualUSD < 0 {
		actualUSD = 0
	}

	b := r.b
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.day != r.day {
		// The day rolled over while this request was in flight. The rollover
		// already zeroed both the hold and the tally, so releasing again
		// would drive the new day's reserve negative. Yesterday's cost is
		// simply not charged to today.
		return
	}

	b.reserved -= r.amount
	if b.reserved < 0 {
		b.reserved = 0
	}
	b.spent += actualUSD

	if cl := b.clients[r.key]; cl != nil {
		cl.reserved -= r.amount
		if cl.reserved < 0 {
			cl.reserved = 0
		}
		cl.spent += actualUSD
	}
}

// rollLocked resets the ledger when the UTC calendar day changes.
func (b *ChatBudget) rollLocked(now time.Time) {
	day := now.UTC().Format("2006-01-02")
	if day == b.day {
		return
	}
	b.day = day
	b.spent = 0
	b.reserved = 0
	b.clients = make(map[string]*clientLedger)
}

// ChatBudgetSnapshot is a read-only view of the ledger, for tests and
// operational reporting. It is not exposed over HTTP: how close the day is to
// exhaustion is useful reconnaissance for anyone trying to exhaust it.
type ChatBudgetSnapshot struct {
	Day               string
	SpentUSD          float64
	ReservedUSD       float64
	DailyBudgetUSD    float64
	PerClientDailyUSD float64
	Clients           int
}

// Snapshot returns the current ledger state.
func (b *ChatBudget) Snapshot() ChatBudgetSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return ChatBudgetSnapshot{
		Day:               b.day,
		SpentUSD:          b.spent,
		ReservedUSD:       b.reserved,
		DailyBudgetUSD:    b.cfg.DailyBudgetUSD,
		PerClientDailyUSD: b.cfg.PerClientDailyUSD(),
		Clients:           len(b.clients),
	}
}

// ClientSpentUSD reports one client's settled spend for the current day.
func (b *ChatBudget) ClientSpentUSD(key string) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cl := b.clients[key]; cl != nil {
		return cl.spent
	}
	return 0
}

// ── Refusal ─────────────────────────────────────────────────────────────────

type denialScope string

const (
	denialScopeDaily  denialScope = "daily"
	denialScopeClient denialScope = "client"
)

// ChatBudgetDenial is a refusal, carrying everything needed to answer honestly.
type ChatBudgetDenial struct {
	Scope      string
	ResetAt    time.Time
	RetryAfter time.Duration
	Response   ErrorResponse
}

func (b *ChatBudget) denialLocked(now time.Time, scope denialScope, estimateUSD float64) *ChatBudgetDenial {
	reset := nextUTCMidnight(now)
	wait := reset.Sub(now)
	when := fmt.Sprintf("The budget resets at %s (in %s).", reset.Format("2006-01-02 15:04 MST"), humanDuration(wait))

	d := &ChatBudgetDenial{
		Scope:      string(scope),
		ResetAt:    reset,
		RetryAfter: wait,
	}

	switch scope {
	case denialScopeClient:
		d.Response = ErrorResponse{
			Error: "this client has used its share of today's chat budget",
			Detail: fmt.Sprintf(
				"Public chat is capped at $%.2f of model spend per day, and any single client may use at most $%.4f of it (about %d exchanges). "+
					"This request was estimated at $%.4f. %s No request was sent upstream.",
				b.cfg.DailyBudgetUSD, b.cfg.PerClientDailyUSD(), b.cfg.ExchangesPerClientPerDay(), estimateUSD, when),
		}
	default:
		remaining := b.cfg.DailyBudgetUSD - b.spent - b.reserved
		if remaining < 0 {
			remaining = 0
		}
		d.Response = ErrorResponse{
			Error: "today's chat budget is exhausted",
			Detail: fmt.Sprintf(
				"Public chat is capped at $%.2f of model spend per day (about %d exchanges at current prices). "+
					"$%.4f remains and this request was estimated at $%.4f. %s No request was sent upstream.",
				b.cfg.DailyBudgetUSD, b.cfg.ExchangesPerDay(), remaining, estimateUSD, when),
		}
	}
	return d
}

// Abort writes the refusal as HTTP 429 with PDI's own ErrorResponse shape.
//
// 429 and not a fabricated success: the frontend renders a non-2xx as a
// labelled "NOT AN ANSWER" notice carrying the status and the server's own
// words, which is exactly the honest outcome. A synthesised "I'm busy right
// now" reply in the assistant's voice would read as an answer, and that
// failure mode has already cost this project once.
func (d *ChatBudgetDenial) Abort(c *gin.Context) {
	secs := int(math.Ceil(d.RetryAfter.Seconds()))
	if secs < 1 {
		secs = 1
	}
	c.Header("Retry-After", strconv.Itoa(secs))
	c.AbortWithStatusJSON(http.StatusTooManyRequests, d.Response)
}

func nextUTCMidnight(now time.Time) time.Time {
	u := now.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC).Add(24 * time.Hour)
}

func humanDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	if h == 0 {
		return fmt.Sprintf("%dm", m)
	}
	return fmt.Sprintf("%dh%02dm", h, m)
}

// ── Client identity ─────────────────────────────────────────────────────────

// ClientKey resolves the per-client bucket for a request, returning the key
// and the source it came from.
//
// Precedence, and the reasoning behind it:
//
//  1. CF-Connecting-IP. PDI sits behind Cloudflare → Caddy → localhost:8340.
//     Cloudflare OVERWRITES this header (it is single-valued and cannot be
//     appended to), so for any request that actually traversed Cloudflare it
//     is the real visitor. X-Forwarded-For is not equivalent: Cloudflare
//     APPENDS to it, so a caller who sends "X-Forwarded-For: 1.2.3.4" gets
//     their value preserved as the leftmost entry.
//  2. gin's c.ClientIP(), which serve.go constrains by trusting only loopback
//     as a proxy — so it resolves to the real peer rather than to a
//     caller-supplied header. (gin's DEFAULT is to trust 0.0.0.0/0 and ::/0,
//     which makes ClientIP the leftmost X-Forwarded-For entry: fully
//     attacker-controlled. serve.go narrows that deliberately.)
//  3. The raw peer address.
//  4. UnknownClientKey.
//
// Honest limitation: anyone who finds the origin IP and bypasses Cloudflare
// can forge CF-Connecting-IP and mint a fresh bucket per request. The
// per-client cap is therefore a speed bump against ordinary over-use, not a
// security boundary. The GLOBAL daily cap is the actual guarantee, and it
// holds regardless of how many identities one actor invents. Preferring
// CF-Connecting-IP anyway is the right trade: the alternative fallback
// collapses every visitor behind Cloudflare into a handful of edge-IP buckets,
// which would rate-limit the whole public as one actor.
//
// Values are parsed and re-serialised through net.IP so that alternate
// spellings of the same address (::ffff:1.2.3.4, 001.002.003.004, casing in
// IPv6) cannot be used to mint extra buckets, and so a garbage header cannot
// become an unbounded map key.
func ClientKey(c *gin.Context) (key string, source string) {
	if ip := parseIPKey(c.GetHeader("CF-Connecting-IP")); ip != "" {
		return ip, "CF-Connecting-IP"
	}
	if ip := parseIPKey(c.ClientIP()); ip != "" {
		return ip, "gin.ClientIP"
	}
	if ip := parseIPKey(c.RemoteIP()); ip != "" {
		return ip, "RemoteIP"
	}
	return UnknownClientKey, "none"
}

func parseIPKey(v string) string {
	v = strings.TrimSpace(v)
	if v == "" || len(v) > 64 {
		return ""
	}
	ip := net.ParseIP(v)
	if ip == nil {
		return ""
	}
	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}
	return ip.String()
}

// ── Settlement ──────────────────────────────────────────────────────────────

// UsageFromResponse extracts token counts from a Gateway chat response.
//
// It is deliberately tolerant of spelling. The Gateway currently serialises
// provider.Usage, which carries NO json tags and therefore emits Go field
// names verbatim — {"InputTokens":80,"OutputTokens":637,"TotalTokens":717} —
// while the same repo also defines a shared.Usage with snake_case tags. Two
// spellings already exist upstream for one concept, so pinning to one of them
// would be the same class of mistake as pinning a model id: it would keep
// compiling, keep returning 200, and quietly stop metering.
//
// Returns ok=false when the shape is unrecognised, which the caller must treat
// as "settle at the conservative estimate" rather than "free".
func UsageFromResponse(body []byte) (promptTokens, completionTokens int, ok bool) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return 0, 0, false
	}

	var usageRaw json.RawMessage
	for k, v := range envelope {
		if normalizeUsageKey(k) == "usage" {
			usageRaw = v
			break
		}
	}
	if len(usageRaw) == 0 {
		return 0, 0, false
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(usageRaw, &fields); err != nil {
		return 0, 0, false
	}

	vals := make(map[string]int, len(fields))
	for k, raw := range fields {
		var n json.Number
		if err := json.Unmarshal(raw, &n); err != nil {
			continue // non-numeric member (a model name, a nested object)
		}
		i, err := n.Int64()
		if err != nil || i < 0 {
			continue
		}
		vals[normalizeUsageKey(k)] = int(i)
	}

	in, hasIn := firstOf(vals, "inputtokens", "prompttokens")
	out, hasOut := firstOf(vals, "outputtokens", "completiontokens")
	if hasIn && hasOut {
		return in, out, true
	}
	// A total without a split cannot be priced exactly. Charge all of it at
	// the completion rate — an upper bound, never an under-count.
	if total, hasTotal := firstOf(vals, "totaltokens"); hasTotal {
		return 0, total, true
	}
	// A half-reported usage block is a shape we do not understand. Say so and
	// let the caller keep the conservative estimate.
	return 0, 0, false
}

// normalizeUsageKey folds case and separators so InputTokens, input_tokens,
// input-tokens and inputTokens all collapse to one lookup.
func normalizeUsageKey(k string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(k) {
		if r == '_' || r == '-' || r == ' ' {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func firstOf(m map[string]int, keys ...string) (int, bool) {
	for _, k := range keys {
		if v, present := m[k]; present {
			return v, true
		}
	}
	return 0, false
}
