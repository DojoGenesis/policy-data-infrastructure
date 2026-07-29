package gateway

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func approx(t *testing.T, got, want float64, label string) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("%s: got %.12f, want %.12f", label, got, want)
	}
}

// TestReferenceExchangeCost pins the arithmetic against the live measurement:
// 6,894 prompt tokens + 637 completion tokens at OpenRouter's published
// deepseek-v4-pro rates is ~$0.0036 an exchange, ~85% of it the system prompt.
func TestReferenceExchangeCost(t *testing.T) {
	cfg := DefaultChatBudgetConfig()

	promptCost := float64(MeasuredPromptTokens) * cfg.PromptUSDPerToken
	total := cfg.ReferenceExchangeUSD()
	approx(t, total, promptCost+float64(MeasuredCompletionTokens)*cfg.CompletionUSDPerToken, "reference exchange")

	if total < 0.0035 || total > 0.0037 {
		t.Fatalf("reference exchange cost %.6f is outside the measured $0.0036 band", total)
	}
	if share := promptCost / total; share < 0.80 || share > 0.90 {
		t.Fatalf("system prompt is %.1f%% of the exchange, expected ~85%%", share*100)
	}
}

// TestExchangesPerDayIsDerived is the anti-magic-number guard: the "$1/day buys
// N exchanges" figure must follow price and budget, never be pinned.
func TestExchangesPerDayIsDerived(t *testing.T) {
	cfg := DefaultChatBudgetConfig()
	if n := cfg.ExchangesPerDay(); n < 270 || n > 290 {
		t.Fatalf("$1.00/day should buy ~280 exchanges at current prices, got %d", n)
	}

	// Double the budget: double the exchanges.
	doubled := cfg
	doubled.DailyBudgetUSD = 2.00
	if got, want := doubled.ExchangesPerDay(), 2*cfg.ExchangesPerDay(); got < want-2 || got > want+2 {
		t.Fatalf("doubling the budget gave %d exchanges, expected ~%d", got, want)
	}

	// Halve the prices: double the exchanges. If this ever stops holding,
	// something hardcoded a count.
	cheap := cfg
	cheap.PromptUSDPerToken /= 2
	cheap.CompletionUSDPerToken /= 2
	if got, want := cheap.ExchangesPerDay(), 2*cfg.ExchangesPerDay(); got < want-2 || got > want+2 {
		t.Fatalf("halving prices gave %d exchanges, expected ~%d", got, want)
	}
}

func TestEstimateRequestIsConservative(t *testing.T) {
	cfg := DefaultChatBudgetConfig()

	// A body the size of the measured system prompt should estimate at or
	// above the real cost of that exchange — admission must never under-guess.
	est := cfg.EstimateRequestUSD(24817)
	if est < cfg.ReferenceExchangeUSD() {
		t.Fatalf("estimate %.6f is below the measured actual %.6f; admission must be pessimistic",
			est, cfg.ReferenceExchangeUSD())
	}

	// Prompt tokens track body size.
	if cfg.EstimateRequestUSD(50000) <= cfg.EstimateRequestUSD(25000) {
		t.Fatal("estimate must grow with body size")
	}
}

func TestUnderBudgetAdmitsAndSettles(t *testing.T) {
	b := NewChatBudget(DefaultChatBudgetConfig())

	res, denial := b.Reserve("1.2.3.4", 0.0038)
	if denial != nil {
		t.Fatalf("first request refused: %+v", denial.Response)
	}
	if snap := b.Snapshot(); snap.ReservedUSD == 0 {
		t.Fatal("reservation did not hold anything")
	}

	res.Settle(0.0035)
	snap := b.Snapshot()
	approx(t, snap.SpentUSD, 0.0035, "settled spend")
	approx(t, snap.ReservedUSD, 0, "hold released")
	approx(t, b.ClientSpentUSD("1.2.3.4"), 0.0035, "client spend")
}

func TestSettleIsIdempotent(t *testing.T) {
	b := NewChatBudget(DefaultChatBudgetConfig())
	res, _ := b.Reserve("1.2.3.4", 0.01)
	res.Settle(0.002)
	res.Settle(0.5) // the deferred catch-all in the handler
	approx(t, b.Snapshot().SpentUSD, 0.002, "spend after double settle")
}

func TestGlobalBudgetExhaustionRefuses(t *testing.T) {
	cfg := DefaultChatBudgetConfig()
	cfg.ClientDailyShare = 1.0 // isolate the global cap from the per-client one
	b := NewChatBudget(cfg)

	admitted := 0
	for i := 0; i < 10000; i++ {
		res, denial := b.Reserve("1.2.3.4", cfg.ReferenceExchangeUSD())
		if denial != nil {
			if denial.Scope != "daily" {
				t.Fatalf("expected a daily-scope refusal, got %q", denial.Scope)
			}
			if denial.Response.Error == "" || denial.Response.Detail == "" {
				t.Fatal("refusal must carry both an error and a detail")
			}
			break
		}
		res.Settle(cfg.ReferenceExchangeUSD())
		admitted++
	}

	if admitted != cfg.ExchangesPerDay() {
		t.Fatalf("admitted %d exchanges, derived budget says %d", admitted, cfg.ExchangesPerDay())
	}
	if spent := b.Snapshot().SpentUSD; spent > cfg.DailyBudgetUSD {
		t.Fatalf("spent $%.4f, over the $%.2f cap", spent, cfg.DailyBudgetUSD)
	}
}

// TestPerClientCapIsIndependentOfGlobalBudget: one actor is stopped while the
// day's budget is still almost entirely intact, and other clients keep working.
func TestPerClientCapIsIndependentOfGlobalBudget(t *testing.T) {
	cfg := DefaultChatBudgetConfig()
	b := NewChatBudget(cfg)
	unit := cfg.ReferenceExchangeUSD()

	var denial *ChatBudgetDenial
	admitted := 0
	for i := 0; i < 10000; i++ {
		res, d := b.Reserve("9.9.9.9", unit)
		if d != nil {
			denial = d
			break
		}
		res.Settle(unit)
		admitted++
	}

	if denial == nil {
		t.Fatal("a single client was never refused")
	}
	if denial.Scope != "client" {
		t.Fatalf("expected a client-scope refusal, got %q", denial.Scope)
	}
	if admitted > cfg.ExchangesPerDay()/10 {
		t.Fatalf("one client got %d exchanges, more than a tenth of the day's %d",
			admitted, cfg.ExchangesPerDay())
	}

	// The global budget is barely touched, and a different client is fine.
	if spent := b.Snapshot().SpentUSD; spent > cfg.DailyBudgetUSD*0.2 {
		t.Fatalf("one client spent $%.4f of the $%.2f day", spent, cfg.DailyBudgetUSD)
	}
	if _, d := b.Reserve("8.8.8.8", unit); d != nil {
		t.Fatalf("a second client was refused because of the first: %+v", d.Response)
	}
}

// TestFirstRequestFromFreshClientAlwaysAdmitted: a request whose estimate alone
// exceeds the per-client allowance must not lock that client out forever.
func TestFirstRequestFromFreshClientAlwaysAdmitted(t *testing.T) {
	cfg := DefaultChatBudgetConfig()
	b := NewChatBudget(cfg)

	oversized := cfg.PerClientDailyUSD() * 3
	res, denial := b.Reserve("1.2.3.4", oversized)
	if denial != nil {
		t.Fatalf("fresh client refused its first request: %+v", denial.Response)
	}
	res.Settle(oversized)

	// But the next one is refused, so the loophole is exactly one request.
	if _, d := b.Reserve("1.2.3.4", 0.001); d == nil || d.Scope != "client" {
		t.Fatal("the over-allowance client should be refused on its second request")
	}
}

// TestRefusedRequestsDoNotAllocate guards the map against a spoofed-key flood:
// only admitted requests may create a client entry.
func TestRefusedRequestsDoNotAllocate(t *testing.T) {
	cfg := DefaultChatBudgetConfig()
	cfg.DailyBudgetUSD = 0.001 // already too small for anything
	b := NewChatBudget(cfg)

	for i := 0; i < 500; i++ {
		if _, d := b.Reserve(string(rune('a'+i%26))+"9.9.9.9", 1.0); d == nil {
			t.Fatal("expected refusal")
		}
	}
	if n := b.Snapshot().Clients; n != 0 {
		t.Fatalf("refusals allocated %d client entries; the map must only grow on admission", n)
	}
}

// TestConcurrentReserveNeverOverspends: the hold has to be taken and the cap
// checked under the same lock, or N goroutines all read "there is room" and
// then all spend.
func TestConcurrentReserveNeverOverspends(t *testing.T) {
	cfg := DefaultChatBudgetConfig()
	cfg.ClientDailyShare = 1.0
	b := NewChatBudget(cfg)
	unit := cfg.ReferenceExchangeUSD()

	var wg sync.WaitGroup
	var admitted int64
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				res, d := b.Reserve("1.2.3.4", unit)
				if d != nil {
					continue
				}
				atomic.AddInt64(&admitted, 1)
				res.Settle(unit)
			}
		}()
	}
	wg.Wait()

	snap := b.Snapshot()
	if snap.SpentUSD > cfg.DailyBudgetUSD {
		t.Fatalf("concurrent load spent $%.6f against a $%.2f cap", snap.SpentUSD, cfg.DailyBudgetUSD)
	}
	if snap.ReservedUSD != 0 {
		t.Fatalf("holds leaked: $%.6f still reserved", snap.ReservedUSD)
	}
	if n := atomic.LoadInt64(&admitted); n != int64(cfg.ExchangesPerDay()) {
		t.Fatalf("admitted %d, derived budget says %d", n, cfg.ExchangesPerDay())
	}
}

func TestDayRollover(t *testing.T) {
	cfg := DefaultChatBudgetConfig()
	cfg.ClientDailyShare = 1.0 // the daily cap is the boundary under test
	b := NewChatBudget(cfg)

	now := time.Date(2026, 7, 29, 23, 59, 0, 0, time.UTC)
	b.SetClock(func() time.Time { return now })

	// Exhaust the day.
	for {
		res, d := b.Reserve("1.2.3.4", cfg.ReferenceExchangeUSD())
		if d != nil {
			if d.Scope == "client" {
				t.Fatal("per-client cap fired before the daily cap in a rollover test")
			}
			break
		}
		res.Settle(cfg.ReferenceExchangeUSD())
	}

	if _, d := b.Reserve("5.5.5.5", cfg.ReferenceExchangeUSD()); d == nil {
		t.Fatal("budget should be exhausted before the rollover")
	} else if !d.ResetAt.Equal(time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("reset time %s is not the next UTC midnight", d.ResetAt)
	}

	// Cross UTC midnight.
	now = time.Date(2026, 7, 30, 0, 0, 1, 0, time.UTC)
	res, d := b.Reserve("5.5.5.5", cfg.ReferenceExchangeUSD())
	if d != nil {
		t.Fatalf("budget did not roll over at the day boundary: %+v", d.Response)
	}
	res.Settle(cfg.ReferenceExchangeUSD())

	snap := b.Snapshot()
	if snap.Day != "2026-07-30" {
		t.Fatalf("ledger day is %q after rollover", snap.Day)
	}
	if snap.SpentUSD > cfg.ReferenceExchangeUSD()*1.01 {
		t.Fatalf("yesterday's spend ($%.4f) carried into the new day", snap.SpentUSD)
	}
}

// TestSettleAcrossRolloverDoesNotCorruptLedger: a request in flight when the
// day turns must not drive the new day's reserve negative.
func TestSettleAcrossRolloverDoesNotCorruptLedger(t *testing.T) {
	b := NewChatBudget(DefaultChatBudgetConfig())
	now := time.Date(2026, 7, 29, 23, 59, 59, 0, time.UTC)
	b.SetClock(func() time.Time { return now })

	res, _ := b.Reserve("1.2.3.4", 0.02)
	now = time.Date(2026, 7, 30, 0, 0, 1, 0, time.UTC)
	if _, d := b.Reserve("1.2.3.4", 0.001); d != nil {
		t.Fatalf("new day refused: %+v", d.Response)
	}
	res.Settle(0.02) // yesterday's request lands late

	snap := b.Snapshot()
	if snap.ReservedUSD < 0 || snap.SpentUSD < 0 {
		t.Fatalf("ledger went negative after a cross-midnight settle: %+v", snap)
	}
	approx(t, snap.SpentUSD, 0, "yesterday's cost must not be charged to today")
}

func TestDenialAbortShape(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := DefaultChatBudgetConfig()
	cfg.DailyBudgetUSD = 0.0001
	b := NewChatBudget(cfg)

	_, d := b.Reserve("1.2.3.4", 1.0)
	if d == nil {
		t.Fatal("expected refusal")
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat", nil)
	d.Abort(c)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d, want 429", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Fatal("missing Retry-After")
	}
	var body ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not a PDI ErrorResponse: %v (%s)", err, w.Body.String())
	}
	if body.Error == "" || body.Detail == "" {
		t.Fatalf("refusal must explain itself: %+v", body)
	}
}

// ── usage parsing ───────────────────────────────────────────────────────────

func TestUsageFromResponse(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantIn  int
		wantOut int
		wantOK  bool
	}{
		{
			// What the Gateway actually emits today: provider.Usage has no
			// json tags, so encoding/json uses the Go field names verbatim.
			name:    "gateway untagged Go field names",
			body:    `{"type":"text","content":"hi","usage":{"InputTokens":6894,"OutputTokens":637,"TotalTokens":7531}}`,
			wantIn:  6894,
			wantOut: 637,
			wantOK:  true,
		},
		{
			// shared.Usage in the same upstream repo uses snake_case.
			name:    "snake_case",
			body:    `{"usage":{"input_tokens":80,"output_tokens":637,"total_tokens":717}}`,
			wantIn:  80,
			wantOut: 637,
			wantOK:  true,
		},
		{
			name:    "openai spelling",
			body:    `{"usage":{"prompt_tokens":100,"completion_tokens":200}}`,
			wantIn:  100,
			wantOut: 200,
			wantOK:  true,
		},
		{
			// Total only: cannot split, so price it all at the dearer rate.
			name:    "total only is charged at the completion rate",
			body:    `{"usage":{"total_tokens":1000}}`,
			wantIn:  0,
			wantOut: 1000,
			wantOK:  true,
		},
		{
			name:   "no usage block",
			body:   `{"type":"text","content":"hi"}`,
			wantOK: false,
		},
		{
			name:   "half a usage block is not understood",
			body:   `{"usage":{"InputTokens":500}}`,
			wantOK: false,
		},
		{
			name:   "not json",
			body:   `<html>gateway error</html>`,
			wantOK: false,
		},
		{
			name:    "non-numeric members are skipped",
			body:    `{"usage":{"model":"deepseek/deepseek-v4-pro","InputTokens":10,"OutputTokens":20}}`,
			wantIn:  10,
			wantOut: 20,
			wantOK:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in, out, ok := UsageFromResponse([]byte(tc.body))
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if in != tc.wantIn || out != tc.wantOut {
				t.Fatalf("got (%d, %d), want (%d, %d)", in, out, tc.wantIn, tc.wantOut)
			}
		})
	}
}

// ── client identity ─────────────────────────────────────────────────────────

func TestClientKeyPrefersCloudflareHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)

	newCtx := func(remote string, headers map[string]string) *gin.Context {
		w := httptest.NewRecorder()
		c, e := gin.CreateTestContext(w)
		// Mirror serve.go: only loopback is a trusted proxy.
		if err := e.SetTrustedProxies([]string{"127.0.0.1/32", "::1/128"}); err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/chat", nil)
		req.RemoteAddr = remote
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		c.Request = req
		return c
	}

	t.Run("cloudflare header wins over a forged XFF", func(t *testing.T) {
		c := newCtx("127.0.0.1:5555", map[string]string{
			"CF-Connecting-IP": "203.0.113.7",
			"X-Forwarded-For":  "1.1.1.1, 198.51.100.4",
		})
		key, source := ClientKey(c)
		if key != "203.0.113.7" || source != "CF-Connecting-IP" {
			t.Fatalf("got (%q, %q)", key, source)
		}
	})

	t.Run("garbage cloudflare header falls through", func(t *testing.T) {
		c := newCtx("198.51.100.9:5555", map[string]string{
			"CF-Connecting-IP": "not-an-ip",
		})
		key, source := ClientKey(c)
		if key != "198.51.100.9" {
			t.Fatalf("got (%q, %q), want the real peer", key, source)
		}
	})

	t.Run("forged XFF from an untrusted peer is ignored", func(t *testing.T) {
		// The peer is not loopback, so gin must not honour its XFF. Before
		// serve.go narrowed the trusted proxies this returned 1.1.1.1.
		c := newCtx("198.51.100.9:5555", map[string]string{
			"X-Forwarded-For": "1.1.1.1",
		})
		if key, _ := ClientKey(c); key != "198.51.100.9" {
			t.Fatalf("got %q, want the real peer 198.51.100.9", key)
		}
	})

	t.Run("equivalent spellings collapse to one bucket", func(t *testing.T) {
		a, _ := ClientKey(newCtx("127.0.0.1:1", map[string]string{"CF-Connecting-IP": "203.0.113.7"}))
		b, _ := ClientKey(newCtx("127.0.0.1:1", map[string]string{"CF-Connecting-IP": "::ffff:203.0.113.7"}))
		if a != b {
			t.Fatalf("%q and %q are the same address but got different buckets", a, b)
		}
	})

	t.Run("oversized header cannot become a map key", func(t *testing.T) {
		huge := make([]byte, 4096)
		for i := range huge {
			huge[i] = 'a'
		}
		c := newCtx("198.51.100.9:5555", map[string]string{"CF-Connecting-IP": string(huge)})
		if key, _ := ClientKey(c); key != "198.51.100.9" {
			t.Fatalf("got %q", key)
		}
	})
}

// ── configuration ───────────────────────────────────────────────────────────

func TestChatBudgetConfigFromEnv(t *testing.T) {
	t.Run("valid values are applied", func(t *testing.T) {
		t.Setenv(EnvDailyBudgetUSD, "5.00")
		t.Setenv(EnvClientDailyShare, "0.10")
		cfg, warnings := ChatBudgetConfigFromEnv()
		if len(warnings) != 0 {
			t.Fatalf("unexpected warnings: %v", warnings)
		}
		approx(t, cfg.DailyBudgetUSD, 5.00, "budget")
		approx(t, cfg.PerClientDailyUSD(), 0.50, "per client")
	})

	t.Run("garbage never disables the cap", func(t *testing.T) {
		for _, bad := range []string{"free", "0", "-1", "NaN", ""} {
			t.Setenv(EnvDailyBudgetUSD, bad)
			cfg, warnings := ChatBudgetConfigFromEnv()
			approx(t, cfg.DailyBudgetUSD, DefaultDailyBudgetUSD, "budget for "+bad)
			if bad != "" && len(warnings) == 0 {
				t.Fatalf("%q should have warned", bad)
			}
		}
	})

	t.Run("a share above 1.0 is rejected", func(t *testing.T) {
		t.Setenv(EnvClientDailyShare, "5")
		cfg, warnings := ChatBudgetConfigFromEnv()
		approx(t, cfg.ClientDailyShare, DefaultClientDailyShare, "share")
		if len(warnings) == 0 {
			t.Fatal("expected a warning")
		}
	})
}

func TestNilBudgetIsNotAnEscapeHatch(t *testing.T) {
	// Documents the contract newChatProxyHandler relies on: the zero value of
	// the config is unusable, so a nil budget must be replaced, never skipped.
	var zero ChatBudgetConfig
	if zero.ExchangesPerDay() != 0 {
		t.Fatal("a zero config must not claim to afford anything")
	}
	if zero.EstimateRequestUSD(1000) != 0 {
		t.Fatal("a zero price list estimates zero; the handler must use DefaultChatBudgetConfig instead")
	}
}
