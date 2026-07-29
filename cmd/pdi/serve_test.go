package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/gateway"
)

// ── harness ─────────────────────────────────────────────────────────────────

// stubGateway stands in for the Dojo Gateway. It records what it was sent and
// replies with whatever the test asked for, so the proxy's header logic and its
// metering can both be exercised without a real upstream or a database.
type stubGateway struct {
	server *httptest.Server

	calls    int
	lastAuth string
	lastBody string

	status int
	body   string
	ctype  string
}

func newStubGateway(t *testing.T) *stubGateway {
	t.Helper()
	s := &stubGateway{
		status: http.StatusOK,
		body:   `{"type":"text","content":"an answer","usage":{"InputTokens":6894,"OutputTokens":637,"TotalTokens":7531}}`,
		ctype:  "application/json",
	}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, r.ContentLength)
		if r.ContentLength > 0 {
			_, _ = r.Body.Read(buf)
		}
		s.calls++
		s.lastAuth = r.Header.Get("Authorization")
		s.lastBody = string(buf)
		w.Header().Set("Content-Type", s.ctype)
		w.WriteHeader(s.status)
		_, _ = w.Write([]byte(s.body))
	}))
	t.Cleanup(s.server.Close)
	return s
}

// newChatRouter wires the handler exactly as runServe does, including the
// narrowed trusted-proxy set that makes ClientIP meaningful.
func newChatRouter(t *testing.T, gwTarget, gwToken string, budget *gateway.ChatBudget) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	if err := r.SetTrustedProxies([]string{"127.0.0.1/32", "::1/128"}); err != nil {
		t.Fatal(err)
	}
	r.POST("/v1/chat", newChatProxyHandler(gwTarget, gwToken, &http.Client{Timeout: 10 * time.Second}, budget))
	return r
}

// chatBody returns a request body of roughly the size the live frontend sends:
// a ~24,800-char system prompt rebuilt on every message.
func chatBody(promptChars int) string {
	return `{"message":"hi","system_prompt":"` + strings.Repeat("x", promptChars) + `","stream":false}`
}

func postChat(r *gin.Engine, clientIP, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "127.0.0.1:40000" // Caddy on loopback, as in production
	if clientIP != "" {
		req.Header.Set("CF-Connecting-IP", clientIP)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func decodeError(t *testing.T, w *httptest.ResponseRecorder) gateway.ErrorResponse {
	t.Helper()
	var e gateway.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
		t.Fatalf("body is not a PDI ErrorResponse: %v (%s)", err, w.Body.String())
	}
	return e
}

// ── existing behaviour must survive ─────────────────────────────────────────

func TestChatProxyUnconfiguredReturns503(t *testing.T) {
	stub := newStubGateway(t)
	r := newChatRouter(t, stub.server.URL, "", gateway.NewChatBudget(gateway.DefaultChatBudgetConfig()))

	w := postChat(r, "203.0.113.1", chatBody(100))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", w.Code)
	}
	if stub.calls != 0 {
		t.Fatal("an unconfigured server must not call upstream")
	}
	if e := decodeError(t, w); !strings.Contains(e.Detail, "DOJO_GATEWAY_TOKEN") {
		t.Fatalf("503 detail should name the missing credential: %+v", e)
	}
}

// The 503 path must win over the budget: a server that cannot call upstream
// never spends, so 429 would point the operator at the wrong knob.
func TestUnconfiguredBeatsExhaustedBudget(t *testing.T) {
	stub := newStubGateway(t)
	cfg := gateway.DefaultChatBudgetConfig()
	cfg.DailyBudgetUSD = 0.0000001
	r := newChatRouter(t, stub.server.URL, "", gateway.NewChatBudget(cfg))

	if w := postChat(r, "203.0.113.1", chatBody(100)); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503 even with the budget exhausted", w.Code)
	}
}

func TestChatProxyUsesServiceCredential(t *testing.T) {
	stub := newStubGateway(t)
	r := newChatRouter(t, stub.server.URL, "svc-token", gateway.NewChatBudget(gateway.DefaultChatBudgetConfig()))

	if w := postChat(r, "203.0.113.1", chatBody(100)); w.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", w.Code)
	}
	if stub.lastAuth != "Bearer svc-token" {
		t.Fatalf("upstream saw Authorization %q", stub.lastAuth)
	}
}

func TestInboundAuthorizationWins(t *testing.T) {
	stub := newStubGateway(t)
	r := newChatRouter(t, stub.server.URL, "svc-token", gateway.NewChatBudget(gateway.DefaultChatBudgetConfig()))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(chatBody(100)))
	req.Header.Set("Authorization", "Bearer visitor-token")
	req.RemoteAddr = "127.0.0.1:40000"
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", w.Code)
	}
	if stub.lastAuth != "Bearer visitor-token" {
		t.Fatalf("the visitor's credential was not forwarded verbatim: %q", stub.lastAuth)
	}
}

// An inbound credential does not buy an exemption from the ledger — that would
// be a bypass anyone could trigger by sending a header.
func TestInboundAuthorizationIsStillMetered(t *testing.T) {
	stub := newStubGateway(t)
	b := gateway.NewChatBudget(gateway.DefaultChatBudgetConfig())
	r := newChatRouter(t, stub.server.URL, "svc-token", b)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(chatBody(24800)))
	req.Header.Set("Authorization", "Bearer visitor-token")
	req.RemoteAddr = "127.0.0.1:40000"
	r.ServeHTTP(httptest.NewRecorder(), req)

	if b.Snapshot().SpentUSD <= 0 {
		t.Fatal("a request carrying an inbound Authorization header was not metered")
	}
}

// ── budget behaviour ────────────────────────────────────────────────────────

func TestUnderBudgetPasses(t *testing.T) {
	stub := newStubGateway(t)
	b := gateway.NewChatBudget(gateway.DefaultChatBudgetConfig())
	r := newChatRouter(t, stub.server.URL, "svc-token", b)

	w := postChat(r, "203.0.113.1", chatBody(24800))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", w.Code, w.Body.String())
	}
	if w.Body.String() != stub.body {
		t.Fatalf("upstream body was not passed through: %s", w.Body.String())
	}
	if stub.calls != 1 {
		t.Fatalf("upstream called %d times", stub.calls)
	}
}

// Settlement must use the Gateway's reported usage, not the admission guess.
func TestSettlementUsesActualUsage(t *testing.T) {
	stub := newStubGateway(t)
	cfg := gateway.DefaultChatBudgetConfig()
	b := gateway.NewChatBudget(cfg)
	r := newChatRouter(t, stub.server.URL, "svc-token", b)

	body := chatBody(24800)
	estimate := cfg.EstimateRequestUSD(len(body))
	actual := cfg.CostUSD(6894, 637) // exactly what the stub reports

	if w := postChat(r, "203.0.113.1", body); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}

	spent := b.Snapshot().SpentUSD
	if diff := spent - actual; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("settled $%.8f, want the reported usage cost $%.8f (estimate was $%.8f)", spent, actual, estimate)
	}
	if estimate <= actual {
		t.Fatalf("the test is not proving anything: estimate $%.8f should exceed actual $%.8f", estimate, actual)
	}
	if r := b.Snapshot().ReservedUSD; r != 0 {
		t.Fatalf("the hold was not released: $%.8f still reserved", r)
	}
}

// A 2xx with no usage block keeps the conservative estimate rather than
// silently metering it as free.
func TestMissingUsageKeepsTheEstimate(t *testing.T) {
	stub := newStubGateway(t)
	stub.body = `{"type":"text","content":"an answer"}`
	cfg := gateway.DefaultChatBudgetConfig()
	b := gateway.NewChatBudget(cfg)
	r := newChatRouter(t, stub.server.URL, "svc-token", b)

	body := chatBody(24800)
	postChat(r, "203.0.113.1", body)

	want := cfg.EstimateRequestUSD(len(body))
	if got := b.Snapshot().SpentUSD; got < want-1e-9 || got > want+1e-9 {
		t.Fatalf("settled $%.8f, want the estimate $%.8f", got, want)
	}
}

// A Gateway refusal costs nothing: no model ran.
func TestUpstreamErrorCostsNothing(t *testing.T) {
	stub := newStubGateway(t)
	stub.status = http.StatusUnauthorized
	stub.body = `{"error":"unauthorized","success":false}`
	b := gateway.NewChatBudget(gateway.DefaultChatBudgetConfig())
	r := newChatRouter(t, stub.server.URL, "svc-token", b)

	w := postChat(r, "203.0.113.1", chatBody(24800))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want the upstream 401 passed through", w.Code)
	}
	if spent := b.Snapshot().SpentUSD; spent != 0 {
		t.Fatalf("an upstream refusal was billed $%.8f", spent)
	}
}

// An unreachable Gateway must not burn the day's budget — otherwise an outage
// locks chat out for hours after the Gateway comes back.
func TestUnreachableGatewayCostsNothing(t *testing.T) {
	b := gateway.NewChatBudget(gateway.DefaultChatBudgetConfig())
	r := newChatRouter(t, "http://127.0.0.1:1", "svc-token", b)

	if w := postChat(r, "203.0.113.1", chatBody(1000)); w.Code != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", w.Code)
	}
	if spent := b.Snapshot().SpentUSD; spent != 0 {
		t.Fatalf("an unreachable gateway was billed $%.8f", spent)
	}
	if reserved := b.Snapshot().ReservedUSD; reserved != 0 {
		t.Fatalf("the hold leaked: $%.8f still reserved", reserved)
	}
}

func TestOverBudgetReturns429(t *testing.T) {
	stub := newStubGateway(t)
	cfg := gateway.DefaultChatBudgetConfig()
	cfg.ClientDailyShare = 1.0 // isolate the daily cap
	b := gateway.NewChatBudget(cfg)
	r := newChatRouter(t, stub.server.URL, "svc-token", b)

	body := chatBody(24800)
	var last *httptest.ResponseRecorder
	for i := 0; i < cfg.ExchangesPerDay()+50; i++ {
		last = postChat(r, "203.0.113.1", body)
		if last.Code == http.StatusTooManyRequests {
			break
		}
		if last.Code != http.StatusOK {
			t.Fatalf("unexpected status %d: %s", last.Code, last.Body.String())
		}
	}

	if last.Code != http.StatusTooManyRequests {
		t.Fatalf("the budget was never enforced; final status %d", last.Code)
	}
	callsAtRefusal := stub.calls

	e := decodeError(t, last)
	if e.Error == "" || e.Detail == "" {
		t.Fatalf("the refusal must explain itself: %+v", e)
	}
	if !strings.Contains(e.Detail, "resets at") {
		t.Fatalf("the refusal must say when the budget resets: %q", e.Detail)
	}
	if !strings.Contains(e.Detail, "No request was sent upstream") {
		t.Fatalf("the refusal must be explicit that nothing was forwarded: %q", e.Detail)
	}
	if last.Header().Get("Retry-After") == "" {
		t.Fatal("missing Retry-After header")
	}

	// Refusing must be cheap and must not reach the Gateway.
	if w := postChat(r, "203.0.113.1", body); w.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d on a second refusal", w.Code)
	}
	if stub.calls != callsAtRefusal {
		t.Fatal("a refused request was still forwarded upstream")
	}

	if spent := b.Snapshot().SpentUSD; spent > cfg.DailyBudgetUSD {
		t.Fatalf("spent $%.4f against a $%.2f cap", spent, cfg.DailyBudgetUSD)
	}
}

// One actor is stopped long before the day's budget is gone, and everyone else
// keeps working.
func TestPerClientLimitTriggersIndependently(t *testing.T) {
	stub := newStubGateway(t)
	cfg := gateway.DefaultChatBudgetConfig()
	b := gateway.NewChatBudget(cfg)
	r := newChatRouter(t, stub.server.URL, "svc-token", b)

	body := chatBody(24800)
	greedy := 0
	var refusal *httptest.ResponseRecorder
	for i := 0; i < cfg.ExchangesPerDay(); i++ {
		w := postChat(r, "203.0.113.9", body)
		if w.Code == http.StatusTooManyRequests {
			refusal = w
			break
		}
		greedy++
	}

	if refusal == nil {
		t.Fatal("one client drained the whole day without being stopped")
	}
	if greedy > cfg.ExchangesPerDay()/10 {
		t.Fatalf("one client got %d of the day's %d exchanges", greedy, cfg.ExchangesPerDay())
	}
	if e := decodeError(t, refusal); !strings.Contains(e.Error, "client") {
		t.Fatalf("the refusal should name the per-client cap: %+v", e)
	}

	// The day still has plenty left, and a different visitor is unaffected.
	if spent := b.Snapshot().SpentUSD; spent > cfg.DailyBudgetUSD*0.25 {
		t.Fatalf("one client spent $%.4f of the $%.2f day", spent, cfg.DailyBudgetUSD)
	}
	if w := postChat(r, "203.0.113.10", body); w.Code != http.StatusOK {
		t.Fatalf("a second client got %d because of the first: %s", w.Code, w.Body.String())
	}
}

// Distinct clients are metered separately — the whole point of the key.
func TestClientsAreBucketedByIP(t *testing.T) {
	stub := newStubGateway(t)
	b := gateway.NewChatBudget(gateway.DefaultChatBudgetConfig())
	r := newChatRouter(t, stub.server.URL, "svc-token", b)

	for i := 0; i < 3; i++ {
		postChat(r, fmt.Sprintf("203.0.113.%d", i+1), chatBody(24800))
	}
	if n := b.Snapshot().Clients; n != 3 {
		t.Fatalf("%d client buckets, want 3", n)
	}
	if b.ClientSpentUSD("203.0.113.1") == 0 {
		t.Fatal("client spend was not attributed")
	}
}

// The counter rolls over at UTC midnight, end to end through the handler.
func TestBudgetRollsOverAtDayBoundary(t *testing.T) {
	stub := newStubGateway(t)
	cfg := gateway.DefaultChatBudgetConfig()
	cfg.ClientDailyShare = 1.0
	b := gateway.NewChatBudget(cfg)

	now := time.Date(2026, 7, 29, 22, 0, 0, 0, time.UTC)
	b.SetClock(func() time.Time { return now })
	r := newChatRouter(t, stub.server.URL, "svc-token", b)

	body := chatBody(24800)
	for i := 0; i < cfg.ExchangesPerDay()+50; i++ {
		if postChat(r, "203.0.113.1", body).Code == http.StatusTooManyRequests {
			break
		}
	}
	if w := postChat(r, "203.0.113.1", body); w.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d, expected the day to be exhausted", w.Code)
	}

	now = time.Date(2026, 7, 30, 0, 0, 1, 0, time.UTC)
	if w := postChat(r, "203.0.113.1", body); w.Code != http.StatusOK {
		t.Fatalf("status %d after the UTC day rolled over: %s", w.Code, w.Body.String())
	}
	if snap := b.Snapshot(); snap.Day != "2026-07-30" {
		t.Fatalf("ledger day is %q", snap.Day)
	}
}

// A nil budget is a programming error, not a way to run uncapped.
func TestNilBudgetStillCaps(t *testing.T) {
	stub := newStubGateway(t)
	r := newChatRouter(t, stub.server.URL, "svc-token", nil)

	body := chatBody(24800)
	saw429 := false
	for i := 0; i < gateway.DefaultChatBudgetConfig().ExchangesPerDay()+50; i++ {
		w := postChat(r, "203.0.113.1", body)
		if w.Code == http.StatusTooManyRequests {
			saw429 = true
			break
		}
	}
	if !saw429 {
		t.Fatal("a nil budget served uncapped")
	}
}

func TestOversizedRequestRefused(t *testing.T) {
	stub := newStubGateway(t)
	r := newChatRouter(t, stub.server.URL, "svc-token", gateway.NewChatBudget(gateway.DefaultChatBudgetConfig()))

	w := postChat(r, "203.0.113.1", chatBody(maxChatRequestBytes+1))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", w.Code)
	}
	if stub.calls != 0 {
		t.Fatal("an oversized body was forwarded upstream")
	}
}
