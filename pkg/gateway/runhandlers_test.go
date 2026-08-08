package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// newRunTestRouter mounts the plugin with a controlled budget and token,
// bypassing NewPlugin's env reads. runner stays nil — enqueue must not
// require a live worker.
func newRunTestRouter(m *mockStore, cfg RunBudgetConfig, token string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	p := &PolicyPlugin{
		store:     m,
		varMeta:   map[string]store.VariableMeta{},
		runBudget: NewRunBudget(cfg),
		runToken:  token,
	}
	group := r.Group("/v1")
	p.RegisterRoutes(group)
	return r
}

func postAnalyses(r *gin.Engine, body string, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/analyses", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

const rollupBody = `{"type":"tract_rollup","scope_level":"county","scope_geoid":"55025",
	"parameters":{"variable_id":"cdc_obesity"}}`

func TestCreateRunRejectsUnknownType(t *testing.T) {
	r := newRunTestRouter(&mockStore{}, DefaultRunBudgetConfig(), "")
	w := postAnalyses(r, `{"type":"nope","scope_level":"county","scope_geoid":"55025"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "tract_rollup") {
		t.Errorf("error should list registered types; body: %s", w.Body.String())
	}
}

func TestCreateRunValidatesScope(t *testing.T) {
	r := newRunTestRouter(&mockStore{}, DefaultRunBudgetConfig(), "")
	// county with a 2-digit geoid
	w := postAnalyses(r, `{"type":"tract_rollup","scope_level":"county","scope_geoid":"55","parameters":{"variable_id":"x"}}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad county geoid: status = %d, want 400", w.Code)
	}
	// unsupported level
	w = postAnalyses(r, `{"type":"tract_rollup","scope_level":"tract","scope_geoid":"55025000100","parameters":{"variable_id":"x"}}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("tract scope: status = %d, want 400", w.Code)
	}
}

func TestCreateRunResolvesVintageAndEnqueues(t *testing.T) {
	m := &mockStore{latestVintages: map[string]string{"cdc_obesity": "CDC-PLACES-2023"}}
	r := newRunTestRouter(m, DefaultRunBudgetConfig(), "")

	w := postAnalyses(r, rollupBody)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body: %s", w.Code, w.Body.String())
	}
	if len(m.createdRuns) != 1 {
		t.Fatalf("created %d runs, want 1", len(m.createdRuns))
	}
	run := m.createdRuns[0]
	if run.Vintage != "CDC-PLACES-2023" {
		t.Errorf("vintage = %q, want resolved CDC-PLACES-2023 — 'latest' must never enter the cache key", run.Vintage)
	}
	// Canonicalization filled the defaults at enqueue.
	if run.Parameters["coverage_threshold"] != defaultCoverageThreshold {
		t.Errorf("coverage_threshold = %v, want canonical default %v", run.Parameters["coverage_threshold"], defaultCoverageThreshold)
	}
	if run.Parameters["denominator_variable_id"] != defaultRollupDenominator {
		t.Errorf("denominator = %v, want %q", run.Parameters["denominator_variable_id"], defaultRollupDenominator)
	}
	if !strings.Contains(w.Header().Get("Location"), "/analyses/runs/") {
		t.Errorf("Location header missing run path: %q", w.Header().Get("Location"))
	}
}

func TestCreateRunRefusesVariableWithNoData(t *testing.T) {
	r := newRunTestRouter(&mockStore{}, DefaultRunBudgetConfig(), "")
	w := postAnalyses(r, rollupBody)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 when the variable has no rows; body: %s", w.Code, w.Body.String())
	}
}

func TestCreateRunReturnsCacheHitWithoutSpendingBudget(t *testing.T) {
	m := &mockStore{
		latestVintages: map[string]string{"cdc_obesity": "CDC-PLACES-2023"},
		findAnalysisByKeyFn: func(key store.AnalysisKey) (*store.AnalysisSummary, error) {
			if key.Vintage != "CDC-PLACES-2023" {
				t.Errorf("cache lookup vintage = %q, want resolved", key.Vintage)
			}
			return &store.AnalysisSummary{ID: "cached-id", ComputedAt: "2026-08-08T00:00:00Z"}, nil
		},
	}
	// Budget of zero runs: only a cache hit can succeed.
	cfg := DefaultRunBudgetConfig()
	cfg.GlobalDailyRuns = 1
	r := newRunTestRouter(m, cfg, "")

	w := postAnalyses(r, rollupBody)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 cache hit; body: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"cached":true`) || !strings.Contains(w.Body.String(), "cached-id") {
		t.Errorf("body should carry cached analysis id: %s", w.Body.String())
	}
	if len(m.createdRuns) != 0 {
		t.Errorf("cache hit must not enqueue a run")
	}
}

func TestCreateRunBudgetDenials(t *testing.T) {
	m := &mockStore{latestVintages: map[string]string{"cdc_obesity": "CDC-PLACES-2023"}}
	cfg := DefaultRunBudgetConfig()
	cfg.GlobalDailyRuns = 1
	r := newRunTestRouter(m, cfg, "")

	if w := postAnalyses(r, rollupBody); w.Code != http.StatusAccepted {
		t.Fatalf("first run: status = %d, want 202", w.Code)
	}
	w := postAnalyses(r, rollupBody)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("second run: status = %d, want 429; body: %s", w.Code, w.Body.String())
	}
	if w.Header().Get("Retry-After") == "" {
		t.Errorf("daily denial should carry Retry-After")
	}

	// Queue-full denial, independent of the daily tally.
	m2 := &mockStore{latestVintages: map[string]string{"cdc_obesity": "CDC-PLACES-2023"}}
	cfg2 := DefaultRunBudgetConfig()
	cfg2.MaxQueueDepth = 3
	m2.activeRuns = 3
	r2 := newRunTestRouter(m2, cfg2, "")
	if w := postAnalyses(r2, rollupBody); w.Code != http.StatusTooManyRequests {
		t.Fatalf("queue full: status = %d, want 429", w.Code)
	}
}

func TestCreateRunAuthToken(t *testing.T) {
	m := &mockStore{latestVintages: map[string]string{"cdc_obesity": "CDC-PLACES-2023"}}
	r := newRunTestRouter(m, DefaultRunBudgetConfig(), "sekret")

	if w := postAnalyses(r, rollupBody); w.Code != http.StatusUnauthorized {
		t.Fatalf("no token: status = %d, want 401", w.Code)
	}
	if w := postAnalyses(r, rollupBody, "Authorization", "Bearer wrong"); w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: status = %d, want 401", w.Code)
	}
	if w := postAnalyses(r, rollupBody, "Authorization", "Bearer sekret"); w.Code != http.StatusAccepted {
		t.Fatalf("right token: status = %d, want 202", w.Code)
	}
	// GET endpoints stay open regardless of the POST gate.
	req := httptest.NewRequest(http.MethodGet, "/v1/analyses", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code == http.StatusUnauthorized {
		t.Errorf("GET /analyses must not be token-gated")
	}
}

func TestGetAnalysisRunStatus(t *testing.T) {
	m := &mockStore{
		getAnalysisRunFn: func(id string) (*store.AnalysisRun, error) {
			if id != "run-42" {
				t.Errorf("id = %q", id)
			}
			return &store.AnalysisRun{ID: "run-42", RunType: "spearman", Status: "done", AnalysisID: "an-7"}, nil
		},
	}
	r := newRunTestRouter(m, DefaultRunBudgetConfig(), "")
	req := httptest.NewRequest(http.MethodGet, "/v1/analyses/runs/run-42", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", w.Code, w.Body.String())
	}
	for _, want := range []string{"run-42", "spearman", `"status":"done"`, "an-7"} {
		if !strings.Contains(w.Body.String(), want) {
			t.Errorf("body missing %q: %s", want, w.Body.String())
		}
	}
}
