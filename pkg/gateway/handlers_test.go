package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// ── mockStore ───────────────────────────────────────────────────────────────

// mockStore is a minimal in-memory implementation of store.Store for use in
// handler tests. It does not require a real database connection.
type mockStore struct {
	geographies []geo.Geography
	indicators  []store.Indicator
	scores      []store.AnalysisScore

	// Optionally override behaviour per-test.
	getGeographyFn func(ctx context.Context, geoid string) (*geo.Geography, error)
}

func (m *mockStore) PutGeographies(_ context.Context, geos []geo.Geography) error {
	m.geographies = append(m.geographies, geos...)
	return nil
}

func (m *mockStore) GetGeography(ctx context.Context, geoid string) (*geo.Geography, error) {
	if m.getGeographyFn != nil {
		return m.getGeographyFn(ctx, geoid)
	}
	for _, g := range m.geographies {
		if g.GEOID == geoid {
			cp := g
			return &cp, nil
		}
	}
	return nil, errNotFound{}
}

func (m *mockStore) QueryGeographies(_ context.Context, q store.GeoQuery) ([]geo.Geography, error) {
	var out []geo.Geography
	for _, g := range m.geographies {
		if q.Level != "" && g.Level != q.Level {
			continue
		}
		if q.ParentGEOID != "" && g.ParentGEOID != q.ParentGEOID {
			continue
		}
		if q.StateFIPS != "" && g.StateFIPS != q.StateFIPS {
			continue
		}
		out = append(out, g)
	}
	// Apply limit/offset.
	if q.Offset >= len(out) {
		return []geo.Geography{}, nil
	}
	out = out[q.Offset:]
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

func (m *mockStore) PutIndicators(_ context.Context, indicators []store.Indicator) error {
	m.indicators = append(m.indicators, indicators...)
	return nil
}

func (m *mockStore) PutIndicatorsBatch(_ context.Context, indicators []store.Indicator, _ int) error {
	m.indicators = append(m.indicators, indicators...)
	return nil
}

func (m *mockStore) QueryIndicators(_ context.Context, q store.IndicatorQuery) ([]store.Indicator, error) {
	geoidSet := make(map[string]bool, len(q.GEOIDs))
	for _, g := range q.GEOIDs {
		geoidSet[g] = true
	}
	varSet := make(map[string]bool, len(q.VariableIDs))
	for _, v := range q.VariableIDs {
		varSet[v] = true
	}

	var out []store.Indicator
	for _, ind := range m.indicators {
		if len(geoidSet) > 0 && !geoidSet[ind.GEOID] {
			continue
		}
		if len(varSet) > 0 && !varSet[ind.VariableID] {
			continue
		}
		if q.Vintage != "" && ind.Vintage != q.Vintage {
			continue
		}
		out = append(out, ind)
	}
	return out, nil
}

func (m *mockStore) Aggregate(_ context.Context, _ store.AggregateQuery) (*store.AggregateResult, error) {
	return &store.AggregateResult{}, nil
}

func (m *mockStore) PutAnalysis(_ context.Context, _ store.AnalysisResult) (string, error) {
	return "mock-analysis-id", nil
}

func (m *mockStore) PutAnalysisScores(_ context.Context, scores []store.AnalysisScore) error {
	m.scores = append(m.scores, scores...)
	return nil
}

func (m *mockStore) QueryAnalysisScores(_ context.Context, analysisID string, tier string) ([]store.AnalysisScore, error) {
	var out []store.AnalysisScore
	for _, s := range m.scores {
		if s.AnalysisID != analysisID {
			continue
		}
		if tier != "" && s.Tier != tier {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

func (m *mockStore) Migrate(_ context.Context) error      { return nil }
func (m *mockStore) RefreshViews(_ context.Context) error { return nil }
func (m *mockStore) Close() error                         { return nil }

// errNotFound is a sentinel error whose message satisfies isNotFound() via the
// strings.Contains(err.Error(), "not found") branch in handlers.go. This lets
// tests exercise the 404 path without importing pgx.
type errNotFound struct{}

func (errNotFound) Error() string { return "record not found" }

// ── test router helper ───────────────────────────────────────────────────────

// newTestRouter builds a Gin engine with the PolicyPlugin routes mounted at /v1.
// gin.TestMode suppresses log output during tests.
func newTestRouter(s store.Store) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	plugin := NewPlugin(s)
	group := r.Group("/v1")
	plugin.RegisterRoutes(group)

	// Add a dedicated health endpoint at /health (outside the plugin group)
	// mirroring what cmd/pdi/serve.go registers.
	r.GET("/health", func(c *gin.Context) {
		if err := plugin.Health(); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unhealthy", "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"status":  "ok",
			"plugin":  plugin.Name(),
			"version": plugin.Version(),
		})
	})
	return r
}

// ── Test: GET /health ────────────────────────────────────────────────────────

func TestHealthEndpoint(t *testing.T) {
	r := newTestRouter(&mockStore{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/health", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d — body: %s", w.Code, w.Body.String())
	}

	var body map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", body["status"])
	}
	if body["plugin"] != "policy-data-infrastructure" {
		t.Errorf("expected plugin name, got %q", body["plugin"])
	}
}

// ── Test: GET /v1/sources ────────────────────────────────────────────────────

func TestListSources(t *testing.T) {
	r := newTestRouter(&mockStore{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/v1/sources", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d — body: %s", w.Code, w.Body.String())
	}

	var body map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}

	total, ok := body["total"].(float64)
	if !ok || total <= 0 {
		t.Errorf("expected non-zero total in sources response, got %v", body["total"])
	}

	sources, ok := body["sources"].([]interface{})
	if !ok || len(sources) == 0 {
		t.Errorf("expected non-empty sources array, got %v", body["sources"])
	}
}

// ── Test: GET /v1/geographies — empty store ──────────────────────────────────

func TestListGeographies_Empty(t *testing.T) {
	r := newTestRouter(&mockStore{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/v1/geographies", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d — body: %s", w.Code, w.Body.String())
	}

	var resp GeographyListResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Total != 0 {
		t.Errorf("expected 0 items, got %d", resp.Total)
	}
	if resp.Limit != 50 {
		t.Errorf("expected default limit=50, got %d", resp.Limit)
	}
}

// ── Test: GET /v1/geographies — with data ────────────────────────────────────

func TestListGeographies_WithData(t *testing.T) {
	s := &mockStore{
		geographies: []geo.Geography{
			{GEOID: "55025", Level: geo.County, Name: "Dane County", StateFIPS: "55"},
			{GEOID: "55079", Level: geo.County, Name: "Milwaukee County", StateFIPS: "55"},
			{GEOID: "17031", Level: geo.County, Name: "Cook County", StateFIPS: "17"},
		},
	}
	r := newTestRouter(s)

	// All geographies.
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/v1/geographies", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp GeographyListResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if resp.Total != 3 {
		t.Errorf("expected 3 items, got %d", resp.Total)
	}

	// Filter by state_fips.
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest(http.MethodGet, "/v1/geographies?state_fips=55", nil)
	r.ServeHTTP(w2, req2)

	var resp2 GeographyListResponse
	if err := json.NewDecoder(w2.Body).Decode(&resp2); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if resp2.Total != 2 {
		t.Errorf("expected 2 WI counties, got %d", resp2.Total)
	}
}

// ── Test: GET /v1/geographies — query param validation ───────────────────────

func TestListGeographies_InvalidLevel(t *testing.T) {
	r := newTestRouter(&mockStore{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/v1/geographies?level=galaxy", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid level, got %d", w.Code)
	}

	var errResp ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if errResp.Error == "" {
		t.Errorf("expected non-empty error field in response")
	}
}

func TestListGeographies_InvalidLimit(t *testing.T) {
	r := newTestRouter(&mockStore{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/v1/geographies?limit=notanumber", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for non-numeric limit, got %d", w.Code)
	}
}

func TestListGeographies_NegativeLimit(t *testing.T) {
	r := newTestRouter(&mockStore{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/v1/geographies?limit=-5", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for negative limit, got %d", w.Code)
	}
}

func TestListGeographies_InvalidOffset(t *testing.T) {
	r := newTestRouter(&mockStore{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/v1/geographies?offset=-1", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for negative offset, got %d", w.Code)
	}
}

// ── Test: GET /v1/geographies — limit cap at 1000 ────────────────────────────

func TestListGeographies_LimitCappedAt1000(t *testing.T) {
	// Seed 10 geographies; request limit=5000 — should get all 10, cap=1000.
	s := &mockStore{}
	for i := 0; i < 10; i++ {
		s.geographies = append(s.geographies, geo.Geography{
			GEOID: strings.Repeat("0", 4-len(strconv.Itoa(i+1))) + strconv.Itoa(i+1) + "1",
			Level: geo.County,
			Name:  "County",
		})
	}
	r := newTestRouter(s)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/v1/geographies?limit=5000", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp GeographyListResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	// Cap is applied to the limit field in response.
	if resp.Limit > 1000 {
		t.Errorf("limit should be capped at 1000, got %d", resp.Limit)
	}
}

// ── Test: GET /v1/geographies/:geoid — found ─────────────────────────────────

func TestGetGeography_Found(t *testing.T) {
	pop := 540000
	s := &mockStore{
		geographies: []geo.Geography{
			{GEOID: "55025", Level: geo.County, Name: "Dane County", StateFIPS: "55", Population: pop},
		},
	}
	r := newTestRouter(s)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/v1/geographies/55025", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d — body: %s", w.Code, w.Body.String())
	}

	var resp GeographyResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if resp.GEOID != "55025" {
		t.Errorf("expected GEOID=55025, got %q", resp.GEOID)
	}
	if resp.Name != "Dane County" {
		t.Errorf("expected Name=Dane County, got %q", resp.Name)
	}
	if resp.Population != pop {
		t.Errorf("expected Population=%d, got %d", pop, resp.Population)
	}
}

// ── Test: GET /v1/geographies/:geoid — not found → 404 ───────────────────────

func TestGetGeography_NotFound(t *testing.T) {
	r := newTestRouter(&mockStore{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/v1/geographies/55999", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown GEOID, got %d — body: %s", w.Code, w.Body.String())
	}

	var errResp ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if errResp.Error == "" {
		t.Errorf("expected non-empty error field")
	}
}

// ── Test: GEOID format validation middleware ──────────────────────────────────

func TestGetGeography_InvalidGEOIDFormat(t *testing.T) {
	// "abc" is not a valid GEOID (not all digits, not canonical length).
	r := newTestRouter(&mockStore{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/v1/geographies/abc", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for non-numeric GEOID, got %d", w.Code)
	}
}

// ── Test: GET /v1/geographies/:geoid/children — no children level ────────────

func TestGetChildren_LeafLevel_EmptyResponse(t *testing.T) {
	// Block group is a leaf — ChildLevel returns false, handler returns empty list.
	s := &mockStore{
		geographies: []geo.Geography{
			{GEOID: "550250001001", Level: geo.BlockGroup, Name: "Block Group 1", StateFIPS: "55"},
		},
	}
	r := newTestRouter(s)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/v1/geographies/550250001001/children", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for leaf geography children, got %d — body: %s", w.Code, w.Body.String())
	}

	var resp GeographyListResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if resp.Total != 0 {
		t.Errorf("expected 0 children for leaf geography, got %d", resp.Total)
	}
}

// ── Test: GET /v1/geographies/:geoid/indicators ───────────────────────────────

func TestGetIndicators_WithData(t *testing.T) {
	val := 42.5
	s := &mockStore{
		geographies: []geo.Geography{
			{GEOID: "55025", Level: geo.County, Name: "Dane County", StateFIPS: "55"},
		},
		indicators: []store.Indicator{
			{GEOID: "55025", VariableID: "B19013_001E", Vintage: "2022", Value: &val},
			{GEOID: "55025", VariableID: "B25077_001E", Vintage: "2022", Value: &val},
		},
	}
	r := newTestRouter(s)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/v1/geographies/55025/indicators", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d — body: %s", w.Code, w.Body.String())
	}

	var body map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if body["geoid"] != "55025" {
		t.Errorf("expected geoid=55025, got %v", body["geoid"])
	}
	total, _ := body["total"].(float64)
	if total != 2 {
		t.Errorf("expected 2 indicators, got %v", total)
	}
}

// ── Test: POST /v1/query ──────────────────────────────────────────────────────

func TestQuery_BasicPost(t *testing.T) {
	s := &mockStore{
		geographies: []geo.Geography{
			{GEOID: "55025", Level: geo.County, Name: "Dane County", StateFIPS: "55"},
		},
	}
	r := newTestRouter(s)

	body := `{"level":"county","state_fips":"55","limit":10}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/v1/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d — body: %s", w.Code, w.Body.String())
	}

	var resp GeographyListResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if resp.Total != 1 {
		t.Errorf("expected 1 item, got %d", resp.Total)
	}
}

func TestQuery_InvalidLevel(t *testing.T) {
	r := newTestRouter(&mockStore{})
	body := `{"level":"galaxy"}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/v1/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid level in POST /query, got %d", w.Code)
	}
}

func TestQuery_InvalidJSON(t *testing.T) {
	r := newTestRouter(&mockStore{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/v1/query", strings.NewReader("{bad json"))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for malformed JSON body, got %d", w.Code)
	}
}

// ── Test: POST /v1/pipeline/run returns 501 ───────────────────────────────────

func TestPipelineRun_Returns501(t *testing.T) {
	r := newTestRouter(&mockStore{})
	body := `{"source":"census","level":"county","vintage":"2022"}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/v1/pipeline/run", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Errorf("expected 501 for unimplemented pipeline run, got %d", w.Code)
	}
}

// ── Test: GET /v1/pipeline/events — missing run_id → 400 ─────────────────────

func TestPipelineEvents_MissingRunID(t *testing.T) {
	r := newTestRouter(&mockStore{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/v1/pipeline/events", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing run_id, got %d", w.Code)
	}
}

// ── Test: helper functions ────────────────────────────────────────────────────

func TestSanitiseFilename(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"Dane County", "Dane_County"},
		{"Milwaukee/County", "Milwaukee_County"},
		{"clean-name_ok.html", "clean-name_ok.html"},
		{"abc 123", "abc_123"},
	}
	for _, c := range cases {
		got := sanitiseFilename(c.in)
		if got != c.want {
			t.Errorf("sanitiseFilename(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestFormatInt(t *testing.T) {
	cases := []struct {
		n    int
		want string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{1000000, "1,000,000"},
		{540000, "540,000"},
	}
	for _, c := range cases {
		got := formatInt(c.n)
		if got != c.want {
			t.Errorf("formatInt(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestIsNotFound_NilError(t *testing.T) {
	if isNotFound(nil) {
		t.Error("isNotFound(nil) should return false")
	}
}

func TestIsNotFound_ErrNotFound(t *testing.T) {
	if !isNotFound(errNotFound{}) {
		t.Error("isNotFound(errNotFound{}) should return true")
	}
}
