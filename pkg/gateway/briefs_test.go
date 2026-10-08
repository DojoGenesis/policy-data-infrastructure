package gateway

// Tests for handleGetPolicyBrief (briefs.go). Deliberately does NOT reuse the
// mockStore defined in handlers_test.go: that type's GetPolicy stub always
// returns (nil, nil) regardless of fixture data, with no override hook, and
// handlers_test.go is outside this track's file-ownership manifest. Adding an
// override hook there would be an edit to a file this track does not own, so
// briefTestStore is a second, self-contained store.Store implementation
// local to this file.
//
// No live database: every test drives handleGetPolicyBrief directly against
// briefTestStore's in-memory fixtures.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/experiment"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// ── briefTestStore ──────────────────────────────────────────────────────────

type briefTestStore struct {
	policies    map[string]store.PolicyRecord
	geographies map[string]geo.Geography
	indicators  []store.Indicator
}

func newBriefTestStore() *briefTestStore {
	return &briefTestStore{
		policies:    map[string]store.PolicyRecord{},
		geographies: map[string]geo.Geography{},
	}
}

func (s *briefTestStore) GetPolicy(_ context.Context, id string) (*store.PolicyRecord, error) {
	p, ok := s.policies[id]
	if !ok {
		return nil, errNotFound{}
	}
	return &p, nil
}

func (s *briefTestStore) GetGeography(_ context.Context, geoid string) (*geo.Geography, error) {
	g, ok := s.geographies[geoid]
	if !ok {
		return nil, errNotFound{}
	}
	return &g, nil
}

func (s *briefTestStore) QueryGeographies(_ context.Context, q store.GeoQuery) ([]geo.Geography, error) {
	var out []geo.Geography
	for _, g := range s.geographies {
		if q.Level != "" && g.Level != q.Level {
			continue
		}
		if q.StateFIPS != "" && g.StateFIPS != q.StateFIPS {
			continue
		}
		if q.CountyFIPS != "" && g.CountyFIPS != q.CountyFIPS {
			continue
		}
		out = append(out, g)
	}
	return out, nil
}

func (s *briefTestStore) QueryIndicators(_ context.Context, q store.IndicatorQuery) ([]store.Indicator, error) {
	geoidSet := make(map[string]bool, len(q.GEOIDs))
	for _, g := range q.GEOIDs {
		geoidSet[g] = true
	}
	varSet := make(map[string]bool, len(q.VariableIDs))
	for _, v := range q.VariableIDs {
		varSet[v] = true
	}
	var out []store.Indicator
	for _, ind := range s.indicators {
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

// The remaining store.Store methods are unused by handleGetPolicyBrief; each
// returns a harmless zero value so briefTestStore satisfies the interface.
func (s *briefTestStore) PutGeographies(context.Context, []geo.Geography) error   { return nil }
func (s *briefTestStore) CountGeographies(context.Context, store.GeoQuery) (int, error) {
	return 0, nil
}
func (s *briefTestStore) PutIndicators(context.Context, []store.Indicator) error { return nil }
func (s *briefTestStore) PutIndicatorsBatch(context.Context, []store.Indicator, int) error {
	return nil
}
func (s *briefTestStore) Aggregate(context.Context, store.AggregateQuery) (*store.AggregateResult, error) {
	return &store.AggregateResult{}, nil
}
func (s *briefTestStore) PutAnalysis(context.Context, store.AnalysisResult) (string, error) {
	return "", nil
}
func (s *briefTestStore) FindAnalysisByKey(context.Context, store.AnalysisKey) (*store.AnalysisSummary, error) {
	return nil, nil
}
func (s *briefTestStore) GetAnalysis(context.Context, string) (*store.AnalysisResult, error) {
	return nil, errNotFound{}
}
func (s *briefTestStore) PutAnalysisScores(context.Context, []store.AnalysisScore) error {
	return nil
}
func (s *briefTestStore) QueryAnalysisScores(context.Context, string, string) ([]store.AnalysisScore, error) {
	return nil, nil
}
func (s *briefTestStore) ListAnalyses(context.Context) ([]store.AnalysisSummary, error) {
	return nil, nil
}
func (s *briefTestStore) CreateAnalysisRun(context.Context, store.AnalysisRun) (string, error) {
	return "", nil
}
func (s *briefTestStore) GetAnalysisRun(context.Context, string) (*store.AnalysisRun, error) {
	return nil, errNotFound{}
}
func (s *briefTestStore) ClaimNextAnalysisRun(context.Context) (*store.AnalysisRun, error) {
	return nil, nil
}
func (s *briefTestStore) CompleteAnalysisRun(context.Context, string, string, string) error {
	return nil
}
func (s *briefTestStore) CountActiveRuns(context.Context) (int, error) { return 0, nil }
func (s *briefTestStore) LatestVintageForVariable(context.Context, string) (string, error) {
	return "", nil
}
func (s *briefTestStore) QueryStateRanks(context.Context, string) (map[string]float64, error) {
	return nil, nil
}
func (s *briefTestStore) PutFactorScores(context.Context, []store.FactorScore) error { return nil }
func (s *briefTestStore) QueryFactorScores(context.Context, string) ([]store.FactorScore, error) {
	return nil, nil
}
func (s *briefTestStore) QueryFactorScoresAtLevel(context.Context, int) ([]store.FactorScore, error) {
	return nil, nil
}
func (s *briefTestStore) DeleteFactorScoresAtLevel(context.Context, int, string) (int64, error) {
	return 0, nil
}
func (s *briefTestStore) QueryValidatedFeatures(context.Context, string) ([]store.ValidatedFeature, error) {
	return nil, nil
}
func (s *briefTestStore) PutPolicies(context.Context, []store.PolicyRecord) error { return nil }
func (s *briefTestStore) QueryPolicies(context.Context, store.PolicyQuery) ([]store.PolicyRecord, error) {
	return nil, nil
}
func (s *briefTestStore) PutEvidenceCards(context.Context, []store.EvidenceCard) error { return nil }
func (s *briefTestStore) QueryEvidenceCards(context.Context, store.EvidenceCardFilter) ([]store.EvidenceCard, error) {
	return nil, nil
}
func (s *briefTestStore) SeedEvidenceCardsFromJSON(context.Context, []byte) error { return nil }
func (s *briefTestStore) QueryLISACountyProfile(context.Context, string) (*store.LISACountyProfile, error) {
	return nil, nil
}
func (s *briefTestStore) QueryVariables(context.Context) ([]store.VariableMeta, error) {
	return nil, nil
}
func (s *briefTestStore) CountSourcesWithData(context.Context) (int, error) { return 0, nil }
func (s *briefTestStore) RegisterSource(context.Context, store.SourceMeta, []store.VariableMeta) error {
	return nil
}
func (s *briefTestStore) Ping(context.Context) error         { return nil }
func (s *briefTestStore) Migrate(context.Context) error      { return nil }
func (s *briefTestStore) RefreshViews(context.Context) error { return nil }
func (s *briefTestStore) Close() error                       { return nil }

var _ store.Store = (*briefTestStore)(nil)

// ── fixtures ─────────────────────────────────────────────────────────────

func rockCountyFixture() *briefTestStore {
	s := newBriefTestStore()
	s.policies["FH-TRANSIT-008"] = store.PolicyRecord{
		ID:              "FH-TRANSIT-008",
		Candidate:       "Francesca Hong",
		Office:          "Governor",
		Category:        "Cities Towns Transit",
		Title:           "Public groceries",
		Description:     "Enable publicly-owned grocery stores in food deserts",
		BillReferences:  "AB1221 (2025)",
		ClaimsEmpirical: "",
	}
	s.policies["FH-OTHER-001"] = store.PolicyRecord{
		ID:       "FH-OTHER-001",
		Title:    "Some other position",
		Category: "Other",
	}

	s.geographies["55105"] = geo.Geography{
		GEOID: "55105", Level: geo.County, Name: "Rock County",
		StateFIPS: "55", CountyFIPS: "105", Population: 164350,
	}
	s.geographies["55"] = geo.Geography{GEOID: "55", Level: geo.State, Name: "Wisconsin", StateFIPS: "55"}

	// Two Rock County tracts: one designated, one not, both with complete
	// USDA rows — enough to exercise reach.areas[] and the county totals.
	s.geographies["55105000100"] = geo.Geography{
		GEOID: "55105000100", Level: geo.Tract, Name: "Census Tract 1, Rock County, Wisconsin",
		StateFIPS: "55", CountyFIPS: "105", ParentGEOID: "55105",
	}
	s.geographies["55105999900"] = geo.Geography{
		GEOID: "55105999900", Level: geo.Tract, Name: "Census Tract 9999, Rock County, Wisconsin",
		StateFIPS: "55", CountyFIPS: "105", ParentGEOID: "55105",
	}

	share := 0.261
	pop := 164350.0
	snapDesignated := 4754.0
	snapOther := 100.0
	popT1 := 41768.0
	popT2 := 5000.0
	shareT1 := 1.0
	shareT2 := 0.0

	s.indicators = []store.Indicator{
		{GEOID: "55105", VariableID: varDesignatedShare, Vintage: usdaVintage, Value: &share},
		{GEOID: "55105", VariableID: varFaraPopulation, Vintage: usdaVintage, Value: &pop},
		{GEOID: "55105", VariableID: varSNAPCount, Vintage: usdaVintage, Value: &snapDesignated},

		{GEOID: "55105000100", VariableID: varDesignatedShare, Vintage: usdaVintage, Value: &shareT1},
		{GEOID: "55105000100", VariableID: varFaraPopulation, Vintage: usdaVintage, Value: &popT1},
		{GEOID: "55105000100", VariableID: varSNAPCount, Vintage: usdaVintage, Value: &snapDesignated},

		{GEOID: "55105999900", VariableID: varDesignatedShare, Vintage: usdaVintage, Value: &shareT2},
		{GEOID: "55105999900", VariableID: varFaraPopulation, Vintage: usdaVintage, Value: &popT2},
		{GEOID: "55105999900", VariableID: varSNAPCount, Vintage: usdaVintage, Value: &snapOther},
	}
	return s
}

// ── test harness ─────────────────────────────────────────────────────────

func newBriefTestRouter(s store.Store) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	p := &PolicyPlugin{store: s, varMeta: map[string]store.VariableMeta{}}
	// Registered directly at the contract's path, without touching
	// plugin.go's RegisterRoutes — route wiring belongs to the integrator.
	r.GET("/v1/policy/briefs/:policy_id", p.handleGetPolicyBrief)
	return r
}

func getBrief(r *gin.Engine, path string) (*httptest.ResponseRecorder, experiment.Brief) {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	var b experiment.Brief
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
			panic(err) // test bug, not a runtime path
		}
	}
	return w, b
}

// ── tests ────────────────────────────────────────────────────────────────

func TestBrief_UnknownPolicy404(t *testing.T) {
	r := newBriefTestRouter(rockCountyFixture())
	w, _ := getBrief(r, "/v1/policy/briefs/NOT-A-POLICY?geoid=55105")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body: %s", w.Code, w.Body.String())
	}
}

func TestBrief_MalformedGeoid400(t *testing.T) {
	r := newBriefTestRouter(rockCountyFixture())
	w, _ := getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008?geoid=abc")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", w.Code, w.Body.String())
	}
	w, _ = getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008?geoid=123") // 3 digits, no valid length
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for 3-digit geoid; body: %s", w.Code, w.Body.String())
	}
}

func TestBrief_UnknownGeoid422(t *testing.T) {
	r := newBriefTestRouter(rockCountyFixture())
	w, _ := getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008?geoid=99999")
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body: %s", w.Code, w.Body.String())
	}
}

// TestBrief_NoGeoidDefaultsToStateFallback is FIX-2's third S4 branch,
// end to end: a request with no ?geoid= AND no model (so no default_geoid
// to fall back to) must resolve to "55" with source "state_fallback", not
// silently 404 or silently pick a county nobody asked for. modelsDir is
// explicitly pointed at an empty (but reachable) temp dir so this exercises
// "no model" on purpose, not an accident of the default root being
// unreachable under `go test`'s CWD.
func TestBrief_NoGeoidDefaultsToStateFallback(t *testing.T) {
	restore := setBriefDataDirs(t, t.TempDir(), t.TempDir())
	defer restore()

	r := newBriefTestRouter(rockCountyFixture())
	w, b := getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	if b.Geography.GEOID != "55" || b.Geography.Source != experiment.GeoStateFallback {
		t.Fatalf("geography = %+v, want geoid=55 source=state_fallback", b.Geography)
	}
}

// TestBrief_NoGeoidUsesModelDefault is FIX-2's second S4 branch: when a
// request carries no ?geoid= AND a model IS loaded, the model's declared
// default_geoid must be used — with source=model_default — rather than
// falling all the way to statewide Wisconsin. Before FIX-2 this rung of S4
// was unreachable: the model was never loaded early enough for its
// default_geoid to be consulted, so every unqualified page load landed on
// "55" regardless of what the model declared (the exact bug this fix
// closes: "a default page load lands on statewide Wisconsin instead of the
// pilot county").
func TestBrief_NoGeoidUsesModelDefault(t *testing.T) {
	dir := t.TempDir()
	restore := setBriefDataDirs(t, dir, t.TempDir())
	defer restore()
	writeFixtureFile(t, dir, "FH-TRANSIT-008.json", fixtureModelJSON) // default_geoid: "55105"

	r := newBriefTestRouter(rockCountyFixture())
	w, b := getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	if b.Geography.GEOID != "55105" || b.Geography.Source != experiment.GeoModelDefault {
		t.Fatalf("geography = %+v, want geoid=55105 source=model_default", b.Geography)
	}
}

// TestBrief_RequestedGeoidBeatsModelDefault is FIX-2's first S4 branch: an
// explicit ?geoid= always wins over the model's default_geoid, even when a
// model is loaded and declares a different one.
func TestBrief_RequestedGeoidBeatsModelDefault(t *testing.T) {
	dir := t.TempDir()
	restore := setBriefDataDirs(t, dir, t.TempDir())
	defer restore()
	writeFixtureFile(t, dir, "FH-TRANSIT-008.json", fixtureModelJSON) // default_geoid: "55105"

	r := newBriefTestRouter(rockCountyFixture())
	w, b := getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008?geoid=55") // explicit state geoid
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	if b.Geography.GEOID != "55" || b.Geography.Source != experiment.GeoRequested {
		t.Fatalf("geography = %+v, want geoid=55 source=requested (explicit request beats model default)", b.Geography)
	}
}

// TestResolveBriefGeoid_S4Branches is the unit-level table for all three S4
// branches plus the malformed-input error path, isolated from HTTP/store
// plumbing.
func TestResolveBriefGeoid_S4Branches(t *testing.T) {
	cases := []struct {
		name              string
		rawQuery          string
		modelDefaultGeoid string
		wantGeoid         string
		wantSource        experiment.GeoSource
		wantErr           bool
	}{
		{"requested beats model default", "55025", "55105", "55025", experiment.GeoRequested, false},
		{"model default used when request empty", "", "55105", "55105", experiment.GeoModelDefault, false},
		{"state fallback when both empty", "", "", "55", experiment.GeoStateFallback, false},
		{"malformed requested geoid errors", "abc", "55105", "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			geoid, source, err := resolveBriefGeoid(tc.rawQuery, tc.modelDefaultGeoid)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveBriefGeoid(%q,%q) = nil error, want one", tc.rawQuery, tc.modelDefaultGeoid)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveBriefGeoid(%q,%q): unexpected error: %v", tc.rawQuery, tc.modelDefaultGeoid, err)
			}
			if geoid != tc.wantGeoid || source != tc.wantSource {
				t.Fatalf("resolveBriefGeoid(%q,%q) = (%q,%q), want (%q,%q)",
					tc.rawQuery, tc.modelDefaultGeoid, geoid, source, tc.wantGeoid, tc.wantSource)
			}
		})
	}
}

// TestPolicyDataRoot_EnvOverride is FIX-1's config-surface test: unset ->
// the repo-relative default; set -> the exact value, verbatim, with no
// joining or trimming beyond whitespace.
func TestPolicyDataRoot_EnvOverride(t *testing.T) {
	t.Setenv(envPolicyDataDir, "")
	if got := policyDataRoot(); got != "data/policies" {
		t.Errorf("policyDataRoot() with unset env = %q, want the repo-relative default %q", got, "data/policies")
	}

	t.Setenv(envPolicyDataDir, "/opt/pdi/data/policies")
	if got := policyDataRoot(); got != "/opt/pdi/data/policies" {
		t.Errorf("policyDataRoot() with %s set = %q, want the env value verbatim", envPolicyDataDir, got)
	}
}

// TestBrief_PolicyWithNoRuleGetsAbsenceNotReach is the S3 check for a policy
// this build cannot express a tier-1 rule for: reach must be nil AND an
// absences[] entry must explain why — never a silently omitted tier.
func TestBrief_PolicyWithNoRuleGetsAbsenceNotReach(t *testing.T) {
	r := newBriefTestRouter(rockCountyFixture())
	w, b := getBrief(r, "/v1/policy/briefs/FH-OTHER-001?geoid=55105")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	if b.Reach != nil {
		t.Fatalf("reach = %+v, want nil for a policy with no expressible rule", b.Reach)
	}
	if !hasAbsence(b.Absences, experiment.TierReach) {
		t.Fatalf("absences = %+v, want a tier=reach entry", b.Absences)
	}
}

// TestBrief_ModelAndEvidenceAbsentWhenNoFileConfigured is the S3 check for
// the ordinary case: no data/policies/models/<id>.json and no
// data/policies/evidence/<id>.csv on disk means model=nil and evidence=[]
// with matching absences[] entries — never a silently omitted tier and
// never a fabricated model/evidence block. modelsDir/evidenceDir are
// pointed at an empty temp directory rather than relying on the default
// relative paths happening not to resolve under `go test`'s CWD, which
// would make this test pass for an accidental reason instead of the real
// one.
func TestBrief_ModelAndEvidenceAbsentWhenNoFileConfigured(t *testing.T) {
	restore := setBriefDataDirs(t, t.TempDir(), t.TempDir())
	defer restore()

	r := newBriefTestRouter(rockCountyFixture())
	_, b := getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008?geoid=55105")
	if b.Model != nil {
		t.Fatalf("model = %+v, want nil (no model file configured)", b.Model)
	}
	if len(b.Evidence) != 0 {
		t.Fatalf("evidence = %+v, want empty (no evidence file configured)", b.Evidence)
	}
	if !hasAbsence(b.Absences, experiment.TierExperiment) {
		t.Fatalf("absences = %+v, want a tier=experiment entry", b.Absences)
	}
	if !hasAbsence(b.Absences, experiment.TierEvidence) {
		t.Fatalf("absences = %+v, want a tier=evidence entry", b.Absences)
	}
}

// fixtureModelJSONWithStoreRefusal is fixtureModelJSON with a
// grocery/store-specific refusal key added to refusal_keys — the FH-TRANSIT-
// 008-only claim "PDI does not simulate where a store would go", which FIX-4
// must NOT attach to any policy other than one whose own model declares it.
const fixtureModelJSONWithStoreRefusal = `{
  "schema_version": "1",
  "policy_id": "FH-TRANSIT-008",
  "model_version": "9",
  "geo_levels": ["county"],
  "default_geoid": "55105",
  "title_key": "pex.model.fh_transit_008.title",
  "summary_key": "pex.model.fh_transit_008.summary",
  "params": [
    {"id": "num_stores", "label_key": "pex.param.num_stores.label",
     "help_key": "pex.param.num_stores.help", "unit": "count",
     "min": 0, "max": 12, "step": 1, "default": 1,
     "default_source_key": "pex.param.num_stores.default_source", "precision": 0}
  ],
  "outputs": [
    {"id": "total_capital_cost", "label_key": "pex.output.total_capital_cost.label",
     "unit": "usd", "precision": 0, "register": "public",
     "expr": {"op": "mul", "terms": [
       {"expr": {"op": "ref", "ref": "param:num_stores"}},
       {"expr": {"op": "const", "const": 1500000}}
     ]}}
  ],
  "refusal_keys": ["pex.refusal.no_causal_claim", "pex.refusal.no_time_series", "pex.refusal.no_siting_simulation"],
  "authored_by": "test fixture", "reviewed_by": "test", "reviewed_date": "2026-08-13"
}`

// TestBrief_NoModelGetsOnlyPageLevelRefusals is FIX-4's known-bad control
// for the "no model" side: a policy with no model file must carry EXACTLY
// the two refusals true of every policy on this page (no causal claim, no
// time series) — never a subject-specific one like "PDI does not simulate
// where a store would go", which has nothing to do with a policy this build
// cannot even express a tier-2 model for.
func TestBrief_NoModelGetsOnlyPageLevelRefusals(t *testing.T) {
	restore := setBriefDataDirs(t, t.TempDir(), t.TempDir())
	defer restore()

	r := newBriefTestRouter(rockCountyFixture())
	_, b := getBrief(r, "/v1/policy/briefs/FH-OTHER-001?geoid=55105")

	if len(b.RefusalKeys) != len(pageLevelRefusalKeys) {
		t.Fatalf("refusal_keys = %v, want exactly the page-level set %v", b.RefusalKeys, pageLevelRefusalKeys)
	}
	for _, want := range pageLevelRefusalKeys {
		if !containsString(b.RefusalKeys, want) {
			t.Errorf("refusal_keys = %v, missing page-level key %q", b.RefusalKeys, want)
		}
	}
	if containsString(b.RefusalKeys, "pex.refusal.no_siting_simulation") {
		t.Error("refusal_keys carries the store-siting refusal for a policy with no model — FIX-4 defect reproduced")
	}
}

// TestBrief_ModelRefusalMergedNotGlobal is FIX-4's positive/negative pair in
// one test: a policy whose OWN model declares a subject-specific refusal
// gets it (merged with the page-level set); a DIFFERENT policy with no model
// at all, checked in the same test run, must not — proving the key travels
// with the model file, not with the handler globally.
func TestBrief_ModelRefusalMergedNotGlobal(t *testing.T) {
	dir := t.TempDir()
	restore := setBriefDataDirs(t, dir, t.TempDir())
	defer restore()
	writeFixtureFile(t, dir, "FH-TRANSIT-008.json", fixtureModelJSONWithStoreRefusal)

	r := newBriefTestRouter(rockCountyFixture())

	_, withModel := getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008?geoid=55105")
	if !containsString(withModel.RefusalKeys, "pex.refusal.no_siting_simulation") {
		t.Errorf("refusal_keys = %v, want the model's own store-siting refusal merged in", withModel.RefusalKeys)
	}
	for _, want := range pageLevelRefusalKeys {
		if !containsString(withModel.RefusalKeys, want) {
			t.Errorf("refusal_keys = %v, missing page-level key %q (merge must be additive, not a replacement)", withModel.RefusalKeys, want)
		}
	}

	// FH-OTHER-001 has no model file in this test's dir at all — a
	// grocery-store refusal must not leak onto it just because SOME policy
	// in the same request batch of the test declared one.
	_, withoutModel := getBrief(r, "/v1/policy/briefs/FH-OTHER-001?geoid=55105")
	if containsString(withoutModel.RefusalKeys, "pex.refusal.no_siting_simulation") {
		t.Errorf("refusal_keys = %v, store-siting refusal leaked onto a policy with no model", withoutModel.RefusalKeys)
	}
}

// setBriefDataDirs overrides modelsDir/evidenceDir for the duration of a
// test and returns a restore func. Kept as a named helper (rather than
// inlined defers everywhere) so every test that touches these package vars
// is doing the same thing.
func setBriefDataDirs(t *testing.T, models, evidence string) func() {
	t.Helper()
	origModels, origEvidence := modelsDir, evidenceDir
	modelsDir, evidenceDir = models, evidence
	return func() { modelsDir, evidenceDir = origModels, origEvidence }
}

// ── tier 2 / tier 3 loaders ──────────────────────────────────────────────

const fixtureModelJSON = `{
  "schema_version": "1",
  "policy_id": "FH-TRANSIT-008",
  "model_version": "9",
  "geo_levels": ["county"],
  "default_geoid": "55105",
  "title_key": "pex.model.fh_transit_008.title",
  "summary_key": "pex.model.fh_transit_008.summary",
  "params": [
    {"id": "num_stores", "label_key": "pex.param.num_stores.label",
     "help_key": "pex.param.num_stores.help", "unit": "count",
     "min": 0, "max": 12, "step": 1, "default": 1,
     "default_source_key": "pex.param.num_stores.default_source", "precision": 0}
  ],
  "outputs": [
    {"id": "total_capital_cost", "label_key": "pex.output.total_capital_cost.label",
     "unit": "usd", "precision": 0, "register": "public",
     "expr": {"op": "mul", "terms": [
       {"expr": {"op": "ref", "ref": "param:num_stores"}},
       {"expr": {"op": "const", "const": 1500000}}
     ]}}
  ],
  "refusal_keys": ["pex.refusal.no_causal_claim"],
  "authored_by": "test fixture", "reviewed_by": "test", "reviewed_date": "2026-08-13"
}`

func writeFixtureFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write fixture %s: %v", name, err)
	}
}

// TestLoadModelDescriptor_MissingFileReturnsNoModelAbsence is the ordinary
// case: the models root exists (t.TempDir() creates a real directory) but
// this policy has no file in it. Must yield MissingNoModel, not an error and
// not the "root unreachable" reason — see
// TestLoadModelDescriptor_RootUnreachableVsFileAbsent for the contrast.
func TestLoadModelDescriptor_MissingFileReturnsNoModelAbsence(t *testing.T) {
	restore := setBriefDataDirs(t, t.TempDir(), t.TempDir())
	defer restore()
	result, err := loadModelDescriptor("FH-TRANSIT-008")
	if err != nil {
		t.Fatalf("loadModelDescriptor: %v", err)
	}
	if result.Descriptor != nil {
		t.Fatalf("descriptor = %+v, want nil for a missing file", result.Descriptor)
	}
	if result.Absence == nil || result.Absence.Reason != experiment.MissingNoModel {
		t.Fatalf("absence = %+v, want reason=no_model", result.Absence)
	}
}

// TestLoadModelDescriptor_RootUnreachableVsFileAbsent is FIX-1's required
// known-bad control: an unreachable models ROOT (the production defect —
// deploy/pdi.service sets no WorkingDirectory, so a relative default
// resolves to nothing under systemd) must NOT be reported the same way as
// an ordinary per-policy absence. Before this fix, os.ReadFile returned the
// identical "file does not exist" error in both cases, so the handler could
// not tell a broken deploy from a policy that simply has no model — which is
// exactly how the defect hid: it looked, from the response, like every
// single one of the 70 positions just happened to have no model.
func TestLoadModelDescriptor_RootUnreachableVsFileAbsent(t *testing.T) {
	// Case 1: the root directory itself does not exist.
	unreachable := filepath.Join(t.TempDir(), "does-not-exist")
	restore := setBriefDataDirs(t, unreachable, t.TempDir())
	rootResult, err := loadModelDescriptor("FH-TRANSIT-008")
	restore()
	if err != nil {
		t.Fatalf("loadModelDescriptor (unreachable root): %v", err)
	}
	if rootResult.Descriptor != nil {
		t.Fatalf("descriptor = %+v, want nil when the models root is unreachable", rootResult.Descriptor)
	}
	if rootResult.Absence == nil || rootResult.Absence.Reason != experiment.MissingNotLoaded {
		t.Fatalf("absence = %+v, want reason=not_loaded for an unreachable root", rootResult.Absence)
	}
	if rootResult.Absence.Detail == "" {
		t.Error("absence.detail is empty, want an operator-actionable message naming the unreachable path")
	}

	// Case 2: the root exists; this policy's file specifically does not —
	// the ordinary state of most of the 70 positions.
	existingEmpty := t.TempDir()
	restore = setBriefDataDirs(t, existingEmpty, t.TempDir())
	fileResult, err := loadModelDescriptor("FH-TRANSIT-008")
	restore()
	if err != nil {
		t.Fatalf("loadModelDescriptor (empty root): %v", err)
	}
	if fileResult.Absence == nil || fileResult.Absence.Reason != experiment.MissingNoModel {
		t.Fatalf("absence = %+v, want reason=no_model when the root exists but this policy's file does not", fileResult.Absence)
	}

	// The known-bad control: the two reasons MUST differ. Before FIX-1 both
	// branches produced MissingNoModel via the same os.IsNotExist(err)
	// check, and this assertion is what would have caught it.
	if rootResult.Absence.Reason == fileResult.Absence.Reason {
		t.Fatalf("root-unreachable and no-model-for-this-policy both produced reason=%q — "+
			"the exact FIX-1 defect: an operator cannot tell a broken deploy from an ordinary absent model",
			rootResult.Absence.Reason)
	}
}

// TestBrief_ModelRootUnreachableSurfacesDistinctAbsence is the end-to-end
// half of FIX-1: hitting the real HTTP handler with an unreachable models
// root must still return 200 with tier 1 intact, model=nil, and a
// tier=experiment absence whose reason is not_loaded — never a 500, and
// never silently indistinguishable from "this policy has no model" (S3: hide
// the apparatus, never hide the absence — and never hide WHICH absence).
func TestBrief_ModelRootUnreachableSurfacesDistinctAbsence(t *testing.T) {
	unreachable := filepath.Join(t.TempDir(), "does-not-exist")
	restore := setBriefDataDirs(t, unreachable, t.TempDir())
	defer restore()

	r := newBriefTestRouter(rockCountyFixture())
	w, b := getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008?geoid=55105")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (an unreachable tier-2 root still yields a valid brief); body: %s", w.Code, w.Body.String())
	}
	if b.Model != nil {
		t.Fatalf("model = %+v, want nil", b.Model)
	}
	if b.Reach == nil {
		t.Fatal("reach = nil, want tier 1 unaffected by a broken tier-2 root")
	}
	var found *experiment.Absence
	for i := range b.Absences {
		if b.Absences[i].Tier == experiment.TierExperiment {
			found = &b.Absences[i]
		}
	}
	if found == nil {
		t.Fatal("no tier=experiment absence, want one explaining the unreachable models root")
	}
	if found.Reason != experiment.MissingNotLoaded {
		t.Fatalf("absence.reason = %q, want not_loaded (distinct from no_model) for an unreachable root", found.Reason)
	}
}

// TestLoadModelDescriptor_StripsExpr is the contract §3 known-bad control:
// the loaded descriptor's outputs must carry NO arithmetic — the real
// fixture's total_capital_cost output has a two-term mul expression, and
// this test fails loudly if that expression leaks into the descriptor.
func TestLoadModelDescriptor_StripsExpr(t *testing.T) {
	dir := t.TempDir()
	restore := setBriefDataDirs(t, dir, t.TempDir())
	defer restore()
	writeFixtureFile(t, dir, "FH-TRANSIT-008.json", fixtureModelJSON)

	result, err := loadModelDescriptor("FH-TRANSIT-008")
	if err != nil {
		t.Fatalf("loadModelDescriptor: %v", err)
	}
	m := result.Descriptor
	if m == nil {
		t.Fatalf("loadModelDescriptor.Descriptor = nil, want the fixture descriptor")
	}
	if m.ModelVersion != "9" || len(m.Params) != 1 || len(m.Outputs) != 1 {
		t.Fatalf("descriptor = %+v, want model_version=9, 1 param, 1 output", m)
	}
	if result.DefaultGEOID != "55105" {
		t.Errorf("DefaultGEOID = %q, want 55105 (the fixture's default_geoid)", result.DefaultGEOID)
	}
	out := m.Outputs[0]
	if out.ID != "total_capital_cost" || out.Unit != experiment.UnitUSD {
		t.Errorf("output = %+v, want id/unit preserved", out)
	}
	if out.Expr.Op != "" || out.Expr.Ref != "" || out.Expr.Const != nil || len(out.Expr.Terms) != 0 {
		t.Errorf("output.Expr = %+v, want fully zeroed — the arithmetic tree must not reach the wire", out.Expr)
	}
}

func TestLoadModelDescriptor_PolicyIDMismatchErrors(t *testing.T) {
	dir := t.TempDir()
	restore := setBriefDataDirs(t, dir, t.TempDir())
	defer restore()
	writeFixtureFile(t, dir, "FH-OTHER-001.json", fixtureModelJSON) // file says FH-TRANSIT-008 inside

	if _, err := loadModelDescriptor("FH-OTHER-001"); err == nil {
		t.Fatal("loadModelDescriptor did not error on a policy_id mismatch between filename and file content")
	}
}

const fixtureEvidenceHeader = "policy_id,evidence_id,jurisdiction,geo_scale,period_start,period_end,intervention,outcome,direction,finding,effect_reported,study_design,evidence_strength,source_title,source_org,source_url,source_year,accessed_date,citation,relates_to_claim,claim_check,transfer_note,reviewed_by,reviewed_date\n"

// TestLoadEvidenceItems_SkipsRowMissingRequiredField is the contract §5
// known-bad control: "the loader refuses a row with an empty source_url,
// citation, or transfer_note" — one complete row and one row missing
// transfer_note; only the complete one should survive.
func TestLoadEvidenceItems_SkipsRowMissingRequiredField(t *testing.T) {
	dir := t.TempDir()
	restore := setBriefDataDirs(t, t.TempDir(), dir)
	defer restore()

	complete := "FH-TRANSIT-008,ev1,Somewhere,city,2020,2021,did a thing,measured a thing,increase,it went up,,case_study,limited,Title,Org,https://example.com/a,2021,2026-08-13,Full citation text,,not_applicable,Transfer note here,tester,2026-08-13\n"
	missingTransferNote := "FH-TRANSIT-008,ev2,Elsewhere,city,2020,2021,did a thing,measured a thing,increase,it went up,,case_study,limited,Title,Org,https://example.com/b,2021,2026-08-13,Full citation text,,not_applicable,,tester,2026-08-13\n"
	writeFixtureFile(t, dir, "FH-TRANSIT-008.csv", fixtureEvidenceHeader+complete+missingTransferNote)

	result, err := loadEvidenceItems("FH-TRANSIT-008")
	if err != nil {
		t.Fatalf("loadEvidenceItems: %v", err)
	}
	items := result.Items
	if len(items) != 1 || items[0].EvidenceID != "ev1" {
		t.Fatalf("items = %+v, want exactly ev1 (ev2 is missing transfer_note)", items)
	}
	if result.Absence != nil {
		t.Errorf("absence = %+v, want nil (one valid row survived the filter)", result.Absence)
	}
}

// TestBrief_LoadsRealModelFileWhenPresent points modelsDir at the actual
// repo data directory. Skips (does not fail) when the file is not present
// in this checkout — this track does not own that file and a sibling track
// landing or moving it should not block this track's build gate.
func TestBrief_LoadsRealModelFileWhenPresent(t *testing.T) {
	realDir := filepath.Join("..", "..", "data", "policies", "models")
	if _, err := os.Stat(filepath.Join(realDir, "FH-TRANSIT-008.json")); err != nil {
		t.Skipf("real model fixture not present: %v", err)
	}
	restore := setBriefDataDirs(t, realDir, t.TempDir())
	defer restore()

	r := newBriefTestRouter(rockCountyFixture())
	w, b := getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008?geoid=55105")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", w.Code, w.Body.String())
	}
	if b.Model == nil {
		t.Fatal("model = nil, want the real committed model file loaded")
	}
	if len(b.Model.Params) == 0 || len(b.Model.Outputs) == 0 {
		t.Errorf("model = %+v, want non-empty params and outputs", b.Model)
	}
	for _, out := range b.Model.Outputs {
		if out.Expr.Op != "" {
			t.Errorf("output %s.Expr.Op = %q, want empty — the real model's arithmetic leaked onto the wire", out.ID, out.Expr.Op)
		}
	}
	if hasAbsence(b.Absences, experiment.TierExperiment) {
		t.Errorf("absences = %+v, want no tier=experiment entry once a model loaded", b.Absences)
	}
}

// TestBrief_LoadsRealEvidenceFileWhenPresent mirrors the model test for
// tier 3.
func TestBrief_LoadsRealEvidenceFileWhenPresent(t *testing.T) {
	realDir := filepath.Join("..", "..", "data", "policies", "evidence")
	if _, err := os.Stat(filepath.Join(realDir, "FH-TRANSIT-008.csv")); err != nil {
		t.Skipf("real evidence fixture not present: %v", err)
	}
	restore := setBriefDataDirs(t, t.TempDir(), realDir)
	defer restore()

	r := newBriefTestRouter(rockCountyFixture())
	_, b := getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008?geoid=55105")
	if len(b.Evidence) == 0 {
		t.Fatal("evidence = [], want rows loaded from the real committed CSV")
	}
	for _, e := range b.Evidence {
		if e.SourceURL == "" || e.Citation == "" || e.TransferNote == "" {
			t.Errorf("evidence item %q missing a required field despite the per-row filter: %+v", e.EvidenceID, e)
		}
	}
	if hasAbsence(b.Absences, experiment.TierEvidence) {
		t.Errorf("absences = %+v, want no tier=evidence entry once evidence rows loaded", b.Absences)
	}
}

// TestBrief_PilotReachTotals is the known-bad-control-carrying core test:
// verifies the designated_share/designated_population/tract-count figures
// against hand-computed expected values, AND that every total's claim_class
// resolves to "measurement" (no assumed input anywhere in tier 1).
func TestBrief_PilotReachTotals(t *testing.T) {
	r := newBriefTestRouter(rockCountyFixture())
	w, b := getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008?geoid=55105")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	if b.Reach == nil {
		t.Fatalf("reach = nil, want populated for the pilot policy")
	}
	if b.Reach.Rule.RuleID != "usda_fara_lila" || b.Reach.Rule.AuthorityKey == "" {
		t.Fatalf("rule = %+v, want usda_fara_lila with a non-empty authority key", b.Reach.Rule)
	}
	if b.Reach.MapLayerKey != "pex.map.designated_share.legend" {
		t.Fatalf("map_layer_key = %q, want the S6 wording key", b.Reach.MapLayerKey)
	}

	byID := map[string]experiment.Value{}
	for _, v := range b.Reach.Totals {
		byID[v.ID] = v
	}

	share, ok := byID["reach.designated_share"]
	if !ok || share.Num == nil {
		t.Fatalf("missing reach.designated_share in totals: %+v", byID)
	}
	if *share.Num != 0.261 {
		t.Errorf("designated_share = %v, want 0.261", *share.Num)
	}
	if share.Unit != experiment.UnitShare {
		t.Errorf("designated_share unit = %q, want share (0-1, never percent_points)", share.Unit)
	}
	if len(share.AssumedInputs) != 0 {
		t.Errorf("designated_share.assumed_inputs = %v, want empty — tier 1 is measurement only", share.AssumedInputs)
	}

	pop, ok := byID["reach.designated_population"]
	if !ok || pop.Num == nil {
		t.Fatalf("missing reach.designated_population in totals: %+v", byID)
	}
	wantPop := 0.261 * 164350.0
	if *pop.Num != wantPop {
		t.Errorf("designated_population = %v, want %v (share * population, unrounded)", *pop.Num, wantPop)
	}
	if pop.Kind != experiment.KindDerived {
		t.Errorf("designated_population.kind = %q, want derived", pop.Kind)
	}

	tractCount, ok := byID["reach.total_tract_count"]
	if !ok || tractCount.Num == nil || *tractCount.Num != 2 {
		t.Fatalf("total_tract_count = %+v, want 2 (fixture has 2 Rock County tracts)", tractCount)
	}
	desigCount, ok := byID["reach.designated_tract_count"]
	if !ok || desigCount.Num == nil || *desigCount.Num != 12 {
		t.Fatalf("designated_tract_count = %+v, want 12 (pinned pilot list)", desigCount)
	}

	// Known-bad control, in the same test: EVERY total in tier 1 must
	// resolve to "measurement" via Value.ClaimClass() — no total here has
	// any assumed input, by construction. If this ever fails, something
	// started tagging a reach figure with an assumed_inputs entry, which
	// would silently turn a measurement into a mislabeled projection.
	for _, v := range b.Reach.Totals {
		if v.ClaimClass() != experiment.ClassMeasurement {
			t.Errorf("total %q claim_class = %q, want measurement (assumed_inputs=%v)",
				v.ID, v.ClaimClass(), v.AssumedInputs)
		}
	}
}

// TestBrief_ReachAreasCarryShareNotFlag is the S6 known-bad-control: the
// designated tract's share must come through as 1.0 (a share), and the
// non-designated tract's as 0.0 — never collapsed into a boolean field, and
// never omitted for the tract that is genuinely 0.
func TestBrief_ReachAreasCarryShareNotFlag(t *testing.T) {
	r := newBriefTestRouter(rockCountyFixture())
	_, b := getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008?geoid=55105")
	if b.Reach == nil {
		t.Fatalf("reach = nil")
	}
	if len(b.Reach.DesignatedIDs) != 12 {
		t.Fatalf("designated_ids has %d entries, want 12 (pinned pilot list)", len(b.Reach.DesignatedIDs))
	}
	byGEOID := map[string]experiment.Area{}
	for _, a := range b.Reach.Areas {
		byGEOID[a.GEOID] = a
	}
	designated, ok := byGEOID["55105000100"]
	if !ok || designated.DesignatedShare == nil || *designated.DesignatedShare != 1.0 {
		t.Fatalf("designated tract area = %+v, want designated_share=1.0", designated)
	}
	other, ok := byGEOID["55105999900"]
	if !ok || other.DesignatedShare == nil || *other.DesignatedShare != 0.0 {
		t.Fatalf("non-designated tract area = %+v, want designated_share=0.0 (present, not omitted)", other)
	}
	for _, a := range b.Reach.Areas {
		for _, v := range a.Values {
			if v.Register != experiment.RegAnalyst {
				t.Errorf("area %s value %s register = %q, want analyst (contract §6)", a.GEOID, v.ID, v.Register)
			}
		}
	}
}

// TestBrief_MissingIndicatorPropagatesAsMissingNotZero is the D5 /
// sentinel-zero known-bad control: a county with no loaded USDA rows must
// come back with value=nil and a stated missing_reason, never a silent 0.
func TestBrief_MissingIndicatorPropagatesAsMissingNotZero(t *testing.T) {
	s := rockCountyFixture()
	s.geographies["55025"] = geo.Geography{GEOID: "55025", Level: geo.County, Name: "Dane County", StateFIPS: "55", CountyFIPS: "025"}
	// Deliberately no indicator rows for 55025 — the known-bad control.
	r := newBriefTestRouter(s)
	w, b := getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008?geoid=55025")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (a brief with absent data is still a valid brief); body: %s", w.Code, w.Body.String())
	}
	var share, pop *experiment.Value
	for i := range b.Reach.Totals {
		switch b.Reach.Totals[i].ID {
		case "reach.designated_share":
			share = &b.Reach.Totals[i]
		case "reach.designated_population":
			pop = &b.Reach.Totals[i]
		}
	}
	if share == nil || share.Num != nil || share.Missing != experiment.MissingNotLoaded {
		t.Fatalf("designated_share = %+v, want value=nil missing_reason=not_loaded", share)
	}
	if pop == nil || pop.Num != nil || pop.Missing != experiment.MissingUpstream {
		t.Fatalf("designated_population = %+v, want value=nil missing_reason=upstream_missing (never a substituted zero)", pop)
	}
}

// TestBrief_ClaimClassRoundTripsThroughJSON exercises the wire-honesty
// invariant this endpoint depends on end to end: marshal, unmarshal, and
// confirm Value.UnmarshalJSON did not reject its own output — i.e. every
// Value this handler emits is internally consistent per pkg/experiment's own
// check, not just per this package's assumptions about it.
func TestBrief_ClaimClassRoundTripsThroughJSON(t *testing.T) {
	r := newBriefTestRouter(rockCountyFixture())
	w, _ := getBrief(r, "/v1/policy/briefs/FH-TRANSIT-008?geoid=55105")
	var roundTrip experiment.Brief
	if err := json.Unmarshal(w.Body.Bytes(), &roundTrip); err != nil {
		t.Fatalf("round-trip unmarshal rejected the handler's own output: %v\nbody: %s", err, w.Body.String())
	}
}

func hasAbsence(absences []experiment.Absence, tier experiment.Tier) bool {
	for _, a := range absences {
		if a.Tier == tier {
			return true
		}
	}
	return false
}
