package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/experiment"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// ── test doubles ─────────────────────────────────────────────────────────
//
// These are independent of mockStore in handlers_test.go on purpose: this
// track owns simulate.go and simulate_test.go only, and GeoPolicyLookup is
// deliberately a two-method interface so a test double for it does not need
// to implement store.Store's full surface. None of these touch a database.

type fakeLookup struct {
	geos     map[string]*geo.Geography
	policies map[string]*store.PolicyRecord
	geoErr   error // returned verbatim from GetGeography when set
}

func newFakeLookup() *fakeLookup {
	return &fakeLookup{geos: map[string]*geo.Geography{}, policies: map[string]*store.PolicyRecord{}}
}

func (f *fakeLookup) GetGeography(_ context.Context, geoid string) (*geo.Geography, error) {
	if f.geoErr != nil {
		return nil, f.geoErr
	}
	g, ok := f.geos[geoid]
	if !ok {
		return nil, errors.New("geography not found")
	}
	return g, nil
}

func (f *fakeLookup) GetPolicy(_ context.Context, id string) (*store.PolicyRecord, error) {
	p, ok := f.policies[id]
	if !ok {
		return nil, errors.New("policy not found") // isNotFound() matches on "not found"
	}
	return p, nil
}

type fakeModelLoader struct {
	models map[string]*experiment.PolicyModel
	err    error
}

func (f *fakeModelLoader) LoadPolicyModel(_ context.Context, policyID string) (*experiment.PolicyModel, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.models[policyID], nil // nil, nil when absent — the "no_model" contract case
}

// fakeEvaluator records the exact arguments Handle passed it, so tests can
// assert on canonicalized param bindings (snapped, defaulted, in
// declaration order) without needing a real pkg/experiment evaluator, which
// does not exist yet — this track is told that is expected.
type fakeEvaluator struct {
	called   bool
	gotModel *experiment.PolicyModel
	gotGEOID string
	gotLevel string
	gotName  string
	gotBind  []experiment.ParamBinding

	result *experiment.EvalResult
	err    error
}

func (f *fakeEvaluator) Evaluate(_ context.Context, model *experiment.PolicyModel, geoid, level, geoName string, bindings []experiment.ParamBinding) (*experiment.EvalResult, error) {
	f.called = true
	f.gotModel = model
	f.gotGEOID = geoid
	f.gotLevel = level
	f.gotName = geoName
	f.gotBind = bindings
	if f.err != nil {
		return nil, f.err
	}
	if f.result != nil {
		return f.result, nil
	}
	return &experiment.EvalResult{
		PolicyID:     model.PolicyID,
		ModelVersion: model.ModelVersion,
		GEOID:        geoid,
		Level:        level,
		GeoName:      geoName,
		Params:       bindings,
		Outputs: []experiment.Value{{
			ID:             "out1",
			LabelKey:       "pex.output.out1.label",
			Num:            floatPtr(42),
			Unit:           experiment.UnitUSD,
			Precision:      0,
			Kind:           experiment.KindDerived,
			Tier:           experiment.TierExperiment,
			Register:       experiment.RegPublic,
			AssumedInputs:  []string{"weekly_spend"},
			MeasuredInputs: []string{"eligible_households"},
		}},
		Absences:    []experiment.Absence{},
		RefusalKeys: []string{"pex.refusal.no_causal_claim"},
		Sources:     []experiment.SourceRef{},
	}, nil
}

func floatPtr(v float64) *float64 { return &v }

// ── fixtures ─────────────────────────────────────────────────────────────

func pilotModel() *experiment.PolicyModel {
	return &experiment.PolicyModel{
		SchemaVersion: experiment.SchemaVersion,
		PolicyID:      "FH-TRANSIT-008",
		ModelVersion:  "1",
		GeoLevels:     []string{"county", "state"},
		DefaultGEOID:  "55105",
		TitleKey:      "pex.model.FH-TRANSIT-008.title",
		SummaryKey:    "pex.model.FH-TRANSIT-008.summary",
		Params: []experiment.Param{
			{
				ID: "weekly_spend", LabelKey: "pex.param.weekly_spend.label",
				HelpKey: "pex.param.weekly_spend.help", Unit: experiment.UnitUSD,
				Min: 0, Max: 200, Step: 5, Default: 60,
				DefaultSourceKey: "pex.param.weekly_spend.default_source", Precision: 0,
			},
		},
		Outputs: []experiment.Output{
			{ID: "out1", LabelKey: "pex.output.out1.label", Unit: experiment.UnitUSD, Precision: 0, Register: experiment.RegPublic},
		},
		RefusalKeys:  []string{"pex.refusal.no_causal_claim", "pex.refusal.no_time_series"},
		AuthoredBy:   "test",
		ReviewedBy:   "test",
		ReviewedDate: "2026-08-13",
	}
}

func rockCounty() *geo.Geography {
	return &geo.Geography{GEOID: "55105", Level: geo.County, Name: "Rock County", Population: 41768}
}

// newSimulateRouter mounts Handle at POST /v1/policy/simulate on a fresh gin
// engine. Route registration here is a test-only concern — this track is
// explicitly not responsible for registering the route in the real plugin
// (plugin.go is owned by the integrator).
func newSimulateRouter(h *SimulateHandler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/v1/policy/simulate", h.Handle)
	return r
}

func doSimulate(r *gin.Engine, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/policy/simulate", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// setup returns a ready-to-use handler + doubles wired with the pilot policy
// (FH-TRANSIT-008 / Rock County) so most tests only need to override one
// thing.
func setup() (*SimulateHandler, *fakeLookup, *fakeModelLoader, *fakeEvaluator) {
	lookup := newFakeLookup()
	lookup.policies["FH-TRANSIT-008"] = &store.PolicyRecord{ID: "FH-TRANSIT-008", Title: "Public groceries"}
	lookup.geos["55105"] = rockCounty()

	models := &fakeModelLoader{models: map[string]*experiment.PolicyModel{"FH-TRANSIT-008": pilotModel()}}
	eval := &fakeEvaluator{}
	h := NewSimulateHandler(lookup, models, eval)
	return h, lookup, models, eval
}

// ── tests ────────────────────────────────────────────────────────────────

func TestHandle_Success(t *testing.T) {
	h, _, _, eval := setup()
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105","params":{"weekly_spend":60}}`)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if !eval.called {
		t.Fatal("evaluator was never invoked")
	}
	if w.Header().Get("ETag") == "" {
		t.Error("ETag header not set on a successful simulation")
	}

	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	// claim_class is emitted by EvalResult.MarshalJSON, not by this handler
	// — this assertion is here to prove the handler returns the type
	// unmodified rather than re-marshalling it through a shape that would
	// drop the computed field.
	if got["claim_class"] != "projection" {
		t.Errorf("claim_class = %v, want %q (out1 has an assumed input)", got["claim_class"], "projection")
	}
}

func TestHandle_UnknownPolicy(t *testing.T) {
	h, _, _, eval := setup()
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-NOPE-999","geoid":"55105"}`)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404, body = %s", w.Code, w.Body.String())
	}
	if eval.called {
		t.Error("evaluator invoked for an unknown policy_id")
	}
}

func TestHandle_MissingPolicyID(t *testing.T) {
	h, _, _, _ := setup()
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"geoid":"55105"}`)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
	}
}

func TestHandle_NoModel(t *testing.T) {
	lookup := newFakeLookup()
	lookup.policies["FH-EDU-001"] = &store.PolicyRecord{ID: "FH-EDU-001"}
	lookup.geos["55"] = &geo.Geography{GEOID: "55", Level: geo.State, Name: "Wisconsin"}
	models := &fakeModelLoader{models: map[string]*experiment.PolicyModel{}} // no model for FH-EDU-001
	eval := &fakeEvaluator{}
	h := NewSimulateHandler(lookup, models, eval)
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-EDU-001","geoid":"55"}`)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422, body = %s", w.Code, w.Body.String())
	}
	var got SimulateErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	if got.MissingReason != experiment.MissingNoModel {
		t.Errorf("missing_reason = %q, want %q", got.MissingReason, experiment.MissingNoModel)
	}
	if eval.called {
		t.Error("evaluator invoked for a policy with no model")
	}
}

func TestHandle_ModelLoadFailure(t *testing.T) {
	h, _, models, eval := setup()
	models.err = errors.New("data/policies/models/FH-TRANSIT-008.json: invalid character")
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105"}`)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500, body = %s", w.Code, w.Body.String())
	}
	if eval.called {
		t.Error("evaluator invoked despite a model load failure")
	}
}

func TestHandle_UnsupportedGeoLevel(t *testing.T) {
	h, _, _, eval := setup()
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105","level":"tract"}`)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422, body = %s", w.Code, w.Body.String())
	}
	var got SimulateErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	if got.MissingReason != experiment.MissingGeoLevelUnsupported {
		t.Errorf("missing_reason = %q, want %q", got.MissingReason, experiment.MissingGeoLevelUnsupported)
	}
	if eval.called {
		t.Error("evaluator invoked for an unsupported geo level")
	}
}

// TestHandle_GeoidLevelMismatch_DefaultedLevel_Rejected is FIX-3's core
// known-bad control, reproducing the exact defect: geoid "55" is
// structurally a STATE GEOID (2 digits). pilotModel's first declared level
// is "county" (GeoLevels[0]), so an omitted `level` defaults to "county" per
// contract §4 — but evaluating a state GEOID "at" county level is the bug:
// before FIX-3, nothing compared the GEOID's own structural level against
// the level about to be evaluated, so this combination silently returned
// {"level":"county"} for a GEOID that was never a county.
func TestHandle_GeoidLevelMismatch_DefaultedLevel_Rejected(t *testing.T) {
	h, lookup, _, eval := setup()
	lookup.geos["55"] = &geo.Geography{GEOID: "55", Level: geo.State, Name: "Wisconsin"}
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55"}`)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422, body = %s", w.Code, w.Body.String())
	}
	var got SimulateErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	if got.MissingReason != experiment.MissingGeoLevelUnsupported {
		t.Errorf("missing_reason = %q, want %q", got.MissingReason, experiment.MissingGeoLevelUnsupported)
	}
	if eval.called {
		t.Fatal("evaluator invoked despite a geoid/level mismatch — this is the exact silent-mislabel defect FIX-3 closes")
	}
}

// TestHandle_GeoidLevelMatch_ExplicitState_Accepted is the toggle-on half of
// the control above: the SAME geoid ("55"), with level explicitly requested
// as "state" (which matches both the geoid's own structure and the model's
// declared levels), must succeed and evaluate at "state" — proving the new
// check is a genuine mismatch check, not a blanket rejection of state
// geoids.
func TestHandle_GeoidLevelMatch_ExplicitState_Accepted(t *testing.T) {
	h, lookup, _, eval := setup()
	lookup.geos["55"] = &geo.Geography{GEOID: "55", Level: geo.State, Name: "Wisconsin"}
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55","level":"state"}`)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body = %s", w.Code, w.Body.String())
	}
	if !eval.called || eval.gotLevel != "state" {
		t.Fatalf("evaluator called=%v level=%q, want called with level=state", eval.called, eval.gotLevel)
	}
}

// TestHandle_GeoidLevelMismatch_ExplicitLevel_Rejected covers the other
// half of FIX-3: an EXPLICITLY requested level that the model DOES declare
// (so the pre-existing "model does not declare this level" check would let
// it through) but that does not match what the submitted geoid actually is.
// A county geoid requested at "tract" is nonsensical regardless of whether
// the model happens to support tract-level evaluation for some other geoid.
func TestHandle_GeoidLevelMismatch_ExplicitLevel_Rejected(t *testing.T) {
	lookup := newFakeLookup()
	lookup.policies["FH-TRANSIT-008"] = &store.PolicyRecord{ID: "FH-TRANSIT-008", Title: "Public groceries"}
	lookup.geos["55105"] = rockCounty()

	m := pilotModel()
	m.GeoLevels = []string{"county", "tract"} // model supports tract in general
	models := &fakeModelLoader{models: map[string]*experiment.PolicyModel{"FH-TRANSIT-008": m}}
	eval := &fakeEvaluator{}
	h := NewSimulateHandler(lookup, models, eval)
	r := newSimulateRouter(h)

	// geoid "55105" is a 5-digit COUNTY geoid; level=tract is explicitly
	// requested and the model does declare "tract" as a valid level in
	// general — so only the NEW geoid-vs-level check (not the pre-existing
	// model.GeoLevels containment check) can catch this.
	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105","level":"tract"}`)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422, body = %s", w.Code, w.Body.String())
	}
	var got SimulateErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	if got.MissingReason != experiment.MissingGeoLevelUnsupported {
		t.Errorf("missing_reason = %q, want %q", got.MissingReason, experiment.MissingGeoLevelUnsupported)
	}
	if eval.called {
		t.Fatal("evaluator invoked for a county geoid explicitly requested at tract level")
	}
}

func TestHandle_LevelDefaultsToModelFirst(t *testing.T) {
	h, _, _, eval := setup()
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105"}`)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if eval.gotLevel != "county" { // pilotModel().GeoLevels[0]
		t.Errorf("level passed to evaluator = %q, want %q", eval.gotLevel, "county")
	}
}

func TestHandle_ParamOutOfRange_RejectedNotClamped(t *testing.T) {
	h, _, _, eval := setup()
	r := newSimulateRouter(h)

	// Max is 200; this is 500 over it.
	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105","params":{"weekly_spend":700}}`)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
	}
	if eval.called {
		t.Fatal("evaluator was invoked for an out-of-range param — a clamp would let this happen silently")
	}
}

func TestHandle_ParamBelowMin_Rejected(t *testing.T) {
	h, _, _, eval := setup()
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105","params":{"weekly_spend":-5}}`)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
	}
	if eval.called {
		t.Fatal("evaluator was invoked for a below-min param")
	}
}

func TestHandle_UnknownParam_Rejected(t *testing.T) {
	h, _, _, eval := setup()
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105","params":{"not_a_real_param":1}}`)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
	}
	if eval.called {
		t.Fatal("evaluator was invoked with an unknown param")
	}
}

func TestHandle_NonNumericParam_Rejected(t *testing.T) {
	h, _, _, eval := setup()
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105","params":{"weekly_spend":"sixty"}}`)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
	}
	if eval.called {
		t.Fatal("evaluator was invoked with a non-numeric param")
	}
}

func TestHandle_OmittedParamUsesDefault(t *testing.T) {
	h, _, _, eval := setup()
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105"}`)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if len(eval.gotBind) != 1 {
		t.Fatalf("got %d bindings, want 1", len(eval.gotBind))
	}
	b := eval.gotBind[0]
	if b.Source != experiment.SrcDefault {
		t.Errorf("source = %q, want %q", b.Source, experiment.SrcDefault)
	}
	if b.Num != 60 {
		t.Errorf("value = %v, want the model's authored default 60", b.Num)
	}
}

func TestHandle_UserParamSnappedToStep(t *testing.T) {
	h, _, _, eval := setup()
	r := newSimulateRouter(h)

	// step=5, min=0 -> 62 snaps to 60.
	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105","params":{"weekly_spend":62}}`)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	b := eval.gotBind[0]
	if b.Source != experiment.SrcUser {
		t.Errorf("source = %q, want %q", b.Source, experiment.SrcUser)
	}
	if b.Num != 60 {
		t.Errorf("snapped value = %v, want 60 (62 snapped to step=5 grid)", b.Num)
	}
}

func TestHandle_MissingGEOID_Rejected(t *testing.T) {
	h, _, _, eval := setup()
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008"}`)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
	}
	if eval.called {
		t.Fatal("evaluator invoked with no geoid")
	}
}

func TestHandle_MalformedGEOID_Rejected(t *testing.T) {
	h, _, _, eval := setup()
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"not-a-geoid"}`)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
	}
	if eval.called {
		t.Fatal("evaluator invoked with a malformed geoid")
	}
}

func TestHandle_TwelveDigitGEOID_Rejected(t *testing.T) {
	// This endpoint's geoid grammar (contract §3/§4) is state/county/tract
	// only — narrower than the project-wide block-group-permitting regex
	// in middleware.go.
	h, _, _, eval := setup()
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"551050001001"}`)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
	}
	if eval.called {
		t.Fatal("evaluator invoked with a 12-digit (block-group) geoid")
	}
}

func TestHandle_UnknownGEOID_Rejected(t *testing.T) {
	h, _, _, eval := setup()
	r := newSimulateRouter(h)

	// Well-formed (5 digits) but not in the lookup's geographies.
	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55025"}`)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422, body = %s", w.Code, w.Body.String())
	}
	if eval.called {
		t.Fatal("evaluator invoked with an unresolvable geoid")
	}
}

func TestHandle_EvaluatorError(t *testing.T) {
	h, _, _, eval := setup()
	eval.err = errors.New("division_by_zero")
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105"}`)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500, body = %s", w.Code, w.Body.String())
	}
}

func TestHandle_InvalidJSONBody(t *testing.T) {
	h, _, _, _ := setup()
	r := newSimulateRouter(h)

	w := doSimulate(r, `{not json`)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
	}
}

// TestHandle_Determinism exercises contract §4's determinism requirement at
// the boundary this handler owns: the same request produces the same ETag,
// and the ETag is exactly the canonical (policy_id, model_version, geoid,
// level, snapped params) tuple — not, e.g., a timestamp or a random
// component that would make two identical requests cache-incoherent.
func TestHandle_Determinism(t *testing.T) {
	h, _, _, _ := setup()
	r := newSimulateRouter(h)

	body := `{"policy_id":"FH-TRANSIT-008","geoid":"55105","params":{"weekly_spend":60}}`
	w1 := doSimulate(r, body)
	w2 := doSimulate(r, body)

	if w1.Code != http.StatusOK || w2.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d, %d", w1.Code, w2.Code)
	}
	e1, e2 := w1.Header().Get("ETag"), w2.Header().Get("ETag")
	if e1 == "" || e1 != e2 {
		t.Errorf("ETag not stable across identical requests: %q vs %q", e1, e2)
	}
}

func TestHandle_ETagChangesWithParams(t *testing.T) {
	h, _, _, _ := setup()
	r := newSimulateRouter(h)

	w1 := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105","params":{"weekly_spend":60}}`)
	w2 := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105","params":{"weekly_spend":100}}`)

	e1, e2 := w1.Header().Get("ETag"), w2.Header().Get("ETag")
	if e1 == "" || e2 == "" {
		t.Fatalf("missing ETag: %q, %q", e1, e2)
	}
	if e1 == e2 {
		t.Error("ETag identical for two different assumption sets")
	}
}

func TestHandle_NoLanguageModelCall(t *testing.T) {
	// There is no LLM client to stub in this package, so this test asserts
	// the property indirectly: an evaluator that errors is the only way
	// this handler could ever produce non-deterministic output, and here
	// it deterministically 500s — nothing in Handle retries, rephrases, or
	// falls back to generated prose. Combined with the package doc's grep
	// claim ("nothing in this file imports pkg/grounding or any LLM
	// client"), this closes the loop a reader can check by hand.
	h, _, _, eval := setup()
	eval.err = fmt.Errorf("boom")
	r := newSimulateRouter(h)

	w := doSimulate(r, `{"policy_id":"FH-TRANSIT-008","geoid":"55105"}`)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

func TestSnapToStep(t *testing.T) {
	cases := []struct {
		v, min, step, want float64
	}{
		{62, 0, 5, 60},
		{63, 0, 5, 65},
		{60, 0, 5, 60},
		{0, 0, 5, 0},
		{7.4, 5, 2.5, 7.5},
		{5, 0, 0, 5}, // step<=0 leaves v unsnapped
	}
	for _, c := range cases {
		got := snapToStep(c.v, c.min, c.step)
		if got != c.want {
			t.Errorf("snapToStep(%v, %v, %v) = %v, want %v", c.v, c.min, c.step, got, c.want)
		}
	}
}

func TestResolveParamBindings_DeclarationOrder(t *testing.T) {
	model := &experiment.PolicyModel{
		Params: []experiment.Param{
			{ID: "z_param", Min: 0, Max: 10, Step: 1, Default: 1},
			{ID: "a_param", Min: 0, Max: 10, Step: 1, Default: 2},
		},
	}
	bindings, err := resolveParamBindings(model, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(bindings) != 2 || bindings[0].ID != "z_param" || bindings[1].ID != "a_param" {
		t.Errorf("bindings not in model declaration order: %+v", bindings)
	}
}
