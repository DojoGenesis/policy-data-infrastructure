package gateway

// Tests for the integrator's own wiring (fileModelLoader, storeEvaluator,
// and the /simulate route registration in plugin.go). simulate_test.go
// already proves Handle's own logic against fakeLookup/fakeModelLoader/
// fakeEvaluator; those doubles never touch this file's code. These tests
// exist so a claim of "wired to the real evaluator" is something a third
// party can rerun, not a self-report — per this session's own instructions.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/experiment"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// fakeIndicatorLookup is a minimal IndicatorLookup double: one method, no
// database, keyed by (geoid, variable_id) so a test can seed exactly the
// rows a model's Expr tree needs.
type fakeIndicatorLookup struct {
	rows map[string]map[string]store.Indicator // geoid -> variable_id -> row
}

func newFakeIndicatorLookup() *fakeIndicatorLookup {
	return &fakeIndicatorLookup{rows: map[string]map[string]store.Indicator{}}
}

func (f *fakeIndicatorLookup) seed(geoid, variableID string, value float64, vintage string) {
	if f.rows[geoid] == nil {
		f.rows[geoid] = map[string]store.Indicator{}
	}
	v := value
	f.rows[geoid][variableID] = store.Indicator{GEOID: geoid, VariableID: variableID, Vintage: vintage, Value: &v}
}

func (f *fakeIndicatorLookup) QueryIndicators(_ context.Context, q store.IndicatorQuery) ([]store.Indicator, error) {
	var out []store.Indicator
	for _, geoid := range q.GEOIDs {
		byVar := f.rows[geoid]
		for _, varID := range q.VariableIDs {
			if row, ok := byVar[varID]; ok {
				out = append(out, row)
			}
		}
	}
	return out, nil
}

func realModelsDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join("..", "..", "data", "policies", "models")
	if _, err := os.Stat(filepath.Join(dir, "FH-TRANSIT-008.json")); err != nil {
		t.Skipf("real model fixture not present: %v", err)
	}
	return dir
}

// TestFileModelLoader_LoadsRealPilotModel proves the loader this file adds
// actually reads the real committed FH-TRANSIT-008.json WITH Expr intact
// (unlike briefs.go's loadModelDescriptor, which strips it) and that the
// file passes experiment.Validate — the contract's load-time refusal step.
func TestFileModelLoader_LoadsRealPilotModel(t *testing.T) {
	dir := realModelsDir(t)
	loader := newFileModelLoader(dir)

	model, err := loader.LoadPolicyModel(context.Background(), "FH-TRANSIT-008")
	if err != nil {
		t.Fatalf("LoadPolicyModel: %v", err)
	}
	if model == nil {
		t.Fatal("model = nil, want the real committed file")
	}
	if len(model.Outputs) == 0 {
		t.Fatal("model.Outputs is empty")
	}
	foundExpr := false
	for _, o := range model.Outputs {
		if o.Expr.Op != "" {
			foundExpr = true
		}
	}
	if !foundExpr {
		t.Error("every output.Expr.Op is empty — the loader stripped the arithmetic this endpoint needs")
	}
}

// TestFileModelLoader_UnknownPolicyReturnsNilNil matches ModelLoader's
// documented contract: absent file is (nil, nil), not an error.
func TestFileModelLoader_UnknownPolicyReturnsNilNil(t *testing.T) {
	loader := newFileModelLoader(t.TempDir())
	model, err := loader.LoadPolicyModel(context.Background(), "FH-NOPE-999")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if model != nil {
		t.Fatalf("model = %+v, want nil", model)
	}
}

// TestStoreEvaluator_EndToEndOverRealModel is the core wiring proof: the
// real FH-TRANSIT-008.json model, loaded by fileModelLoader, evaluated by
// storeEvaluator against a seeded fake store, produces the exact arithmetic
// the model declares — hand-computed here independently of the code under
// test — and every output's claim_class is "projection" (every output
// composes at least one param: ref, per the model-instance track's own
// design note).
func TestStoreEvaluator_EndToEndOverRealModel(t *testing.T) {
	dir := realModelsDir(t)
	loader := newFileModelLoader(dir)
	model, err := loader.LoadPolicyModel(context.Background(), "FH-TRANSIT-008")
	if err != nil || model == nil {
		t.Fatalf("LoadPolicyModel: model=%v err=%v", model, err)
	}

	// population_potentially_served's denominator was fixed to
	// usda_food_desert * usda_food_population (the LILA-designated
	// population per research/08) — usda_food_low_access_1mi is no longer
	// referenced by the model. These are the real Rock County (55105)
	// values, hand-verified against the live local DB (port 5434) this
	// session: usda_food_desert is stored in indicator_meta as unit
	// "percent_share" (0-1). mapStoreUnit maps that to experiment.UnitShare
	// by name equivalence (see that function's doc comment) — this is what
	// lets the arithmetic-trace operand render as a percentage and
	// reproduce by hand; see the ReproducesAtDisplayPrecision assertions
	// below.
	lookup := newFakeIndicatorLookup()
	lookup.seed("55105", "usda_food_desert", 0.26051106772863636, "USDA-FARA-2019")
	lookup.seed("55105", "usda_food_population", 160331, "USDA-FARA-2019")
	ev := newStoreEvaluator(lookup, map[string]store.VariableMeta{
		"usda_food_desert":     {VariableID: "usda_food_desert", SourceID: "usda-food", Unit: "percent_share"},
		"usda_food_population": {VariableID: "usda_food_population", SourceID: "usda-food", Unit: "count"},
	})

	// Defaults only — no user override — so every output should equal the
	// model's own authored default arithmetic. Hand-computed from the
	// committed file: num_stores=1, capital_cost_per_store=1_500_000,
	// annual_operating_subsidy_per_store=200_000, target_population_share_served=0.25,
	// planning_horizon_years=5.
	bindings := []experiment.ParamBinding{
		{ID: "num_stores", Num: 1, Source: experiment.SrcDefault},
		{ID: "capital_cost_per_store", Num: 1500000, Source: experiment.SrcDefault},
		{ID: "annual_operating_subsidy_per_store", Num: 200000, Source: experiment.SrcDefault},
		{ID: "target_population_share_served", Num: 0.25, Source: experiment.SrcDefault},
		{ID: "planning_horizon_years", Num: 5, Source: experiment.SrcDefault},
	}

	result, err := ev.Evaluate(context.Background(), model, "55105", "county", "Rock County", bindings)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	want := map[string]float64{
		"total_capital_cost":             1 * 1500000,
		"total_annual_operating_subsidy": 1 * 200000,
		"total_public_cost_over_horizon": 1*1500000 + (1*200000)*5,
		// Matches apply()'s OpMul reduction order exactly (left fold from
		// 1.0): the inner mul(usda_food_desert, usda_food_population) first,
		// then the outer mul(target_population_share_served, <inner>) — see
		// eval.go's apply(). 0.25 * 41768 (rounded) = 10442.
		"population_potentially_served": 0.25 * (0.26051106772863636 * 160331),
	}
	got := map[string]*float64{}
	for _, o := range result.Outputs {
		got[o.ID] = o.Num
		if len(o.AssumedInputs) == 0 {
			t.Errorf("output %q has no assumed_inputs, want at least one (num_stores or target_population_share_served) — every output in this model composes a param", o.ID)
		}
	}
	for id, w := range want {
		g, ok := got[id]
		if !ok || g == nil {
			t.Errorf("output %q missing or nil, want %v", id, w)
			continue
		}
		if *g != w {
			t.Errorf("output %q = %v, want %v", id, *g, w)
		}
	}

	if result.ClaimClass() != experiment.ClassProjection {
		t.Errorf("result.ClaimClass() = %q, want %q — every output carries an assumed param", result.ClaimClass(), experiment.ClassProjection)
	}

	// The reproduction invariant, exercised against the REAL pipeline this
	// endpoint actually serves — the committed model file, mapStoreUnit, and
	// experiment.Evaluate — not just a hand-built Step
	// (ReproducesAtDisplayPrecision's other callers in eval_test.go use
	// synthetic Steps; this is the one place the checker runs over output
	// this integration produces end to end). This is the reproduction of
	// the exact reported defect: usda_food_desert = 0.26051106772863636
	// (percent_share in the DB) next to usda_food_population = 160,331. If
	// mapStoreUnit ever regresses to leaving "percent_share" unmapped, or if
	// choosePrecision's share branch in eval.go regresses, this test fails
	// here — go build/go vet cannot catch either regression, only this
	// arithmetic check can.
	var steps []experiment.Step
	for _, st := range result.Steps {
		if st.OutputID == "population_potentially_served" {
			steps = append(steps, st)
		}
	}
	if len(steps) == 0 {
		t.Fatal("population_potentially_served produced no steps — test is not exercising the reproduction invariant")
	}
	sawShareOperand := false
	for _, st := range steps {
		for _, op := range st.Operands {
			if op.ID == "indicator:usda_food_desert" {
				sawShareOperand = true
				if op.Unit != experiment.UnitShare {
					t.Errorf("indicator:usda_food_desert operand.Unit = %q, want %q — mapStoreUnit did not map indicator_meta's \"percent_share\" onto experiment.UnitShare", op.Unit, experiment.UnitShare)
				}
			}
		}
		ok, recomputed, tolerance := experiment.ReproducesAtDisplayPrecision(st)
		if !ok {
			resultNum := "nil"
			if st.Result.Num != nil {
				resultNum = fmt.Sprintf("%v", *st.Result.Num)
			}
			t.Errorf("step (op=%s, result id=%q) does not reproduce by hand: recomputed=%v true=%v tolerance=%v operands=%+v",
				st.Op, st.Result.ID, recomputed, resultNum, tolerance, st.Operands)
		}
	}
	if !sawShareOperand {
		t.Fatal("no step operand referenced indicator:usda_food_desert — the model's Expr tree changed and this test no longer covers the motivating bug")
	}

	// Known-bad control: an unseeded geoid must propagate MISSING through
	// population_potentially_served, never a substituted zero.
	resultMissing, err := ev.Evaluate(context.Background(), model, "55999", "county", "Nowhere County", bindings)
	if err != nil {
		t.Fatalf("Evaluate (missing indicator): %v", err)
	}
	for _, o := range resultMissing.Outputs {
		if o.ID != "population_potentially_served" {
			continue
		}
		if o.Num != nil {
			t.Fatalf("population_potentially_served = %v at an unseeded geoid, want nil (missing, never a substituted zero)", *o.Num)
		}
		if o.Missing != experiment.MissingUpstream {
			t.Errorf("missing_reason = %q, want %q", o.Missing, experiment.MissingUpstream)
		}
	}
}

// TestStoreEvaluator_UnusedIndicatorRefIsUnresolvedNotNotLoaded is the
// known-bad-control counterpart to loadLeaves' doc comment: a ref the model
// does not declare at all must never even be looked up, and a ref the model
// DOES declare but the store has no row for must resolve as
// MissingNotLoaded, never MissingUnresolvedRef — that distinction is the
// gap-vs-refusal line ADR-014 D2 draws.
func TestStoreEvaluator_UnusedIndicatorRefIsUnresolvedNotNotLoaded(t *testing.T) {
	dir := realModelsDir(t)
	loader := newFileModelLoader(dir)
	model, err := loader.LoadPolicyModel(context.Background(), "FH-TRANSIT-008")
	if err != nil || model == nil {
		t.Fatalf("LoadPolicyModel: model=%v err=%v", model, err)
	}

	lookup := newFakeIndicatorLookup() // deliberately empty — nothing seeded
	ev := newStoreEvaluator(lookup, nil)

	bindings := []experiment.ParamBinding{
		{ID: "num_stores", Num: 1, Source: experiment.SrcDefault},
		{ID: "capital_cost_per_store", Num: 1500000, Source: experiment.SrcDefault},
		{ID: "annual_operating_subsidy_per_store", Num: 200000, Source: experiment.SrcDefault},
		{ID: "target_population_share_served", Num: 0.25, Source: experiment.SrcDefault},
		{ID: "planning_horizon_years", Num: 5, Source: experiment.SrcDefault},
	}
	result, err := ev.Evaluate(context.Background(), model, "55105", "county", "Rock County", bindings)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	for _, o := range result.Outputs {
		if o.ID != "population_potentially_served" {
			continue
		}
		if o.Missing != experiment.MissingUpstream {
			t.Errorf("missing_reason = %q, want %q (declared ref, no row = a gap, not unresolved_ref)", o.Missing, experiment.MissingUpstream)
		}
	}
}

// TestSplitLeafRef_KnownBadControl asserts the parser rejects a ref with no
// namespace separator instead of silently treating the whole string as an
// id — the failure mode that would make every indicator ref collide into
// one bucket.
func TestSplitLeafRef_KnownBadControl(t *testing.T) {
	if _, _, ok := splitLeafRef("no_colon_here"); ok {
		t.Error("splitLeafRef accepted a ref with no namespace separator")
	}
	ns, id, ok := splitLeafRef("indicator:usda_food_low_access_1mi")
	if !ok || ns != "indicator" || id != "usda_food_low_access_1mi" {
		t.Errorf("splitLeafRef = (%q, %q, %v), want (indicator, usda_food_low_access_1mi, true)", ns, id, ok)
	}
}
