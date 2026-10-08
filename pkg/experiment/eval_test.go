package experiment

import (
	"encoding/json"
	"math"
	"os"
	"regexp"
	"strings"
	"testing"
)

// ── helpers ────────────────────────────────────────────────────────────

func f(v float64) *float64 { return &v }

func refExpr(ref string) Expr { return Expr{Op: OpRef, Ref: ref} }

func constExpr(v float64) Expr {
	return Expr{Op: OpConst, Const: f(v)}
}

func opExpr(op Op, terms ...Expr) Expr {
	ts := make([]Term, len(terms))
	for i, e := range terms {
		ts[i] = Term{Expr: e}
	}
	return Expr{Op: op, Terms: ts}
}

// baseModel is a minimal, individually-valid model that every test mutates.
func baseModel() *PolicyModel {
	return &PolicyModel{
		SchemaVersion: SchemaVersion,
		PolicyID:      "FH-TEST-001",
		ModelVersion:  "1",
		GeoLevels:     []string{"county"},
		DefaultGEOID:  "55105",
		TitleKey:      "pex.model.FH-TEST-001.title",
		SummaryKey:    "pex.model.FH-TEST-001.summary",
		Params: []Param{
			{ID: "weekly_spend", LabelKey: "pex.param.weekly_spend.label", HelpKey: "pex.param.weekly_spend.help",
				Unit: UnitUSD, Min: 0, Max: 200, Step: 5, Default: 60, DefaultSourceKey: "pex.param.weekly_spend.default_source", Precision: 0},
		},
		Outputs:     []Output{},
		RefusalKeys: []string{"pex.refusal.no_causal_claim"},
	}
}

func leafPresent(labelKey string, val float64, vintage string) MeasuredLeaf {
	return MeasuredLeaf{LabelKey: labelKey, Value: f(val), Vintage: vintage, Unit: UnitPeople, Reliability: RelHigh}
}

func leafMissing(labelKey string, reason MissingReason) MeasuredLeaf {
	return MeasuredLeaf{LabelKey: labelKey, Value: nil, Missing: reason}
}

// ── Validate: unknown-ref rejection (static, model-authoring) ──────────

func TestValidate_UnknownParamRefRejected(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "out1", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegPublic,
			Expr: refExpr("param:does_not_exist")},
	}
	err := Validate(m)
	if err == nil {
		t.Fatal("expected an error for a ref naming an undeclared param, got nil")
	}
	ee, ok := err.(*EvalError)
	if !ok || ee.Code != ErrUnknownRef {
		t.Fatalf("expected EvalError{Code: %q}, got %#v", ErrUnknownRef, err)
	}
}

func TestValidate_UnknownOutputRefRejected(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "out1", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegPublic,
			Expr: refExpr("output:nonexistent")},
	}
	err := Validate(m)
	if err == nil {
		t.Fatal("expected an error for a ref naming an undeclared output, got nil")
	}
	if ee, ok := err.(*EvalError); !ok || ee.Code != ErrUnknownRef {
		t.Fatalf("expected EvalError{Code: %q}, got %#v", ErrUnknownRef, err)
	}
}

// ── Validate: forward reference and cycle rejection (§2) ───────────────

func TestValidate_ForwardReferenceRejected(t *testing.T) {
	m := baseModel()
	// "a" (index 0) references "b", which is declared after it (index 1).
	m.Outputs = []Output{
		{ID: "a", LabelKey: "k", Unit: UnitCount, Precision: 0, Register: RegPublic, Expr: refExpr("output:b")},
		{ID: "b", LabelKey: "k", Unit: UnitCount, Precision: 0, Register: RegPublic, Expr: constExpr(1)},
	}
	err := Validate(m)
	if err == nil {
		t.Fatal("expected a forward reference to be rejected")
	}
	if ee, ok := err.(*EvalError); !ok || (ee.Code != ErrForwardRef && ee.Code != ErrCycle) {
		t.Fatalf("expected ErrForwardRef or ErrCycle, got %#v", err)
	}
}

// TestValidate_SelfReferenceCycleRejected is the required "a reference cycle
// is rejected at validation, not at eval time" case, instantiated as the
// simplest possible cycle: an output whose own tree references itself. If
// this were not caught here, Evaluate would recurse into resolve(self) ->
// resolve(self) -> ... without terminating.
func TestValidate_SelfReferenceCycleRejected(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "x", LabelKey: "k", Unit: UnitCount, Precision: 0, Register: RegPublic, Expr: refExpr("output:x")},
	}

	if err := Validate(m); err == nil {
		t.Fatal("expected Validate to reject a self-referencing output")
	}

	// The load-bearing behavioral claim: Evaluate must refuse this at
	// validation and never attempt to resolve it. If it were resolved
	// instead, this call would stack-overflow rather than return.
	_, err := Evaluate(m, "55105", "county", "Rock County", nil, nil)
	if err == nil {
		t.Fatal("expected Evaluate to refuse a cyclic model rather than attempt resolution")
	}
}

// TestFindCycle_Direct exercises the cycle-detection mechanism itself,
// independent of Validate's forward-reference shortcut (see the package doc
// comment on why both exist). A hand-built 3-node cycle has no notion of
// declaration order, so this is the only test that isolates findCycle.
func TestFindCycle_Direct(t *testing.T) {
	graph := map[string][]string{
		"a": {"b"},
		"b": {"c"},
		"c": {"a"},
	}
	got := findCycle(graph)
	if got == "" {
		t.Fatal("expected findCycle to report a cycle for a -> b -> c -> a")
	}
	if !strings.Contains(got, "a") || !strings.Contains(got, "b") || !strings.Contains(got, "c") {
		t.Fatalf("cycle report %q does not mention all three nodes", got)
	}
}

func TestFindCycle_Acyclic(t *testing.T) {
	graph := map[string][]string{
		"a": {"b"},
		"b": {"c"},
		"c": nil,
	}
	if got := findCycle(graph); got != "" {
		t.Fatalf("expected no cycle, got %q", got)
	}
}

// ── Validate: model-lint errors ─────────────────────────────────────────

func TestValidate_EmptyTermsRejected(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "out1", LabelKey: "k", Unit: UnitCount, Precision: 0, Register: RegPublic,
			Expr: Expr{Op: OpAdd, Terms: nil}},
	}
	err := Validate(m)
	if ee, ok := err.(*EvalError); !ok || ee.Code != ErrEmptyTerms {
		t.Fatalf("expected EvalError{Code: %q}, got %#v", ErrEmptyTerms, err)
	}
}

func TestValidate_MeasuredConstWithoutSourceRejected(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "out1", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegPublic,
			Expr: Expr{Op: OpConst, Const: f(3.5), ConstKind: KindMeasured}}, // no ConstSource
	}
	err := Validate(m)
	if ee, ok := err.(*EvalError); !ok || ee.Code != ErrMissingConstSrc {
		t.Fatalf("expected EvalError{Code: %q}, got %#v", ErrMissingConstSrc, err)
	}
}

// ── Validate: measured const_source must RESOLVE, not merely be non-empty ──
//
// This is the data-file boundary the audit flagged: const_kind: "measured"
// with a non-empty const_source used to be enough to launder an assumption
// into a measurement, because the check only asked "is this string empty?"
// and never "does this string name anything real?" These tests assert the
// closed defect and its known-bad control.

// TestKnownBadControl_UnresolvableMeasuredConstSourceRejected is the
// required known-bad control: a model claiming const_kind: "measured" with a
// const_source that resolves to nothing in the source registry MUST be
// rejected by Validate, at load time, before Evaluate ever produces a
// number from it.
//
// Toggle-verified 2026-08-14: commenting out the `!knownSources[...]` branch
// in collectOutputRefs's OpConst case (leaving only the non-empty check)
// makes this test fail with "expected Validate to reject a measured const
// whose source does not resolve, got nil" — confirming the assertion is
// load-bearing on the new check and not decorative. The branch was then
// restored; go build/test above reflect the restored state.
func TestKnownBadControl_UnresolvableMeasuredConstSourceRejected(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "laundered", LabelKey: "k", Unit: UnitPeople, Precision: 0, Register: RegPublic,
			// A pure assumption, declared measured, with a source that names
			// nothing in data/sources.toml — exactly the "trust me" case.
			Expr: Expr{Op: OpConst, Const: f(41768), ConstKind: KindMeasured, ConstSource: "trust me"}},
	}
	err := Validate(m)
	if err == nil {
		t.Fatal("expected Validate to reject a measured const whose source does not resolve, got nil")
	}
	ee, ok := err.(*EvalError)
	if !ok || ee.Code != ErrUnknownConstSrc {
		t.Fatalf("expected EvalError{Code: %q}, got %#v", ErrUnknownConstSrc, err)
	}

	// The defect's whole danger is that a bad model file could still produce
	// a number. Confirm Evaluate refuses it too, rather than silently
	// re-validating differently.
	if _, err := Evaluate(m, "55105", "county", "Rock County", nil, nil); err == nil {
		t.Fatal("expected Evaluate to refuse a model with an unresolvable measured const_source")
	}
}

// TestValidate_MeasuredConstWithKnownSourceAccepted is the positive control
// for the same check: a const_source that DOES resolve in the registry must
// not be rejected on that basis.
func TestValidate_MeasuredConstWithKnownSourceAccepted(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "lila_population", LabelKey: "k", Unit: UnitPeople, Precision: 0, Register: RegPublic,
			Expr: Expr{Op: OpConst, Const: f(41768), ConstKind: KindMeasured, ConstSource: "usda-food"}},
	}
	if err := Validate(m); err != nil {
		t.Fatalf("expected a const_source that resolves in the registry to be accepted, got %v", err)
	}
}

// TestValidateWithSources_InjectedRegistry demonstrates that the registry is
// genuinely injected rather than hardcoded behind Validate: the same model,
// with the same const_source, is rejected against DefaultKnownSources and
// accepted against a caller-supplied registry that happens to name it.
func TestValidateWithSources_InjectedRegistry(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "out1", LabelKey: "k", Unit: UnitPeople, Precision: 0, Register: RegPublic,
			Expr: Expr{Op: OpConst, Const: f(1), ConstKind: KindMeasured, ConstSource: "custom-internal-source"}},
	}

	if err := Validate(m); err == nil {
		t.Fatal("expected the default registry to reject a source it does not declare")
	}

	injected := map[string]bool{"custom-internal-source": true}
	if err := ValidateWithSources(m, injected); err != nil {
		t.Fatalf("expected the injected registry to accept a source it declares, got %v", err)
	}

	// And a nil registry is fail-closed, not "anything goes".
	if err := ValidateWithSources(m, nil); err == nil {
		t.Fatal("expected a nil source registry to reject every measured const, got nil")
	}
}

// TestDefaultKnownSources_MatchesSourcesToml guards DefaultKnownSources
// against drifting from the real manifest it is a hardcoded copy of. It
// parses data/sources.toml's own [sources.<key>] headers — the same data
// CLAUDE.md's documented grep extracts — and diffs the key set against
// DefaultKnownSources in both directions, so an added or removed source in
// the manifest that nobody updates here fails a test instead of shipping a
// silent gap (a real source rejected) or hole (a fabricated one accepted).
func TestDefaultKnownSources_MatchesSourcesToml(t *testing.T) {
	path := "../../data/sources.toml"
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("could not read %s to verify DefaultKnownSources against the real manifest: %v", path, err)
	}
	re := regexp.MustCompile(`(?m)^\[sources\.([a-z0-9-]+)\]`)
	matches := re.FindAllStringSubmatch(string(b), -1)
	if len(matches) == 0 {
		t.Fatalf("found no [sources.<key>] headers in %s — parser or path is wrong", path)
	}

	fromToml := make(map[string]bool, len(matches))
	for _, m := range matches {
		fromToml[m[1]] = true
	}

	for k := range fromToml {
		if !DefaultKnownSources[k] {
			t.Errorf("data/sources.toml declares [sources.%s] but DefaultKnownSources does not — a legitimate const_source would be wrongly rejected", k)
		}
	}
	for k := range DefaultKnownSources {
		if !fromToml[k] {
			t.Errorf("DefaultKnownSources contains %q but data/sources.toml has no [sources.%s] — a const_source that resolves to nothing real would be wrongly accepted", k, k)
		}
	}
}

// ── Evaluate: the taint algorithm ───────────────────────────────────────

// TestEvaluate_AllMeasuredChainIsMeasurement builds an output entirely out of
// indicator/reach refs and a properly-sourced measured const. It must
// resolve to ClassMeasurement, with an empty AssumedInputs list.
func TestEvaluate_AllMeasuredChainIsMeasurement(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "total_eligible", LabelKey: "k", Unit: UnitPeople, Precision: 0, Register: RegPublic,
			Expr: opExpr(OpAdd,
				refExpr("indicator:eligible_tract_a"),
				refExpr("reach:eligible_tract_b"),
				Expr{Op: OpConst, Const: f(0), ConstKind: KindMeasured, ConstSource: "usda-food"},
			)},
	}
	leaves := map[string]MeasuredLeaf{
		"indicator:eligible_tract_a": leafPresent("k", 100, "USDA-FARA-2019"),
		"reach:eligible_tract_b":     leafPresent("k", 250, "USDA-FARA-2019"),
	}

	res, err := Evaluate(m, "55105", "county", "Rock County", leaves, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := res.Outputs[0]
	if out.Num == nil || *out.Num != 350 {
		t.Fatalf("expected 350, got %v", out.Num)
	}
	if len(out.AssumedInputs) != 0 {
		t.Fatalf("expected no assumed inputs, got %v", out.AssumedInputs)
	}
	if out.ClaimClass() != ClassMeasurement {
		t.Fatalf("expected ClassMeasurement, got %v", out.ClaimClass())
	}
	if res.ClaimClass() != ClassMeasurement {
		t.Fatalf("expected result-level ClassMeasurement, got %v", res.ClaimClass())
	}
	if out.Vintage != "USDA-FARA-2019" {
		t.Fatalf("expected agreed vintage USDA-FARA-2019, got %q", out.Vintage)
	}
}

// TestEvaluate_OneAssumedLeafTaintsWholeChain is the central claim of this
// track: a single param buried three levels deep in an otherwise entirely
// measured tree must make the WHOLE output — and the whole EvalResult — a
// projection.
func TestEvaluate_OneAssumedLeafTaintsWholeChain(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "projected_spend", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegPublic,
			Expr: opExpr(OpAdd,
				refExpr("indicator:baseline_spend"), // measured
				opExpr(OpMul, // nested one level
					refExpr("indicator:store_count"), // measured
					opExpr(OpMul, // nested two levels — the assumption is buried here
						refExpr("param:weekly_spend"), // <-- the one assumed leaf
						constExpr(52),                 // assumed const (default kind)
					),
				),
			)},
	}
	leaves := map[string]MeasuredLeaf{
		"indicator:baseline_spend": leafPresent("k", 1000, "ACS-2024-5yr"),
		"indicator:store_count":    leafPresent("k", 3, "ACS-2024-5yr"),
	}

	res, err := Evaluate(m, "55105", "county", "Rock County", leaves, map[string]float64{"weekly_spend": 60})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := res.Outputs[0]
	if out.Num == nil {
		t.Fatal("expected a value, got missing")
	}
	want := 1000.0 + 3.0*(60.0*52.0)
	if *out.Num != want {
		t.Fatalf("expected %v, got %v", want, *out.Num)
	}
	found := false
	for _, a := range out.AssumedInputs {
		if a == "weekly_spend" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected weekly_spend in assumed_inputs, got %v", out.AssumedInputs)
	}
	if out.ClaimClass() != ClassProjection {
		t.Fatalf("expected ClassProjection, got %v", out.ClaimClass())
	}
	if res.ClaimClass() != ClassProjection {
		t.Fatalf("expected result-level ClassProjection, got %v", res.ClaimClass())
	}
	// The derived value must never carry an MOE, tainted or not.
	if out.MOE != nil {
		t.Fatalf("expected nil MOE on a derived output, got %v", *out.MOE)
	}
	// Disagreement/absence of vintage: the tree mixes measured leaves that
	// agree (ACS-2024-5yr) with assumed nodes that carry none, so the
	// measured leaves still agree and the vintage should be carried.
	if out.Vintage != "ACS-2024-5yr" {
		t.Fatalf("expected vintage ACS-2024-5yr to survive assumed siblings, got %q", out.Vintage)
	}
}

// TestKnownBadControl_TaintSurvivesDeepNesting is named separately from the
// test above to record the verification this track's instructions require:
// this exact test was run against a deliberately neutered build of
// resolveOp (assumedInputs hard-coded to an empty slice instead of the
// union) and FAILED as expected, before the neutering was reverted. See this
// package's PR/handoff notes. A taint check that cannot fail this way is not
// evidence of anything.
func TestKnownBadControl_TaintSurvivesDeepNesting(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "deep", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegPublic,
			Expr: opExpr(OpAdd,
				refExpr("indicator:a"),
				opExpr(OpAdd,
					refExpr("indicator:b"),
					opExpr(OpAdd,
						refExpr("indicator:c"),
						refExpr("param:weekly_spend"),
					),
				),
			)},
	}
	leaves := map[string]MeasuredLeaf{
		"indicator:a": leafPresent("k", 1, "V1"),
		"indicator:b": leafPresent("k", 1, "V1"),
		"indicator:c": leafPresent("k", 1, "V1"),
	}
	res, err := Evaluate(m, "55105", "county", "Rock County", leaves, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Outputs[0].ClaimClass() != ClassProjection {
		t.Fatalf("a param four levels deep must still taint the output to projection, got %v",
			res.Outputs[0].ClaimClass())
	}
}

// ── Evaluate: MISSING propagation ───────────────────────────────────────

func TestEvaluate_MissingMeasuredInputPropagatesAsMissingNeverZero(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "total", LabelKey: "k", Unit: UnitPeople, Precision: 0, Register: RegPublic,
			Expr: opExpr(OpAdd, refExpr("indicator:present_one"), refExpr("indicator:missing_one"))},
	}
	leaves := map[string]MeasuredLeaf{
		"indicator:present_one": leafPresent("k", 42, "V1"),
		"indicator:missing_one": leafMissing("k", MissingSuppressed),
	}
	res, err := Evaluate(m, "55105", "county", "Rock County", leaves, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := res.Outputs[0]
	if out.Num != nil {
		t.Fatalf("expected value to be nil (missing), got %v — a missing input must never silently become zero", *out.Num)
	}
	if out.Missing != MissingUpstream {
		t.Fatalf("expected MissingUpstream, got %q", out.Missing)
	}
	wantDetail := "indicator:missing_one:" + string(MissingSuppressed)
	if out.MissingDetail != wantDetail {
		t.Fatalf("expected missing_detail %q, got %q", wantDetail, out.MissingDetail)
	}
}

func TestEvaluate_UnresolvedIndicatorRefIsMissingNotError(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "total", LabelKey: "k", Unit: UnitPeople, Precision: 0, Register: RegPublic,
			Expr: refExpr("indicator:never_loaded")},
	}
	// No leaves supplied at all — the data layer never heard of this ref.
	res, err := Evaluate(m, "55105", "county", "Rock County", map[string]MeasuredLeaf{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := res.Outputs[0]
	if out.Num != nil {
		t.Fatalf("expected nil value for an unresolved indicator ref, got %v", *out.Num)
	}
	if out.Missing != MissingUnresolvedRef {
		t.Fatalf("expected MissingUnresolvedRef, got %q", out.Missing)
	}
}

// ── Evaluate: division by zero ──────────────────────────────────────────

func TestEvaluate_DivisionByZeroYieldsMissingNotInfNotZero(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "rate", LabelKey: "k", Unit: UnitRatio, Precision: 2, Register: RegPublic,
			Expr: opExpr(OpDiv, refExpr("indicator:numerator"), refExpr("indicator:zero_denominator"))},
	}
	leaves := map[string]MeasuredLeaf{
		"indicator:numerator":        leafPresent("k", 10, "V1"),
		"indicator:zero_denominator": leafPresent("k", 0, "V1"),
	}
	res, err := Evaluate(m, "55105", "county", "Rock County", leaves, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := res.Outputs[0]
	if out.Num != nil {
		t.Fatalf("division by zero must not produce a number (Inf or otherwise), got %v", *out.Num)
	}
	if out.Missing != MissingDivideByZero {
		t.Fatalf("expected MissingDivideByZero, got %q", out.Missing)
	}
}

func TestEvaluate_NonFiniteResultIsMissing(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "overflow", LabelKey: "k", Unit: UnitRatio, Precision: 0, Register: RegAnalyst,
			Expr: opExpr(OpMul,
				Expr{Op: OpConst, Const: f(1e308), ConstKind: KindMeasured, ConstSource: "acs-5yr"},
				Expr{Op: OpConst, Const: f(1e308), ConstKind: KindMeasured, ConstSource: "acs-5yr"},
			)},
	}
	res, err := Evaluate(m, "55105", "county", "Rock County", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := res.Outputs[0]
	if out.Num != nil {
		t.Fatalf("expected a non-finite product to be reported as missing, got %v", *out.Num)
	}
	if out.Missing != MissingNonFinite {
		t.Fatalf("expected MissingNonFinite, got %q", out.Missing)
	}
}

// ── Evaluate: param range validation ────────────────────────────────────

func TestEvaluate_ParamOutOfRangeRejected(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "echo", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegPublic,
			Expr: refExpr("param:weekly_spend")},
	}
	_, err := Evaluate(m, "55105", "county", "Rock County", nil, map[string]float64{"weekly_spend": 999})
	if err == nil {
		t.Fatal("expected an out-of-range param to be rejected")
	}
	ee, ok := err.(*EvalError)
	if !ok || ee.Code != ErrParamOutOfRange {
		t.Fatalf("expected EvalError{Code: %q}, got %#v", ErrParamOutOfRange, err)
	}
}

func TestEvaluate_ParamOutOfRangeNeverClamped(t *testing.T) {
	// A regression guard on the specific failure mode the contract calls
	// out: clamping silently substitutes a different assumption than the
	// one submitted. If the implementation clamped instead of rejecting,
	// this call would return 200 with weekly_spend=200 instead of erroring.
	m := baseModel()
	m.Outputs = []Output{
		{ID: "echo", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegPublic,
			Expr: refExpr("param:weekly_spend")},
	}
	res, err := Evaluate(m, "55105", "county", "Rock County", nil, map[string]float64{"weekly_spend": 300})
	if err == nil {
		t.Fatalf("expected rejection, got a clamped result: %+v", res)
	}
}

func TestEvaluate_UnknownParamRejected(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "echo", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegPublic,
			Expr: refExpr("param:weekly_spend")},
	}
	_, err := Evaluate(m, "55105", "county", "Rock County", nil, map[string]float64{"not_a_real_param": 1})
	if err == nil {
		t.Fatal("expected an unknown submitted param to be rejected")
	}
	if ee, ok := err.(*EvalError); !ok || ee.Code != ErrUnknownParam {
		t.Fatalf("expected EvalError{Code: %q}, got %#v", ErrUnknownParam, err)
	}
}

func TestEvaluate_OmittedParamUsesDefault(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "echo", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegPublic,
			Expr: refExpr("param:weekly_spend")},
	}
	res, err := Evaluate(m, "55105", "county", "Rock County", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Params[0].Source != SrcDefault {
		t.Fatalf("expected SrcDefault, got %v", res.Params[0].Source)
	}
	if res.Params[0].Num != 60 {
		t.Fatalf("expected the authored default 60, got %v", res.Params[0].Num)
	}
	if res.Outputs[0].Num == nil || *res.Outputs[0].Num != 60 {
		t.Fatalf("expected the output to echo the default, got %v", res.Outputs[0].Num)
	}
}

// ── Evaluate: output: passthrough and geo-level ─────────────────────────

func TestEvaluate_OutputRefInheritsClassVerbatim(t *testing.T) {
	m := baseModel()
	m.Outputs = []Output{
		{ID: "projection", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegAnalyst,
			Expr: refExpr("param:weekly_spend")},
		{ID: "restated", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegPublic,
			Expr: refExpr("output:projection")},
	}
	res, err := Evaluate(m, "55105", "county", "Rock County", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Outputs[1].ClaimClass() != ClassProjection {
		t.Fatalf("an output: ref must inherit the referenced output's class verbatim, got %v",
			res.Outputs[1].ClaimClass())
	}
}

func TestEvaluate_UnsupportedGeoLevelRejected(t *testing.T) {
	m := baseModel() // GeoLevels: ["county"]
	m.Outputs = []Output{
		{ID: "echo", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegPublic, Expr: constExpr(1)},
	}
	_, err := Evaluate(m, "55105000100", "tract", "Tract 1", nil, nil)
	if err == nil {
		t.Fatal("expected a geo level the model does not declare to be rejected")
	}
	if ee, ok := err.(*EvalError); !ok || ee.Code != ErrGeoLevelUnsupport {
		t.Fatalf("expected EvalError{Code: %q}, got %#v", ErrGeoLevelUnsupport, err)
	}
}

// ── Evaluate: EvalResult-level invariants ───────────────────────────────

func TestEvaluate_ResultRoundTripsThroughJSONInvariants(t *testing.T) {
	// This is the payload-honesty check from types.go: marshal, then
	// unmarshal, and confirm it does not error — proving the emitted
	// claim_class actually agrees with assumed_inputs for every value this
	// evaluator produces, not just the ones this test happens to inspect.
	m := baseModel()
	m.Outputs = []Output{
		{ID: "measured_out", LabelKey: "k", Unit: UnitPeople, Precision: 0, Register: RegPublic,
			Expr: refExpr("indicator:x")},
		{ID: "projected_out", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegPublic,
			Expr: opExpr(OpMul, refExpr("indicator:x"), refExpr("param:weekly_spend"))},
	}
	leaves := map[string]MeasuredLeaf{"indicator:x": leafPresent("k", 5, "V1")}
	res, err := Evaluate(m, "55105", "county", "Rock County", leaves, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var round EvalResult
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatalf("round-trip unmarshal rejected the evaluator's own honest payload: %v", err)
	}
}

// ── Reproduction invariant ───────────────────────────────────────────────
//
// "Every Step, rendered at its own declared precisions, must reproduce its
// own displayed result within the tolerance implied by that precision" --
// see ReproducesAtDisplayPrecision's doc comment in eval.go. This is what
// makes the analyst arithmetic trace checkable by a reader with a
// calculator instead of merely decorative (docs/contracts/experiment-
// contract.md §2, "Rounding": "a reader who redoes the multiplication and
// gets a different answer has caught the page lying whether or not it
// was").
//
// The motivating bug: POST /v1/policy/simulate for FH-TRANSIT-008 /
// geoid=55105, 2026-08-14. usda_food_desert = 0.26051106772863636 (a
// 0-1 share), displayed at a blind Precision=2 with no unit tag, next to
// usda_food_population = 160331. "0.26 x 160,331.00 = 41,768.00" does not
// reproduce: 0.26 x 160331 = 41,686.06, off by 82 people.

func stepsFor(res *EvalResult, outputID string) []Step {
	var out []Step
	for _, s := range res.Steps {
		if s.OutputID == outputID {
			out = append(out, s)
		}
	}
	return out
}

// TestReproducesAtDisplayPrecision_KnownBadControl is the control the task
// asked for: a step deliberately built to NOT reproduce, so a broken checker
// (or a checker that always returns true) is caught. This is the exact
// shape of the live bug, replayed as a hand-built Step rather than through
// Evaluate: a share operand shown at 2 RAW decimal places (no unit tag, so
// no percent-scale resolution boost) next to a six-digit count.
func TestReproducesAtDisplayPrecision_KnownBadControl(t *testing.T) {
	bad := Step{
		OutputID: "population_potentially_served",
		Op:       OpMul,
		Operands: []Value{
			{ID: "indicator:usda_food_desert", Num: f(0.26051106772863636), Precision: 2}, // unit "" -- NOT tagged share
			{ID: "indicator:usda_food_population", Num: f(160331), Precision: 2},
		},
		Result: Value{ID: "population_potentially_served:1", Num: f(41767.99999999999), Precision: 2},
	}
	ok, recomputed, tolerance := ReproducesAtDisplayPrecision(bad)
	if ok {
		t.Fatalf("known-bad control did not fail: recomputed=%v tolerance=%v against displayed result 41,768.00 -- "+
			"the checker is not catching the bug it exists to catch (0.26 x 160,331.00 should NOT reproduce 41,768.00)",
			recomputed, tolerance)
	}
	wantRecompute := 0.26 * 160331.00
	if math.Abs(recomputed-wantRecompute) > 1e-9 {
		t.Fatalf("recomputed = %v, want the naive hand-multiplication %v (0.26 x 160,331.00)", recomputed, wantRecompute)
	}
}

// TestReproducesAtDisplayPrecision_KnownGoodControl is the same arithmetic,
// correctly precision-and-unit-tagged the way Evaluate now produces it: the
// share carries Unit: UnitShare (so displayScale renders it as a percentage,
// recovering two more significant decimal digits within the same Precision
// 2 ceiling) and the population carries Precision 0 (it is already an
// integer; a decimal place on it would only add noise). 26.05% x 160,331
// lands a couple of people from the true product -- comfortably inside
// reproductionTolerance, nothing like the 82-person miss above.
func TestReproducesAtDisplayPrecision_KnownGoodControl(t *testing.T) {
	good := Step{
		OutputID: "population_potentially_served",
		Op:       OpMul,
		Operands: []Value{
			{ID: "indicator:usda_food_desert", Num: f(0.26051106772863636), Unit: UnitShare, Precision: 2},
			{ID: "indicator:usda_food_population", Num: f(160331), Unit: UnitPeople, Precision: 0},
		},
		Result: Value{ID: "population_potentially_served:1", Num: f(41767.99999999999), Unit: "", Precision: 0},
	}
	ok, recomputed, tolerance := ReproducesAtDisplayPrecision(good)
	if !ok {
		t.Fatalf("expected the correctly-tagged step to reproduce: recomputed=%v tolerance=%v true=%v",
			recomputed, tolerance, *good.Result.Num)
	}
}

// TestEvaluate_ParamShareUnitPropagation_FixesReproduction exercises the
// OTHER half of this fix directly: resolveRef's "param" case now carries a
// param's own declared Unit and Precision into the trace (see paramDefs on
// evalCtx), instead of leaving every param node's Unit as "" the way it did
// before. This binds a share-typed param to a NON-round value -- the
// real-world analogue of a measured share like usda_food_desert's
// 0.26051106772863636 -- so the reproduction check actually depends on
// whether the propagation ran.
func TestEvaluate_ParamShareUnitPropagation_FixesReproduction(t *testing.T) {
	m := baseModel()
	m.Params = append(m.Params, Param{
		ID: "share_assumption", LabelKey: "k", HelpKey: "k",
		Unit: UnitShare, Min: 0, Max: 1, Step: 0.05, Default: 0.25,
		DefaultSourceKey: "k", Precision: 2,
	})
	m.Outputs = []Output{
		{ID: "served", LabelKey: "k", Unit: UnitPeople, Precision: 0, Register: RegPublic,
			Expr: opExpr(OpMul, refExpr("param:share_assumption"), refExpr("indicator:population"))},
	}
	leaves := map[string]MeasuredLeaf{
		"indicator:population": {LabelKey: "k", Value: f(160331), Vintage: "V1", Unit: UnitPeople},
	}
	res, err := Evaluate(m, "55105", "county", "Rock County", leaves,
		map[string]float64{"share_assumption": 0.26051106772863636})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	steps := stepsFor(res, "served")
	if len(steps) != 1 {
		t.Fatalf("expected exactly one step, got %d", len(steps))
	}
	shareOperand := steps[0].Operands[0]
	if shareOperand.Unit != UnitShare {
		t.Errorf("param leaf did not propagate its declared Unit: got %q, want %q -- "+
			"without this, the client cannot render the operand as a percentage and the "+
			"reproduction check below is expected to fail too", shareOperand.Unit, UnitShare)
	}
	ok, recomputed, tolerance := ReproducesAtDisplayPrecision(steps[0])
	if !ok {
		t.Errorf("step does not reproduce: recomputed=%v true=%v tolerance=%v -- "+
			"a share-typed param bound to a non-round value must still reproduce once "+
			"its Unit is correctly propagated from the model's own declaration",
			recomputed, *steps[0].Result.Num, tolerance)
	}
}

// TestReproducesAtDisplayPrecision_TableOverEvaluate runs the checker over
// every Step a real Evaluate() call produces, across several shapes,
// including a share x count multiplication like the failing one and a
// three-level-nested expression tree.
func TestReproducesAtDisplayPrecision_TableOverEvaluate(t *testing.T) {
	cases := []struct {
		name    string
		model   func() *PolicyModel
		leaves  map[string]MeasuredLeaf
		params  map[string]float64
		outputs []string // output ids whose steps must all reproduce
	}{
		{
			name: "share x large count, nested one level (the motivating bug, corrected)",
			model: func() *PolicyModel {
				m := baseModel()
				m.Params = append(m.Params, Param{
					ID: "target_population_share_served", LabelKey: "k", HelpKey: "k",
					Unit: UnitShare, Min: 0, Max: 1, Step: 0.05, Default: 0.25,
					DefaultSourceKey: "k", Precision: 2,
				})
				m.Outputs = []Output{
					{ID: "population_potentially_served", LabelKey: "k", Unit: UnitPeople, Precision: 0, Register: RegPublic,
						Expr: opExpr(OpMul,
							refExpr("param:target_population_share_served"),
							opExpr(OpMul, refExpr("indicator:usda_food_desert"), refExpr("indicator:usda_food_population")),
						)},
				}
				return m
			},
			leaves: map[string]MeasuredLeaf{
				// Unit: UnitShare here is what a correct caller (contract §1
				// says Unit is caller-supplied for a measured leaf) supplies
				// for a 0-1 designated-share indicator -- see briefs.go's
				// explicit UnitShare override for this exact variable in the
				// tier-1 path, and pkg/gateway/simulate_wire.go's
				// mapStoreUnit, which now maps indicator_meta's
				// "percent_share" onto UnitShare by name equivalence for
				// tier 2 (pkg/gateway/simulate_wire_test.go's
				// TestStoreEvaluator_EndToEndOverRealModel exercises that
				// path end to end against the real model file).
				"indicator:usda_food_desert":     {LabelKey: "k", Value: f(0.26051106772863636), Vintage: "USDA-FARA-2019", Unit: UnitShare},
				"indicator:usda_food_population": {LabelKey: "k", Value: f(160331), Vintage: "USDA-FARA-2019", Unit: UnitPeople},
			},
			outputs: []string{"population_potentially_served"},
		},
		{
			name: "small ratio times a large dollar figure",
			model: func() *PolicyModel {
				m := baseModel()
				m.Outputs = []Output{
					{ID: "admin_overhead", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegPublic,
						Expr: opExpr(OpMul, refExpr("indicator:overhead_rate"), refExpr("indicator:program_budget"))},
				}
				return m
			},
			leaves: map[string]MeasuredLeaf{
				"indicator:overhead_rate":  {LabelKey: "k", Value: f(0.0725), Vintage: "V1", Unit: UnitShare},
				"indicator:program_budget": {LabelKey: "k", Value: f(2450000), Vintage: "V1", Unit: UnitUSD},
			},
			outputs: []string{"admin_overhead"},
		},
		{
			name: "three nested operation levels: mul(div(add(a,b),c), share)",
			model: func() *PolicyModel {
				m := baseModel()
				m.Params = append(m.Params, Param{
					ID: "phase_in_share", LabelKey: "k", HelpKey: "k",
					Unit: UnitShare, Min: 0, Max: 1, Step: 0.05, Default: 0.4,
					DefaultSourceKey: "k", Precision: 2,
				})
				m.Outputs = []Output{
					{ID: "triple_nested", LabelKey: "k", Unit: UnitUSD, Precision: 0, Register: RegPublic,
						Expr: opExpr(OpMul,
							opExpr(OpDiv,
								opExpr(OpAdd, refExpr("indicator:cost_a"), refExpr("indicator:cost_b")),
								refExpr("indicator:num_sites"),
							),
							refExpr("param:phase_in_share"),
						)},
				}
				return m
			},
			leaves: map[string]MeasuredLeaf{
				"indicator:cost_a":    {LabelKey: "k", Value: f(1000000), Vintage: "V1", Unit: UnitUSD},
				"indicator:cost_b":    {LabelKey: "k", Value: f(2000000), Vintage: "V1", Unit: UnitUSD},
				"indicator:num_sites": {LabelKey: "k", Value: f(3), Vintage: "V1", Unit: UnitCount},
			},
			outputs: []string{"triple_nested"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.model()
			res, err := Evaluate(m, "55105", "county", "Rock County", tc.leaves, tc.params)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checked := 0
			for _, outID := range tc.outputs {
				steps := stepsFor(res, outID)
				if len(steps) == 0 {
					t.Fatalf("output %q produced no steps -- test is not exercising anything", outID)
				}
				for _, step := range steps {
					ok, recomputed, tolerance := ReproducesAtDisplayPrecision(step)
					checked++
					if !ok {
						t.Errorf("step for output %q (op=%s, result id=%q) does not reproduce: "+
							"recomputed=%v true=%v tolerance=%v operands=%+v",
							outID, step.Op, step.Result.ID, recomputed, *step.Result.Num, tolerance, step.Operands)
					}
				}
			}
			if checked == 0 {
				t.Fatal("no steps were checked -- test is not exercising the invariant")
			}
		})
	}
}
