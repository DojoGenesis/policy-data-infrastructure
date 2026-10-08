package grounding

import (
	"context"
	"testing"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/experiment"
)

// ── fixtures ───────────────────────────────────────────────────────────
//
// A tiny, hand-built policy scope: one reach total that is a measurement,
// one tier-2 output that is a projection because it depends on a param the
// reader set. The numbers are chosen so a collision test can also reuse
// them (see TestScopedLedger_ProjectionDominatesOnCollision).

func fp(f float64) *float64 { return &f }

func measuredValue(id string, num float64, unit experiment.Unit, vintage string) experiment.Value {
	return experiment.Value{
		ID: id, LabelKey: "pex.output." + id + ".label",
		Num: fp(num), Unit: unit, Precision: 0,
		Kind: experiment.KindMeasured, Tier: experiment.TierReach, Register: experiment.RegPublic,
		Vintage: vintage, MeasuredInputs: []string{id},
	}
}

func projectedValue(id string, num float64, unit experiment.Unit, assumed []string) experiment.Value {
	return experiment.Value{
		ID: id, LabelKey: "pex.output." + id + ".label",
		Num: fp(num), Unit: unit, Precision: 0,
		Kind: experiment.KindDerived, Tier: experiment.TierExperiment, Register: experiment.RegPublic,
		AssumedInputs: assumed, MeasuredInputs: []string{"eligible_households"},
	}
}

func testBrief() *experiment.Brief {
	return &experiment.Brief{
		Policy: experiment.PolicyRef{
			PolicyID: "FH-TEST-008", Title: "Public groceries", Description: "Enable public grocery stores",
			Candidate: "Francesca Hong",
		},
		Geography: experiment.Geography{
			GEOID: "55105", Name: "Rock County", Level: "county", Source: experiment.GeoRequested,
		},
		Reach: &experiment.Reach{
			Rule: experiment.EligibilityRule{
				RuleID: "usda_fara_lila", AuthorityKey: "pex.rule.usda_fara_lila.authority",
				StatementKey: "pex.rule.usda_fara_lila.statement",
				Vintage:      "USDA-FARA-2019", GeoVintage: "2010-tract",
				SourceIDs: []string{"usda-food-access"},
			},
			Totals: []experiment.Value{
				measuredValue("eligible_population", 41768, experiment.UnitPeople, "ACS-2024-5yr"),
				measuredValue("eligible_households", 4754, experiment.UnitHouseholds, "ACS-2024-5yr"),
			},
			Areas: []experiment.Area{
				{GEOID: "55105000100", Name: "Census Tract 1, Rock County", Level: "tract", DesignatedShare: fp(0.62)},
			},
			DesignatedIDs: []string{"55105000100"},
			MapLayerKey:   "pex.map.designated_share.legend",
			Sources: []experiment.SourceRef{
				{SourceID: "usda-food-access", Name: "USDA Food Access Research Atlas", Vintage: "USDA-FARA-2019"},
			},
		},
		Evidence: []experiment.EvidenceItem{
			{
				EvidenceID: "kansas_grocery_2021", Jurisdiction: "Erie, Kansas", GeoScale: "city",
				PeriodStart: "2019", PeriodEnd: "2022", Intervention: "publicly-owned grocery store",
				Outcome: "grocery access", Direction: experiment.EvidenceIncrease,
				Finding: "The store remained the only grocer in town.", EffectReported: "one store, ongoing",
				SourceTitle: "Erie's public grocery store", SourceOrg: "NPR", SourceURL: "https://example.org/erie",
				SourceYear: "2022", AccessedDate: "2026-08-01", Citation: "NPR, 2022.",
				TransferNote: "Population scale differs sharply from Rock County.",
			},
		},
	}
}

// testEval returns an EvalResult where weekly_reach_spend is a genuine
// projection: it descends from param weekly_spend (assumed) times the
// measured eligible_households reach total.
func testEval() *experiment.EvalResult {
	return &experiment.EvalResult{
		PolicyID: "FH-TEST-008", ModelVersion: "1", GEOID: "55105", Level: "county", GeoName: "Rock County",
		Params: []experiment.ParamBinding{
			{ID: "weekly_spend", LabelKey: "pex.param.weekly_spend.label", Num: 60,
				Unit: experiment.UnitUSD, Precision: 0, Source: experiment.SrcUser},
		},
		Inputs: []experiment.Value{
			measuredValue("eligible_households", 4754, experiment.UnitHouseholds, "ACS-2024-5yr"),
		},
		Outputs: []experiment.Value{
			projectedValue("weekly_reach_spend", 285240, experiment.UnitUSD, []string{"weekly_spend"}),
		},
	}
}

// ── ScopedLedger ───────────────────────────────────────────────────────

func TestScopedLedger_ClassifiesByTaint(t *testing.T) {
	l := BuildScopedLedger(testBrief(), testEval())

	// The reach total: a real measurement. Allowed, no hedge required.
	if !l.IsAllowed(41768) {
		t.Fatal("41768 (measured reach total) should be allowed")
	}
	if l.RequiresHedge(41768) {
		t.Fatal("41768 is a measurement; it must not require a hedge")
	}

	// The tier-2 output: a real projection. Allowed, hedge required.
	if !l.IsAllowed(285240) {
		t.Fatal("285240 (projected output) should be allowed")
	}
	if !l.RequiresHedge(285240) {
		t.Fatal("285240 depends on an assumed input; it must require a hedge")
	}

	// The param echo: the reader's own number. Allowed, no hedge required.
	if !l.IsAllowed(60) {
		t.Fatal("60 (the param value) should be allowed")
	}
	if l.RequiresHedge(60) {
		t.Fatal("60 is a restatement of the reader's own assumption; it must not require a hedge")
	}

	// Known-bad control: a number nowhere on the page must not be allowed.
	// Without this, a ledger that (by bug) allows everything would pass
	// every other assertion above for the wrong reason.
	if l.IsAllowed(999999) {
		t.Fatal("999999 is not on the page and must not be allowed")
	}
}

func TestScopedLedger_NoEvalHasNoProjection(t *testing.T) {
	l := BuildScopedLedger(testBrief(), nil)
	if l.HasProjection() {
		t.Fatal("with no eval, the scope has no projection at all")
	}
	if !l.IsAllowed(41768) {
		t.Fatal("reach totals must still be allowed with no eval present")
	}
}

// TestScopedLedger_ProjectionDominatesOnCollision is the deliberately
// surprising case documented on ScopedLedger: if a rounded number happens
// to be produced by BOTH a measurement and a projection somewhere on the
// page, the ledger must treat it as requiring a hedge everywhere, because
// ScopedVerify cannot tell which source a given prose token refers to.
// Taint dominates on collision, matching experiment.Join.
func TestScopedLedger_ProjectionDominatesOnCollision(t *testing.T) {
	brief := testBrief()
	eval := testEval()
	// Force a collision: an output that happens to equal the measured
	// eligible_population total (41768), but is still a projection.
	eval.Outputs = append(eval.Outputs,
		projectedValue("colliding_projection", 41768, experiment.UnitPeople, []string{"weekly_spend"}))

	l := BuildScopedLedger(brief, eval)
	if !l.RequiresHedge(41768) {
		t.Fatal("41768 is ALSO produced by a projection on this page; it must require a hedge " +
			"even though a measurement produces the same number")
	}
}

// ── ScopedVerify ───────────────────────────────────────────────────────

func TestScopedVerify_CatchesInventedNumber(t *testing.T) {
	l := BuildScopedLedger(testBrief(), testEval())
	draft := "Rock County has 12,345 eligible households."
	v := ScopedVerify(draft, l)
	if len(v) == 0 {
		t.Fatal("12,345 is not on the page and must be flagged")
	}
	if v[0].Kind != "invented" {
		t.Fatalf("expected kind=invented, got %q", v[0].Kind)
	}
}

func TestScopedVerify_MeasurementNeedsNoHedge(t *testing.T) {
	l := BuildScopedLedger(testBrief(), testEval())
	draft := "Rock County has 41,768 eligible people, per ACS-2024-5yr."
	if v := ScopedVerify(draft, l); len(v) != 0 {
		t.Fatalf("a plainly-stated measurement should pass with no violations, got %v", v)
	}
}

// TestScopedVerify_RejectsUnhedgedProjection is the core guarantee this
// track was asked to add: a model can quote a REAL number from the page
// (285240 genuinely is the projected output) and still be rejected, because
// it narrated a projection as if it were a measured fact. The base Verify
// in verify.go would accept this draft; ScopedVerify must not.
func TestScopedVerify_RejectsUnhedgedProjection(t *testing.T) {
	l := BuildScopedLedger(testBrief(), testEval())
	draft := "Rock County spends 285,240 dollars a week reaching eligible households."
	v := ScopedVerify(draft, l)
	if len(v) == 0 {
		t.Fatal("a projection stated with no hedge language must be rejected")
	}
	found := false
	for _, viol := range v {
		if viol.Kind == "unhedged_projection" && viol.Number == "285,240" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected an unhedged_projection violation for 285,240, got %+v", v)
	}
}

func TestScopedVerify_AcceptsHedgedProjection(t *testing.T) {
	l := BuildScopedLedger(testBrief(), testEval())
	draft := "If you assume $60 a week in spending, the projected weekly total is about 285,240 dollars."
	if v := ScopedVerify(draft, l); len(v) != 0 {
		t.Fatalf("a properly hedged projection should pass, got %v", v)
	}
}

func TestScopedVerify_HedgeMustBeInTheSameSentence(t *testing.T) {
	l := BuildScopedLedger(testBrief(), testEval())
	// The hedge word is in a DIFFERENT sentence from the number. A model
	// could otherwise front-load "this is a projection" once and then
	// state every subsequent number as flat fact.
	draft := "This section covers a projected scenario. Rock County spends 285,240 dollars a week."
	v := ScopedVerify(draft, l)
	if len(v) == 0 {
		t.Fatal("a hedge in an earlier sentence must not cover a later unhedged sentence")
	}
}

// ── sentence splitting ───────────────────────────────────────────────

func TestSplitSentences_DoesNotBreakDecimals(t *testing.T) {
	got := splitSentences("The rate is 3.5 percent. It is measured.")
	if len(got) != 2 {
		t.Fatalf("expected 2 sentences, got %d: %v", len(got), got)
	}
	if got[0] != "The rate is 3.5 percent. " {
		t.Fatalf("decimal 3.5 was split across sentences: %q", got[0])
	}
}

// ── ScopedFacts: the deterministic floor ────────────────────────────

// TestScopedFactsPassesItsOwnGuarantee proves the floor is safe by
// construction, not by inspection: it feeds ScopedFacts' own output back
// through ScopedVerify and requires zero violations, for both the
// no-eval case (no projections exist yet) and the with-eval case (a real
// projection is on the page and must still be printed safely).
func TestScopedFactsPassesItsOwnGuarantee(t *testing.T) {
	cases := []struct {
		name string
		eval *experiment.EvalResult
	}{
		{"no eval yet", nil},
		{"with a projection", testEval()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			brief := testBrief()
			text := ScopedFacts(brief, tc.eval)
			if text == "" {
				t.Fatal("floor must never be empty when a brief is present")
			}
			l := BuildScopedLedger(brief, tc.eval)
			if v := ScopedVerify(text, l); len(v) != 0 {
				t.Fatalf("the deterministic floor violated its own guarantee: %v\ntext:\n%s", v, text)
			}
		})
	}
}

func TestScopedFacts_NilBrief(t *testing.T) {
	if got := ScopedFacts(nil, nil); got == "" {
		t.Fatal("ScopedFacts must return something even with no brief, never an empty string")
	}
}

// ── ScopedEngine ─────────────────────────────────────────────────────

type stubScopedComposer struct {
	text string
	err  error
}

func (s stubScopedComposer) ComposeScoped(_ context.Context, _ string, _ *experiment.Brief, _ *experiment.EvalResult) (string, error) {
	return s.text, s.err
}
func (s stubScopedComposer) Name() string { return "stub" }

func TestScopedEngine_NoComposerFallsBackToFloor(t *testing.T) {
	e := &ScopedEngine{}
	brief, eval := testBrief(), testEval()
	ans := e.Answer(context.Background(), "explain this", brief, eval)
	if ans.Source != SourceDeterministic {
		t.Fatalf("expected deterministic source with no composer, got %q", ans.Source)
	}
	if ans.Text != ScopedFacts(brief, eval) {
		t.Fatal("with no composer, the answer text must be exactly the deterministic floor")
	}
}

// TestScopedEngine_RejectsModelNarratingProjectionAsFact is the end-to-end
// version of the core guarantee: a composer that writes a truthful number
// in a factual, unhedged sentence must be REJECTED and the reader must
// receive the safe deterministic floor instead — never the bad draft.
func TestScopedEngine_RejectsModelNarratingProjectionAsFact(t *testing.T) {
	brief, eval := testBrief(), testEval()
	badDraft := "Rock County now spends 285,240 dollars every week reaching eligible households."
	e := &ScopedEngine{Composer: stubScopedComposer{text: badDraft}}

	ans := e.Answer(context.Background(), "explain this", brief, eval)

	if ans.Source != SourceModelRejected {
		t.Fatalf("expected the draft to be rejected, got source %q", ans.Source)
	}
	if len(ans.Violations) == 0 {
		t.Fatal("a rejected draft must report why")
	}
	if ans.Text != ScopedFacts(brief, eval) {
		t.Fatal("a rejected draft must fall back to the deterministic floor, not ship the bad text")
	}
	if ans.Text == badDraft {
		t.Fatal("the rejected draft must never reach the reader")
	}
}

func TestScopedEngine_AcceptsHedgedModelDraft(t *testing.T) {
	brief, eval := testBrief(), testEval()
	goodDraft := "If you assume $60 a week, the projected weekly total is about 285,240 dollars, " +
		"reaching the county's 4,754 eligible households."
	e := &ScopedEngine{Composer: stubScopedComposer{text: goodDraft}}

	ans := e.Answer(context.Background(), "explain this", brief, eval)

	if ans.Source != SourceModelVerified {
		t.Fatalf("expected the hedged draft to be accepted, got source %q (violations: %v)", ans.Source, ans.Violations)
	}
	if ans.Text != goodDraft {
		t.Fatal("an accepted draft's text must be the model's own text")
	}
}

func TestScopedEngine_RejectsInventedNumber(t *testing.T) {
	brief, eval := testBrief(), testEval()
	badDraft := "Rock County has 8,000,000 eligible households."
	e := &ScopedEngine{Composer: stubScopedComposer{text: badDraft}}

	ans := e.Answer(context.Background(), "explain this", brief, eval)

	if ans.Source != SourceModelRejected {
		t.Fatalf("expected rejection, got %q", ans.Source)
	}
	found := false
	for _, v := range ans.Violations {
		if v.Kind == "invented" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected an invented-number violation, got %+v", ans.Violations)
	}
}

func TestScopedEngine_ComposerErrorFallsBackToFloor(t *testing.T) {
	brief, eval := testBrief(), testEval()
	e := &ScopedEngine{Composer: stubScopedComposer{err: context.DeadlineExceeded}}

	ans := e.Answer(context.Background(), "explain this", brief, eval)

	if ans.Source != SourceDeterministic {
		t.Fatalf("a composer error must fall back to the deterministic floor, got %q", ans.Source)
	}
}
