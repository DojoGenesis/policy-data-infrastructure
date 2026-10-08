// Scoped chat for the policy experiment page (ADR-016, S19).
//
// # What this file adds to ADR-006's guarantee
//
// The base pipeline in this package (intent.go/execute.go/verify.go) grounds
// a chat answer in the Atlas dataset: Plan -> Execute -> Verify, where Verify
// checks that every number in a model's prose appears in the query result it
// was given. That check is necessary and, for the Atlas dataset, sufficient:
// every number the Atlas can produce is a measurement, so "is this number
// real" is the whole question.
//
// The policy experiment page breaks that assumption. Its payload
// (experiment.Brief, experiment.EvalResult) carries numbers of TWO different
// epistemic classes side by side: tier-1 REACH figures, which are
// measurements, and tier-2 EXPERIMENT outputs, which are projections —
// arithmetic consequences of a reader's own assumptions, never findings
// (ADR-016 D1, D3; contract S19). A model composing prose over this payload
// can satisfy the base Verify check — every number it wrote really is on the
// page — while still narrating a projection as if it were a measured fact.
// "There are 12,000 people affected" is indistinguishable, lexically, from
// "there are 12,000 eligible households" unless something also checks WHICH
// kind of number 12,000 is and whether the sentence around it says so.
//
// That is the property this file adds: ScopedLedger carries each number's
// resolved experiment.ClaimClass forward from the payload (it does not
// recompute the class — it reads Value.ClaimClass(), which is itself
// computed from AssumedInputs and cannot be set wrong; see types.go).
// ScopedVerify then requires that any sentence stating a projection-class
// number also carries a hedge marker from a small closed vocabulary. This is
// the same "necessary, not sufficient, but real" bar ADR-006 accepted for
// invented numbers, applied one level up: from "is this number real" to "is
// this number's status told truthfully."
//
// # What this does NOT catch — read before trusting it
//
// This is a sentence-scoped, positive-vocabulary lexical check, not a
// semantic parse of what a sentence claims. It requires a hedge word to be
// PRESENT; it cannot tell whether the sentence, taken as a whole, actually
// means what the hedge word suggests. A model that writes "This is
// projected to happen, and it basically will" has satisfied the check and
// still narrated a projection as near-certain. It also cannot help a model
// that hedges the WRONG number in a two-number sentence ("Projected costs
// are $40; the county has 41,768 people" — the hedge marker is present in
// the sentence but arguably reads as scoped to the first clause only; this
// checker accepts the whole sentence once any hedge marker appears anywhere
// in it). Narrowing sentence-level attribution to clause-level would need a
// real parse, which is future work and is not pretended to exist here.
//
// What it DOES guarantee, mechanically and testably: a sentence containing a
// projection-class number with NO hedging language anywhere in it is always
// rejected. That is the failure mode this track was asked to close — a
// projection stated as flatly as a measurement — and it is closed.
package grounding

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/experiment"
)

// ── the scoped ledger ──────────────────────────────────────────────

// NumberOrigin classifies why a number is allowed to appear in a scoped chat
// answer at all. Only OriginProjection imposes an additional obligation on
// the prose (a hedge marker in the same sentence); every other origin may be
// stated plainly, the same way base Verify's AllowedNumbers exempts
// citation digits, place names, and the asker's own threshold without
// asking anything further of them.
type NumberOrigin string

const (
	// OriginMeasurement — a number PDI measured: a tier-1 reach total or
	// area value, a tier-2 input leaf, or a tier-2 output whose
	// experiment.Value.ClaimClass() resolved to ClassMeasurement (zero
	// assumed inputs). May be stated as fact.
	OriginMeasurement NumberOrigin = "measurement"
	// OriginProjection — a number belonging to a Value whose ClaimClass()
	// resolved to ClassProjection: at least one assumed input. May be
	// stated only as the consequence of an assumption the reader can see
	// and change. ScopedVerify requires a hedge marker in the same
	// sentence.
	OriginProjection NumberOrigin = "projection"
	// OriginParamEcho — a reader-set (or authored-default) assumption
	// value itself, from EvalResult.Params. Restating "you set $60/week"
	// is not a claim about the world, it is a restatement of the reader's
	// own choice — the same reasoning base Verify uses to exempt the
	// asker's own Threshold. No hedge required.
	OriginParamEcho NumberOrigin = "param_echo"
	// OriginStructural — a count or index describing the SHAPE of the
	// payload (how many evidence items, how many designated tracts), not
	// a value drawn from any dataset. No hedge required.
	OriginStructural NumberOrigin = "structural"
	// OriginText — a number embedded in identifiers, vintages, GEOIDs,
	// citations, or other payload text rather than asserted as a figure.
	// Mirrors base Verify's addTextNumbers. No hedge required.
	OriginText NumberOrigin = "text"
)

// ScopedLedger is the number ledger for one policy+geography scope: every
// figure a chat answer about THIS page may legitimately contain, and — for
// each one — whether it belongs to a projection anywhere on the page.
//
// Two independent maps, not one map of "the" origin per number, because a
// rounded numeral can coincidentally match more than one field on the page
// (a reach total and an output happening to share a value at display
// precision). Membership in projection is tracked separately and is never
// cleared by also being a measurement elsewhere: if a number is a
// projection ANYWHERE on the page, prose using that number must hedge,
// regardless of what else it might also equal. This mirrors
// experiment.Join — taint dominates, and there is deliberately no code path
// that lets a coincidental collision launder a projection into looking
// unconditionally safe.
type ScopedLedger struct {
	allowed       map[float64]bool
	projection    map[float64]bool
	hasProjection bool
}

func newScopedLedger() *ScopedLedger {
	return &ScopedLedger{allowed: map[float64]bool{}, projection: map[float64]bool{}}
}

// put registers f at the same three roundings AllowedNumbers uses, because
// prose legitimately rounds a published or projected figure to the
// declared precision.
func (l *ScopedLedger) put(f float64, origin NumberOrigin) {
	for _, r := range []float64{f, round(f, 0), round(f, 1), round(f, 2)} {
		l.allowed[r] = true
		if origin == OriginProjection {
			l.projection[r] = true
			l.hasProjection = true
		}
	}
}

// IsAllowed reports whether f is a number this scope's chat may state at
// all. Mirrors the membership test in base Verify.
func (l *ScopedLedger) IsAllowed(f float64) bool {
	return l.allowed[f] || l.allowed[round(f, 1)] || l.allowed[round(f, 2)]
}

// RequiresHedge reports whether f belongs, anywhere on this page, to a
// projection — and therefore may not be stated in prose without a hedge
// marker in the same sentence.
func (l *ScopedLedger) RequiresHedge(f float64) bool {
	return l.projection[f] || l.projection[round(f, 1)] || l.projection[round(f, 2)]
}

// HasProjection reports whether this scope contains any projection at all.
// When false (no model, or the reader has not run one yet), the hedge rule
// is vacuously satisfied: there is nothing on the page yet that could be
// misnarrated as a finding.
func (l *ScopedLedger) HasProjection() bool { return l.hasProjection }

// numberRe, round, and excerpt are declared in verify.go, in this same
// package. Reused here rather than reimplemented so the two verifiers agree
// on what a "number" is; not touching verify.go itself.

// BuildScopedLedger assembles the ledger for a chat scoped to brief (always)
// and, when the reader has run the tier-2 model, eval — the EvalResult for
// their current assumptions. eval is nil when the policy has no model or the
// reader has not run it yet; the ledger is then reach-and-evidence only.
func BuildScopedLedger(brief *experiment.Brief, eval *experiment.EvalResult) *ScopedLedger {
	l := newScopedLedger()
	if brief == nil {
		return l
	}

	if brief.Reach != nil {
		ledgerText(l, brief.Reach.Rule.Vintage, brief.Reach.Rule.GeoVintage,
			brief.Reach.Rule.RuleID, brief.Reach.Rule.AuthorityKey)
		for _, v := range brief.Reach.Totals {
			ledgerValue(l, v)
		}
		for _, a := range brief.Reach.Areas {
			ledgerText(l, a.GEOID, a.Name)
			if a.DesignatedShare != nil {
				l.put(*a.DesignatedShare, OriginMeasurement)
			}
			for _, v := range a.Values {
				ledgerValue(l, v)
			}
		}
		for _, id := range brief.Reach.DesignatedIDs {
			ledgerText(l, id)
		}
		l.put(float64(len(brief.Reach.DesignatedIDs)), OriginStructural)
		l.put(float64(len(brief.Reach.Areas)), OriginStructural)
		for _, s := range brief.Reach.Sources {
			ledgerText(l, s.Name, s.Vintage, s.URL)
		}
	}

	l.put(float64(len(brief.Evidence)), OriginStructural)
	for _, e := range brief.Evidence {
		ledgerText(l, e.Jurisdiction, e.PeriodStart, e.PeriodEnd, e.SourceYear,
			e.AccessedDate, e.EffectReported, e.Citation, e.SourceTitle, e.SourceOrg,
			e.Outcome, e.Intervention, e.Finding, e.EvidenceID)
	}

	ledgerText(l, brief.Policy.PolicyID, brief.Policy.Title, brief.Policy.Description,
		brief.Policy.ClaimEmpirical, brief.Policy.BillReferences, brief.Policy.Candidate,
		brief.Policy.Office, brief.Geography.Name, brief.Geography.GEOID)
	for _, s := range brief.Sources {
		ledgerText(l, s.Name, s.Vintage, s.URL)
	}

	if eval != nil {
		ledgerText(l, eval.GEOID, eval.GeoName, eval.Level, eval.ModelVersion, eval.PolicyID)
		for _, p := range eval.Params {
			l.put(p.Num, OriginParamEcho)
			ledgerText(l, p.ID)
		}
		for _, v := range eval.Inputs {
			ledgerValue(l, v)
		}
		for _, v := range eval.Outputs {
			ledgerValue(l, v)
		}
		for _, st := range eval.Steps {
			for _, op := range st.Operands {
				ledgerValue(l, op)
			}
			ledgerValue(l, st.Result)
		}
		for _, s := range eval.Sources {
			ledgerText(l, s.Name, s.Vintage, s.URL)
		}
	}

	return l
}

// ledgerValue registers one experiment.Value at the class its OWN taint
// resolves to — never a class this package decides. ScopedLedger reads
// experiment.Value.ClaimClass(); it does not recompute or guess it, for the
// exact reason types.go computes that method instead of storing a field:
// the only way to get it wrong here would be to ignore what the value
// itself already says.
func ledgerValue(l *ScopedLedger, v experiment.Value) {
	ledgerText(l, v.Vintage, v.MissingDetail)
	if v.Num == nil {
		return // missing values carry no number; nothing to allow or guard
	}
	origin := OriginMeasurement
	if v.ClaimClass() == experiment.ClassProjection {
		origin = OriginProjection
	}
	l.put(*v.Num, origin)
}

// ledgerText pulls digits out of payload text — vintages, GEOIDs, source
// names, citation years — the same way verify.go's addTextNumbers does for
// the Atlas dataset. These are identifiers and citations, not claims about
// the world, and an answer that names its source or its subject must not
// be flagged for doing so, nor required to hedge a year it merely quoted.
func ledgerText(l *ScopedLedger, fields ...string) {
	for _, s := range fields {
		if s == "" {
			continue
		}
		for _, tok := range numberRe.FindAllString(s, -1) {
			if f, err := strconv.ParseFloat(strings.ReplaceAll(tok, ",", ""), 64); err == nil {
				l.put(f, OriginText)
			}
		}
	}
}

// ── the hedge vocabulary and sentence scoping ──────────────────────

// hedgeMarkers is the closed, positive vocabulary that makes a sentence's
// use of a projection number acceptable. At least one substring must occur
// in the SAME sentence as the number. Deliberately an allow-list, not a
// ban-list: banning "is"/"has" is unworkable ("the total would be" and "the
// total is" are both fine ways to say the same hedge), so the sentence must
// affirmatively carry language this project's own frozen copy already uses
// for tier 2 (contract S9's "explore"/"try your own numbers" framing, S19's
// required disclosure).
var hedgeMarkers = []string{
	"projected", "projection",
	"would be", "would reach", "would cover", "would need", "would cost",
	"assum", // matches assume/assumed/assuming/assumption(s)
	"if you", "if a reader", "if these", "if this",
	"under this", "under your", "under these",
	"based on the numbers", "based on your", "based on these",
	"your own numbers", "your input", "your assumption",
	"this scenario", "this estimate", "not a measurement", "not a finding",
}

func sentenceHasHedge(s string) bool {
	low := strings.ToLower(s)
	for _, m := range hedgeMarkers {
		if strings.Contains(low, m) {
			return true
		}
	}
	return false
}

// sentenceSplitRe finds sentence boundaries: a run of . ! ? followed by
// whitespace or end of string. Requiring whitespace-or-end after the
// punctuation (rather than splitting on every ".") is what keeps a decimal
// number like "3.5" from being cut in half — "." followed immediately by a
// digit never matches.
var sentenceSplitRe = regexp.MustCompile(`[.!?]+(?:\s+|$)`)

func splitSentences(s string) []string {
	idxs := sentenceSplitRe.FindAllStringIndex(s, -1)
	if len(idxs) == 0 {
		return []string{s}
	}
	out := make([]string, 0, len(idxs)+1)
	start := 0
	for _, m := range idxs {
		out = append(out, s[start:m[1]])
		start = m[1]
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

// ── ScopedVerify ────────────────────────────────────────────────────

// ClassViolation is one problem ScopedVerify found in a draft. Kind
// distinguishes the two failure modes: "invented" is the same failure base
// Verify catches (a number the payload does not contain at all);
// "unhedged_projection" is the new one — a real number from the page, but
// one that only exists as a projection, stated in a sentence with no hedge.
type ClassViolation struct {
	Number  string
	Kind    string
	Context string
}

func (v ClassViolation) Error() string {
	switch v.Kind {
	case "unhedged_projection":
		return fmt.Sprintf(
			"number %q is a projection (depends on a reader assumption) but is stated with no hedge in: …%s…",
			v.Number, v.Context)
	default:
		return fmt.Sprintf("number %q is not on this page (…%s…)", v.Number, v.Context)
	}
}

// ScopedVerify extends the ADR-006 guarantee for the policy experiment page.
// It performs the same duty base Verify does — every number in the draft
// must be a number the payload actually contains — and adds the one this
// track exists to add: every number that is a projection anywhere on the
// page must be stated in a sentence carrying an explicit hedge marker.
//
// See the package doc comment at the top of this file for exactly what this
// does and does not catch.
func ScopedVerify(prose string, ledger *ScopedLedger) []ClassViolation {
	var out []ClassViolation
	for _, sentence := range splitSentences(prose) {
		hedge := sentenceHasHedge(sentence)
		for _, loc := range numberRe.FindAllStringIndex(sentence, -1) {
			tok := sentence[loc[0]:loc[1]]
			f, err := strconv.ParseFloat(strings.ReplaceAll(tok, ",", ""), 64)
			if err != nil {
				continue
			}
			if !ledger.IsAllowed(f) {
				out = append(out, ClassViolation{
					Number: tok, Kind: "invented", Context: excerpt(sentence, loc[0], loc[1]),
				})
				continue
			}
			if ledger.RequiresHedge(f) && !hedge {
				out = append(out, ClassViolation{
					Number: tok, Kind: "unhedged_projection", Context: excerpt(sentence, loc[0], loc[1]),
				})
			}
		}
	}
	return out
}

// ── the deterministic floor ─────────────────────────────────────────

// ScopedFacts is the deterministic floor for a policy-scoped answer, in the
// same role facts.go's renderFacts plays for the base engine: it renders
// directly from brief and eval, with no model and no template a model could
// bypass, so it ships whenever no model lane is configured or a model's
// draft fails ScopedVerify. A feature whose only output path runs through a
// model has no floor (task requirement); this is that floor.
//
// Every projection sentence this function writes carries its hedge BY
// CONSTRUCTION — the sentence template itself is the hedge, chosen once,
// here, rather than composed per-answer. There is no code path in this
// function that can print a projection number without going through
// writeOutputSentence's projection branch, the same way there is no path in
// types.go that can build a Value with a false claim_class: the guarantee
// is structural, not a discipline applied while writing this function.
//
// grounding_test.go's TestScopedFactsPassesItsOwnGuarantee is the check that
// this claim is true: it feeds this function's own output back through
// ScopedVerify and requires zero violations. A floor that failed its own
// verifier would be a worse bug than an unhelpful floor.
func ScopedFacts(brief *experiment.Brief, eval *experiment.EvalResult) string {
	if brief == nil {
		return "No policy is loaded for this scope."
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s — %s.\n", brief.Policy.PolicyID, brief.Geography.Name)

	if brief.Reach != nil {
		for _, v := range brief.Reach.Totals {
			writeMeasuredSentence(&b, v)
		}
	} else {
		b.WriteString("This policy has no measured reach for this geography yet.\n")
	}

	if eval != nil {
		for _, o := range eval.Outputs {
			writeOutputSentence(&b, o)
		}
	} else if brief.Model != nil {
		b.WriteString("You have not tried your own numbers yet. The outputs above would change if you did, and none of them are shown until you do.\n")
	}

	if n := len(brief.Evidence); n > 0 {
		fmt.Fprintf(&b, "%d external finding(s) are cited for this policy. PDI did not compute them; each carries its own source.\n", n)
	}

	for _, a := range brief.Absences {
		fmt.Fprintf(&b, "Not available for tier %s: %s.\n", a.Tier, a.Reason)
	}

	return strings.TrimSpace(b.String())
}

func writeMeasuredSentence(b *strings.Builder, v experiment.Value) {
	if v.Num == nil {
		fmt.Fprintf(b, "%s: no published figure (%s).\n", v.ID, v.Missing)
		return
	}
	if v.Vintage != "" {
		fmt.Fprintf(b, "Measured: %s is %s %s (%s).\n", v.ID, formatNum(*v.Num, v.Precision), unitWord(v.Unit), v.Vintage)
		return
	}
	fmt.Fprintf(b, "Measured: %s is %s %s.\n", v.ID, formatNum(*v.Num, v.Precision), unitWord(v.Unit))
}

func writeOutputSentence(b *strings.Builder, v experiment.Value) {
	if v.Num == nil {
		fmt.Fprintf(b, "%s: not computable (%s).\n", v.ID, v.Missing)
		return
	}
	if v.ClaimClass() == experiment.ClassProjection {
		fmt.Fprintf(b, "Projected: if these assumptions hold, %s would be about %s %s. This is not a measurement — it depends on the numbers entered.\n",
			v.ID, formatNum(*v.Num, v.Precision), unitWord(v.Unit))
		return
	}
	fmt.Fprintf(b, "Measured: %s is %s %s.\n", v.ID, formatNum(*v.Num, v.Precision), unitWord(v.Unit))
}

func formatNum(v float64, precision int) string {
	if precision < 0 || precision > 2 {
		precision = 0
	}
	return strconv.FormatFloat(v, 'f', precision, 64)
}

func unitWord(u experiment.Unit) string {
	switch u {
	case experiment.UnitPeople:
		return "people"
	case experiment.UnitHouseholds:
		return "households"
	case experiment.UnitUSD:
		return "dollars"
	case experiment.UnitUSDPerYear:
		return "dollars per year"
	case experiment.UnitPercentPoints:
		return "percentage points"
	case experiment.UnitShare:
		return "(share, 0-1)"
	case experiment.UnitMiles:
		return "miles"
	case experiment.UnitDays:
		return "days"
	case experiment.UnitRatio:
		return "(ratio)"
	default:
		return ""
	}
}

// ── the model lane ──────────────────────────────────────────────────

// ScopedComposer turns a policy+geography payload into prose. Its draft is
// verified by ScopedVerify before use; a failing draft is discarded in
// favour of ScopedFacts, exactly as base Composer/Verify degrade to
// Result.Facts.
type ScopedComposer interface {
	ComposeScoped(ctx context.Context, question string, brief *experiment.Brief, eval *experiment.EvalResult) (string, error)
	Name() string
}

// ScopedComposerPrompt constrains the model's draft. It is a REQUEST;
// ScopedVerify is the GUARANTEE — this package treats the prompt as
// advisory for the same reason ADR-006 does: cheap, mechanical enforcement
// beats an unenforced instruction, and the prompt exists to make the model
// less likely to be rejected, not to be trusted on its own.
const ScopedComposerPrompt = `You answer a reader's question about ONE policy on ONE page, using only
the JSON payload you are given: the page's measured tier-1 reach figures,
and, if present, the reader's own tier-2 projection for the assumptions
they entered.

Absolute rules:
- Every number in your answer must appear in the payload. Do not round,
  convert units, or introduce a comparison figure that is not there.
- The payload distinguishes MEASURED figures from PROJECTED ones (a
  projection depends on the reader's own assumptions and is never a
  finding). A measured figure may be stated as fact. A PROJECTED FIGURE MAY
  NEVER BE STATED AS A FACT: every sentence containing a projected number
  must also say, in that same sentence, that it depends on the assumptions
  entered — use language like "projected", "if these assumptions hold",
  "under this scenario", or "based on the numbers entered". A sentence with
  a projected number and no such language will be rejected.
- Never say the policy caused, would cause, reduces, or improves an
  outcome. This data has no time series and supports no causal claim about
  the real world.
- Never introduce a place, study, or figure that is not in the payload's
  evidence list.
- Two or three sentences. Say where the numbers came from.`

// ComposeScoped implements ScopedComposer on the existing ChatModel lane
// (OpenRouter/Ollama, planner.go), so the scoped chat needs no new model
// client — only a new prompt and a new verifier.
func (m *ChatModel) ComposeScoped(ctx context.Context, question string, brief *experiment.Brief, eval *experiment.EvalResult) (string, error) {
	payload, err := json.Marshal(struct {
		Brief *experiment.Brief      `json:"brief"`
		Eval  *experiment.EvalResult `json:"eval,omitempty"`
	}{brief, eval})
	if err != nil {
		return "", err
	}
	user := "Question: " + question + "\n\nPage payload JSON:\n" + string(payload)
	return m.complete(ctx, ScopedComposerPrompt, user, 400)
}

// ScopedEngine composes a chat answer for one policy+geography scope. It is
// the S19 "explain this in plain language" action's engine, and it also
// answers a free-form question over the same scope — the mechanism (compose,
// class-verify, fall back to the deterministic floor) does not care which;
// see the note on contract S19 in this package's return-value writeup.
type ScopedEngine struct {
	Composer ScopedComposer
}

// AnswerSource values are shared with the base engine (answer.go) — the
// same three states apply here: deterministic (no model, or none
// configured), model-verified, or model-rejected-fell-back.

// ScopedAnswer mirrors Answer's shape for the same audit reason: a caller
// must be able to tell a model-written sentence from a generated one, and
// see exactly which numbers or which sentences were rejected and why.
type ScopedAnswer struct {
	Question   string           `json:"question"`
	Text       string           `json:"text"`
	Source     AnswerSource     `json:"source"`
	Violations []ClassViolation `json:"violations,omitempty"`
}

// Answer runs compose -> ScopedVerify -> fallback for one question about one
// policy+geography scope. brief is required; eval is nil when the reader has
// not run the tier-2 model (or the policy has none) — the ledger then
// contains no projections, RequiresHedge is vacuously false for every
// number, and the hedge rule never fires, because there is nothing on the
// page yet that could be misnarrated as a finding.
func (e *ScopedEngine) Answer(ctx context.Context, question string, brief *experiment.Brief, eval *experiment.EvalResult) *ScopedAnswer {
	floor := ScopedFacts(brief, eval)
	ans := &ScopedAnswer{Question: question, Text: floor, Source: SourceDeterministic}

	if e.Composer == nil || brief == nil {
		return ans
	}

	draft, err := e.Composer.ComposeScoped(ctx, question, brief, eval)
	if err != nil || strings.TrimSpace(draft) == "" {
		// A composer failure is not an answer failure: the deterministic
		// floor is already correct and already carries its hedges.
		return ans
	}

	ledger := BuildScopedLedger(brief, eval)
	if v := ScopedVerify(draft, ledger); len(v) > 0 {
		ans.Violations = v
		ans.Source = SourceModelRejected
		return ans
	}

	ans.Text = strings.TrimSpace(draft)
	ans.Source = SourceModelVerified
	return ans
}
