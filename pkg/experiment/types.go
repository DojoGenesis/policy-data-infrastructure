// Package experiment holds the type vocabulary of the policy experiment page:
// the per-policy model file, the closed expression language its outputs are
// written in, and the shape of an evaluated result.
//
// This package declares types only. Evaluation lives elsewhere; the contract it
// must satisfy is docs/contracts/experiment-contract.md and the decision behind
// it is ADR-016.
//
// # The constraint the whole design is built around
//
// PDI holds exactly one vintage per source for a live county — ACS-2024-5yr,
// CDC PLACES 2022, USDA FARA 2019. The data is cross-sectional. There is no
// before and after anywhere in it, so nothing computed here can show that a
// policy caused an outcome, and no arrangement of these types can be made to
// say so. That is why Value carries a single Vintage string and not a slice:
// the absence of a time axis is expressed as the absence of a field, where an
// author who wanted to imply a trend would have to add one and be seen doing it.
//
// # Three tiers, and what each is allowed to claim
//
//	REACH       who a policy's own eligibility rule covers, where, how many.
//	            PDI measures this. Every value is a MEASUREMENT.
//	EXPERIMENT  the user sets assumptions; PDI does arithmetic over real
//	            denominators. Every output is a PROJECTION.
//	EVIDENCE    what happened where the policy was actually tried, from external
//	            published sources, cited. PDI never computes it.
//
// # The taint invariant
//
// Every value carries a provenance Kind and resolves to an epistemic
// ClaimClass. A derived value with ANY assumed input is a projection, and
// nothing downstream can demote it back to a measurement.
//
// This is enforced by arithmetic rather than by discipline. ClaimClass is not a
// field on Value — there is no field to set wrongly, and no JSON payload can
// carry a class that disagrees with its own inputs. It is a method over
// AssumedInputs, and the wire form is produced from that method. A privately
// constructed invariant is undone by the first Deserialize somebody generates;
// an invariant that must be re-derived from the value's own input list survives
// serialization, because faking it means faking the input list too, and the
// input list is checkable against the model file that produced it.
//
// # Missing is missing
//
// A missing input propagates as MISSING with a stated reason. Never zero, never
// infinity, never a silent drop. This repository has shipped a sentinel-zero
// bug that faked a $2.6M spending trend out of placeholder dollars, and a
// projection built on a silent zero is the same failure with a user's own
// assumptions attached to it.
package experiment

import (
	"encoding/json"
	"fmt"
	"time"
)

// SchemaVersion is the model-file format this package understands. A model file
// declaring anything else is refused rather than best-effort parsed: the cost of
// a misread coefficient is a wrong number on a public page.
const SchemaVersion = "1"

// Tier names which of the three epistemic layers a thing belongs to. It is a
// property of the claim, not of the page layout — an output cannot be moved
// between tiers by rendering it somewhere else.
type Tier string

const (
	// TierReach — measured coverage of a policy's own eligibility rule.
	TierReach Tier = "reach"
	// TierExperiment — deterministic arithmetic over user-set assumptions.
	TierExperiment Tier = "experiment"
	// TierEvidence — external published findings, cited, never computed here.
	TierEvidence Tier = "evidence"
)

// Kind is where a number came from. It is descriptive: it tells a reader which
// of three very different things they are looking at.
//
// ClaimClass is the load-bearing one — Kind can be read as a label, ClaimClass
// governs what the page is permitted to say.
type Kind string

const (
	// KindMeasured — a published figure from a loaded dataset, carrying a
	// vintage and (where the source publishes one) a margin of error.
	KindMeasured Kind = "measured"
	// KindAssumed — a number a human chose. Every slider position is this, and
	// so is every literal in a model file that is not a cited published
	// constant.
	KindAssumed Kind = "assumed"
	// KindDerived — the result of an operation over other values. Derived says
	// nothing about trustworthiness on its own; its ClaimClass does.
	KindDerived Kind = "derived"
)

// ClaimClass is the epistemic status of a value: what a page is allowed to say
// about it out loud.
//
// The set has exactly two members and there is no third. "Estimate",
// "modeled", "projected measurement" and similar middle terms are precisely
// the vocabulary that lets a projection be read as a finding, so the type
// refuses to offer one.
type ClaimClass string

const (
	// ClassMeasurement — every input is measured. The page may state it as a
	// fact about the world, with its vintage and source.
	ClassMeasurement ClaimClass = "measurement"
	// ClassProjection — at least one input was assumed. The page may state it
	// only as the arithmetic consequence of assumptions the user can see and
	// change. It is never a finding, and it is never evidence of an effect.
	ClassProjection ClaimClass = "projection"
)

// Join is the taint lattice: measurement ⊑ projection, and projection absorbs.
//
// There is deliberately no inverse. Nothing in this package lowers a
// ClaimClass, which is what makes the taint total over a closed operation set —
// every operation has children, every child's class is joined, and an operation
// that could clear the taint would have to be added by hand and reviewed.
func Join(a, b ClaimClass) ClaimClass {
	if a == ClassProjection || b == ClassProjection {
		return ClassProjection
	}
	return ClassMeasurement
}

// MissingReason says why a value has no number. It is required whenever a value
// is missing, and it is a closed set so the page can translate it rather than
// print an English sentence a backend author happened to write.
//
// The distinction ADR-014 D2 draws — "no data loaded" is not the same as
// "cannot be computed defensibly" — is preserved here as separate members. A
// reader who cannot tell a gap from a refusal learns the wrong lesson from
// both.
type MissingReason string

const (
	// MissingNone is the zero value: the value is present.
	MissingNone MissingReason = ""
	// MissingNotLoaded — the platform holds no rows for this variable at this
	// geography. A gap.
	MissingNotLoaded MissingReason = "not_loaded"
	// MissingSuppressed — the publisher withheld it (small-population
	// suppression). A gap in the source, not in PDI.
	MissingSuppressed MissingReason = "suppressed"
	// MissingWithheld — PDI can compute something here and refuses to publish
	// it because it would not be defensible (ADR-014 D2). A refusal.
	MissingWithheld MissingReason = "withheld"
	// MissingBelowCoverage — an aggregate whose input coverage did not clear
	// the recorded threshold (ADR-014 D7, ADR-015).
	MissingBelowCoverage MissingReason = "below_coverage_threshold"
	// MissingDivideByZero — a division whose denominator resolved to zero.
	// Never Inf, never a dropped term, never a substituted 1.
	MissingDivideByZero MissingReason = "division_by_zero"
	// MissingNonFinite — arithmetic produced NaN or ±Inf. JSON cannot carry
	// these, and a page should not either.
	MissingNonFinite MissingReason = "non_finite"
	// MissingUnresolvedRef — a ref names something the evaluation context does
	// not contain. A model-authoring bug; it must surface, not degrade.
	MissingUnresolvedRef MissingReason = "unresolved_ref"
	// MissingUpstream — at least one input to this operation was missing. The
	// originating reason travels in Value.MissingDetail so a reader can reach
	// the actual cause without walking the tree.
	MissingUpstream MissingReason = "upstream_missing"
	// MissingNoModel — the policy has no model file, so tier 2 does not exist
	// for it. Not an error; most of the 70 positions are in this state.
	MissingNoModel MissingReason = "no_model"
	// MissingGeoLevelUnsupported — the model does not declare the requested
	// geographic level.
	MissingGeoLevelUnsupported MissingReason = "geo_level_unsupported"
)

// Register is the disclosure layer a field belongs to. The page shows the
// public register by default and expands into the analyst register on demand.
//
// The expansion is strictly additive: analyst is a superset of public, and no
// figure changes value when a reader expands it. A page where expanding a panel
// changes the number is a page that was lying in one of the two states.
type Register string

const (
	// RegPublic — shown without interaction. Plain language, no MOE, no CI.
	RegPublic Register = "public"
	// RegAnalyst — shown when the reader expands. Adds margins of error,
	// intervals, coverage, denominators, vintages, and the arithmetic trace.
	RegAnalyst Register = "analyst"
)

// Reliability is a measured value's sampling reliability, banded so the public
// register can say it in words and the analyst register can show the numbers
// behind it.
//
// The bands are the Census Bureau's own convention for coefficient of
// variation, used because a threshold PDI invented would have to be defended
// and this one is already defended. CV = (MOE / 1.645) / |estimate| × 100.
type Reliability string

const (
	// RelHigh — CV ≤ 15%.
	RelHigh Reliability = "high"
	// RelMedium — 15% < CV ≤ 30%.
	RelMedium Reliability = "medium"
	// RelLow — CV > 30%. The public register says so in words rather than
	// printing a number the reader has no way to discount.
	RelLow Reliability = "low"
	// RelUnknown — no published MOE, or an estimate of zero. Distinct from
	// high: not knowing the error is not the same as knowing it is small.
	RelUnknown Reliability = "unknown"
)

// Unit is the closed set of units this page can render and translate.
//
// The two percent-shaped members exist separately on purpose. UnitPercentPoints
// is 0–100 and UnitShare is 0–1, and every value declares which it is, because
// a factor-of-100 error is invisible in a payload and obvious on a page only
// after somebody has already screenshotted it.
type Unit string

const (
	UnitCount         Unit = "count"
	UnitPeople        Unit = "people"
	UnitHouseholds    Unit = "households"
	UnitUSD           Unit = "usd"
	UnitUSDPerYear    Unit = "usd_per_year"
	UnitPercentPoints Unit = "percent_points"
	UnitShare         Unit = "share"
	UnitMiles         Unit = "miles"
	UnitDays          Unit = "days"
	UnitRatio         Unit = "ratio"
)

// Op is the complete set of operations a model file may use.
//
// It is closed for the same reason grounding.Operation is closed: a closed set
// lets an unrepresentable model be refused instead of approximated, and here it
// buys a second thing — the taint is provably total. Eight operations, each of
// which either has children (and therefore joins their classes) or is a leaf
// (and therefore declares its own). There is no path by which a value acquires
// a number without acquiring a class alongside it.
//
// There is no general expression parser and no string is ever evaluated. A
// model file that needs an operation not on this list is a request to extend
// this list in review, not a reason to add an escape hatch.
type Op string

const (
	// OpRef — the value of a named input. See the ref namespace on Expr.Ref.
	OpRef Op = "ref"
	// OpConst — a literal from the model file.
	OpConst Op = "const"
	// OpAdd — sum of all terms.
	OpAdd Op = "add"
	// OpSub — first term minus every subsequent term.
	OpSub Op = "sub"
	// OpMul — product of all terms.
	OpMul Op = "mul"
	// OpDiv — first term divided by every subsequent term. A zero divisor
	// yields MissingDivideByZero, never Inf and never a dropped term.
	OpDiv Op = "div"
	// OpMin — smallest present term.
	OpMin Op = "min"
	// OpMax — largest present term.
	OpMax Op = "max"
)

// Expr is one node of a model's arithmetic. Exactly one shape is valid per Op:
// ref carries Ref, const carries Const (and its Kind), and every other Op
// carries Terms.
//
// Nothing here is a string to be parsed at evaluation time. The tree is the
// program.
type Expr struct {
	// Op selects the node shape. Any value outside the declared set is a
	// refusal, not a default.
	Op Op `json:"op"`

	// Ref names an input, in a namespaced form so the evaluator never has to
	// guess where a name resolves and two model authors cannot collide:
	//
	//	param:<param_id>          a user-set assumption      → KindAssumed
	//	indicator:<variable_id>   a loaded indicator          → KindMeasured
	//	reach:<field_id>          a tier-1 measured field     → KindMeasured
	//	output:<output_id>        an earlier output, same model
	//
	// An output ref may only name an output declared before it in
	// PolicyModel.Outputs. Forward and cyclic references are model-authoring
	// errors, refused at load, not resolved at evaluation.
	Ref string `json:"ref,omitempty"`

	// Const is a literal. Present only when Op is OpConst.
	Const *float64 `json:"const,omitempty"`

	// ConstKind classifies a literal, and defaults to KindAssumed when
	// omitted. This default is the safe direction: an unclassified number
	// somebody typed into a JSON file is an assumption until a source says
	// otherwise, and the failure mode of the other default is a projection
	// that presents as a measurement.
	//
	// KindMeasured requires a non-empty ConstSource — a published constant
	// with nowhere to check it is an assumption wearing a source's clothes.
	ConstKind Kind `json:"const_kind,omitempty"`

	// ConstSource cites a measured literal: the publisher, the document, the
	// figure. Required when ConstKind is KindMeasured, ignored otherwise.
	ConstSource string `json:"const_source,omitempty"`

	// Terms are the children of an operation node. Present for every Op except
	// OpRef and OpConst; empty Terms on an operation node is a model error.
	Terms []Term `json:"terms,omitempty"`
}

// Term is a child of an operation, with the label the analyst register uses to
// show the arithmetic as a sentence a person can check by hand.
//
// It exists because an unlabeled expression tree renders as a number with no
// derivation, and the derivation is the entire argument for trusting a
// projection: the reader is not being asked to believe the output, they are
// being shown the multiplication and invited to disagree with an input.
type Term struct {
	// Expr is the child node.
	Expr Expr `json:"expr"`
	// LabelKey is the i18n key naming this term in the arithmetic trace, e.g.
	// "pex.term.eligible_households". Empty means the term is rendered by its
	// referent's own label.
	LabelKey string `json:"label_key,omitempty"`
}

// Param is one assumption the user sets. It is the only place a number enters
// the arithmetic that did not come from a dataset.
//
// Every field that governs the control is declared here rather than inferred by
// the frontend, because a slider whose bounds are chosen at render time is a
// slider whose bounds two implementations will disagree about.
type Param struct {
	// ID is stable and unique within the model, snake_case. It is also the
	// linking key in comparison mode: two policies whose models declare the
	// same ID and the same Unit are driven by one control.
	ID string `json:"id"`

	// LabelKey is the i18n key for the control's label.
	LabelKey string `json:"label_key"`

	// HelpKey is the i18n key for the one-sentence explanation of what the
	// reader is being asked to decide.
	HelpKey string `json:"help_key"`

	// Unit is what the number means. Required — a bare number on a slider is a
	// number two readers will read differently.
	Unit Unit `json:"unit"`

	// Min, Max and Step bound the control. A submitted value outside
	// [Min, Max] is rejected, never clamped: clamping silently substitutes a
	// different assumption than the one the reader made and then attributes
	// the result to them.
	Min  float64 `json:"min"`
	Max  float64 `json:"max"`
	Step float64 `json:"step"`

	// Default is the position the control opens at. It is authored by a human
	// in the model file and it is never derived from data and never proposed
	// by a language model.
	Default float64 `json:"default"`

	// DefaultSourceKey is required and says where Default came from — a
	// published figure, or an explicitly arbitrary starting point. A default
	// with no stated basis is a recommendation the page is making without
	// admitting it.
	DefaultSourceKey string `json:"default_source_key"`

	// Precision is decimal places for display, 0–2. The bound is not
	// cosmetic: pkg/grounding.Verify registers result values at 0, 1 and 2
	// decimals, so a figure rendered at 3 would be rejected as unsupported in
	// any generated prose that quoted the page correctly.
	Precision int `json:"precision"`
}

// Output is one number the model produces. Outputs are always projections when
// any input is assumed, which for a tier-2 model is the normal case.
type Output struct {
	// ID is stable and unique within the model, snake_case. Also the target of
	// an "output:<id>" ref.
	ID string `json:"id"`

	// LabelKey is the i18n key for the output's label.
	LabelKey string `json:"label_key"`

	// Unit is declared, never inferred. This package does no unit algebra:
	// dividing dollars by people does not silently become usd_per_person here.
	// A mismatch between an output's declared unit and its arithmetic is
	// caught by model lint at author time, where a person can fix it, rather
	// than papered over at runtime where nobody sees it.
	Unit Unit `json:"unit"`

	// Precision is decimal places for display, 0–2. See Param.Precision.
	Precision int `json:"precision"`

	// Register is the disclosure layer this output first appears in. Public
	// outputs are the two or three numbers the page is actually about;
	// intermediate quantities belong in the analyst register.
	Register Register `json:"register"`

	// Expr is the arithmetic.
	Expr Expr `json:"expr"`
}

// PolicyModel is a per-policy tier-2 model, one JSON file at
// data/policies/models/<policy_id>.json. A policy with no such file has no
// tier 2, which is the state of nearly all of the 70 positions and is not an
// error.
type PolicyModel struct {
	// SchemaVersion must equal the package SchemaVersion constant.
	SchemaVersion string `json:"schema_version"`

	// PolicyID must match a record in data/policies/*.csv. It is also the
	// filename stem, so a mismatch is a load-time refusal rather than a file
	// that quietly models the wrong policy.
	PolicyID string `json:"policy_id"`

	// ModelVersion is bumped whenever any Expr, Param bound or Default
	// changes. It travels in every EvalResult and in every share link, so a
	// link produced before a revision cannot silently render different numbers
	// after one.
	ModelVersion string `json:"model_version"`

	// GeoLevels are the levels this model may be evaluated at, e.g.
	// ["county", "state"]. A request for any other level yields
	// MissingGeoLevelUnsupported rather than an evaluation at a level the
	// denominators do not support.
	GeoLevels []string `json:"geo_levels"`

	// DefaultGEOID is the geography used when the request carries none. The
	// response always says which geography it used and why, so a reader is
	// never shown a default place they believe they asked for.
	DefaultGEOID string `json:"default_geoid"`

	// TitleKey and SummaryKey are i18n keys for the model's own framing — what
	// this arithmetic is and is not.
	TitleKey   string `json:"title_key"`
	SummaryKey string `json:"summary_key"`

	// Params are the assumptions, in display order.
	Params []Param `json:"params"`

	// Outputs are the results, in display and evaluation order. An
	// "output:<id>" ref may only name an earlier entry.
	Outputs []Output `json:"outputs"`

	// RefusalKeys are i18n keys for the specific things this model may not be
	// read as saying — the sentences a reader would otherwise supply for
	// themselves. Required and non-empty: a model that cannot name what it
	// does not show has not been thought through.
	RefusalKeys []string `json:"refusal_keys"`

	// CaveatKeys are i18n keys for standing caveats on this model's inputs,
	// such as a vintage collision between a designation and a population count.
	CaveatKeys []string `json:"caveat_keys,omitempty"`

	// AuthoredBy, ReviewedBy and ReviewedDate are the human record. A model
	// with no reviewer does not ship; the arithmetic is easy and the
	// assumptions are the whole argument.
	AuthoredBy   string `json:"authored_by"`
	ReviewedBy   string `json:"reviewed_by"`
	ReviewedDate string `json:"reviewed_date"`
}

// Value is a single number on its way to a page, with everything needed to say
// what it is and what it is not.
//
// Note what is absent: there is no field for a second vintage, no prior value,
// no change and no percent change. The analysis schema has all four because a
// county analysis can span vintages; this page cannot, and giving it the fields
// would be an invitation.
type Value struct {
	// ID is stable within the result — an output id, a param id, or a ref.
	ID string `json:"id"`

	// LabelKey is the i18n key for this value's label. The server does not
	// send display prose; see the i18n rule in the contract.
	LabelKey string `json:"label_key"`

	// Num is the number, or nil when missing. The biconditional is a hard
	// invariant checkable in one line by anybody:
	//
	//	Num == nil  ⟺  Missing != MissingNone
	//
	// A missing value never carries zero. Zero is a real quantity and this
	// repository has already shipped a page whose dollar trend was made of
	// placeholder zeroes.
	Num *float64 `json:"value"`

	// Unit is what Num means.
	Unit Unit `json:"unit"`

	// Precision is decimal places for display, 0–2.
	Precision int `json:"precision"`

	// Kind is where the number came from.
	Kind Kind `json:"kind"`

	// Tier is which epistemic layer this value belongs to.
	Tier Tier `json:"tier"`

	// Register is the disclosure layer this value first appears in.
	Register Register `json:"register"`

	// Missing states why there is no number, and is MissingNone when there is
	// one.
	Missing MissingReason `json:"missing_reason"`

	// MissingDetail carries the originating reason when Missing is
	// MissingUpstream, and the id of the input that caused it. It exists so a
	// reader chasing a blank does not have to walk the expression tree.
	MissingDetail string `json:"missing_detail,omitempty"`

	// AssumedInputs are the sorted, deduplicated param ids and assumed
	// constant ids this value descends from. This field IS the taint: it is
	// what ClaimClass is computed from, and it is what a third party
	// re-derives to check that a payload is honest.
	AssumedInputs []string `json:"assumed_inputs"`

	// MeasuredInputs are the sorted, deduplicated variable ids and reach field
	// ids this value descends from. Used by the analyst register to list the
	// data behind a figure.
	MeasuredInputs []string `json:"measured_inputs"`

	// Vintage is the single vintage of a measured leaf, e.g. "ACS-2024-5yr".
	// A derived value carries the empty string when its measured leaves
	// disagree, and the disagreement is reported in the result's provenance
	// rather than resolved by picking one.
	Vintage string `json:"vintage,omitempty"`

	// MOE is the 90% margin of error, and is present only on measured leaves.
	//
	// It is deliberately nil on every derived value that has any assumed
	// input. A margin of error describes sampling error in an estimate of
	// something that exists; multiplied by an assumption it describes the
	// sampling error of a hypothetical, which is not a quantity. Publishing an
	// interval around a projection would dress the least certain number on the
	// page as the best characterized one.
	MOE *float64 `json:"moe,omitempty"`

	// Reliability bands MOE against Num. See the Reliability constants.
	Reliability Reliability `json:"reliability,omitempty"`

	// SourceIDs are keys into data/sources.toml for every dataset behind this
	// value.
	SourceIDs []string `json:"source_ids,omitempty"`
}

// ClaimClass resolves this value's epistemic status from its own inputs.
//
// This is a method and not a field on purpose. A stored class can be set wrong
// by any code path that constructs a Value, and a private constructor does not
// help because the first generated Deserialize walks around it. Computing it
// means the only way to publish a projection labeled as a measurement is to
// also publish a false input list — and the input list is checkable against the
// model file that produced the value.
//
// The wire form is emitted from this method by MarshalJSON, and UnmarshalJSON
// rejects any payload whose claim_class disagrees with its assumed_inputs. The
// biconditional a verifier checks is exactly:
//
//	claim_class == "projection"  ⟺  len(assumed_inputs) > 0
func (v Value) ClaimClass() ClaimClass {
	if len(v.AssumedInputs) > 0 {
		return ClassProjection
	}
	return ClassMeasurement
}

// valueWire exists only to give MarshalJSON and UnmarshalJSON a form of Value
// with no methods, so they do not recurse.
type valueWire Value

// MarshalJSON emits the computed claim_class alongside the stored fields. There
// is no code path that emits a stored one, because there is no stored one.
func (v Value) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		valueWire
		ClaimClass ClaimClass `json:"claim_class"`
	}{valueWire(v), v.ClaimClass()})
}

// UnmarshalJSON restores a Value and refuses a payload that lies about its own
// taint. A claim_class of "measurement" over a non-empty assumed_inputs is
// exactly the corruption this design exists to prevent, so it is an error and
// not a warning.
func (v *Value) UnmarshalJSON(b []byte) error {
	var w struct {
		valueWire
		ClaimClass ClaimClass `json:"claim_class"`
	}
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	out := Value(w.valueWire)
	if w.ClaimClass != "" && w.ClaimClass != out.ClaimClass() {
		return fmt.Errorf(
			"experiment: value %q declares claim_class %q but its %d assumed input(s) resolve to %q",
			out.ID, w.ClaimClass, len(out.AssumedInputs), out.ClaimClass())
	}
	if (out.Num == nil) != (out.Missing != MissingNone) {
		present := "absent"
		if out.Num != nil {
			present = fmt.Sprintf("%g", *out.Num)
		}
		return fmt.Errorf(
			"experiment: value %q violates the missing invariant: value=%s missing_reason=%q",
			out.ID, present, out.Missing)
	}
	*v = out
	return nil
}

// ParamSource says whether a bound assumption came from the reader or from the
// model file's authored default. The page says which, because "you chose this"
// and "we opened here" are different statements about the same number.
type ParamSource string

const (
	// SrcUser — the reader set it.
	SrcUser ParamSource = "user"
	// SrcDefault — the request omitted it and the model's Default was used.
	SrcDefault ParamSource = "default"
)

// ParamBinding is one assumption as actually evaluated.
type ParamBinding struct {
	ID        string      `json:"id"`
	LabelKey  string      `json:"label_key"`
	Num       float64     `json:"value"`
	Unit      Unit        `json:"unit"`
	Precision int         `json:"precision"`
	Source    ParamSource `json:"source"`
	// Linked is true in comparison mode when this param is driven by a shared
	// control across both policies. It is decided by the server so two
	// frontends cannot decide it differently.
	Linked bool `json:"linked"`
}

// Step is one line of the arithmetic trace shown in the analyst register.
//
// The trace is the argument. A projection with a hidden derivation asks for
// trust; a projection with a visible one asks for disagreement, which is the
// only thing a reader can usefully do with an assumption.
type Step struct {
	// OutputID is the output this step contributes to.
	OutputID string `json:"output_id"`
	// LabelKey names the step.
	LabelKey string `json:"label_key"`
	// Op is the operation applied.
	Op Op `json:"op"`
	// Operands are the values entering the operation, in order.
	Operands []Value `json:"operands"`
	// Result is the value leaving it.
	Result Value `json:"result"`
}

// Absence is a stated gap. The page hides the apparatus for a tier it cannot
// serve; it never hides the fact that the tier is missing.
//
// This is the resolution of a question that would otherwise be answered twice:
// an empty interactive panel on 70 policies is noise, and a silently omitted
// one erases PDI's refusal, which is the most defensible thing on the page.
type Absence struct {
	// Tier is the layer that is absent.
	Tier Tier `json:"tier"`
	// Reason is the machine-readable cause.
	Reason MissingReason `json:"reason"`
	// ReasonKey is the i18n key for the sentence shown to the reader.
	ReasonKey string `json:"reason_key"`
	// Detail is optional non-translated specificity, such as a variable id.
	Detail string `json:"detail,omitempty"`
}

// SourceRef is one dataset behind a result. Source and publisher names are
// proper nouns and are not translated; the method note is a key because it is
// prose.
type SourceRef struct {
	SourceID  string `json:"source_id"`
	Name      string `json:"name"`
	Vintage   string `json:"vintage"`
	URL       string `json:"url,omitempty"`
	MethodKey string `json:"method_key,omitempty"`
}

// SimulateRequest is the body of POST /v1/policy/simulate.
//
// It is a pure function's argument list. Nothing about it is stored, and the
// same request always produces the same result for a given model version, which
// is what lets the response be cached by the edge and shared as a link.
type SimulateRequest struct {
	PolicyID string `json:"policy_id"`
	GEOID    string `json:"geoid"`
	// Level is "county", "tract" or "state". Empty means the model's first
	// declared level.
	Level string `json:"level,omitempty"`
	// ModelVersion pins the model. Empty means the currently loaded version,
	// and the response always echoes the version actually used.
	ModelVersion string `json:"model_version,omitempty"`
	// Params maps param id to value. Omitted params use their authored
	// default; a value outside [Min, Max] is a 400, never a clamp.
	Params map[string]float64 `json:"params,omitempty"`
}

// EvalResult is the response of POST /v1/policy/simulate and the tier-2 block
// of a brief.
type EvalResult struct {
	PolicyID     string `json:"policy_id"`
	ModelVersion string `json:"model_version"`
	GEOID        string `json:"geoid"`
	Level        string `json:"level"`
	// GeoName is the resolved place name. Place names are proper nouns and are
	// not translated.
	GeoName string `json:"geo_name"`

	// Params are the assumptions as evaluated, including defaults filled in.
	Params []ParamBinding `json:"params"`
	// Inputs are the resolved measured leaves, for the analyst register.
	Inputs []Value `json:"inputs"`
	// Outputs are the model's declared outputs, in declaration order.
	Outputs []Value `json:"outputs"`
	// Steps is the arithmetic trace, analyst register only.
	Steps []Step `json:"steps"`

	// Absences are stated gaps within this evaluation.
	Absences []Absence `json:"absences"`
	// RefusalKeys are the model's refusals, carried into the result so a
	// consumer of the payload alone cannot separate the numbers from what they
	// may not be read as saying.
	RefusalKeys []string `json:"refusal_keys"`
	// CaveatKeys are standing caveats on these inputs.
	CaveatKeys []string `json:"caveat_keys,omitempty"`
	// Sources are the datasets behind the measured inputs.
	Sources []SourceRef `json:"sources"`

	// ComputedAt is when this evaluation ran. It is not a vintage and must
	// never be rendered as one: it says when arithmetic happened, not when the
	// world was measured.
	ComputedAt time.Time `json:"computed_at"`
}

// ClaimClass resolves the result's overall status: a projection if any output
// is one. It is computed for the same reason Value.ClaimClass is — so no
// payload can carry a headline class that disagrees with the numbers under it.
func (r EvalResult) ClaimClass() ClaimClass {
	class := ClassMeasurement
	for _, o := range r.Outputs {
		class = Join(class, o.ClaimClass())
	}
	return class
}

// evalResultWire mirrors valueWire: a method-free form so the JSON methods do
// not recurse.
type evalResultWire EvalResult

// MarshalJSON emits the computed result-level claim_class.
func (r EvalResult) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		evalResultWire
		ClaimClass ClaimClass `json:"claim_class"`
	}{evalResultWire(r), r.ClaimClass()})
}

// UnmarshalJSON refuses a result whose headline class disagrees with its
// outputs.
func (r *EvalResult) UnmarshalJSON(b []byte) error {
	var w struct {
		evalResultWire
		ClaimClass ClaimClass `json:"claim_class"`
	}
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	out := EvalResult(w.evalResultWire)
	if w.ClaimClass != "" && w.ClaimClass != out.ClaimClass() {
		return fmt.Errorf(
			"experiment: result for %q declares claim_class %q but its outputs resolve to %q",
			out.PolicyID, w.ClaimClass, out.ClaimClass())
	}
	*r = out
	return nil
}

// EligibilityRule is a policy's own coverage test, stated in the words of
// whoever defined it.
//
// AuthorityKey matters more than it looks. Where a bill's eligibility language
// differs from the dataset PDI holds, the page presents the dataset's test as
// the dataset's — USDA's test attributed to USDA — and never as the bill's.
type EligibilityRule struct {
	// RuleID is stable, e.g. "usda_fara_lila".
	RuleID string `json:"rule_id"`
	// AuthorityKey is the i18n key naming who defined this test.
	AuthorityKey string `json:"authority_key"`
	// StatementKey is the i18n key for the test in plain language.
	StatementKey string `json:"statement_key"`
	// Vintage is the vintage of the designation itself, which is frequently
	// not the vintage of the people counted inside it.
	Vintage string `json:"vintage"`
	// GeoVintage is the geographic vintage the designation is keyed to, e.g.
	// "2010-tract". When it differs from the population data's vintage, the
	// crosswalk turns a categorical flag into a population share and the page
	// must say so in both languages. See ADR-016 D6.
	GeoVintage string `json:"geo_vintage"`
	// SourceIDs are keys into data/sources.toml.
	SourceIDs []string `json:"source_ids"`
}

// Area is one geography inside a brief's reach layer — a tract row in the
// per-area table.
type Area struct {
	GEOID string `json:"geoid"`
	// Name is a proper noun and is not translated.
	Name string `json:"name"`
	// Level is "tract" or "county".
	Level string `json:"level"`
	// DesignatedShare is the share (0–1) of this area's population living in
	// an area the authority designated. It is a share and not a flag because
	// the designation and the population are on different geographic
	// vintages; the page may never render it as "this area is designated".
	DesignatedShare *float64 `json:"designated_share"`
	// Values are this area's measured indicators.
	Values []Value `json:"values"`
}

// Reach is the tier-1 block: who the policy's own rule covers, where, and how
// many. Every value in it is a measurement.
type Reach struct {
	Rule EligibilityRule `json:"rule"`
	// Totals are the headline measured figures for the requested geography.
	Totals []Value `json:"totals"`
	// Areas are the per-area rows, analyst register by default.
	Areas []Area `json:"areas"`
	// DesignatedIDs lists the areas the authority designated, at the
	// authority's own geographic vintage, kept separate from Areas precisely
	// because the two vintages are different objects.
	DesignatedIDs []string `json:"designated_ids"`
	// MapLayerKey is the i18n key for the map legend's wording. It is a key
	// and not a string because the wording is the load-bearing part of the
	// whole tier and must be identical in both languages.
	MapLayerKey string `json:"map_layer_key"`
	Sources     []SourceRef `json:"sources"`
}

// EvidenceDirection is what an external finding reported. It describes the
// published result, not PDI's opinion of it.
type EvidenceDirection string

const (
	EvidenceIncrease    EvidenceDirection = "increase"
	EvidenceDecrease    EvidenceDirection = "decrease"
	EvidenceNoChange    EvidenceDirection = "no_change"
	EvidenceMixed       EvidenceDirection = "mixed"
	EvidenceNotReported EvidenceDirection = "not_reported"
)

// ClaimCheck is how a piece of evidence bears on the candidate's own empirical
// claim, where the policy CSV records one.
type ClaimCheck string

const (
	ClaimSupports      ClaimCheck = "supports"
	ClaimComplicates   ClaimCheck = "complicates"
	ClaimContradicts   ClaimCheck = "contradicts"
	ClaimNotApplicable ClaimCheck = "not_applicable"
)

// EvidenceItem is one external published finding, loaded verbatim from
// data/policies/evidence/<policy_id>.csv.
//
// Nothing in this struct is computed by PDI, and nothing in it is translated —
// a finding is a quotation of somebody else's published work, and paraphrasing
// it into a second language inside the platform would make PDI the author of a
// claim it is explicitly not making. The page labels these blocks with their
// language and translates the surrounding scaffolding only.
type EvidenceItem struct {
	EvidenceID     string            `json:"evidence_id"`
	Jurisdiction   string            `json:"jurisdiction"`
	GeoScale       string            `json:"geo_scale"`
	PeriodStart    string            `json:"period_start"`
	PeriodEnd      string            `json:"period_end,omitempty"`
	Intervention   string            `json:"intervention"`
	Outcome        string            `json:"outcome"`
	Direction      EvidenceDirection `json:"direction"`
	Finding        string            `json:"finding"`
	EffectReported string            `json:"effect_reported,omitempty"`
	StudyDesign    string            `json:"study_design"`
	Strength       string            `json:"evidence_strength"`
	SourceTitle    string            `json:"source_title"`
	SourceOrg      string            `json:"source_org"`
	SourceURL      string            `json:"source_url"`
	SourceYear     string            `json:"source_year"`
	AccessedDate   string            `json:"accessed_date"`
	Citation       string            `json:"citation"`
	RelatesToClaim string            `json:"relates_to_claim,omitempty"`
	ClaimCheck     ClaimCheck        `json:"claim_check"`
	// TransferNote is required and states why this finding may or may not
	// carry to the requested geography. An uncontextualized result from
	// another jurisdiction is the most persuasive wrong thing this page could
	// show.
	TransferNote string `json:"transfer_note"`
	ReviewedBy   string `json:"reviewed_by"`
	ReviewedDate string `json:"reviewed_date"`
}

// GeoSource says how the brief's geography was chosen. The page states it,
// because a reader who believes they are looking at their own county and is
// not has been misinformed by a default.
type GeoSource string

const (
	// GeoRequested — the request named it.
	GeoRequested GeoSource = "requested"
	// GeoModelDefault — the request named none and the model declared one.
	GeoModelDefault GeoSource = "model_default"
	// GeoStateFallback — no request, no model default; the statewide view.
	GeoStateFallback GeoSource = "state_fallback"
)

// Geography identifies the place a brief is about.
type Geography struct {
	GEOID string `json:"geoid"`
	// Name is a proper noun and is not translated.
	Name   string    `json:"name"`
	Level  string    `json:"level"`
	Source GeoSource `json:"source"`
}

// PolicyRef is the policy record as the page needs it. The title and
// description come from an English-authored CSV; the contract's i18n rule
// governs how they are shown in Spanish.
type PolicyRef struct {
	PolicyID string `json:"policy_id"`
	// Title, Description and ClaimEmpirical are verbatim CSV text.
	Title          string `json:"title"`
	Description    string `json:"description"`
	ClaimEmpirical string `json:"claim_empirical,omitempty"`
	// Candidate, Office and BillReferences are attribution, rendered in the
	// provenance register rather than the headline. See ADR-016 D8.
	Candidate      string `json:"candidate"`
	Office         string `json:"office"`
	BillReferences string `json:"bill_references,omitempty"`
	Category       string `json:"category"`
}

// ModelDescriptor is the tier-2 model as advertised by a brief: enough for the
// page to build the controls, without evaluating anything. Nil when the policy
// has no model.
type ModelDescriptor struct {
	ModelVersion string   `json:"model_version"`
	TitleKey     string   `json:"title_key"`
	SummaryKey   string   `json:"summary_key"`
	GeoLevels    []string `json:"geo_levels"`
	Params       []Param  `json:"params"`
	// Outputs carries declaration metadata only — id, label, unit, precision,
	// register — never an Expr. The expression tree is a model-authoring
	// artifact and shipping it to the browser invites a second, divergent
	// evaluator in JavaScript.
	Outputs     []Output `json:"outputs"`
	RefusalKeys []string `json:"refusal_keys"`
	CaveatKeys  []string `json:"caveat_keys,omitempty"`
}

// Brief is the response of GET /v1/policy/briefs/:policy_id — the whole page in
// one payload, all three tiers, with every absence stated.
type Brief struct {
	Policy    PolicyRef `json:"policy"`
	Geography Geography `json:"geography"`

	// Reach is tier 1, and is nil only when the policy has no expressible
	// eligibility rule — in which case Absences carries the reason.
	Reach *Reach `json:"reach"`
	// Model is tier 2's descriptor, nil when the policy has no model.
	Model *ModelDescriptor `json:"model"`
	// Evidence is tier 3, empty when no evidence file exists.
	Evidence []EvidenceItem `json:"evidence"`

	// Absences names every tier that is not here and why. A brief with a nil
	// Model and no matching Absence entry is malformed: the mechanism that
	// keeps a missing tier visible is the entry, not the empty field.
	Absences []Absence `json:"absences"`
	// RefusalKeys are the page-level refusals — what PDI cannot tell you about
	// any policy, independent of this one.
	RefusalKeys []string `json:"refusal_keys"`
	// Sources are every dataset behind the brief.
	Sources []SourceRef `json:"sources"`
	// GeneratedAt is when this payload was assembled. Not a vintage.
	GeneratedAt time.Time `json:"generated_at"`
}
