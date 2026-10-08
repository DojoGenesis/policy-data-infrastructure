package gateway

// This file is owned by the serial integrator (plugin.go's owner), not by
// either the briefs or simulate track. It exists because simulate.go
// deliberately declares ModelLoader and SimulateEvaluator as interfaces it
// depends on rather than concrete types — pkg/experiment had no model loader
// and no evaluator when that track started (see simulate.go's package doc) —
// and briefs.go's loadModelDescriptor reads the same JSON file but
// deliberately STRIPS Expr for the wire (contract §3: "model.outputs carries
// no expr"). Evaluate needs the arithmetic, so this integration cannot reuse
// that function; it needs its own full-fidelity loader.
//
// Two small adapters live here:
//
//   - fileModelLoader satisfies simulate.go's ModelLoader by reading
//     data/policies/models/<policy_id>.json WITH Expr intact and running
//     pkg/experiment.Validate on it — the contract's "refused at load, not
//     evaluation" step (§2) — before ever handing it to Evaluate.
//   - storeEvaluator satisfies simulate.go's SimulateEvaluator by resolving
//     every "indicator:" ref a model's Expr trees name against the live
//     store, building the MeasuredLeaf map pkg/experiment.Evaluate needs,
//     and then calling that pure function.
//
// KNOWN GAP, flagged rather than guessed: "reach:" refs are NOT resolved by
// storeEvaluator. No model file shipped this session uses one (verified: the
// model-instance track's own report, and the grep below), so leaving them
// unresolved is an honest absence, not a silent wrong answer — eval.go's
// resolver turns an absent leaf into MissingUnresolvedRef, which is the
// correct signal for "this integration does not support this ref yet"
// rather than MissingNotLoaded, which would claim the data layer was asked
// and had nothing. The first tier-2 model that references a reach: field
// needs this file extended to call the same reach-assembly path briefs.go
// uses (buildReach), not a new query invented here.
//
//	grep -l '"ref": "reach:' data/policies/models/*.json   # → no matches, 2026-08-13

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/experiment"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// fileModelLoader implements simulate.go's ModelLoader by reading a
// policy's tier-2 model file straight off disk, Expr intact. It is
// deliberately a distinct type from briefs.go's loadModelDescriptor
// function: that one builds a wire-safe descriptor with Expr zeroed for
// GET /briefs (contract §3), and reusing it here would mean either
// evaluating with no arithmetic tree or growing a second code path inside a
// function whose whole job is to strip the thing this one needs.
type fileModelLoader struct {
	dir string
}

func newFileModelLoader(dir string) *fileModelLoader {
	return &fileModelLoader{dir: dir}
}

// LoadPolicyModel returns (nil, nil) when the policy has no model file —
// the ordinary state for most of the 70 Hong positions — and a non-nil
// error for anything that makes the file itself untrustworthy: it does not
// parse, its policy_id does not match the requested one, or it fails
// pkg/experiment.Validate's load-time refusals (forward refs, cycles, an
// operation node with no terms, a measured const with no source). All three
// are data problems on PDI's side, not something a caller's request can fix
// by resubmitting, so simulate.go's Handle turns every non-nil error here
// into a 500 — see that file's ModelLoader doc comment.
func (l *fileModelLoader) LoadPolicyModel(ctx context.Context, policyID string) (*experiment.PolicyModel, error) {
	path := filepath.Join(l.dir, policyID+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read model file %s: %w", path, err)
	}

	var m experiment.PolicyModel
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("parse model file %s: %w", path, err)
	}
	if m.PolicyID != policyID {
		return nil, fmt.Errorf("model file %s declares policy_id %q, want %q", path, m.PolicyID, policyID)
	}
	if err := experiment.Validate(&m); err != nil {
		return nil, fmt.Errorf("model file %s failed load-time validation: %w", path, err)
	}
	return &m, nil
}

// IndicatorLookup is the one store method storeEvaluator reads, declared
// narrow for the same reason simulate.go's GeoPolicyLookup is (see that
// file): a test double only has to implement one method, not store.Store's
// full surface, and store.Store already satisfies this interface
// structurally, so the integrator passes p.store directly with no adapter.
type IndicatorLookup interface {
	QueryIndicators(ctx context.Context, q store.IndicatorQuery) ([]store.Indicator, error)
}

// storeEvaluator implements simulate.go's SimulateEvaluator by loading the
// measured leaves a model's Expr trees actually reference from the live
// store, then handing them to pkg/experiment.Evaluate — a pure function
// this type never reimplements, only feeds.
type storeEvaluator struct {
	store IndicatorLookup
	vars  map[string]store.VariableMeta // variable_id -> metadata; PolicyPlugin already loads this at startup
}

func newStoreEvaluator(s IndicatorLookup, vars map[string]store.VariableMeta) *storeEvaluator {
	return &storeEvaluator{store: s, vars: vars}
}

// Evaluate builds the leaves and userParams Evaluate needs from bindings
// simulate.go's Handle has already validated (every value inside its
// declared [Min, Max]) and canonicalized (snapped to Step) — see
// resolveParamBindings — and calls the pure function unchanged.
//
// Only bindings whose Source is SrcUser become entries in userParams.
// Evaluate fills every other declared param from the model's own Default
// and reports Source = SrcDefault itself (see eval.go's Evaluate). Passing
// every binding through — including the ones this handler already resolved
// to a default — would make Evaluate believe the reader supplied every
// value, which is exactly the "we opened here" vs. "you chose this"
// distinction contract §1.3's ParamSource exists to preserve.
func (e *storeEvaluator) Evaluate(ctx context.Context, model *experiment.PolicyModel, geoid, level, geoName string, bindings []experiment.ParamBinding) (*experiment.EvalResult, error) {
	leaves, err := e.loadLeaves(ctx, model, geoid)
	if err != nil {
		return nil, err
	}

	userParams := make(map[string]float64, len(bindings))
	for _, b := range bindings {
		if b.Source == experiment.SrcUser {
			userParams[b.ID] = b.Num
		}
	}

	return experiment.Evaluate(model, geoid, level, geoName, leaves, userParams)
}

// loadLeaves walks every output's Expr tree for "indicator:<variable_id>"
// refs, queries the store once for the union of every variable id found at
// geoid, and returns a MeasuredLeaf map keyed by the SAME full ref string
// the Expr tree uses ("indicator:usda_food_low_access_1mi"), matching the
// namespace eval.go's resolveRef looks up by.
//
// Every declared indicator: ref gets an entry in the returned map even when
// the store has no row for it — a leaf that is absent from the map entirely
// resolves as MissingUnresolvedRef (contract §2: "target does not exist"),
// which is the wrong signal for an ordinary data gap. Setting
// Missing: MissingNotLoaded explicitly is the caller-side half of the
// contract's LEAF algorithm ("if target has no value: return ... reason:
// target.reason") — see MeasuredLeaf's own doc comment in eval.go for the
// same rule stated from the callee's side.
func (e *storeEvaluator) loadLeaves(ctx context.Context, model *experiment.PolicyModel, geoid string) (map[string]experiment.MeasuredLeaf, error) {
	refs := map[string]bool{}
	for _, o := range model.Outputs {
		collectIndicatorRefs(o.Expr, refs)
	}

	leaves := make(map[string]experiment.MeasuredLeaf, len(refs))
	if len(refs) == 0 {
		return leaves, nil
	}

	var varIDs []string
	refByVar := make(map[string]string, len(refs))
	for ref := range refs {
		ns, id, ok := splitLeafRef(ref)
		if !ok || ns != "indicator" {
			// reach: refs land here too (collectIndicatorRefs keeps both
			// namespaces so this function has one place, not two, that
			// decides what is supported) — see this file's package doc for
			// why they are deliberately left unresolved rather than guessed.
			continue
		}
		varIDs = append(varIDs, id)
		refByVar[id] = ref
	}
	sort.Strings(varIDs)
	if len(varIDs) == 0 {
		return leaves, nil
	}

	rows, err := e.store.QueryIndicators(ctx, store.IndicatorQuery{
		GEOIDs:      []string{geoid},
		VariableIDs: varIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("query indicators for %s: %w", geoid, err)
	}
	byVar := make(map[string]*store.Indicator, len(rows))
	for i := range rows {
		byVar[rows[i].VariableID] = &rows[i]
	}

	for _, varID := range varIDs {
		leaves[refByVar[varID]] = e.leafFromIndicator(varID, byVar[varID])
	}
	return leaves, nil
}

// leafFromIndicator converts one store.Indicator row (or its absence) into
// the MeasuredLeaf shape eval.go's LEAF algorithm expects. A nil row or a
// nil Value both mean "not loaded at this geography" — never a zero (this
// repo has shipped a sentinel-zero bug that faked a dollar trend out of
// exactly that substitution).
func (e *storeEvaluator) leafFromIndicator(variableID string, row *store.Indicator) experiment.MeasuredLeaf {
	meta := e.vars[variableID]
	sourceIDs := sourceIDsFor(meta)

	if row == nil || row.Value == nil {
		return experiment.MeasuredLeaf{
			Missing:   experiment.MissingNotLoaded,
			SourceIDs: sourceIDs,
		}
	}
	return experiment.MeasuredLeaf{
		Value:       row.Value,
		Vintage:     row.Vintage,
		Unit:        mapStoreUnit(meta.Unit),
		MOE:         row.MarginOfError,
		Reliability: reliabilityFromStore(row.Reliability),
		SourceIDs:   sourceIDs,
	}
}

func sourceIDsFor(meta store.VariableMeta) []string {
	if meta.SourceID == "" {
		return nil
	}
	return []string{meta.SourceID}
}

// reliabilityFromStore maps store.Indicator.Reliability's vocabulary
// ("high" / "moderate" / "low" / "") onto experiment.Reliability's closed
// set ("high" / "medium" / "low" / "unknown"). The two disagree on the
// middle band's spelling ("moderate" vs. "medium") — a direct string cast
// would silently produce an experiment.Reliability value outside the
// declared enum instead of RelMedium.
func reliabilityFromStore(s string) experiment.Reliability {
	switch s {
	case "high":
		return experiment.RelHigh
	case "moderate":
		return experiment.RelMedium
	case "low":
		return experiment.RelLow
	default:
		return experiment.RelUnknown
	}
}

// mapStoreUnit maps indicator_meta's free-text unit column onto
// experiment.Unit's closed set. This field reaches the wire on intermediate
// arithmetic-trace operands (EvalResult.Steps, .Inputs) — Evaluate always
// overwrites the actual Output.Value.Unit from the model's own declaration
// (contract §1.3: "unit is declared on every output, never inferred";
// eval.go's Evaluate does exactly that after calling toValue) — but those
// intermediate operands are exactly what the analyst arithmetic trace
// renders, and a missing unit there is not harmless: eval.go's
// choosePrecision/displayScale (and the frontend's fmtValue) both key off
// Unit to decide whether a 0-1 figure gets shown as a raw decimal or a
// percentage, and a share shown as a raw 2-decimal fraction next to a
// six-digit count is the exact bug that shipped "0.26 x 160,331 = 41,768"
// (should read "26.05% x 160,331 = 41,768" — see
// docs/contracts/experiment-contract.md §2 and pkg/experiment/eval.go's
// ReproducesAtDisplayPrecision).
//
// The switch below is by NAME EQUIVALENCE — a DB unit string that clearly
// denotes the same quantity as a frozen experiment.Unit member — not a
// per-variable special case, so the next indicator_meta row tagged
// "percent_share" or "percent" gets the correct wire unit for free instead
// of needing its own override the way briefs.go's tier-1 path currently
// does for usda_food_desert (briefs.go:411-412's explicit UnitShare
// argument to measuredLeaf). That override is intentionally left in place:
// it is harmless (this mapping now agrees with it) and briefs.go has no
// other way to declare a leaf's unit.
//
// verified against the live DB (2026-08-14): `select distinct unit from
// indicator_meta` returns count, dollars, per_1000_households, percent,
// percentile, percent_share, rate_per_100k. Three of those are covered by
// name equivalence below. The remaining three -- per_1000_households,
// percentile, rate_per_100k -- have NO member in experiment.Unit's frozen
// enumeration (contract §1.1, "every member, frozen"); mapping one of them
// to an unrelated existing Unit (e.g. "ratio") would misrepresent the
// quantity, which is worse than the honest "" this function already
// returns for anything it cannot map. Adding new Unit members is a contract
// change, out of this fix's scope, and is flagged here rather than guessed
// at so the gap is visible to whoever picks up the next indicator that
// needs one of these three.
func mapStoreUnit(s string) experiment.Unit {
	switch experiment.Unit(s) {
	case experiment.UnitCount, experiment.UnitPeople, experiment.UnitHouseholds,
		experiment.UnitUSD, experiment.UnitUSDPerYear, experiment.UnitPercentPoints,
		experiment.UnitShare, experiment.UnitMiles, experiment.UnitDays, experiment.UnitRatio:
		return experiment.Unit(s)
	}
	switch s {
	case "percent_share":
		return experiment.UnitShare // 0-1, per contract S17 -- verified against indicators.usda_food_desert's live values (0.0-1.0 range), not just the name
	case "percent":
		return experiment.UnitPercentPoints // 0-100, per contract S17
	case "dollars":
		return experiment.UnitUSD
	default:
		return ""
	}
}

// collectIndicatorRefs walks one Expr tree and records every "indicator:"
// or "reach:" ref it finds (full ref string, e.g. "indicator:usda_food_low_access_1mi")
// into out. param: and output: refs are not the data layer's concern —
// param: resolves from bindings and output: resolves from an
// already-evaluated sibling, neither needs a store lookup — so they are not
// collected here.
func collectIndicatorRefs(e experiment.Expr, out map[string]bool) {
	switch e.Op {
	case experiment.OpRef:
		if ns, _, ok := splitLeafRef(e.Ref); ok && (ns == "indicator" || ns == "reach") {
			out[e.Ref] = true
		}
	case experiment.OpConst:
		// no refs
	default:
		for _, t := range e.Terms {
			collectIndicatorRefs(t.Expr, out)
		}
	}
}

// splitLeafRef splits a namespaced ref ("indicator:usda_food_low_access_1mi")
// into its namespace and id. It duplicates eval.go's unexported splitRef
// rather than importing it, because pkg/experiment does not export that
// helper and this package has no other reason to reach into pkg/experiment's
// internals.
func splitLeafRef(ref string) (ns, id string, ok bool) {
	i := strings.IndexByte(ref, ':')
	if i < 0 {
		return "", "", false
	}
	return ref[:i], ref[i+1:], true
}
