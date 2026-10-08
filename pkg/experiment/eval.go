// Evaluator for the policy experiment page's tier-2 arithmetic.
//
// This file implements ADR-016 D3/D4/D5 and the taint algorithm in
// docs/contracts/experiment-contract.md §2: given a PolicyModel, the
// measured leaves it references, and a caller's assumptions, produce the
// EvalResult defined in that contract with every value carrying the
// provenance it is computed from.
//
// # Why the taint cannot be dropped here
//
// Value.ClaimClass is a method over Value.AssumedInputs (see types.go); it is
// not a field this file can set. So the only way this evaluator could ship a
// projection mislabeled as a measurement is to hand a Value an empty
// AssumedInputs slice while some ancestor of it was actually assumed — and
// every path that builds a Value in this file goes through resolve(), whose
// operation-node case takes the UNION of every child's assumed_inputs with no
// branch that discards it (see resolveOp). There is no struct literal
// anywhere in this file that constructs a Value directly with a claim of its
// own; every Value on the wire is built by toValue() from a resolved node,
// and every resolved node's AssumedInputs is either a leaf's own declaration
// or a union of its children's. Forging a measurement out of a projection
// would require forging the union itself, which is the one thing every
// operation case in resolveOp does unconditionally, before it even looks at
// whether the arithmetic can run.
//
// # Two kinds of "unknown ref", handled at two different times
//
// A `param:` or `output:` ref names something the MODEL FILE declares, so an
// unresolvable one is a model-authoring bug and is refused at Validate —
// "load" time, in the contract's words, never at Evaluate. An `indicator:` or
// `reach:` ref names something the DATA LAYER provides, which this package
// has no way to enumerate in advance; those are resolved (and, if absent,
// marked MissingUnresolvedRef) at Evaluate time, per §2's LEAF algorithm.
//
// # Cycle detection and forward-reference rejection are two mechanisms, not one
//
// Validate rejects a forward output: ref (one whose target is declared at or
// after the referencing output) the moment it is found, before any graph is
// walked. Given the model's outputs are a linearly ordered list and refs can
// only target other outputs, that check alone is enough to reject every
// possible cycle: a directed cycle over distinct positions in a total order
// cannot be strictly decreasing, so it must contain at least one forward
// edge. findCycle is kept anyway, and exercised directly by its own test,
// because that argument is a property of today's ref namespace and not a
// property Go's type system enforces — the day the namespace grows a way to
// reference something other than an earlier output, the argument breaks
// silently unless the second mechanism is already there and already tested.
package experiment

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// MeasuredLeaf is one measured value as loaded from outside this package — an
// indicator row or a tier-1 reach field — keyed in the map passed to
// Evaluate by its full ref: "indicator:<variable_id>" or "reach:<field_id>",
// matching the namespace an Expr tree uses to name it.
//
// Value is nil exactly when the leaf is missing. A caller that has no row for
// this variable at this geography should set Missing (MissingNotLoaded is the
// ordinary case); the zero value of Missing is treated as MissingNotLoaded
// rather than as "present with nil", so a caller cannot accidentally publish
// a value-less leaf with no stated reason.
type MeasuredLeaf struct {
	LabelKey    string
	Value       *float64
	Missing     MissingReason
	Vintage     string
	Unit        Unit
	MOE         *float64
	Reliability Reliability
	SourceIDs   []string
}

// EvalError is a refusal to evaluate the request at all — a malformed model,
// a reference this package cannot resolve statically, or a submitted
// assumption outside its declared range. None of these become a
// MissingReason on a Value, because none of them are a gap in the data: they
// are reasons the arithmetic never starts.
type EvalError struct {
	// Code is stable for callers that want to switch on it (e.g. to choose an
	// HTTP status per contract §4), independent of Message's wording.
	Code    string
	Message string
}

func (e *EvalError) Error() string { return e.Message }

// Error codes. See the doc comment on EvalError for what distinguishes these
// from a MissingReason.
const (
	ErrBadSchemaVersion  = "bad_schema_version"
	ErrUnknownRef        = "unknown_ref"
	ErrForwardRef        = "forward_ref"
	ErrCycle             = "ref_cycle"
	ErrEmptyTerms        = "empty_terms"
	ErrMissingConst      = "missing_const_value"
	ErrMissingConstSrc   = "missing_const_source"
	ErrUnknownOp         = "unknown_op"
	ErrUnknownParam      = "unknown_param"
	ErrParamOutOfRange   = "param_out_of_range"
	ErrGeoLevelUnsupport = "geo_level_unsupported"
	ErrUnknownConstSrc   = "unknown_const_source"
)

// DefaultKnownSources is the source registry Validate checks a measured
// const's const_source against. Its members are exactly the keys
// data/sources.toml declares — the manifest types.go's SourceIDs doc comment
// (on Value.SourceIDs) already names as their namespace: "SourceIDs are keys
// into data/sources.toml."
//
// It is hardcoded, not loaded from the file at runtime, because this package
// loads no file, no DB and no network — see the doc comment on Evaluate's
// EvalResult.Sources field ("this package does not load [data/sources.toml],
// no DB, no network, per its contract"). A model file's const_source is
// static, author-time data; resolving it against a static, compile-time list
// keeps that guarantee rather than punching a hole in it.
//
// Re-derive and diff against this list with:
//
//	grep -oE '^\[sources\.[a-z0-9-]+\]' data/sources.toml | sed -E 's/^\[sources\.(.+)\]$/\1/' | sort
//
// TestDefaultKnownSources_MatchesSourcesToml re-runs the equivalent parse at
// test time and fails if this list drifts from the real manifest.
var DefaultKnownSources = newKnownSources(
	"acs-5yr", "tiger", "hud-chas", "epa-ejscreen", "hmda", "bls-laus",
	"fema-nfhl", "cdc-places", "usda-food", "osm", "hud-pit", "hrsa",
	"gtfs", "wi-dpi", "cdc-svi", "epa-tri", "fbi-nibrs", "fcc-broadband",
)

func newKnownSources(keys ...string) map[string]bool {
	m := make(map[string]bool, len(keys))
	for _, k := range keys {
		m[k] = true
	}
	return m
}

// Validate checks everything about a PolicyModel that can be known without
// any measured data: every op is one of the closed eight, every operation
// node has at least one term, every measured const cites a source THAT
// RESOLVES in the source registry (not merely a non-empty string — see
// ValidateWithSources), every param: ref names a declared param, every
// output: ref names an EARLIER output, and the output reference graph has no
// cycle.
//
// This is the contract's "refused at model load, not resolved at
// evaluation" step (§2, §1.2). Evaluate calls it before touching any data,
// so a cyclic or forward-referencing model is rejected before resolve() ever
// runs — the alternative is unbounded recursion over attacker- or
// author-controlled input.
//
// Validate is ValidateWithSources against DefaultKnownSources. Its signature
// is frozen at one argument because pkg/gateway/simulate_wire.go already
// calls it that way; ValidateWithSources is where a caller that needs a
// different registry (a test, or a future caller with a live
// indicator_sources-backed list) injects one instead.
func Validate(model *PolicyModel) error {
	return ValidateWithSources(model, DefaultKnownSources)
}

// ValidateWithSources is Validate with the measured-const source registry
// supplied by the caller instead of assumed from the package-level
// DefaultKnownSources. knownSources is keyed by const_source; a nil or empty
// map rejects every measured const that requires one, which is the safe
// (fail-closed) default for an unconfigured registry.
//
// This is the data-file boundary of the taint invariant: a hand-authored
// model JSON can declare const_kind: "measured" and const_source: "trust
// me", and without this check that string alone was enough to launder a
// pure assumption into a measurement — the taint system believed the
// declaration because it never verified it resolved to anything. Checking
// containment in knownSources here, at load time, closes that boundary the
// same way collectOutputRefs already closes it for param: and output: refs.
func ValidateWithSources(model *PolicyModel, knownSources map[string]bool) error {
	if model.SchemaVersion != SchemaVersion {
		return &EvalError{ErrBadSchemaVersion, fmt.Sprintf(
			"model %q declares schema_version %q; this package understands %q",
			model.PolicyID, model.SchemaVersion, SchemaVersion)}
	}

	paramIDs := make(map[string]bool, len(model.Params))
	for _, p := range model.Params {
		paramIDs[p.ID] = true
	}
	outputIndex := make(map[string]int, len(model.Outputs))
	for i, o := range model.Outputs {
		outputIndex[o.ID] = i
	}

	graph := make(map[string][]string, len(model.Outputs))
	for i, o := range model.Outputs {
		refs, err := collectOutputRefs(o.Expr, o.ID, paramIDs, outputIndex, knownSources)
		if err != nil {
			return err
		}
		graph[o.ID] = refs
		for _, r := range refs {
			if outputIndex[r] >= i {
				return &EvalError{ErrForwardRef, fmt.Sprintf(
					"output %q references %q, which is declared at or after it; forward references are refused at load",
					o.ID, r)}
			}
		}
	}

	if cyc := findCycle(graph); cyc != "" {
		return &EvalError{ErrCycle, "reference cycle among outputs: " + cyc}
	}
	return nil
}

// collectOutputRefs walks one output's expression tree once. It checks every
// model-authoring invariant that does not depend on measured data, and
// returns the ids of every OTHER output this tree references via
// "output:<id>", for the caller's forward-reference and cycle checks.
//
// indicator: and reach: refs are deliberately not validated here — their
// existence is a fact about the data layer, not the model file, and is
// resolved (or refused as MissingUnresolvedRef) at Evaluate time instead.
//
// knownSources is the registry a measured const's const_source must resolve
// against (see ValidateWithSources); it is threaded through recursion rather
// than read from a package global so the whole call tree honors whatever
// registry the caller injected.
func collectOutputRefs(e Expr, outputID string, paramIDs map[string]bool, outputIndex map[string]int, knownSources map[string]bool) ([]string, error) {
	switch e.Op {
	case OpRef:
		ns, id, ok := splitRef(e.Ref)
		if !ok {
			return nil, &EvalError{ErrUnknownRef, fmt.Sprintf(
				"output %q has a ref with no namespace: %q", outputID, e.Ref)}
		}
		switch ns {
		case "param":
			if !paramIDs[id] {
				return nil, &EvalError{ErrUnknownRef, fmt.Sprintf(
					"output %q references undeclared param %q", outputID, id)}
			}
			return nil, nil
		case "output":
			if _, ok := outputIndex[id]; !ok {
				return nil, &EvalError{ErrUnknownRef, fmt.Sprintf(
					"output %q references undeclared output %q", outputID, id)}
			}
			return []string{id}, nil
		case "indicator", "reach":
			return nil, nil
		default:
			return nil, &EvalError{ErrUnknownRef, fmt.Sprintf(
				"output %q has a ref with unknown namespace %q", outputID, ns)}
		}

	case OpConst:
		if e.Const == nil {
			return nil, &EvalError{ErrMissingConst, fmt.Sprintf(
				"output %q has a const node with no value", outputID)}
		}
		if e.ConstKind == KindMeasured {
			if e.ConstSource == "" {
				return nil, &EvalError{ErrMissingConstSrc, fmt.Sprintf(
					"output %q has a measured const with no const_source", outputID)}
			}
			// Non-empty is not enough — a data file can declare const_kind:
			// "measured" and any string for const_source, and the taint
			// system would believe it without this check. const_source must
			// resolve against the injected registry, exactly as param: and
			// output: refs above must resolve against paramIDs/outputIndex.
			if !knownSources[e.ConstSource] {
				return nil, &EvalError{ErrUnknownConstSrc, fmt.Sprintf(
					"output %q has a measured const with const_source %q, which does not resolve to any entry in the source registry (data/sources.toml)",
					outputID, e.ConstSource)}
			}
		}
		return nil, nil

	case OpAdd, OpSub, OpMul, OpDiv, OpMin, OpMax:
		if len(e.Terms) == 0 {
			return nil, &EvalError{ErrEmptyTerms, fmt.Sprintf(
				"output %q has an operation node (%s) with no terms", outputID, e.Op)}
		}
		var refs []string
		for _, t := range e.Terms {
			r, err := collectOutputRefs(t.Expr, outputID, paramIDs, outputIndex, knownSources)
			if err != nil {
				return nil, err
			}
			refs = append(refs, r...)
		}
		return refs, nil

	default:
		return nil, &EvalError{ErrUnknownOp, fmt.Sprintf(
			"output %q has a node with unknown op %q", outputID, e.Op)}
	}
}

// findCycle runs a three-color DFS over the output reference graph and
// returns a human-readable "a -> b -> a" path through the first cycle it
// finds, or "" when the graph is acyclic. See the package doc comment for why
// this is kept as an independent mechanism from Validate's forward-reference
// check even though, today, the latter already rejects everything the former
// would catch.
func findCycle(graph map[string][]string) string {
	const white, gray, black = 0, 1, 2
	color := make(map[string]int, len(graph))
	var path []string
	var found string

	var visit func(n string) bool
	visit = func(n string) bool {
		color[n] = gray
		path = append(path, n)
		for _, m := range graph[n] {
			switch color[m] {
			case gray:
				start := 0
				for i, p := range path {
					if p == m {
						start = i
						break
					}
				}
				cyc := append(append([]string{}, path[start:]...), m)
				found = strings.Join(cyc, " -> ")
				return true
			case white:
				if visit(m) {
					return true
				}
			}
		}
		path = path[:len(path)-1]
		color[n] = black
		return false
	}

	ids := make([]string, 0, len(graph))
	for id := range graph {
		ids = append(ids, id)
	}
	sort.Strings(ids) // deterministic error text regardless of map iteration order
	for _, id := range ids {
		if color[id] == white {
			if visit(id) {
				return found
			}
		}
	}
	return ""
}

func splitRef(ref string) (ns, id string, ok bool) {
	i := strings.IndexByte(ref, ':')
	if i < 0 {
		return "", "", false
	}
	return ref[:i], ref[i+1:], true
}

// ── resolution ──────────────────────────────────────────────────────────

// node is the resolver's internal working value: everything a Value needs,
// plus a stable id used to build MissingDetail chains ("<id>:<reason>")
// without re-walking the tree, and used as the map key for output: refs and
// the dedup key for EvalResult.Inputs.
type node struct {
	id             string
	labelKey       string
	num            *float64
	unit           Unit
	precision      int
	missing        MissingReason
	missingDetail  string
	kind           Kind
	assumedInputs  []string
	measuredInputs []string
	vintage        string
	moe            *float64
	reliability    Reliability
	sourceIDs      []string
}

// evalCtx carries everything one Evaluate call needs. It is built fresh per
// call and holds no package-level state, so concurrent Evaluate calls never
// share a ctx.
type evalCtx struct {
	leaves    map[string]MeasuredLeaf
	params    map[string]float64 // bound: every declared param, user value or default
	paramDefs map[string]Param   // the model's own Param declarations, keyed by id --
	// used so a param: ref can carry its AUTHOR-DECLARED Unit and Precision into
	// the analyst trace instead of a blind default. See resolveRef's "param"
	// case and the package doc comment "Why intermediate precision is derived,
	// not defaulted" below.
	outputs   map[string]node // resolved outputs so far, keyed by output id
	inputSeen map[string]node // every distinct measured leaf actually touched
	steps     []Step
	outputID  string // the output currently being resolved
}

func emptyStrings() []string { return []string{} }

func unresolvedNode(id string) node {
	return node{
		id: id, missing: MissingUnresolvedRef, kind: KindDerived,
		assumedInputs: emptyStrings(), measuredInputs: emptyStrings(),
	}
}

// resolve implements the contract §2 algorithm for one Expr node. path is a
// dot-joined chain of term indices from the output's root, used only to make
// const and operation-node ids unique and stable within one output's tree.
func (c *evalCtx) resolve(e Expr, path string) node {
	switch e.Op {
	case OpRef:
		return c.resolveRef(e.Ref)
	case OpConst:
		return c.resolveConst(e, path)
	case OpAdd, OpSub, OpMul, OpDiv, OpMin, OpMax:
		return c.resolveOp(e, path)
	default:
		// Validate refuses this at load; defensive only.
		return unresolvedNode(c.outputID + ":" + path)
	}
}

func (c *evalCtx) resolveRef(ref string) node {
	ns, id, ok := splitRef(ref)
	if !ok {
		return unresolvedNode(ref)
	}
	switch ns {
	case "param":
		v, ok := c.params[id]
		if !ok {
			return unresolvedNode(ref)
		}
		val := v
		n := node{
			id: ref, num: &val, kind: KindAssumed,
			assumedInputs: []string{id}, measuredInputs: emptyStrings(),
			precision: 2, // safe fallback; overwritten below when the param declares itself
		}
		// A param's Unit and Precision are author-declared in the model file
		// (schema-validated 0-2) -- carrying them into the trace is strictly
		// better than guessing, and it is what makes a share-typed param (e.g.
		// target_population_share_served) render consistently with how tier 1
		// renders the same kind of quantity, per the contract's percent/share
		// convention (S6, S7, S17). paramDefs is always populated by Evaluate
		// before resolve() runs; a nil map (a node built directly by a test
		// that skips Evaluate) degrades to the safe fallback above rather than
		// panicking.
		if def, ok := c.paramDefs[id]; ok {
			n.unit = def.Unit
			n.precision = def.Precision
		}
		return n

	case "indicator", "reach":
		leaf, ok := c.leaves[ref]
		if !ok {
			return unresolvedNode(ref)
		}
		n := node{
			id: ref, labelKey: leaf.LabelKey, kind: KindMeasured, unit: leaf.Unit,
			assumedInputs: emptyStrings(), measuredInputs: []string{id},
		}
		if leaf.Value == nil {
			reason := leaf.Missing
			if reason == MissingNone {
				reason = MissingNotLoaded
			}
			n.missing = reason
			// No vintage/MOE/reliability for a number that does not exist —
			// see the doc comment on MeasuredLeaf.
		} else {
			val := *leaf.Value
			n.num = &val
			n.vintage = leaf.Vintage
			n.moe = leaf.MOE
			n.reliability = leaf.Reliability
			n.sourceIDs = append([]string(nil), leaf.SourceIDs...)
			n.precision = choosePrecision(n.unit, n.num)
		}
		c.recordInput(n)
		return n

	case "output":
		prev, ok := c.outputs[id]
		if !ok {
			return unresolvedNode(ref)
		}
		return prev // verbatim; inherits its input lists unchanged, per §2

	default:
		return unresolvedNode(ref)
	}
}

func (c *evalCtx) resolveConst(e Expr, path string) node {
	id := "const:" + c.outputID + ":" + path
	kind := e.ConstKind
	if kind == "" {
		kind = KindAssumed // the safe default: an unclassified literal is an assumption until a source says otherwise
	}
	n := node{id: id, kind: kind, assumedInputs: emptyStrings(), measuredInputs: emptyStrings()}
	if e.Const == nil {
		// Validate refuses this at load; defensive only.
		n.missing = MissingUnresolvedRef
		return n
	}
	val := *e.Const
	n.num = &val
	n.precision = choosePrecision(n.unit, n.num) // n.unit is always "" here -- a
	// const node carries no unit declaration (Expr has no const_unit field) --
	// so this falls through to choosePrecision's magnitude-only branch.
	if kind == KindAssumed {
		n.assumedInputs = []string{id}
	} else {
		n.measuredInputs = []string{id}
	}
	return n
}

func (c *evalCtx) resolveOp(e Expr, path string) node {
	children := make([]node, len(e.Terms))
	operands := make([]Value, len(e.Terms))
	for i, t := range e.Terms {
		childPath := strconv.Itoa(i)
		if path != "" {
			childPath = path + "." + childPath
		}
		children[i] = c.resolve(t.Expr, childPath)
		operands[i] = c.toValue(children[i], t.LabelKey)
	}

	id := c.outputID
	if path != "" {
		id += ":" + path
	}

	result := node{
		id:             id,
		kind:           KindDerived,
		assumedInputs:  unionSorted(children, func(n node) []string { return n.assumedInputs }),
		measuredInputs: unionSorted(children, func(n node) []string { return n.measuredInputs }),
		sourceIDs:      unionSorted(children, func(n node) []string { return n.sourceIDs }),
	}

	emit := func() node {
		// result.precision is set here, at the point result.num is final for
		// every return path (missing, division-by-zero, non-finite, and
		// success all flow through emit()), rather than once up front --
		// choosePrecision looks at the actual computed magnitude, and that
		// magnitude does not exist yet at the top of this function.
		result.precision = choosePrecision(result.unit, result.num)
		c.steps = append(c.steps, Step{
			OutputID: c.outputID, Op: e.Op, Operands: operands, Result: c.toValue(result, ""),
		})
		return result
	}

	// MISSING dominates and is checked BEFORE any arithmetic, so no
	// operation ever runs on a value that does not exist.
	for _, ch := range children {
		if ch.num == nil {
			result.missing = MissingUpstream
			result.missingDetail = ch.id + ":" + string(ch.missing)
			return emit()
		}
	}

	if e.Op == OpDiv {
		for _, ch := range children[1:] {
			if *ch.num == 0 {
				result.missing = MissingDivideByZero
				return emit()
			}
		}
	}

	n := apply(e.Op, children)
	if math.IsNaN(n) || math.IsInf(n, 0) {
		result.missing = MissingNonFinite
		return emit()
	}

	result.num = &n
	result.vintage = agreeVintage(children)
	result.moe = nil // derived values never carry MOE — see types.go Value.MOE and contract §1.4
	return emit()
}

// recordInput remembers a genuine measured leaf (an actual indicator: or
// reach: lookup, never an output: passthrough) so EvalResult.Inputs can list
// every distinct one touched anywhere in the model, deduplicated by ref.
func (c *evalCtx) recordInput(n node) {
	if c.inputSeen == nil {
		c.inputSeen = map[string]node{}
	}
	c.inputSeen[n.id] = n
}

// toValue renders a resolved node as a wire Value. Every Value this package
// emits is built here, which is what makes the taint invariant total: there
// is no other place in this file that constructs a Value's AssumedInputs.
//
// Unit is only authoritatively known at the Output that owns a tree's root —
// Expr nodes do not carry one, by design (§1.3: "unit is declared on every
// output, never inferred"). Evaluate overwrites it on the top-level Output
// Value with the model's own declaration; the value here (n.unit, "" unless
// a param or measured leaf supplied one — see resolveRef) is a reasonable
// default for the intermediate Values inside Steps and Inputs, which are
// display aids for the analyst register rather than the number the page
// states as a fact.
//
// Precision is NOT a blind default. It used to be a flat 2 for every
// intermediate Value regardless of role or magnitude, which is exactly what
// let an analyst trace print an unreproducible multiplication: a share
// operand displayed at 2 raw decimal places ("0.26") carries far less
// information than the same quantity displayed as a percentage at 2 decimal
// places ("26.05%") does, because "decimal places" is a fixed-point
// resolution and a share's whole value lives below 1 -- the frontend's
// existing percent-style rendering for unit=="share"/"percent_points"
// recovers that resolution for free once the Unit is actually correct. Every
// node-construction site in this file now derives its own precision (see
// choosePrecision) or inherits an author-declared one (param refs); toValue
// only renders whatever the node already decided.
func (c *evalCtx) toValue(n node, labelKey string) Value {
	if labelKey == "" {
		labelKey = n.labelKey
	}
	return Value{
		ID:             n.id,
		LabelKey:       labelKey,
		Num:            n.num,
		Unit:           n.unit,
		Precision:      n.precision,
		Kind:           n.kind,
		Tier:           TierExperiment,
		Register:       RegAnalyst,
		Missing:        n.missing,
		MissingDetail:  n.missingDetail,
		AssumedInputs:  n.assumedInputs,
		MeasuredInputs: n.measuredInputs,
		Vintage:        n.vintage,
		MOE:            n.moe,
		Reliability:    n.reliability,
		SourceIDs:      n.sourceIDs,
	}
}

func unionSorted(children []node, get func(node) []string) []string {
	set := map[string]struct{}{}
	for _, c := range children {
		for _, s := range get(c) {
			set[s] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// choosePrecision derives display precision (decimal places, 0-2 -- the
// type's hard ceiling; see the doc comment on Value.Precision and contract
// §1.3) for an intermediate node that has no author-declared precision of
// its own: a measured indicator/reach leaf, a const, or a derived operation
// result. Param leaf nodes do NOT go through this function -- they carry the
// model author's own declared Precision (and Unit) instead; see resolveRef's
// "param" case.
//
// The two branches exist for one reason: "decimal places" is a fixed-point
// resolution, so its usefulness depends entirely on the value's own
// magnitude, and this package's values span several orders of magnitude in
// the same expression tree (a 0-1 share multiplied by a six-digit
// population count is the case this function exists to fix -- see PDI's
// analyst-trace reproduction bug).
//
//   - unit == share | percent_points: always 2. This is not "more decimal
//     places" in the sense that broke reproduction before -- it is the
//     existing, already-shipped convention (contract S6, S7, S17; see
//     cmd/pdi/frontend/policy-app.js's fmtValue) that a share renders as a
//     PERCENTAGE. Two decimal places on a percentage is four significant
//     decimal digits of the underlying 0-1 share, which is what actually
//     closes the reproduction gap -- 26.05% x 160,331 lands within a few
//     people of the true product; 0.26 x 160,331 does not. This function
//     does not perform the percent conversion itself (Value.Num stays the
//     raw 0-1 share on the wire, per contract S17 -- "share is 0-1, always,
//     on the wire"); it only certifies that 2 decimal places is the right
//     number to declare, because the client already knows how to render a
//     share-unit Value as a percentage.
//
//   - every other unit, including "" (unmapped -- see the caveat below):
//     0 decimal places once the magnitude is >= 1, because at that
//     magnitude a) sub-unit resolution is not meaningful for a count,
//     dollar amount, or population figure, and b) rounding to the nearest
//     integer contributes at most 0.5 of absolute error, which is
//     negligible against a co-operand of similar or larger magnitude in the
//     same product -- exactly what makes the population operand in the
//     motivating bug harmless once it stops carrying two invented decimal
//     places ("160,331.00"). Below magnitude 1, this function returns the
//     ceiling (2): it is the best available resolution within the hard cap,
//     but for a value whose true unit IS a share or ratio and simply never
//     got tagged as one by the caller, 2 raw decimal places is NOT always
//     enough to reproduce a downstream product against a large co-operand
//     -- see the eval_test.go case marked "known limitation: untagged
//     fraction" and the CLAUDE.md return-value note this fix reports
//     alongside it. Fixing that fully requires the caller to supply the
//     correct Unit; this function cannot infer a share from magnitude alone
//     without risking a false claim about a value that is a small ratio for
//     an unrelated reason.
func choosePrecision(unit Unit, num *float64) int {
	switch unit {
	case UnitShare, UnitPercentPoints:
		return 2
	}
	if num == nil {
		return 2 // irrelevant: Precision has no effect on a Value with no value
	}
	if math.Abs(*num) >= 1 {
		return 0
	}
	return 2
}

// agreeVintage carries a vintage forward only when every child that has one
// agrees. It does not gate on Kind: a derived child already carries its own
// correctly-agreed vintage (or "" if its own leaves disagreed), so checking
// direct children's non-empty vintages is checking transitive leaf agreement
// for free, recursively.
func agreeVintage(children []node) string {
	v := ""
	for _, c := range children {
		if c.vintage == "" {
			continue
		}
		if v == "" {
			v = c.vintage
		} else if v != c.vintage {
			return ""
		}
	}
	return v
}

func apply(op Op, children []node) float64 {
	switch op {
	case OpAdd:
		s := 0.0
		for _, c := range children {
			s += *c.num
		}
		return s
	case OpSub:
		s := *children[0].num
		for _, c := range children[1:] {
			s -= *c.num
		}
		return s
	case OpMul:
		p := 1.0
		for _, c := range children {
			p *= *c.num
		}
		return p
	case OpDiv:
		d := *children[0].num
		for _, c := range children[1:] {
			d /= *c.num
		}
		return d
	case OpMin:
		m := *children[0].num
		for _, c := range children[1:] {
			if *c.num < m {
				m = *c.num
			}
		}
		return m
	case OpMax:
		m := *children[0].num
		for _, c := range children[1:] {
			if *c.num > m {
				m = *c.num
			}
		}
		return m
	default:
		return math.NaN()
	}
}

// applyFloats is apply()'s operation set, over plain float64s instead of
// resolver nodes. It exists so ReproducesAtDisplayPrecision can run the same
// eight-operation arithmetic that resolveOp runs, but over ROUNDED display
// values instead of full-precision ones -- resolveOp itself must never round
// an intermediate (contract §2, "Rounding": "Never round an intermediate"),
// so that function cannot be reused directly for this purpose.
func applyFloats(op Op, vals []float64) float64 {
	switch op {
	case OpAdd:
		s := 0.0
		for _, v := range vals {
			s += v
		}
		return s
	case OpSub:
		s := vals[0]
		for _, v := range vals[1:] {
			s -= v
		}
		return s
	case OpMul:
		p := 1.0
		for _, v := range vals {
			p *= v
		}
		return p
	case OpDiv:
		d := vals[0]
		for _, v := range vals[1:] {
			if v == 0 {
				return math.NaN()
			}
			d /= v
		}
		return d
	case OpMin:
		m := vals[0]
		for _, v := range vals[1:] {
			if v < m {
				m = v
			}
		}
		return m
	case OpMax:
		m := vals[0]
		for _, v := range vals[1:] {
			if v > m {
				m = v
			}
		}
		return m
	default:
		return math.NaN()
	}
}

// displayScale is the number of raw wire-value units represented by one
// increment of v's own last DISPLAYED digit. For most units this is just
// 10^Precision. For a share, it is 100x that: contract S17 keeps a share's
// wire value in [0,1] always, but the reader never sees the raw fraction --
// fmtValue's unit=="share" branch renders it as a PERCENTAGE, so a share
// declared at Precision 2 is shown to 2 decimal places of a number that is
// itself 100x the wire value ("26.05", not "0.2605"), which is 2 more
// significant decimal digits of the underlying fraction than "2 decimal
// places" sounds like it should be. This is exactly the mechanism
// choosePrecision's doc comment describes, made concrete for arithmetic
// instead of prose.
func displayScale(v Value) float64 {
	scale := math.Pow(10, float64(v.Precision))
	if v.Unit == UnitShare {
		scale *= 100
	}
	return scale
}

// displayRoundedRaw is the number a reader actually has in hand for v, in
// v's own wire (raw) scale -- i.e., what you get if you take the digits
// printed on the page and convert them back to the units the arithmetic
// actually runs in ("26.05%" -> 0.2605, not "26.05"). This is the value a
// reader redoing the printed arithmetic by hand is actually multiplying,
// dividing, adding or subtracting.
func displayRoundedRaw(v Value) (float64, bool) {
	if v.Num == nil {
		return 0, false
	}
	scale := displayScale(v)
	return math.Round(*v.Num*scale) / scale, true
}

// reproductionRelTol is the external, precision-independent bar a
// recomputation must clear: within 0.1% of the result's own magnitude,
// roughly three significant figures -- the resolution the type's 0-2
// decimal-place ceiling can realistically deliver once a share is rendered
// as a percentage (see choosePrecision and displayScale). It is deliberately
// NOT derived from the step's own operand precisions the way an
// error-propagation bound would be, because that bound is tautologically
// satisfied by construction: a coarser operand precision produces both a
// larger recomputation error AND a proportionally larger "implied"
// tolerance, so a self-referential bound can never catch a badly-chosen
// precision -- which is exactly how "0.26 x 160,331.00 = 41,768.00" shipped
// without tripping any internal consistency check (its own recompute,
// 41,686.06, is comfortably inside the ~800-person tolerance that a raw
// 2-decimal-place share implies for a six-digit co-operand). An external,
// fixed standard is required to say that implied tolerance is itself too
// coarse to be useful. The two numbers this constant exists to separate:
// the motivating bug misses it by ~2x (81.94 / 41768 = 0.196%); the fix
// (percent-scale display for the same share) clears it with >20x margin
// (1.77 / 41768 = 0.004%). See TestReproducesAtDisplayPrecision_*.
const reproductionRelTol = 0.001

// reproductionTolerance is the absolute tolerance for one step: the larger
// of reproductionRelTol's share of the result's own magnitude, and half a
// unit of the result's own last displayed digit (the floor, for results
// near zero where a purely relative bound would collapse to nothing).
func reproductionTolerance(result Value) float64 {
	halfUnit := 0.5 / displayScale(result)
	if result.Num == nil {
		return halfUnit
	}
	if rel := reproductionRelTol * math.Abs(*result.Num); rel > halfUnit {
		return rel
	}
	return halfUnit
}

// ReproducesAtDisplayPrecision is the reproduction invariant this file's
// precision derivation exists to satisfy, made checkable by code instead of
// only by a reviewer with a calculator:
//
//	Every Step, rendered at its own declared precisions, must reproduce
//	its own displayed result within the tolerance implied by that
//	precision.
//
// Concretely: take every operand's own DISPLAYED value (per displayScale --
// a share is read as a percentage, everything else at its own Precision),
// apply the step's operation to those displayed values exactly as a reader
// with a calculator would, and require the result to land within
// reproductionTolerance of the step's own (full-precision, per contract §2
// "never round an intermediate") result. A step with any missing operand or
// missing result is vacuously true -- there is no arithmetic for a reader to
// redo when a number is absent, and contract §2's MISSING-dominates rule
// already guarantees the result IS missing whenever any operand is.
func ReproducesAtDisplayPrecision(step Step) (ok bool, recomputed float64, tolerance float64) {
	if step.Result.Num == nil {
		return true, 0, 0
	}
	rounded := make([]float64, len(step.Operands))
	for i, op := range step.Operands {
		r, has := displayRoundedRaw(op)
		if !has {
			return true, 0, 0
		}
		rounded[i] = r
	}
	recomputed = applyFloats(step.Op, rounded)
	if math.IsNaN(recomputed) || math.IsInf(recomputed, 0) {
		return false, recomputed, 0
	}
	tolerance = reproductionTolerance(step.Result)
	return math.Abs(recomputed-*step.Result.Num) <= tolerance, recomputed, tolerance
}

// ── Evaluate ────────────────────────────────────────────────────────────

// Evaluate runs a validated PolicyModel's outputs, in declaration order,
// against a set of measured leaves and a caller's assumptions, and returns
// the EvalResult the contract defines for POST /v1/policy/simulate and the
// tier-2 block of a brief.
//
// It is a pure function: no database, no network, no model in the loop. The
// same (model, geoid, level, leaves, userParams) always produces the same
// EvalResult modulo ComputedAt.
//
// leaves is keyed by full ref ("indicator:<id>" or "reach:<id>"); userParams
// is keyed by bare param id and need only contain params the caller actually
// wants to override — every declared param not present here uses its
// authored Default, reported back with Source = SrcDefault (contract §4:
// "Partial submissions are legal").
func Evaluate(model *PolicyModel, geoid, level, geoName string, leaves map[string]MeasuredLeaf, userParams map[string]float64) (*EvalResult, error) {
	if err := Validate(model); err != nil {
		return nil, err
	}

	if level != "" && len(model.GeoLevels) > 0 {
		supported := false
		for _, l := range model.GeoLevels {
			if l == level {
				supported = true
				break
			}
		}
		if !supported {
			return nil, &EvalError{ErrGeoLevelUnsupport, fmt.Sprintf(
				"model %q does not declare geo level %q (declares %v)", model.PolicyID, level, model.GeoLevels)}
		}
	}

	paramByID := make(map[string]Param, len(model.Params))
	for _, p := range model.Params {
		paramByID[p.ID] = p
	}
	for k := range userParams {
		if _, ok := paramByID[k]; !ok {
			return nil, &EvalError{ErrUnknownParam, fmt.Sprintf("unknown param %q", k)}
		}
	}

	bound := make(map[string]float64, len(model.Params))
	bindings := make([]ParamBinding, 0, len(model.Params))
	for _, p := range model.Params {
		val := p.Default
		src := SrcDefault
		if uv, ok := userParams[p.ID]; ok {
			// Rejected, never clamped — clamping substitutes a different
			// assumption than the one the reader made. Contract §1.3, §4.
			if uv < p.Min || uv > p.Max {
				return nil, &EvalError{ErrParamOutOfRange, fmt.Sprintf(
					"param %q = %g is outside [%g, %g]", p.ID, uv, p.Min, p.Max)}
			}
			val = uv
			src = SrcUser
		}
		bound[p.ID] = val
		bindings = append(bindings, ParamBinding{
			ID: p.ID, LabelKey: p.LabelKey, Num: val, Unit: p.Unit,
			Precision: p.Precision, Source: src, Linked: false,
		})
	}

	ctx := &evalCtx{
		leaves:    leaves,
		params:    bound,
		paramDefs: paramByID, // already built above for the unknown-param check
		outputs:   make(map[string]node, len(model.Outputs)),
		inputSeen: map[string]node{},
	}

	outVals := make([]Value, 0, len(model.Outputs))
	for _, o := range model.Outputs {
		ctx.outputID = o.ID
		n := ctx.resolve(o.Expr, "")
		ctx.outputs[o.ID] = n

		v := ctx.toValue(n, o.LabelKey)
		v.ID = o.ID
		v.Unit = o.Unit // declared, never inferred — contract §1.3
		v.Precision = o.Precision
		v.Register = o.Register
		outVals = append(outVals, v)
	}

	inputKeys := make([]string, 0, len(ctx.inputSeen))
	for k := range ctx.inputSeen {
		inputKeys = append(inputKeys, k)
	}
	sort.Strings(inputKeys)
	inputVals := make([]Value, 0, len(inputKeys))
	for _, k := range inputKeys {
		inputVals = append(inputVals, ctx.toValue(ctx.inputSeen[k], ""))
	}

	return &EvalResult{
		PolicyID:     model.PolicyID,
		ModelVersion: model.ModelVersion,
		GEOID:        geoid,
		Level:        level,
		GeoName:      geoName,
		Params:       bindings,
		Inputs:       inputVals,
		Outputs:      outVals,
		Steps:        ctx.steps,
		Absences:     []Absence{},
		RefusalKeys:  model.RefusalKeys,
		CaveatKeys:   model.CaveatKeys,
		// Sources needs data/sources.toml's Name/URL/MethodKey per SourceID,
		// which this package does not load (no DB, no network, per its
		// contract). The caller hydrates SourceRefs from the union of every
		// Value.SourceIDs in this result against its own source registry.
		Sources:    []SourceRef{},
		ComputedAt: time.Now().UTC(),
	}, nil
}
