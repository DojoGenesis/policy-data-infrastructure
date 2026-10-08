// Package gateway — this file implements POST /v1/policy/simulate, the
// tier-2 "try your own numbers" endpoint of docs/contracts/experiment-contract.md
// §4 (ADR-016 D9, D12, S20).
//
// # What this endpoint is, and is not
//
// It is a pure function over numbers the brief already published: given a
// policy's tier-2 model, a geography, and a set of reader-supplied
// assumptions, it snaps those assumptions to their declared grids, validates
// them against their declared bounds, and asks the pkg/experiment evaluator
// to run the model's closed eight-operation arithmetic over them. It writes
// nothing, reserves no budget, and needs no run token (S20) — gating tier 2
// behind the POST /analyses token would gate the page's entire interactive
// layer behind a secret. It never calls a language model: nothing in this
// file imports pkg/grounding or any LLM client, and the taint/claim-class
// machine-readability the contract requires (§1.4, §1.5) is not something
// this handler constructs — it falls out of experiment.Value's and
// experiment.EvalResult's own MarshalJSON, which computes claim_class from
// assumed_inputs and refuses to let a caller set it directly. This handler's
// only job is to get honest inputs to that machinery and hand its honest
// output back unmodified.
//
// # Why this file does not touch PolicyPlugin
//
// The ownership manifest for this track is exactly two files: this one and
// its test. PolicyPlugin (pkg/gateway/plugin.go) is owned by the serial
// integrator, so SimulateHandler is a standalone type with its own
// constructor. The integrator wires a store.Store (which already satisfies
// GeoPolicyLookup below via structural typing — no adapter needed), a
// ModelLoader, and a SimulateEvaluator, then registers the route:
//
//	h := gateway.NewSimulateHandler(p.store, modelLoader, evaluator)
//	group.POST("/simulate", h.Handle)
//
// ModelLoader and SimulateEvaluator are declared here, not consumed from
// pkg/experiment, because pkg/experiment currently holds only its type
// vocabulary (types.go) — no model loader and no evaluator exist yet, and
// this track is explicitly told that is expected. Depending on an interface
// this file owns, rather than a concrete function in a sibling package that
// may not exist at build time, is what lets `go build ./...` and `go test
// ./...` pass for this package today, independent of when that other track
// lands. Whatever pkg/experiment eventually exports for evaluation only
// needs to satisfy SimulateEvaluator's method set (or be wrapped in one line
// by whoever does the wiring) for this handler to start calling the real
// thing instead of a test double.
package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/experiment"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// simulateGEOIDPattern is the contract's geoid format for this page —
// state, county, or tract only (§3: "^\d{2}$|^\d{5}$|^\d{11}$"). It is
// deliberately narrower than the project-wide reGEOID in middleware.go,
// which also accepts block-group and ward GEOIDs: those levels are not among
// the geo levels a tier-2 model may declare (contract §1.3 PolicyModel.
// GeoLevels example is ["county","state"]; research/08 adds "tract" for
// rollups), so accepting them here would let a malformed request pass format
// validation and fail later with a less specific error.
var simulateGEOIDPattern = regexp.MustCompile(`^\d{2}$|^\d{5}$|^\d{11}$`)

// GeoPolicyLookup is the subset of store.Store this handler reads: whether a
// policy_id names a real position (404 vs. everything else), and the
// human-readable name of a requested geography for EvalResult.GeoName.
//
// store.Store already implements this interface via Go's structural typing,
// so the integrator passes p.store directly — no adapter type is needed.
// The interface is declared narrow, rather than depending on store.Store's
// full ~40-method surface, so a test double here only has to implement two
// methods, and so this file documents exactly what it reads from the store.
type GeoPolicyLookup interface {
	GetGeography(ctx context.Context, geoid string) (*geo.Geography, error)
	GetPolicy(ctx context.Context, id string) (*store.PolicyRecord, error)
}

// ModelLoader loads a policy's tier-2 model by policy id (contract §9:
// data/policies/models/<policy_id>.json).
//
// LoadPolicyModel returns (nil, nil) — not an error — when the policy exists
// but has no model file. That is the ordinary state of most of the 70 Hong
// positions (ADR-016 "Negative and accepted") and the handler turns it into
// a 422 with missing_reason "no_model" per contract §4, not a 500. A non-nil
// error means the load itself failed (the file exists but does not parse,
// fails schema validation, or fails the load-time ref/cycle checks the
// contract's §1.3 describes) and the handler treats that as a 500: it is a
// data problem on PDI's side, not something a caller's request can fix by
// resubmitting.
type ModelLoader interface {
	LoadPolicyModel(ctx context.Context, policyID string) (*experiment.PolicyModel, error)
}

// SimulateEvaluator is the deterministic tier-2 arithmetic engine — the
// taint algorithm in contract §2, resolve(node, ctx) -> Value, run over
// every declared Output. It is owned by another build track inside
// pkg/experiment; this handler depends only on this method set.
//
// Evaluate must be a pure function of its arguments: contract §4's
// determinism requirement ("the same (policy_id, model_version, geoid,
// level, canonical params) must always produce byte-identical outputs")
// is a property Evaluate itself must hold, not something this handler can
// impose from outside. bindings are already validated (every value inside
// its declared [Min, Max]) and canonicalized (snapped to Step) by the time
// Evaluate sees them — see resolveParamBindings below — so Evaluate's own
// job is exactly the taint algorithm, not input hygiene.
//
// geoName is passed in, already resolved, rather than looked up again
// inside Evaluate: this handler already called GeoPolicyLookup.GetGeography
// once to confirm the geoid exists (contract §4, 422 unknown geoid), and an
// evaluator that repeated that lookup would need its own store dependency
// for a fact the caller already has.
type SimulateEvaluator interface {
	Evaluate(ctx context.Context, model *experiment.PolicyModel, geoid, level, geoName string, bindings []experiment.ParamBinding) (*experiment.EvalResult, error)
}

// SimulateErrorResponse extends the gateway's standard ErrorResponse
// (Error/Detail, handlers.go) with a machine-readable MissingReason for the
// two gaps contract §4 calls out by exact wire shape: a policy with no
// model file, and a model that does not declare the requested geo level.
// ErrorResponse is embedded (not duplicated) so every existing consumer of
// the Error/Detail shape keeps working, and MarshalJSON needs no override:
// Go's encoding/json flattens an embedded struct's fields into the same
// JSON object by default, so the wire form is exactly
// {"error": "...", "detail": "...", "missing_reason": "no_model"}.
type SimulateErrorResponse struct {
	ErrorResponse
	MissingReason experiment.MissingReason `json:"missing_reason"`
}

// SimulateHandler implements POST /v1/policy/simulate. Construct one with
// NewSimulateHandler and register Handle on whatever route the integrator
// chooses — this file does not register a route itself (see the file's
// package doc).
type SimulateHandler struct {
	lookup    GeoPolicyLookup
	models    ModelLoader
	evaluator SimulateEvaluator
}

// NewSimulateHandler builds a SimulateHandler. lookup is typically a
// PolicyPlugin's store.Store field passed directly (see GeoPolicyLookup);
// models and evaluator are the two pieces this track does not own.
func NewSimulateHandler(lookup GeoPolicyLookup, models ModelLoader, evaluator SimulateEvaluator) *SimulateHandler {
	return &SimulateHandler{lookup: lookup, models: models, evaluator: evaluator}
}

// Handle is the gin.HandlerFunc for POST /v1/policy/simulate.
//
// Validation order follows contract §4's own status list top to bottom —
// structural checks (does the policy exist, does it have a model, does the
// model support the requested level) before per-request checks (are the
// submitted params valid, does the geoid exist) — so a caller who gets the
// policy_id wrong is told that before PDI spends a store round-trip
// resolving a geography it will not use.
func (h *SimulateHandler) Handle(c *gin.Context) {
	ctx := c.Request.Context()

	var req experiment.SimulateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid request body", Detail: err.Error()})
		return
	}

	if req.PolicyID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "policy_id is required"})
		return
	}

	// 404 — policy_id is not a real position (contract §4).
	pol, err := h.lookup.GetPolicy(ctx, req.PolicyID)
	if err != nil {
		if isNotFound(err) {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "policy not found", Detail: req.PolicyID})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "policy lookup failed", Detail: err.Error()})
		return
	}
	if pol == nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "policy not found", Detail: req.PolicyID})
		return
	}

	// 422 no_model — the policy exists but tier 2 does not (contract §4,
	// experiment.MissingNoModel). Most of the 70 positions are in this
	// state; it is not an error condition for PDI, only for this request.
	model, err := h.models.LoadPolicyModel(ctx, req.PolicyID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "model load failed", Detail: err.Error()})
		return
	}
	if model == nil {
		c.JSON(http.StatusUnprocessableEntity, SimulateErrorResponse{
			ErrorResponse: ErrorResponse{Error: "policy has no experiment model"},
			MissingReason: experiment.MissingNoModel,
		})
		return
	}

	// geoid: required (contract's request example always carries one, and
	// the page that calls this endpoint has already resolved a geoid to
	// render tier 1 — see this file's package doc). Format-validated here,
	// ahead of its previous position near the bottom of this function,
	// because FIX-3 needs the GEOID's own structural level — state / county
	// / tract, derived from its digit length via pkg/geo's own convention —
	// as a precondition for resolving `level` at all (see the geo-level
	// checks immediately below). Existence (422 unknown geoid) is still
	// checked later, unchanged, once params are validated too — nothing
	// about EXISTENCE needs to move, only FORMAT.
	if req.GEOID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "geoid is required"})
		return
	}
	if !simulateGEOIDPattern.MatchString(req.GEOID) {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:  "invalid geoid format",
			Detail: "geoid must be 2, 5, or 11 zero-padded digits (state / county / tract)",
		})
		return
	}
	geoidInfo, err := geo.ParseGEOID(req.GEOID)
	if err != nil {
		// simulateGEOIDPattern above already accepts only what geo.ParseGEOID
		// accepts (2/5/11 all-digit), so this is unreachable in practice.
		// Treated as PDI's own defect, not the caller's, if it ever fires.
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "geoid parse failed", Detail: err.Error()})
		return
	}
	geoidLevel := string(geoidInfo.Level)

	// Level: empty request level -> the model's first declared level
	// (contract §4). A model that declares no levels at all is a
	// model-authoring defect this handler cannot repair, not a request
	// the caller can fix by resubmitting, so it is a 500.
	level := req.Level
	if level == "" {
		if len(model.GeoLevels) == 0 {
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Error:  "model declares no geo levels",
				Detail: req.PolicyID,
			})
			return
		}
		level = model.GeoLevels[0]
	} else if !containsString(model.GeoLevels, level) {
		// 422 geo_level_unsupported (contract §4): the model does not
		// declare this level at all.
		c.JSON(http.StatusUnprocessableEntity, SimulateErrorResponse{
			ErrorResponse: ErrorResponse{
				Error:  "geo level not supported by this model",
				Detail: fmt.Sprintf("model declares %v, request asked for %q", model.GeoLevels, req.Level),
			},
			MissingReason: experiment.MissingGeoLevelUnsupported,
		})
		return
	}

	// FIX-3: the GEOID's OWN structural level must agree with the level
	// about to be evaluated. Nothing previously checked this — a 2-digit
	// state GEOID submitted with (or defaulted into) level="county" was
	// evaluated and returned {"level":"county"} for a GEOID that was never
	// a county, silently mislabeling it rather than refusing. This is
	// distinct from the check above: that one asks "does the MODEL support
	// this level at all"; this one asks "does the GEOID the caller actually
	// supplied match the level about to be recorded on the result" — a
	// model can perfectly well support both "county" and "tract" and still
	// be handed a county GEOID at level="tract", which the check above
	// would let through. Same wire shape as the model-does-not-declare-it
	// case (geo_level_unsupported) because from the caller's perspective
	// both are "this geoid cannot be evaluated at this level".
	if geoidLevel != level {
		c.JSON(http.StatusUnprocessableEntity, SimulateErrorResponse{
			ErrorResponse: ErrorResponse{
				Error:  "geoid level does not match the evaluation level",
				Detail: fmt.Sprintf("geoid %q is a %s GEOID, evaluation level is %q", req.GEOID, geoidLevel, level),
			},
			MissingReason: experiment.MissingGeoLevelUnsupported,
		})
		return
	}

	// model_version: empty request pins the loaded version; a request that
	// names a version is not a request this loader can satisfy a second,
	// older copy of (ModelLoader has no version parameter — there is
	// exactly one loaded model per policy_id, matching how the rest of
	// this repo's model file layout works: one JSON file per policy_id,
	// contract §9). The response always echoes what was actually used
	// (contract §4), which is this value, so a caller pinning a version
	// PDI is not currently serving finds out from the response rather
	// than from a silently substituted answer.
	modelVersion := model.ModelVersion

	// Params: build canonical bindings in model declaration order.
	bindings, badErr := resolveParamBindings(model, req.Params)
	if badErr != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: badErr.Error()})
		return
	}

	// geoid existence — format already validated above.
	g, err := h.lookup.GetGeography(ctx, req.GEOID)
	if err != nil || g == nil {
		detail := req.GEOID
		if err != nil {
			detail = err.Error()
		}
		c.JSON(http.StatusUnprocessableEntity, ErrorResponse{Error: "unknown geoid", Detail: detail})
		return
	}

	result, err := h.evaluator.Evaluate(ctx, model, req.GEOID, level, g.Name, bindings)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "simulation failed", Detail: err.Error()})
		return
	}

	c.Header("ETag", canonicalETag(req.PolicyID, modelVersion, req.GEOID, level, bindings))
	c.JSON(http.StatusOK, result)
}

// resolveParamBindings builds the canonical ParamBinding list — every
// declared param, in model declaration order, each snapped to its Step grid
// — from a request's raw params map. It is the one place that enforces
// contract §4's hardest requirement: "a param is unknown, non-numeric, or
// outside [min, max] — never a clamp, never a silent drop." The submitted
// value itself is what gets range-checked; only after that check passes is
// it snapped to the Step grid (a different operation from clamping — S18
// establishes step-snapping as this system's standing convention for
// canonicalizing an assumption, used identically for share-link params).
//
// An error return is always the caller's fault (400); nothing here can fail
// for a reason attributable to PDI's own data.
func resolveParamBindings(model *experiment.PolicyModel, raw map[string]float64) ([]experiment.ParamBinding, error) {
	declared := make(map[string]experiment.Param, len(model.Params))
	for _, p := range model.Params {
		declared[p.ID] = p
	}
	for name, v := range raw {
		if _, ok := declared[name]; !ok {
			return nil, fmt.Errorf("unknown param %q", name)
		}
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("param %q must be a finite number", name)
		}
	}

	bindings := make([]experiment.ParamBinding, 0, len(model.Params))
	for _, decl := range model.Params {
		var (
			value  float64
			source experiment.ParamSource
		)
		if v, ok := raw[decl.ID]; ok {
			if v < decl.Min || v > decl.Max {
				return nil, fmt.Errorf("param %q value %v is outside [%v, %v]", decl.ID, v, decl.Min, decl.Max)
			}
			value = v
			source = experiment.SrcUser
		} else {
			value = decl.Default
			source = experiment.SrcDefault
		}

		snapped := snapToStep(value, decl.Min, decl.Step)
		// Step-snapping right at a boundary can push a hair outside
		// [Min, Max] on floating-point rounding alone (e.g. Max-Min not
		// an exact multiple of Step). This is not a second, hidden
		// clamp of the reader's assumption — the reader's raw value
		// already passed the range check above unmodified — it is a
		// numerical-safety floor under a snap this handler is already
		// performing.
		if snapped < decl.Min {
			snapped = decl.Min
		}
		if snapped > decl.Max {
			snapped = decl.Max
		}

		bindings = append(bindings, experiment.ParamBinding{
			ID:        decl.ID,
			LabelKey:  decl.LabelKey,
			Num:       snapped,
			Unit:      decl.Unit,
			Precision: decl.Precision,
			Source:    source,
			// Linked is always false here: comparison mode (contract
			// S13) shares a param across two policy models, a decision
			// that requires evaluating both models together. This
			// endpoint evaluates exactly one model, so there is never a
			// second side to link against. OPEN-8 in the contract
			// notes comparison mode's v1 scope is not yet decided.
			Linked: false,
		})
	}
	return bindings, nil
}

// snapToStep rounds v to the nearest point on the grid {min, min+step,
// min+2*step, ...}. A non-positive step (a model-authoring defect the
// evaluator's own load-time checks should catch, per contract §1.3) leaves
// v unsnapped rather than dividing by zero.
func snapToStep(v, min, step float64) float64 {
	if step <= 0 {
		return v
	}
	n := math.Round((v - min) / step)
	return min + n*step
}

// canonicalETag renders the determinism tuple contract §4 defines — policy
// id, model version, geoid, level, and every declared param snapped to Step
// and rendered at its Precision — into a strong ETag. Two requests that
// differ only in float noise below a param's declared precision (60 vs.
// 60.00000001, both precision 0) collapse to the same cache key, which is
// the point: the contract calls this exact tuple "also the ETag."
//
// This intentionally uses each param's Precision for the string rendering,
// not the full-precision Step-snapped value bindings already carry — the
// snapped value is what Evaluate computes with (S16: the server never
// rounds a number it computes with, only what it displays), while the ETag
// is a display-grade cache key that two clients requesting "the same
// assumption" in different float representations should collide on.
func canonicalETag(policyID, modelVersion, geoid, level string, bindings []experiment.ParamBinding) string {
	ordered := make([]experiment.ParamBinding, len(bindings))
	copy(ordered, bindings)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })

	var sb strings.Builder
	sb.WriteString(policyID)
	sb.WriteByte('|')
	sb.WriteString(modelVersion)
	sb.WriteByte('|')
	sb.WriteString(geoid)
	sb.WriteByte('|')
	sb.WriteString(level)
	for _, b := range ordered {
		sb.WriteByte('|')
		sb.WriteString(b.ID)
		sb.WriteByte('=')
		sb.WriteString(strconv.FormatFloat(b.Num, 'f', b.Precision, 64))
	}

	sum := sha256.Sum256([]byte(sb.String()))
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// containsString reports whether s appears in list.
func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
