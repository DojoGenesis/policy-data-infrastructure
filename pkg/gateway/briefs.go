package gateway

// GET /v1/policy/briefs/:policy_id — the tier-1 REACH assembly of
// docs/contracts/experiment-contract.md §3 (ADR-016). This file implements
// handleGetPolicyBrief only; the route is NOT registered here — plugin.go is
// owned by a serial integrator and two tracks writing it would clobber each
// other silently (see this track's dispatch note).
//
// # What is actually built here
//
// The full experiment.Brief envelope (policy, geography, reach, model,
// evidence, absences, refusals, sources) is assembled and returned, because
// that is the one response shape the contract defines for this route (§3) —
// a payload with a nil Model and no matching Absences entry is malformed
// (ADR-016 D9, contract S3). But the actual CONTENT this track can honestly
// populate is narrower than the envelope:
//
//   - reach (tier 1) is fully populated, from real store queries, for the
//     one policy whose eligibility rule maps onto data this platform holds
//     today: FH-TRANSIT-008, USDA FARA's LILA designation (research/08 §2).
//     Every other policy_id gets reach == nil plus an absences[] entry — S3,
//     not a gap in this handler.
//   - model (tier 2) and evidence (tier 3) are read directly from
//     data/policies/models/<policy_id>.json and
//     data/policies/evidence/<policy_id>.csv when present (contract §9),
//     each with a matching absences[] entry when its file is absent — which,
//     as of this session, is every policy_id except FH-TRANSIT-008. This
//     file does NOT implement or depend on pkg/gateway/simulate.go's
//     ModelLoader interface (that one exists for POST /simulate's evaluator
//     to consume; this handler never touches a model's Expr, only exposes
//     its declarations) — see this track's return-value notes for the one
//     real risk that leaves: two independent readers of the same JSON file
//     agreeing on shape by convention, not by a shared type-check.
//
// # The vintage-collision wording rule (research/08 §4, contract S6)
//
// USDA's LILA designation is a 2010-tract fact; this platform's tract
// geographies are 2020. ingest/fetch_usda_food.py's crosswalk turns the flag
// into a 0-1 POPULATION SHARE at both the county and (2020-)tract grain
// before it ever reaches the `indicators` table — usda_food_desert's stored
// value already IS that share, not a 0/1 flag. This file therefore never
// treats usda_food_desert as a boolean and never prints "this tract is a
// food desert" from it; MapLayerKey carries the required wording as an i18n
// KEY (pex.map.designated_share.legend) so the qualifying language travels
// as DATA a consumer cannot drop, the same precedent
// analysis/build_policy_impact.py sets with its own "interpretation" field.

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/experiment"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// briefGEOIDPattern is the contract's own regex (docs/contracts/experiment-contract.md
// §3), deliberately narrower than geo.ValidateGEOID: this route never accepts
// a 12-digit block group.
var briefGEOIDPattern = regexp.MustCompile(`^\d{2}$|^\d{5}$|^\d{11}$`)

// USDA Food Access Research Atlas identifiers this handler reads. See
// pkg/datasource/usda_food.go and ingest/fetch_usda_food.py for provenance;
// the variable descriptions there are the canonical documentation.
const (
	usdaSourceID  = "usda-food"
	usdaVintage   = "USDA-FARA-2019"
	usdaGeoVintage = "2010-tract"

	// varDesignatedShare is usda_food_desert: NOT a 0/1 flag in this
	// platform's stored data. ingest/fetch_usda_food.py's crosswalk-aware
	// load (§ "crosswalk-aware load", resolved 2026-08-08) turns FARA's
	// 2010-tract LILA flag into a 0-1 population share at every geography
	// it writes, county and 2020-tract alike. That is why this file always
	// treats it as UnitShare and never as a boolean.
	varDesignatedShare = "usda_food_desert"
	// varFaraPopulation is usda_food_population: FARA's own 2010-census
	// population base, crosswalked through the same allocation so it stays
	// the correct denominator for varDesignatedShare (same source, same
	// vintage, same crosswalk — multiplying the two never crosses a vintage
	// boundary the way pairing the share with ACS population would).
	varFaraPopulation = "usda_food_population"
	// varSNAPCount is usda_food_snap_count: a "count"-class variable, summed
	// county-wide by the ingest script. It is NOT restricted to designated
	// tracts at the county grain — see the totals-building code below for
	// why this handler does not attempt a designated-only SNAP total.
	varSNAPCount = "usda_food_snap_count"
)

// pilotDesignatedTracts pins the USDA FARA LILA designation at its OWN
// 2010-tract vintage for the one pilot geography this build ships with:
// Rock County, GEOID 55105. Verified against the ERS source in
// research/08_mvp_public_groceries_rock_county.md §3 (12 tracts, all urban
// 1-mile standard).
//
// This is reference data, not a query result, and that is deliberate: the
// repo holds no 2010 tract geometry (ADR-016 D6) and there is no Go-side
// 2010->2020 crosswalk join (data/crosswalks/wi_tract2010_tract2020.csv is
// read only by the Python ingest path). reach.designated_ids must carry the
// authority's own vintage (contract §3), and for any geoid not in this map
// the handler correctly omits designated_ids and the per-tract areas table
// rather than guess at one from the crosswalked share — a 2020 tract can
// straddle a 2010 designated/undesignated boundary, so there is no crisp
// membership test to derive from the share alone.
var pilotDesignatedTracts = map[string][]string{
	"55105": {
		"55105000100", "55105000300", "55105000600", "55105001000",
		"55105001100", "55105001600", "55105001700", "55105002000",
		"55105002100", "55105002300", "55105002400", "55105002500",
	},
}

// reachablePolicies lists every policy_id this build can express a tier-1
// eligibility rule for. research/08 §2 tested all 70 Hong positions against
// what PDI has loaded; FH-TRANSIT-008 (public groceries, keyed to USDA's
// food-access tract designation) is the one clean pass. Extending this map
// to a second policy needs a second rule and a second variable set — it is
// not a config toggle, so the map intentionally starts at one entry.
var reachablePolicies = map[string]bool{
	"FH-TRANSIT-008": true,
}

// usdaFaraLilaRule is FH-TRANSIT-008's eligibility test, attributed to USDA
// (contract S21 / ADR-016 D6): where a bill's language differs from the
// dataset PDI holds, the page must present the dataset's test as the
// dataset's, never as the bill's. AB1221's own text is unverified
// (contract OPEN-1) — this rule statement is deliberately silent about the
// bill and only names USDA's LILA test.
func usdaFaraLilaRule() experiment.EligibilityRule {
	return experiment.EligibilityRule{
		RuleID:       "usda_fara_lila",
		AuthorityKey: "pex.rule.usda_fara_lila.authority",
		StatementKey: "pex.rule.usda_fara_lila.statement",
		Vintage:      usdaVintage,
		GeoVintage:   usdaGeoVintage,
		SourceIDs:    []string{usdaSourceID},
	}
}

// pageLevelRefusalKeys are refusals true of every policy on this page,
// independent of which model (if any) is loaded — ADR-016 D1/D2 (no causal
// claim can ever be made from PDI's cross-sectional data; there is no time
// series anywhere in it).
//
// FIX-4: this used to be the head of a four-entry slice that also hardcoded
// "pex.refusal.no_siting_simulation" ("PDI does not simulate where a store
// would go") and "pex.refusal.no_food_desert_binary" onto all 70 policies —
// both are specific to the grocery-store / food-access subject matter of
// FH-TRANSIT-008, not true of a policy about, say, school meals or transit
// fares. Anything policy-specific belongs in that policy's OWN model file's
// refusal_keys (contract §1.3: "required and non-empty on every model") and
// is merged in only when a model is actually loaded — see
// handleGetPolicyBrief below. This slice is deliberately kept to the two
// claims that hold for every policy PDI could ever model, model or no model.
var pageLevelRefusalKeys = []string{
	"pex.refusal.no_causal_claim",
	"pex.refusal.no_time_series",
}

// handleGetPolicyBrief serves GET /v1/policy/briefs/:policy_id?geoid=&level=.
// Not registered onto any router group by this file — see the package
// comment above.
func (p *PolicyPlugin) handleGetPolicyBrief(c *gin.Context) {
	policyID := c.Param("policy_id")
	if policyID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "policy_id path parameter required"})
		return
	}

	ctx := c.Request.Context()

	pol, err := p.store.GetPolicy(ctx, policyID)
	if err != nil {
		if isNotFound(err) {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "policy not found", Detail: policyID})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "policy lookup failed", Detail: err.Error()})
		return
	}
	if pol == nil {
		// The mock store pattern used in this package's tests returns
		// (nil, nil) for "no such row" rather than a wrapped pgx.ErrNoRows;
		// treat that the same as isNotFound so a live Postgres error and a
		// mock's zero value both reach the same 404.
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "policy not found", Detail: policyID})
		return
	}

	// Tier 2 model is loaded BEFORE geography resolution: contract S4's
	// default-geography chain is requested -> model.default_geoid -> state
	// fallback "55", so the model's declared default has to be known before
	// resolveBriefGeoid can decide what "absent ?geoid=" means (FIX-2 — this
	// rung of S4 was previously unreachable because nothing loaded the model
	// this early, so every unqualified page load landed on statewide
	// Wisconsin instead of the pilot county).
	modelResult, err := loadModelDescriptor(policyID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "model assembly failed", Detail: err.Error()})
		return
	}

	geoid, geoSource, err := resolveBriefGeoid(c.Query("geoid"), modelResult.DefaultGEOID)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid geoid", Detail: err.Error()})
		return
	}

	geog, err := p.store.GetGeography(ctx, geoid)
	if err != nil && !isNotFound(err) {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "geography lookup failed", Detail: err.Error()})
		return
	}
	if geog == nil {
		c.JSON(http.StatusUnprocessableEntity, ErrorResponse{Error: "unknown geoid", Detail: geoid})
		return
	}

	brief := experiment.Brief{
		Policy: policyRefFromRecord(*pol),
		Geography: experiment.Geography{
			GEOID:  geoid,
			Name:   geog.Name,
			Level:  string(geog.Level),
			Source: geoSource,
		},
		GeneratedAt: time.Now().UTC(),
		RefusalKeys: append([]string(nil), pageLevelRefusalKeys...),
		Absences:    []experiment.Absence{},
		Sources:     []experiment.SourceRef{},
	}

	reach, reachAbsence, err := buildReach(ctx, p.store, policyID, geoid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "reach assembly failed", Detail: err.Error()})
		return
	}
	brief.Reach = reach
	if reachAbsence != nil {
		brief.Absences = append(brief.Absences, *reachAbsence)
	} else if reach != nil {
		brief.Sources = append(brief.Sources, reach.Sources...)
	}

	// Tier 2 — data/policies/models/<policy_id>.json, when present (contract
	// §9). S3: a nil Model MUST carry a matching absences entry. FIX-1: the
	// absence reason now distinguishes "this policy has no model" (ordinary,
	// MissingNoModel) from "the models directory itself is unreachable"
	// (an operational defect, MissingNotLoaded) — see loadModelDescriptor.
	brief.Model = modelResult.Descriptor
	if modelResult.Absence != nil {
		brief.Absences = append(brief.Absences, *modelResult.Absence)
	}
	if brief.Model != nil {
		// FIX-4: model-level refusals are declared per policy in the model
		// file's own refusal_keys (contract §1.3) and merged in only for
		// policies that actually have a model — never applied globally.
		brief.RefusalKeys = sortedUnion(brief.RefusalKeys, brief.Model.RefusalKeys)
	}

	// Tier 3 — data/policies/evidence/<policy_id>.csv, when present (contract
	// §5/§9). Same S3 requirement as tier 2: an empty Evidence[] MUST carry a
	// matching absences entry, a non-empty one must not. FIX-1 applies here
	// too: an unreachable evidence root is distinguished from an ordinary
	// missing per-policy file — see loadEvidenceItems.
	evidenceResult, err := loadEvidenceItems(policyID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "evidence assembly failed", Detail: err.Error()})
		return
	}
	brief.Evidence = evidenceResult.Items
	if brief.Evidence == nil {
		// Contract §3 shows "evidence": [] for the no-file case, not null —
		// encoding/json marshals a nil slice as null, so this must be
		// explicit.
		brief.Evidence = []experiment.EvidenceItem{}
	}
	if evidenceResult.Absence != nil {
		brief.Absences = append(brief.Absences, *evidenceResult.Absence)
	}

	c.JSON(http.StatusOK, brief)
}

// resolveBriefGeoid implements contract S4's full default-geography chain:
// requested -> model's declared default_geoid -> "55" (Wisconsin, state
// level). modelDefaultGeoid is the empty string when no model was loaded or
// the model declares none, which correctly collapses the chain to
// requested-or-state-fallback (FIX-2's known-bad-control case).
func resolveBriefGeoid(raw, modelDefaultGeoid string) (geoid string, source experiment.GeoSource, err error) {
	if raw != "" {
		if !briefGEOIDPattern.MatchString(raw) {
			return "", "", fmt.Errorf("geoid %q must be a 2, 5, or 11-digit FIPS code", raw)
		}
		return raw, experiment.GeoRequested, nil
	}
	if modelDefaultGeoid != "" {
		return modelDefaultGeoid, experiment.GeoModelDefault, nil
	}
	return "55", experiment.GeoStateFallback, nil
}

// policyRefFromRecord maps a store.PolicyRecord onto the wire shape the
// contract defines for policy.* (§3). Candidate/office/bill are attribution
// data (ADR-016 D8), not the headline.
func policyRefFromRecord(p store.PolicyRecord) experiment.PolicyRef {
	return experiment.PolicyRef{
		PolicyID:       p.ID,
		Title:          p.Title,
		Description:    p.Description,
		ClaimEmpirical: p.ClaimsEmpirical,
		Candidate:      p.Candidate,
		Office:         p.Office,
		BillReferences: p.BillReferences,
		Category:       p.Category,
	}
}

// buildReach assembles the tier-1 block for policyID at geoid. It returns
// (nil, an absences entry, nil) for any policy this build cannot express an
// eligibility rule for — contract §3: "null only when no rule is
// expressible" — and never returns both a non-nil Reach and a non-nil
// Absence.
func buildReach(ctx context.Context, s store.Store, policyID, geoid string) (*experiment.Reach, *experiment.Absence, error) {
	if !reachablePolicies[policyID] {
		return nil, &experiment.Absence{
			Tier:      experiment.TierReach,
			Reason:    experiment.MissingNotLoaded,
			ReasonKey: "pex.absence.not_loaded",
			Detail:    "no eligibility rule is expressible for this policy against loaded data",
		}, nil
	}

	totals, err := reachTotals(ctx, s, geoid)
	if err != nil {
		return nil, nil, err
	}

	var areas []experiment.Area
	var designatedIDs []string
	if pinned, ok := pilotDesignatedTracts[geoid]; ok {
		designatedIDs = append([]string(nil), pinned...)
		sort.Strings(designatedIDs)
		areas, err = reachAreas(ctx, s, geoid)
		if err != nil {
			return nil, nil, err
		}
	}

	reach := &experiment.Reach{
		Rule:          usdaFaraLilaRule(),
		Totals:        totals,
		Areas:         areas,
		DesignatedIDs: designatedIDs,
		MapLayerKey:   "pex.map.designated_share.legend",
		Sources: []experiment.SourceRef{
			{
				SourceID:  usdaSourceID,
				Name:      "USDA Food Access Research Atlas",
				Vintage:   usdaVintage,
				URL:       "https://www.ers.usda.gov/data-products/food-access-research-atlas/",
				MethodKey: "pex.method.usda_fara_crosswalk",
			},
		},
	}
	return reach, nil, nil
}

// reachTotals builds the headline measured figures for the requested
// geography. Every total here is either a direct measured leaf or a
// measured*measured product (never touching an assumption), so
// ClaimClass() resolves to "measurement" for all of them by construction
// (len(AssumedInputs) == 0) — see pkg/experiment.Value.ClaimClass.
//
// What is deliberately NOT here: a "SNAP households in designated tracts"
// total. usda_food_snap_count is registered "count" and sums cleanly at the
// county grain (stats.ClassifyVariable → AggSum), but that county-level sum
// is county-WIDE, not restricted to designated tracts — the ingest script
// sums every tract in the county regardless of the LILA flag. Producing a
// designated-only figure needs the same 2010->2020 crosswalk join
// pilotDesignatedTracts' doc comment explains this file does not have in
// Go. Approximating it by re-deriving membership from the crosswalked share
// risks exactly the class of bug this repo has already shipped once
// (sentinel-zero dollar trends, MEMORY: sentinel-cost-values-fake-a-dollar-trend)
// — a plausible number nobody asked PDI to invent. Per-tract SNAP counts are
// still available in reach.areas[] for the pilot geography; summing a
// reader-chosen subset there is a presentation concern, not a claim this API
// needs to make on the reader's behalf.
func reachTotals(ctx context.Context, s store.Store, geoid string) ([]experiment.Value, error) {
	rows, err := s.QueryIndicators(ctx, store.IndicatorQuery{
		GEOIDs:      []string{geoid},
		VariableIDs: []string{varDesignatedShare, varFaraPopulation},
		Vintage:     usdaVintage,
	})
	if err != nil {
		return nil, fmt.Errorf("query reach totals for %s: %w", geoid, err)
	}
	byVar := indicatorByVariable(rows)

	shareVal := measuredLeaf("reach.designated_share", "pex.output.designated_share.label",
		experiment.UnitShare, 2, byVar[varDesignatedShare], varDesignatedShare, usdaSourceID, usdaVintage)
	popVal := measuredLeaf("reach.fara_population", "pex.output.fara_population.label",
		experiment.UnitPeople, 0, byVar[varFaraPopulation], varFaraPopulation, usdaSourceID, usdaVintage)
	designatedPop := deriveProduct("reach.designated_population", "pex.output.designated_population.label",
		experiment.UnitPeople, 0, shareVal, popVal)

	totals := []experiment.Value{shareVal, designatedPop}

	// Tract-count context ("N of M tracts") only makes sense at county
	// granularity, and only when the platform's tract catalog can answer
	// it directly — no USDA data involved, so no vintage collision risk.
	if len(geoid) == 5 {
		tracts, err := s.QueryGeographies(ctx, store.GeoQuery{
			Level:      geo.Tract,
			StateFIPS:  geoid[:2],
			CountyFIPS: geoid[2:5],
			Limit:      10000,
		})
		if err != nil {
			return nil, fmt.Errorf("query tract count for %s: %w", geoid, err)
		}
		totals = append(totals, countValue("reach.total_tract_count",
			"pex.output.total_tract_count.label", len(tracts)))

		if pinned, ok := pilotDesignatedTracts[geoid]; ok {
			totals = append(totals, designatedTractCountValue(len(pinned)))
		}
	}

	return totals, nil
}

// reachAreas builds the per-tract analyst-register table for a pilot
// geography — every 2020-vintage tract in the county, each carrying its
// crosswalked designated-population share (contract S6 wording; never a
// crisp flag) plus its own measured indicators.
func reachAreas(ctx context.Context, s store.Store, geoid string) ([]experiment.Area, error) {
	tracts, err := s.QueryGeographies(ctx, store.GeoQuery{
		Level:      geo.Tract,
		StateFIPS:  geoid[:2],
		CountyFIPS: geoid[2:5],
		Limit:      10000,
	})
	if err != nil {
		return nil, fmt.Errorf("query tracts for %s: %w", geoid, err)
	}
	if len(tracts) == 0 {
		return nil, nil
	}

	tractIDs := make([]string, len(tracts))
	for i, t := range tracts {
		tractIDs[i] = t.GEOID
	}

	rows, err := s.QueryIndicators(ctx, store.IndicatorQuery{
		GEOIDs:      tractIDs,
		VariableIDs: []string{varDesignatedShare, varSNAPCount, varFaraPopulation},
		Vintage:     usdaVintage,
	})
	if err != nil {
		return nil, fmt.Errorf("query tract indicators for %s: %w", geoid, err)
	}
	byTractVar := make(map[string]map[string]*store.Indicator, len(tractIDs))
	for i := range rows {
		r := &rows[i]
		if byTractVar[r.GEOID] == nil {
			byTractVar[r.GEOID] = make(map[string]*store.Indicator)
		}
		byTractVar[r.GEOID][r.VariableID] = r
	}

	sort.Slice(tracts, func(i, j int) bool { return tracts[i].GEOID < tracts[j].GEOID })

	areas := make([]experiment.Area, 0, len(tracts))
	for _, t := range tracts {
		vars := byTractVar[t.GEOID]

		var designatedShare *float64
		if ind := vars[varDesignatedShare]; ind != nil && ind.Value != nil {
			v := *ind.Value
			designatedShare = &v
		}

		values := []experiment.Value{
			measuredLeaf(t.GEOID+":"+varSNAPCount, "pex.output.tract_snap.label",
				experiment.UnitHouseholds, 0, vars[varSNAPCount], varSNAPCount, usdaSourceID, usdaVintage),
			measuredLeaf(t.GEOID+":"+varFaraPopulation, "pex.output.tract_population.label",
				experiment.UnitPeople, 0, vars[varFaraPopulation], varFaraPopulation, usdaSourceID, usdaVintage),
		}
		for i := range values {
			values[i].Register = experiment.RegAnalyst
		}

		areas = append(areas, experiment.Area{
			GEOID:           t.GEOID,
			Name:            t.Name,
			Level:           "tract",
			DesignatedShare: designatedShare,
			Values:          values,
		})
	}
	return areas, nil
}

// envPolicyDataDir is the FIX-1 escape hatch: an explicit, operator-set root
// for the contract §9 data directories, checked ahead of any cwd-relative
// default. Named in the PDI_ convention this repo already uses
// (PDI_DATABASE_URL, PDI_WARM_CACHE, PDI_TRUSTED_PROXIES, the PDI_CHAT_* and
// PDI_RUN_* families — grepped across pkg/gateway and cmd/pdi before adding
// this one). deploy/pdi.service sets no WorkingDirectory, so systemd runs
// `pdi serve` with cwd "/" — a bare relative "data/policies" then resolves
// to "/data/policies", which does not exist, and both tiers 2 and 3 vanish
// in production while working perfectly on a developer's checkout. Setting
// PDI_POLICY_DATA_DIR=/opt/pdi/data/policies (or wherever the deploy
// actually stages it) in /etc/pdi/env makes the root explicit instead of an
// implicit cwd assumption, independent of whether deploy/pdi.service also
// gets a WorkingDirectory (a separate track's fix on the ops side; this is
// the Go side, per this track's dispatch note).
const envPolicyDataDir = "PDI_POLICY_DATA_DIR"

// policyDataRoot resolves the contract §9 data root: PDI_POLICY_DATA_DIR
// when set, else "data/policies" — the same relative default
// cmd/pdi/load_policies.go's --dir flag already uses for the same tree, kept
// as the default so a developer's checkout (cwd = repo root) needs no env
// var at all. The point of FIX-1 is not to change the default; it is to
// make the root an explicit, named, overridable value instead of two
// separate literals baked silently into this file with no way to point them
// anywhere else.
func policyDataRoot() string {
	if v := strings.TrimSpace(os.Getenv(envPolicyDataDir)); v != "" {
		return v
	}
	return "data/policies"
}

// modelsDir and evidenceDir are the contract §9 data roots. Package-level
// vars, not consts, so tests can point them at fixture directories without
// touching the real repo tree or relying on the test binary's CWD, which is
// the package directory under `go test`, not the repo root — the same
// override mechanism setBriefDataDirs (briefs_test.go) already uses,
// unaffected by adding the env var above it.
var (
	modelsDir   = filepath.Join(policyDataRoot(), "models")
	evidenceDir = filepath.Join(policyDataRoot(), "evidence")
)

// dataRootAbsence stats dir and returns a non-nil Absence when it does not
// exist or is not a directory — the FIX-1 "root unreachable" case, which
// MUST be reported distinctly from "this policy has no file in an otherwise-
// reachable directory" (MissingNoModel / a plain missing-evidence-file). Both
// previously collapsed to the same os.IsNotExist(err) branch: os.ReadFile
// and os.Open return exactly the same "file does not exist" error whether
// the leaf file is missing or an ancestor directory is missing, so a broken
// deploy (root gone) and an ordinary absent model (root fine, this policy's
// file just isn't there) were indistinguishable from the response alone —
// the exact silent-empty-tier failure mode this fix closes. tier/reasonKey
// are filled in by the caller, which knows whether it is tier 2 or tier 3.
func dataRootAbsence(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("%s: %w", dir, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s: not a directory", dir)
	}
	return nil
}

// modelLoadResult is loadModelDescriptor's full outcome. Exactly one of
// Descriptor and Absence is non-nil (the load-time-error case aside, which
// returns a non-nil error instead of either). DefaultGEOID carries
// PolicyModel.DefaultGEOID even when Descriptor is populated, because
// ModelDescriptor (the wire type, contract §3) deliberately does not expose
// default_geoid to the client — S4's resolution happens server-side, before
// the client ever sees a payload — so the handler needs it out-of-band
// rather than re-parsing the model file a second time.
type modelLoadResult struct {
	Descriptor   *experiment.ModelDescriptor
	DefaultGEOID string
	Absence      *experiment.Absence
}

// loadModelDescriptor reads data/policies/models/<policy_id>.json when
// present and returns the tier-2 descriptor the contract's §3 payload wants:
// params in full, outputs WITHOUT Expr. Result.Absence is non-nil — never an
// error — for both gaps FIX-1 requires to be distinguishable:
//   - the models directory itself is unreachable (MissingNotLoaded): a
//     deploy/config defect, not a fact about this policy.
//   - this policy specifically has no model file, in an otherwise-reachable
//     directory (MissingNoModel): the ordinary state of most of the 70 Hong
//     positions.
//
// KNOWN GAP (flagged in this track's return value, not fixed here — outside
// this file's ownership): pkg/experiment.ModelDescriptor.Outputs is typed
// []experiment.Output, and Output.Expr has no `omitempty` and is not a
// pointer, so a "descriptor-only" Output still marshals a
// `"expr":{"op":""}` object — never the real arithmetic tree, but not
// literally absent either, which is what the type's own doc comment
// promises ("never an Expr"). This function zeroes Expr on every output; it
// cannot make the key disappear without a type it does not own.
func loadModelDescriptor(policyID string) (*modelLoadResult, error) {
	if rootErr := dataRootAbsence(modelsDir); rootErr != nil {
		return &modelLoadResult{
			Absence: &experiment.Absence{
				Tier:      experiment.TierExperiment,
				Reason:    experiment.MissingNotLoaded,
				ReasonKey: "pex.absence.not_loaded",
				Detail:    fmt.Sprintf("models directory unreachable: %v", rootErr),
			},
		}, nil
	}

	path := filepath.Join(modelsDir, policyID+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &modelLoadResult{
				Absence: &experiment.Absence{
					Tier:      experiment.TierExperiment,
					Reason:    experiment.MissingNoModel,
					ReasonKey: "pex.absence.no_model",
				},
			}, nil
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

	outputs := make([]experiment.Output, len(m.Outputs))
	for i, o := range m.Outputs {
		outputs[i] = experiment.Output{
			ID:        o.ID,
			LabelKey:  o.LabelKey,
			Unit:      o.Unit,
			Precision: o.Precision,
			Register:  o.Register,
			// Expr deliberately zeroed — see the KNOWN GAP note above for
			// why this is the closest this function can get to "absent".
		}
	}

	return &modelLoadResult{
		Descriptor: &experiment.ModelDescriptor{
			ModelVersion: m.ModelVersion,
			TitleKey:     m.TitleKey,
			SummaryKey:   m.SummaryKey,
			GeoLevels:    m.GeoLevels,
			Params:       m.Params,
			Outputs:      outputs,
			RefusalKeys:  m.RefusalKeys,
			CaveatKeys:   m.CaveatKeys,
		},
		DefaultGEOID: m.DefaultGEOID,
	}, nil
}

// evidenceColumns is the contract §5 CSV column order, by header name, used
// to index each row rather than assuming position — a loader keyed to
// position breaks silently the day a column is reordered.
var evidenceColumns = []string{
	"policy_id", "evidence_id", "jurisdiction", "geo_scale", "period_start",
	"period_end", "intervention", "outcome", "direction", "finding",
	"effect_reported", "study_design", "evidence_strength", "source_title",
	"source_org", "source_url", "source_year", "accessed_date", "citation",
	"relates_to_claim", "claim_check", "transfer_note", "reviewed_by",
	"reviewed_date",
}

// evidenceLoadResult is loadEvidenceItems's full outcome, mirroring
// modelLoadResult's split (FIX-1): Absence is non-nil whenever Items ends up
// empty, and states WHY — root unreachable, file absent, or file present but
// every row filtered out (contract §5's per-row required-field check) — so a
// broken deploy no longer reads identically to "no evidence authored yet"
// (contract OPEN-4, the ordinary state of 69 of the 70 policies).
type evidenceLoadResult struct {
	Items   []experiment.EvidenceItem
	Absence *experiment.Absence
}

// loadEvidenceItems reads data/policies/evidence/<policy_id>.csv when
// present.
//
// Contract §5: "The loader refuses a row with an empty source_url, citation,
// or transfer_note." This is enforced per row, not per file — a bad row is
// skipped rather than failing the whole brief, because one unreviewed
// citation should not take down every other verified one on the same page.
func loadEvidenceItems(policyID string) (*evidenceLoadResult, error) {
	if rootErr := dataRootAbsence(evidenceDir); rootErr != nil {
		return &evidenceLoadResult{
			Absence: &experiment.Absence{
				Tier:      experiment.TierEvidence,
				Reason:    experiment.MissingNotLoaded,
				ReasonKey: "pex.absence.not_loaded",
				Detail:    fmt.Sprintf("evidence directory unreachable: %v", rootErr),
			},
		}, nil
	}

	path := filepath.Join(evidenceDir, policyID+".csv")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &evidenceLoadResult{
				Absence: &experiment.Absence{
					Tier:      experiment.TierEvidence,
					Reason:    experiment.MissingNotLoaded,
					ReasonKey: "pex.absence.not_loaded",
					Detail:    fmt.Sprintf("data/policies/evidence/%s.csv not present", policyID),
				},
			}, nil
		}
		return nil, fmt.Errorf("open evidence file %s: %w", path, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.LazyQuotes = true
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse evidence file %s: %w", path, err)
	}
	if len(rows) == 0 {
		return &evidenceLoadResult{
			Absence: &experiment.Absence{
				Tier:      experiment.TierEvidence,
				Reason:    experiment.MissingNotLoaded,
				ReasonKey: "pex.absence.not_loaded",
				Detail:    fmt.Sprintf("data/policies/evidence/%s.csv is empty (no header row)", policyID),
			},
		}, nil
	}

	idx := make(map[string]int, len(rows[0]))
	for i, h := range rows[0] {
		idx[strings.TrimSpace(h)] = i
	}
	for _, want := range evidenceColumns {
		if _, ok := idx[want]; !ok {
			return nil, fmt.Errorf("evidence file %s missing required column %q", path, want)
		}
	}
	get := func(row []string, col string) string {
		i := idx[col]
		if i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	var items []experiment.EvidenceItem
	for _, row := range rows[1:] {
		allEmpty := true
		for _, c := range row {
			if strings.TrimSpace(c) != "" {
				allEmpty = false
				break
			}
		}
		if allEmpty {
			continue
		}
		if get(row, "policy_id") != policyID {
			continue
		}
		// Contract §5: source_url, citation, and transfer_note are required
		// non-empty on every row.
		if get(row, "source_url") == "" || get(row, "citation") == "" || get(row, "transfer_note") == "" {
			continue
		}
		items = append(items, experiment.EvidenceItem{
			EvidenceID:     get(row, "evidence_id"),
			Jurisdiction:   get(row, "jurisdiction"),
			GeoScale:       get(row, "geo_scale"),
			PeriodStart:    get(row, "period_start"),
			PeriodEnd:      get(row, "period_end"),
			Intervention:   get(row, "intervention"),
			Outcome:        get(row, "outcome"),
			Direction:      experiment.EvidenceDirection(get(row, "direction")),
			Finding:        get(row, "finding"),
			EffectReported: get(row, "effect_reported"),
			StudyDesign:    get(row, "study_design"),
			Strength:       get(row, "evidence_strength"),
			SourceTitle:    get(row, "source_title"),
			SourceOrg:      get(row, "source_org"),
			SourceURL:      get(row, "source_url"),
			SourceYear:     get(row, "source_year"),
			AccessedDate:   get(row, "accessed_date"),
			Citation:       get(row, "citation"),
			RelatesToClaim: get(row, "relates_to_claim"),
			ClaimCheck:     experiment.ClaimCheck(get(row, "claim_check")),
			TransferNote:   get(row, "transfer_note"),
			ReviewedBy:     get(row, "reviewed_by"),
			ReviewedDate:   get(row, "reviewed_date"),
		})
	}

	result := &evidenceLoadResult{Items: items}
	if len(items) == 0 {
		result.Absence = &experiment.Absence{
			Tier:      experiment.TierEvidence,
			Reason:    experiment.MissingNotLoaded,
			ReasonKey: "pex.absence.not_loaded",
			Detail:    fmt.Sprintf("data/policies/evidence/%s.csv present but contributed no valid rows", policyID),
		}
	}
	return result, nil
}

// indicatorByVariable indexes a single-geography indicator query result by
// variable_id. Callers that queried more than one GEOID must not use this —
// it silently keeps only the last row per variable.
func indicatorByVariable(rows []store.Indicator) map[string]*store.Indicator {
	out := make(map[string]*store.Indicator, len(rows))
	for i := range rows {
		out[rows[i].VariableID] = &rows[i]
	}
	return out
}

// measuredLeaf builds a tier-1 measured Value from an indicator row, or a
// MISSING value with a stated reason when the row is absent — the leaf case
// of the §2 taint algorithm (op=ref, namespace "indicator"), applied by hand
// because tier 1 does not go through pkg/experiment's expression evaluator.
// USDA FARA publishes no sampling margin of error at all (it is a designated-
// tract dataset, not a survey), so Reliability is always "unknown" here —
// distinct from "high": not knowing the error is not the same as knowing it
// is small (pkg/experiment doc comment on RelUnknown).
func measuredLeaf(id, labelKey string, unit experiment.Unit, precision int, ind *store.Indicator, variableID, sourceID, vintage string) experiment.Value {
	v := experiment.Value{
		ID:             id,
		LabelKey:       labelKey,
		Unit:           unit,
		Precision:      precision,
		Kind:           experiment.KindMeasured,
		Tier:           experiment.TierReach,
		Register:       experiment.RegPublic,
		AssumedInputs:  []string{},
		MeasuredInputs: []string{},
		SourceIDs:      []string{sourceID},
	}
	if ind == nil || ind.Value == nil {
		v.Missing = experiment.MissingNotLoaded
		v.MeasuredInputs = []string{variableID}
		return v
	}
	val := *ind.Value
	v.Num = &val
	v.Vintage = vintage
	v.MeasuredInputs = []string{variableID}
	v.Reliability = experiment.RelUnknown
	return v
}

// countValue is a structural count from the platform's own geography
// catalog (e.g. "how many tracts in this county") — not a dataset value, so
// it carries no source_id and no dataset vintage. Always measured, never
// missing: the catalog either has rows or the caller would not have reached
// this geoid in the first place.
func countValue(id, labelKey string, n int) experiment.Value {
	val := float64(n)
	return experiment.Value{
		ID:             id,
		LabelKey:       labelKey,
		Num:            &val,
		Unit:           experiment.UnitCount,
		Precision:      0,
		Kind:           experiment.KindMeasured,
		Tier:           experiment.TierReach,
		Register:       experiment.RegPublic,
		AssumedInputs:  []string{},
		MeasuredInputs: []string{},
	}
}

// designatedTractCountValue reports the pinned designation count
// (pilotDesignatedTracts) as a measured figure sourced to USDA — see that
// map's doc comment for why this is reference data rather than a query
// result.
func designatedTractCountValue(n int) experiment.Value {
	val := float64(n)
	return experiment.Value{
		ID:             "reach.designated_tract_count",
		LabelKey:       "pex.output.designated_tract_count.label",
		Num:            &val,
		Unit:           experiment.UnitCount,
		Precision:      0,
		Kind:           experiment.KindMeasured,
		Tier:           experiment.TierReach,
		Register:       experiment.RegPublic,
		AssumedInputs:  []string{},
		MeasuredInputs: []string{},
		Vintage:        usdaVintage,
		SourceIDs:      []string{usdaSourceID},
	}
}

// deriveProduct multiplies two measured leaves and propagates their
// provenance under the §2 NODE algorithm (op=mul), applied by hand for the
// same reason measuredLeaf is: MISSING dominates and is checked before the
// arithmetic runs (never a substituted zero), the input lists union rather
// than overwrite, and MOE is never computed for a derived value (D5 — kept
// nil unconditionally here since a and b are both MOE-less USDA leaves
// already).
func deriveProduct(id, labelKey string, unit experiment.Unit, precision int, a, b experiment.Value) experiment.Value {
	out := experiment.Value{
		ID:             id,
		LabelKey:       labelKey,
		Unit:           unit,
		Precision:      precision,
		Kind:           experiment.KindDerived,
		Tier:           experiment.TierReach,
		Register:       experiment.RegPublic,
		AssumedInputs:  sortedUnion(a.AssumedInputs, b.AssumedInputs),
		MeasuredInputs: sortedUnion(a.MeasuredInputs, b.MeasuredInputs),
	}
	if a.Missing != experiment.MissingNone {
		out.Missing = experiment.MissingUpstream
		out.MissingDetail = a.ID + ":" + string(a.Missing)
		return out
	}
	if b.Missing != experiment.MissingNone {
		out.Missing = experiment.MissingUpstream
		out.MissingDetail = b.ID + ":" + string(b.Missing)
		return out
	}
	val := *a.Num * *b.Num
	out.Num = &val
	if a.Vintage != "" && a.Vintage == b.Vintage {
		out.Vintage = a.Vintage
	}
	out.Reliability = experiment.RelUnknown
	out.SourceIDs = sortedUnion(a.SourceIDs, b.SourceIDs)
	return out
}

func sortedUnion(a, b []string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, s := range append(append([]string{}, a...), b...) {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
