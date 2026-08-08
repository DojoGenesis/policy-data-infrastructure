package gateway

// Executors for the queued-run API. Two jobs live here:
//
//   - tract_rollup (ADR-014 D7/D8, handoff item 3): the population-weighted,
//     coverage-checked, CI-carrying tract→county aggregation. The statistics
//     live in pkg/stats/rollup.go; this file is data plumbing and provenance.
//   - The five finished-but-unreachable statistics (D11, handoff item 5):
//     spearman, isolation_index, blinder_oaxaca, interaction_ols,
//     bootstrap_mean — routed instead of rewritten.
//
// Cache-key discipline: canon() runs once, at enqueue. The canonical
// parameter map is used for the FindAnalysisByKey lookup, stored on the run
// row, and copied verbatim into the persisted AnalysisResult. Run-time
// discoveries (the denominator's observed vintage, counts, warnings) go in
// Results, never Parameters — Parameters IS the cache key.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/stats"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// defaultRollupDenominator is CDC SVI's tract population (E_TOTPOP), loaded
// by ingest/fetch_cdc_svi.py. Chosen because it shares its 2022 vintage with
// the CDC PLACES rates that were blocked on a same-vintage denominator
// (ADR-014 F3; handoff 2026-08-02 item 3).
const defaultRollupDenominator = "svi_total_population"

const (
	defaultCoverageThreshold = 0.8
	defaultNBoot             = 1000
	defaultAlpha             = 0.05
)

var runExecutors = map[string]runExecutor{
	"tract_rollup": {
		canon: func(p map[string]interface{}) (map[string]interface{}, error) {
			varID, err := requireStringParam(p, "variable_id")
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"variable_id":              varID,
				"denominator_variable_id":  stringParam(p, "denominator_variable_id", defaultRollupDenominator),
				"coverage_threshold":       floatParam(p, "coverage_threshold", defaultCoverageThreshold),
				"n_boot":                   intParam(p, "n_boot", defaultNBoot),
				"alpha":                    floatParam(p, "alpha", defaultAlpha),
			}, nil
		},
		primaryVariable: func(p map[string]interface{}) string {
			return stringParam(p, "variable_id", "")
		},
		execute: executeTractRollup,
	},
	"spearman": {
		canon: func(p map[string]interface{}) (map[string]interface{}, error) {
			x, err := requireStringParam(p, "x_variable")
			if err != nil {
				return nil, err
			}
			y, err := requireStringParam(p, "y_variable")
			if err != nil {
				return nil, err
			}
			if x == y {
				return nil, fmt.Errorf("x_variable and y_variable must differ")
			}
			return map[string]interface{}{
				"x_variable": x,
				"y_variable": y,
				"n_boot":     intParam(p, "n_boot", defaultNBoot),
				"alpha":      floatParam(p, "alpha", defaultAlpha),
			}, nil
		},
		primaryVariable: func(p map[string]interface{}) string {
			return stringParam(p, "x_variable", "")
		},
		execute: executeSpearman,
	},
	"isolation_index": {
		canon: func(p map[string]interface{}) (map[string]interface{}, error) {
			g, err := requireStringParam(p, "group_variable")
			if err != nil {
				return nil, err
			}
			tot, err := requireStringParam(p, "total_variable")
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"group_variable": g,
				"total_variable": tot,
			}, nil
		},
		primaryVariable: func(p map[string]interface{}) string {
			return stringParam(p, "group_variable", "")
		},
		execute: executeIsolationIndex,
	},
	"blinder_oaxaca": {
		canon: func(p map[string]interface{}) (map[string]interface{}, error) {
			outcome, err := requireStringParam(p, "outcome_variable")
			if err != nil {
				return nil, err
			}
			preds, err := requireStringSliceParam(p, "predictor_variables")
			if err != nil {
				return nil, err
			}
			ga, err := requireStringParam(p, "group_a_geoid")
			if err != nil {
				return nil, err
			}
			gb, err := requireStringParam(p, "group_b_geoid")
			if err != nil {
				return nil, err
			}
			if len(ga) != 5 || len(gb) != 5 {
				return nil, fmt.Errorf("group_a_geoid and group_b_geoid must be 5-digit county GEOIDs")
			}
			if ga == gb {
				return nil, fmt.Errorf("group_a_geoid and group_b_geoid must differ")
			}
			return map[string]interface{}{
				"outcome_variable":    outcome,
				"predictor_variables": toIfaceSlice(preds),
				"group_a_geoid":       ga,
				"group_b_geoid":       gb,
			}, nil
		},
		primaryVariable: func(p map[string]interface{}) string {
			return stringParam(p, "outcome_variable", "")
		},
		execute: executeBlinderOaxaca,
	},
	"interaction_ols": {
		canon: func(p map[string]interface{}) (map[string]interface{}, error) {
			outcome, err := requireStringParam(p, "outcome_variable")
			if err != nil {
				return nil, err
			}
			preds, err := requireStringSliceParam(p, "predictor_variables")
			if err != nil {
				return nil, err
			}
			pairs, err := pairParam(p, "interactions")
			if err != nil {
				return nil, err
			}
			for _, pr := range pairs {
				if pr[0] < 0 || pr[0] >= len(preds) || pr[1] < 0 || pr[1] >= len(preds) {
					return nil, fmt.Errorf("interaction pair [%d,%d] out of range for %d predictors", pr[0], pr[1], len(preds))
				}
			}
			return map[string]interface{}{
				"outcome_variable":    outcome,
				"predictor_variables": toIfaceSlice(preds),
				"interactions":        pairsToIface(pairs),
			}, nil
		},
		primaryVariable: func(p map[string]interface{}) string {
			return stringParam(p, "outcome_variable", "")
		},
		execute: executeInteractionOLS,
	},
	"composite_index": {
		canon: func(p map[string]interface{}) (map[string]interface{}, error) {
			vars, err := requireStringSliceParam(p, "variables")
			if err != nil {
				return nil, err
			}
			if len(vars) < 2 {
				return nil, fmt.Errorf("a composite needs at least 2 variables")
			}
			out := map[string]interface{}{"variables": toIfaceSlice(vars)}
			// Weights, when given, must cover the variable list exactly —
			// the same loud contract the CLI enforces (an unlisted variable
			// used to be silently weighted 0). Explicit 0 remains a
			// deliberate exclusion.
			if raw, ok := p["weights"]; ok && raw != nil {
				wm, ok := raw.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("weights must be an object of {variable_id: weight}")
				}
				varSet := map[string]bool{}
				for _, v := range vars {
					varSet[v] = true
				}
				weights := map[string]interface{}{}
				for k, v := range wm {
					if !varSet[k] {
						return nil, fmt.Errorf("weights names %q, which is not in variables", k)
					}
					f, ok := v.(float64)
					if !ok {
						return nil, fmt.Errorf("weight for %q must be a number", k)
					}
					weights[k] = f
				}
				for _, v := range vars {
					if _, ok := weights[v]; !ok {
						return nil, fmt.Errorf("weights must cover every variable; missing %q (pass an explicit 0 to exclude)", v)
					}
				}
				out["weights"] = weights
				out["method"] = "weighted_zscore"
			} else {
				out["method"] = "equal_percentile"
			}
			return out, nil
		},
		primaryVariable: func(p map[string]interface{}) string {
			vars := stringSliceParam(p, "variables")
			if len(vars) > 0 {
				return vars[0]
			}
			return ""
		},
		execute: executeCompositeIndex,
	},
	"correlation_matrix": {
		canon: func(p map[string]interface{}) (map[string]interface{}, error) {
			vars, err := requireStringSliceParam(p, "variables")
			if err != nil {
				return nil, err
			}
			if len(vars) < 2 {
				return nil, fmt.Errorf("a correlation matrix needs at least 2 variables")
			}
			return map[string]interface{}{"variables": toIfaceSlice(vars)}, nil
		},
		primaryVariable: func(p map[string]interface{}) string {
			vars := stringSliceParam(p, "variables")
			if len(vars) > 0 {
				return vars[0]
			}
			return ""
		},
		execute: executeCorrelationMatrix,
	},
	"factor_rollup": {
		canon: func(p map[string]interface{}) (map[string]interface{}, error) {
			return map[string]interface{}{
				"model_vintage":           stringParam(p, "model_vintage", "2023-efa-v2"),
				"denominator_variable_id": stringParam(p, "denominator_variable_id", defaultRollupDenominator),
				"coverage_threshold":      floatParam(p, "coverage_threshold", defaultCoverageThreshold),
				"n_boot":                  intParam(p, "n_boot", defaultNBoot),
				"alpha":                   floatParam(p, "alpha", defaultAlpha),
			}, nil
		},
		primaryVariable: func(p map[string]interface{}) string {
			// The run's vintage key resolves from the DENOMINATOR's data —
			// the model vintage lives in parameters, so a model revision
			// changes the cache key through params and a denominator
			// refresh changes it through vintage. Either way: recompute.
			return stringParam(p, "denominator_variable_id", defaultRollupDenominator)
		},
		execute: executeFactorRollup,
	},
	"dissimilarity": {
		canon: func(p map[string]interface{}) (map[string]interface{}, error) {
			g, err := requireStringParam(p, "group_variable")
			if err != nil {
				return nil, err
			}
			ref, err := requireStringParam(p, "reference_variable")
			if err != nil {
				return nil, err
			}
			if g == ref {
				return nil, fmt.Errorf("group_variable and reference_variable must differ")
			}
			pair, err := requireStringParam(p, "group_pair")
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"group_variable":     g,
				"reference_variable": ref,
				"group_pair":         pair,
				"n_boot":             intParam(p, "n_boot", defaultNBoot),
				"alpha":              floatParam(p, "alpha", defaultAlpha),
			}, nil
		},
		primaryVariable: func(p map[string]interface{}) string {
			return stringParam(p, "group_variable", "")
		},
		execute: executeDissimilarity,
	},
	"bootstrap_mean": {
		canon: func(p map[string]interface{}) (map[string]interface{}, error) {
			varID, err := requireStringParam(p, "variable_id")
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"variable_id": varID,
				"n_boot":      intParam(p, "n_boot", defaultNBoot),
				"alpha":       floatParam(p, "alpha", defaultAlpha),
			}, nil
		},
		primaryVariable: func(p map[string]interface{}) string {
			return stringParam(p, "variable_id", "")
		},
		execute: executeBootstrapMean,
	},
}

// ── data plumbing ───────────────────────────────────────────────────────────

// childTracts returns the tract GEOIDs inside a scope, sorted.
func childTracts(ctx context.Context, s store.Store, scopeLevel, scopeGEOID string) ([]string, error) {
	q := store.GeoQuery{Level: geo.Tract, Limit: 10000}
	switch scopeLevel {
	case "state":
		q.StateFIPS = scopeGEOID
	case "county":
		if len(scopeGEOID) != 5 {
			return nil, fmt.Errorf("county scope GEOID must be 5 digits, got %q", scopeGEOID)
		}
		q.StateFIPS = scopeGEOID[:2]
		q.CountyFIPS = scopeGEOID[2:5]
	default:
		return nil, fmt.Errorf("unsupported scope level %q (want county or state)", scopeLevel)
	}
	geos, err := s.QueryGeographies(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query tracts: %w", err)
	}
	ids := make([]string, len(geos))
	for i, g := range geos {
		ids[i] = g.GEOID
	}
	sort.Strings(ids)
	return ids, nil
}

// indicatorVector fetches geoid→value for one variable. vintage "" means
// latest; the returned vintage is the one observed on the rows (for
// provenance), or "" when no rows matched.
func indicatorVector(ctx context.Context, s store.Store, geoids []string, variableID, vintage string) (map[string]*float64, string, error) {
	q := store.IndicatorQuery{
		GEOIDs:      geoids,
		VariableIDs: []string{variableID},
	}
	if vintage == "" {
		q.LatestOnly = true
	} else {
		q.Vintage = vintage
	}
	rows, err := s.QueryIndicators(ctx, q)
	if err != nil {
		return nil, "", fmt.Errorf("query %s: %w", variableID, err)
	}
	vec := make(map[string]*float64, len(rows))
	observed := ""
	for _, r := range rows {
		vec[r.GEOID] = r.Value
		if observed == "" {
			observed = r.Vintage
		}
	}
	return vec, observed, nil
}

// variableUnit returns the registered unit for a variable ("" if the
// variable is unregistered). Queried fresh per run so variables registered
// after server start still classify correctly.
func variableUnit(ctx context.Context, s store.Store, variableID string) (string, error) {
	vars, err := s.QueryVariables(ctx)
	if err != nil {
		return "", fmt.Errorf("query variables: %w", err)
	}
	for _, v := range vars {
		if v.VariableID == variableID {
			return v.Unit, nil
		}
	}
	return "", nil
}

// ── executors ───────────────────────────────────────────────────────────────

// executeTractRollup aggregates one variable from tracts to counties under
// the D7 class rules, with a bootstrap interval per published county (D8)
// and a recorded coverage threshold. Withheld counties get NO score row —
// absent, not caveated (D2) — and the reason is recorded in Results so the
// API can distinguish a refusal from a gap.
func executeTractRollup(ctx context.Context, s store.Store, run *store.AnalysisRun) (store.AnalysisResult, []store.AnalysisScore, error) {
	var zero store.AnalysisResult
	p := run.Parameters
	varID := stringParam(p, "variable_id", "")
	denomID := stringParam(p, "denominator_variable_id", defaultRollupDenominator)
	threshold := floatParam(p, "coverage_threshold", defaultCoverageThreshold)
	nBoot := intParam(p, "n_boot", defaultNBoot)
	alpha := floatParam(p, "alpha", defaultAlpha)

	unit, err := variableUnit(ctx, s, varID)
	if err != nil {
		return zero, nil, err
	}
	rule := stats.ClassifyVariable(varID, unit)

	tracts, err := childTracts(ctx, s, run.ScopeLevel, run.ScopeGEOID)
	if err != nil {
		return zero, nil, err
	}
	if len(tracts) == 0 {
		return zero, nil, fmt.Errorf("no tracts in scope %s:%s", run.ScopeLevel, run.ScopeGEOID)
	}

	values, _, err := indicatorVector(ctx, s, tracts, varID, run.Vintage)
	if err != nil {
		return zero, nil, err
	}
	weights, denomVintage, err := indicatorVector(ctx, s, tracts, denomID, "")
	if err != nil {
		return zero, nil, err
	}

	// Group tracts by county prefix.
	byCounty := make(map[string][]string)
	for _, t := range tracts {
		byCounty[t[:5]] = append(byCounty[t[:5]], t)
	}
	counties := make([]string, 0, len(byCounty))
	for c := range byCounty {
		counties = append(counties, c)
	}
	sort.Strings(counties)

	scores := make([]store.AnalysisScore, 0, len(counties))
	withheld := make(map[string]interface{})
	published := 0

	for _, county := range counties {
		inputs := make([]stats.RollupInput, 0, len(byCounty[county]))
		for _, t := range byCounty[county] {
			inputs = append(inputs, stats.RollupInput{
				GEOID:  t,
				Value:  values[t],
				Weight: weights[t],
			})
		}
		res, reason := stats.RollupChildren(rule, inputs, threshold, nBoot, alpha)
		if res == nil {
			withheld[county] = reason
			continue
		}
		published++
		scores = append(scores, store.AnalysisScore{
			GEOID:      county,
			Score:      res.Value,
			Percentile: 0,
			Details: map[string]interface{}{
				"ci_lower":  res.CI.Lower,
				"ci_upper":  res.CI.Upper,
				"n_tracts":  res.N,
				"of_tracts": res.Total,
				"coverage":  res.Coverage,
			},
		})
	}

	result := store.AnalysisResult{
		Type:       run.RunType,
		ScopeGEOID: run.ScopeGEOID,
		ScopeLevel: run.ScopeLevel,
		Vintage:    run.Vintage,
		Parameters: run.Parameters,
		Results: map[string]interface{}{
			"rule":                rule.String(),
			"unit":                unit,
			"published_counties":  published,
			"withheld_counties":   withheld,
			"denominator":         denomID,
			"denominator_vintage": denomVintage,
			"tracts_in_scope":     len(tracts),
		},
	}
	return result, scores, nil
}

// executeSpearman computes the rank correlation between two variables over
// the scope's tracts, with a paired-bootstrap interval on the complete cases.
func executeSpearman(ctx context.Context, s store.Store, run *store.AnalysisRun) (store.AnalysisResult, []store.AnalysisScore, error) {
	var zero store.AnalysisResult
	p := run.Parameters
	xVar := stringParam(p, "x_variable", "")
	yVar := stringParam(p, "y_variable", "")
	nBoot := intParam(p, "n_boot", defaultNBoot)
	alpha := floatParam(p, "alpha", defaultAlpha)

	tracts, err := childTracts(ctx, s, run.ScopeLevel, run.ScopeGEOID)
	if err != nil {
		return zero, nil, err
	}
	xs, xVint, err := indicatorVector(ctx, s, tracts, xVar, run.Vintage)
	if err != nil {
		return zero, nil, err
	}
	ys, yVint, err := indicatorVector(ctx, s, tracts, yVar, "")
	if err != nil {
		return zero, nil, err
	}

	xcol := make([]*float64, len(tracts))
	ycol := make([]*float64, len(tracts))
	var cx, cy []float64
	for i, t := range tracts {
		xcol[i] = xs[t]
		ycol[i] = ys[t]
		if xs[t] != nil && ys[t] != nil {
			cx = append(cx, *xs[t])
			cy = append(cy, *ys[t])
		}
	}
	if len(cx) < 3 {
		return zero, nil, fmt.Errorf("only %d complete (x,y) pairs in scope — %s and %s may not share a geography level yet", len(cx), xVar, yVar)
	}

	rho := stats.SpearmanRho(xcol, ycol)
	ci := stats.BootstrapPairs(func(a, b []float64) float64 {
		pa := make([]*float64, len(a))
		pb := make([]*float64, len(b))
		for i := range a {
			pa[i] = &a[i]
			pb[i] = &b[i]
		}
		return stats.SpearmanRho(pa, pb)
	}, cx, cy, nBoot, alpha)

	result := store.AnalysisResult{
		Type:       run.RunType,
		ScopeGEOID: run.ScopeGEOID,
		ScopeLevel: run.ScopeLevel,
		Vintage:    run.Vintage,
		Parameters: run.Parameters,
		Results: map[string]interface{}{
			"rho":       rho,
			"ci_lower":  ci.Lower,
			"ci_upper":  ci.Upper,
			"n_pairs":   len(cx),
			"x_vintage": xVint,
			"y_vintage": yVint,
		},
	}
	return result, nil, nil
}

// executeIsolationIndex computes Lieberson's P* over the scope's tracts.
func executeIsolationIndex(ctx context.Context, s store.Store, run *store.AnalysisRun) (store.AnalysisResult, []store.AnalysisScore, error) {
	var zero store.AnalysisResult
	p := run.Parameters
	groupVar := stringParam(p, "group_variable", "")
	totalVar := stringParam(p, "total_variable", "")

	tracts, err := childTracts(ctx, s, run.ScopeLevel, run.ScopeGEOID)
	if err != nil {
		return zero, nil, err
	}
	groups, gVint, err := indicatorVector(ctx, s, tracts, groupVar, run.Vintage)
	if err != nil {
		return zero, nil, err
	}
	totals, tVint, err := indicatorVector(ctx, s, tracts, totalVar, "")
	if err != nil {
		return zero, nil, err
	}

	gcol := make([]*float64, len(tracts))
	tcol := make([]*float64, len(tracts))
	n := 0
	for i, t := range tracts {
		gcol[i] = groups[t]
		tcol[i] = totals[t]
		if groups[t] != nil && totals[t] != nil {
			n++
		}
	}
	if n == 0 {
		return zero, nil, fmt.Errorf("no tracts carry both %s and %s", groupVar, totalVar)
	}

	idx, err := stats.IsolationIndex(gcol, tcol)
	if err != nil {
		return zero, nil, err
	}
	if idx == nil {
		return zero, nil, fmt.Errorf("isolation index undefined: total %s across scope is zero", groupVar)
	}

	result := store.AnalysisResult{
		Type:       run.RunType,
		ScopeGEOID: run.ScopeGEOID,
		ScopeLevel: run.ScopeLevel,
		Vintage:    run.Vintage,
		Parameters: run.Parameters,
		Results: map[string]interface{}{
			"isolation_index": *idx,
			"n_tracts":        n,
			"group_vintage":   gVint,
			"total_vintage":   tVint,
		},
	}
	return result, nil, nil
}

// buildDesign assembles a complete-case design matrix (with intercept) and
// outcome vector for the given tracts.
func buildDesign(tracts []string, outcome map[string]*float64, predictors []map[string]*float64) (X [][]float64, y []float64) {
	for _, t := range tracts {
		ov := outcome[t]
		if ov == nil {
			continue
		}
		row := make([]float64, 1+len(predictors))
		row[0] = 1.0
		complete := true
		for j, pv := range predictors {
			v := pv[t]
			if v == nil {
				complete = false
				break
			}
			row[1+j] = *v
		}
		if !complete {
			continue
		}
		X = append(X, row)
		y = append(y, *ov)
	}
	return X, y
}

// fetchPredictors loads each predictor's vector.
func fetchPredictors(ctx context.Context, s store.Store, tracts, predIDs []string) ([]map[string]*float64, error) {
	out := make([]map[string]*float64, len(predIDs))
	for i, id := range predIDs {
		vec, _, err := indicatorVector(ctx, s, tracts, id, "")
		if err != nil {
			return nil, err
		}
		out[i] = vec
	}
	return out, nil
}

// executeBlinderOaxaca decomposes the outcome gap between two counties'
// tract populations into endowment/coefficient/interaction components —
// the equity finding D11 called out (BlinderOaxaca had zero callers).
func executeBlinderOaxaca(ctx context.Context, s store.Store, run *store.AnalysisRun) (store.AnalysisResult, []store.AnalysisScore, error) {
	var zero store.AnalysisResult
	p := run.Parameters
	outcomeVar := stringParam(p, "outcome_variable", "")
	predIDs := stringSliceParam(p, "predictor_variables")
	geoidA := stringParam(p, "group_a_geoid", "")
	geoidB := stringParam(p, "group_b_geoid", "")

	build := func(county string) ([][]float64, []float64, int, error) {
		tracts, err := childTracts(ctx, s, "county", county)
		if err != nil {
			return nil, nil, 0, err
		}
		ovec, _, err := indicatorVector(ctx, s, tracts, outcomeVar, run.Vintage)
		if err != nil {
			return nil, nil, 0, err
		}
		pvecs, err := fetchPredictors(ctx, s, tracts, predIDs)
		if err != nil {
			return nil, nil, 0, err
		}
		X, y := buildDesign(tracts, ovec, pvecs)
		return X, y, len(tracts), nil
	}

	xA, yA, nA, err := build(geoidA)
	if err != nil {
		return zero, nil, fmt.Errorf("group A (%s): %w", geoidA, err)
	}
	xB, yB, nB, err := build(geoidB)
	if err != nil {
		return zero, nil, fmt.Errorf("group B (%s): %w", geoidB, err)
	}
	minRows := len(predIDs) + 2 // intercept + predictors, plus one degree of freedom
	if len(yA) < minRows || len(yB) < minRows {
		return zero, nil, fmt.Errorf("insufficient complete cases: group A %d/%d tracts, group B %d/%d (need ≥%d each)",
			len(yA), nA, len(yB), nB, minRows)
	}

	dec, err := stats.BlinderOaxaca(xA, yA, xB, yB)
	if err != nil {
		return zero, nil, err
	}

	result := store.AnalysisResult{
		Type:       run.RunType,
		ScopeGEOID: run.ScopeGEOID,
		ScopeLevel: run.ScopeLevel,
		Vintage:    run.Vintage,
		Parameters: run.Parameters,
		Results: map[string]interface{}{
			"mean_a":           dec.MeanA,
			"mean_b":           dec.MeanB,
			"gap":              dec.Gap,
			"endowment":        dec.Endowment,
			"coefficients":     dec.Coefficients,
			"interaction":      dec.Interaction,
			"endowment_pct":    dec.EndowmentPct,
			"coefficients_pct": dec.CoefficientsPct,
			"n_complete_a":     len(yA),
			"n_complete_b":     len(yB),
		},
	}
	return result, nil, nil
}

// executeInteractionOLS runs OLS with interaction terms over the scope's
// tracts. Interaction indices refer to the predictor list; the intercept is
// added here and offset transparently.
func executeInteractionOLS(ctx context.Context, s store.Store, run *store.AnalysisRun) (store.AnalysisResult, []store.AnalysisScore, error) {
	var zero store.AnalysisResult
	p := run.Parameters
	outcomeVar := stringParam(p, "outcome_variable", "")
	predIDs := stringSliceParam(p, "predictor_variables")
	pairs, _ := pairParam(p, "interactions")

	tracts, err := childTracts(ctx, s, run.ScopeLevel, run.ScopeGEOID)
	if err != nil {
		return zero, nil, err
	}
	ovec, _, err := indicatorVector(ctx, s, tracts, outcomeVar, run.Vintage)
	if err != nil {
		return zero, nil, err
	}
	pvecs, err := fetchPredictors(ctx, s, tracts, predIDs)
	if err != nil {
		return zero, nil, err
	}
	X, y := buildDesign(tracts, ovec, pvecs)
	if len(y) < len(predIDs)+len(pairs)+2 {
		return zero, nil, fmt.Errorf("insufficient complete cases: %d rows for %d coefficients",
			len(y), len(predIDs)+len(pairs)+1)
	}

	// Offset predictor indices past the intercept column.
	shifted := make([][2]int, len(pairs))
	for i, pr := range pairs {
		shifted[i] = [2]int{pr[0] + 1, pr[1] + 1}
	}

	res, err := stats.InteractionOLS(X, y, shifted)
	if err != nil {
		return zero, nil, err
	}

	// Coefficient naming: intercept, predictors, then interaction products.
	names := append([]string{"intercept"}, predIDs...)
	for _, pr := range pairs {
		names = append(names, predIDs[pr[0]]+"*"+predIDs[pr[1]])
	}

	result := store.AnalysisResult{
		Type:       run.RunType,
		ScopeGEOID: run.ScopeGEOID,
		ScopeLevel: run.ScopeLevel,
		Vintage:    run.Vintage,
		Parameters: run.Parameters,
		Results: map[string]interface{}{
			"coefficient_names": names,
			"betas":             res.Betas,
			"std_errors":        res.StdErrors,
			"t_stats":           res.TStats,
			"p_values":          res.PValues,
			"r_squared":         res.RSquared,
			"n":                 len(y),
			// Predictions/residuals are deliberately not persisted: they are
			// n-length vectors that bloat the cache row without being part
			// of the finding.
		},
	}
	return result, nil, nil
}

// buildScopeMatrix fetches a variables × tracts value matrix for the scope.
// The primary (first) variable is pinned to the run's vintage — it keys the
// cache — and the rest resolve to their own latest, because cross-domain
// analyses span sources whose vintages legitimately differ. Every variable
// must have at least one row in scope: an analysis quietly computed over an
// all-nil column looks like success while measuring nothing.
func buildScopeMatrix(ctx context.Context, s store.Store, tracts, varIDs []string, primaryVintage string) ([][]*float64, map[string]string, error) {
	matrix := make([][]*float64, len(varIDs))
	vintages := map[string]string{}
	for k, varID := range varIDs {
		vintage := ""
		if k == 0 {
			vintage = primaryVintage
		}
		vec, observed, err := indicatorVector(ctx, s, tracts, varID, vintage)
		if err != nil {
			return nil, nil, err
		}
		col := make([]*float64, len(tracts))
		nonNil := 0
		for i, t := range tracts {
			col[i] = vec[t]
			if col[i] != nil {
				nonNil++
			}
		}
		if nonNil == 0 {
			return nil, nil, fmt.Errorf("variable %q has no values in scope", varID)
		}
		matrix[k] = col
		vintages[varID] = observed
	}
	return matrix, vintages, nil
}

// compositeTiers is the CLI's standard 5-tier assignment, shared verbatim.
var compositeTiers = []stats.TierDef{
	{Name: "very_high", MinPercentile: 0.80, MaxPercentile: 1.01},
	{Name: "high", MinPercentile: 0.60, MaxPercentile: 0.80},
	{Name: "moderate", MinPercentile: 0.40, MaxPercentile: 0.60},
	{Name: "low", MinPercentile: 0.20, MaxPercentile: 0.40},
	{Name: "minimal", MinPercentile: 0.00, MaxPercentile: 0.20},
}

// executeCompositeIndex routes the CLI's composite through the run queue —
// the ADR-014 gap in its own words: "analyses cannot be created through the
// API... every visitor recomputes from scratch and no one inherits anyone
// else's work." Same statistics (stats.CompositeIndex), same tiers, same
// score shape as `pdi analyze --type composite`; the D9 upsert makes the
// result shared instead of per-visitor.
func executeCompositeIndex(ctx context.Context, s store.Store, run *store.AnalysisRun) (store.AnalysisResult, []store.AnalysisScore, error) {
	var zero store.AnalysisResult
	p := run.Parameters
	varIDs := stringSliceParam(p, "variables")
	method := stringParam(p, "method", "equal_percentile")

	wslice := make([]float64, len(varIDs))
	if method == "weighted_zscore" {
		wm, _ := p["weights"].(map[string]interface{})
		for k, v := range varIDs {
			w, _ := wm[v].(float64)
			wslice[k] = w
		}
	}

	tracts, err := childTracts(ctx, s, run.ScopeLevel, run.ScopeGEOID)
	if err != nil {
		return zero, nil, err
	}
	matrix, vintages, err := buildScopeMatrix(ctx, s, tracts, varIDs, run.Vintage)
	if err != nil {
		return zero, nil, err
	}

	scores, err := stats.CompositeIndex(matrix, wslice, method)
	if err != nil {
		return zero, nil, err
	}
	tiers := stats.AssignTiers(scores, compositeTiers)

	type ranked struct {
		geoid string
		score float64
		tier  string
	}
	var rows []ranked
	missing := 0
	for i, g := range tracts {
		if scores[i] == nil {
			missing++
			continue
		}
		rows = append(rows, ranked{g, *scores[i], tiers[i]})
	}
	if len(rows) == 0 {
		return zero, nil, fmt.Errorf("composite produced no scores in scope")
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].score > rows[j].score })

	out := make([]store.AnalysisScore, 0, len(rows))
	for i, r := range rows {
		rank := i + 1
		out = append(out, store.AnalysisScore{
			GEOID:      r.geoid,
			Score:      r.score,
			Rank:       &rank,
			Percentile: r.score,
			Tier:       r.tier,
		})
	}

	result := store.AnalysisResult{
		Type:       run.RunType,
		ScopeGEOID: run.ScopeGEOID,
		ScopeLevel: run.ScopeLevel,
		Vintage:    run.Vintage,
		Parameters: run.Parameters,
		Results: map[string]interface{}{
			"method":            method,
			"tract_count":       len(rows),
			"missing_tracts":    missing,
			"variable_vintages": vintages,
		},
	}
	return result, out, nil
}

// executeCorrelationMatrix routes the CLI's pairwise Pearson matrix through
// the run queue: upper-triangle correlations with complete-case counts per
// pair (the CLI omitted n — a correlation without its n hides how little
// data produced it).
func executeCorrelationMatrix(ctx context.Context, s store.Store, run *store.AnalysisRun) (store.AnalysisResult, []store.AnalysisScore, error) {
	var zero store.AnalysisResult
	varIDs := stringSliceParam(run.Parameters, "variables")

	tracts, err := childTracts(ctx, s, run.ScopeLevel, run.ScopeGEOID)
	if err != nil {
		return zero, nil, err
	}
	matrix, vintages, err := buildScopeMatrix(ctx, s, tracts, varIDs, run.Vintage)
	if err != nil {
		return zero, nil, err
	}

	corrMap := map[string]interface{}{}
	pairN := map[string]interface{}{}
	pairs := 0
	for i, a := range varIDs {
		for j, b := range varIDs {
			if j <= i {
				continue
			}
			r := stats.PearsonR(matrix[i], matrix[j])
			row, ok := corrMap[a].(map[string]interface{})
			if !ok {
				row = map[string]interface{}{}
				corrMap[a] = row
			}
			row[b] = r
			n := 0
			for k := range tracts {
				if matrix[i][k] != nil && matrix[j][k] != nil {
					n++
				}
			}
			pairN[a+"|"+b] = n
			pairs++
		}
	}

	result := store.AnalysisResult{
		Type:       run.RunType,
		ScopeGEOID: run.ScopeGEOID,
		ScopeLevel: run.ScopeLevel,
		Vintage:    run.Vintage,
		Parameters: run.Parameters,
		Results: map[string]interface{}{
			"correlations":      corrMap,
			"pair_count":        pairs,
			"pair_n":            pairN,
			"variable_vintages": vintages,
		},
	}
	return result, nil, nil
}

// executeFactorRollup implements ADR-015: county factor scores as
// population-weighted rollups of tract factor scores, published only with a
// bootstrap interval and coverage above the recorded threshold. Published
// rows are written to factor_scores under '<model_vintage>-popw' — the
// suffix marks a rollup as a rollup forever — with CI/coverage/method in
// loadings_json. The analyses row is the D9 cache identity; the launch
// warm re-establishes the rollup at boot.
func executeFactorRollup(ctx context.Context, s store.Store, run *store.AnalysisRun) (store.AnalysisResult, []store.AnalysisScore, error) {
	var zero store.AnalysisResult
	p := run.Parameters
	modelVintage := stringParam(p, "model_vintage", "2023-efa-v2")
	denomID := stringParam(p, "denominator_variable_id", defaultRollupDenominator)
	threshold := floatParam(p, "coverage_threshold", defaultCoverageThreshold)
	nBoot := intParam(p, "n_boot", defaultNBoot)
	alpha := floatParam(p, "alpha", defaultAlpha)
	outVintage := modelVintage + "-popw"

	tractScores, err := s.QueryFactorScoresAtLevel(ctx, 11)
	if err != nil {
		return zero, nil, err
	}
	inScope := map[string]bool{}
	tracts, err := childTracts(ctx, s, run.ScopeLevel, run.ScopeGEOID)
	if err != nil {
		return zero, nil, err
	}
	for _, t := range tracts {
		inScope[t] = true
	}
	weights, denomVintage, err := indicatorVector(ctx, s, tracts, denomID, "")
	if err != nil {
		return zero, nil, err
	}

	// factor -> county -> inputs; track each county's total tract count for
	// coverage against the county's FULL tract universe, not just scored ones.
	tractsPerCounty := map[string]int{}
	for _, t := range tracts {
		tractsPerCounty[t[:5]]++
	}
	type fc struct{ factor, county string }
	inputs := map[fc][]stats.RollupInput{}
	factorSet := map[string]bool{}
	for _, ts := range tractScores {
		if ts.AnalysisVintage != modelVintage || !inScope[ts.GEOID] {
			continue
		}
		factorSet[ts.FactorName] = true
		key := fc{ts.FactorName, ts.GEOID[:5]}
		inputs[key] = append(inputs[key], stats.RollupInput{
			GEOID:  ts.GEOID,
			Value:  ts.FactorScore,
			Weight: weights[ts.GEOID],
		})
	}
	if len(factorSet) == 0 {
		return zero, nil, fmt.Errorf("no tract factor scores at model vintage %q in scope", modelVintage)
	}

	published := 0
	withheld := map[string]interface{}{}
	type outRow struct {
		county, factor string
		res            *stats.RollupResult
	}
	var outs []outRow
	factors := make([]string, 0, len(factorSet))
	for f := range factorSet {
		factors = append(factors, f)
	}
	sort.Strings(factors)
	counties := make([]string, 0, len(tractsPerCounty))
	for c := range tractsPerCounty {
		counties = append(counties, c)
	}
	sort.Strings(counties)

	for _, f := range factors {
		for _, county := range counties {
			ins := inputs[fc{f, county}]
			// Pad coverage accounting with the county's unscored tracts.
			total := tractsPerCounty[county]
			for len(ins) < total {
				ins = append(ins, stats.RollupInput{})
			}
			res, reason := stats.RollupChildren(stats.AggWeightedMean, ins, threshold, nBoot, alpha)
			if res == nil {
				withheld[county+"/"+f] = reason
				continue
			}
			published++
			outs = append(outs, outRow{county, f, res})
		}
	}

	// State percentile per factor across published counties.
	byFactorVals := map[string][]float64{}
	for _, o := range outs {
		byFactorVals[o.factor] = append(byFactorVals[o.factor], o.res.Value)
	}
	pctOf := func(f string, v float64) float64 {
		vals := byFactorVals[f]
		if len(vals) <= 1 {
			return 50
		}
		below := 0
		for _, x := range vals {
			if x < v {
				below++
			}
		}
		return float64(below) / float64(len(vals)-1) * 100
	}

	fsRows := make([]store.FactorScore, 0, len(outs))
	for _, o := range outs {
		score := o.res.Value
		pct := pctOf(o.factor, score)
		prov, _ := json.Marshal(map[string]interface{}{
			"method":              "population_weighted_rollup_of_tract_factor_scores",
			"adr":                 "015",
			"ci_lower":            o.res.CI.Lower,
			"ci_upper":            o.res.CI.Upper,
			"n_tracts":            o.res.N,
			"of_tracts":           o.res.Total,
			"coverage":            o.res.Coverage,
			"coverage_threshold":  threshold,
			"denominator":         denomID,
			"denominator_vintage": denomVintage,
			"model_vintage":       modelVintage,
		})
		sc, pf := score, pct
		fsRows = append(fsRows, store.FactorScore{
			GEOID:            o.county,
			FactorName:       o.factor,
			FactorScore:      &sc,
			FactorPercentile: &pf,
			LoadingsJSON:     string(prov),
			AnalysisVintage:  outVintage,
		})
	}
	// Generation hygiene: exactly one live rollup generation. Factor names
	// and coverage change between model revisions; without this, v1-popw
	// rows would sit beside v2-popw and the endpoint would serve two models.
	if _, err := s.DeleteFactorScoresAtLevel(ctx, 5, "-popw"); err != nil {
		return zero, nil, fmt.Errorf("clear prior rollup generation: %w", err)
	}
	if err := s.PutFactorScores(ctx, fsRows); err != nil {
		return zero, nil, fmt.Errorf("persist county factor rollups: %w", err)
	}

	result := store.AnalysisResult{
		Type:       run.RunType,
		ScopeGEOID: run.ScopeGEOID,
		ScopeLevel: run.ScopeLevel,
		Vintage:    run.Vintage,
		Parameters: run.Parameters,
		Results: map[string]interface{}{
			"model_vintage":       modelVintage,
			"output_vintage":      outVintage,
			"factors":             factors,
			"published":           published,
			"withheld":            withheld,
			"denominator":         denomID,
			"denominator_vintage": denomVintage,
		},
	}
	return result, nil, nil
}

// executeDissimilarity computes Massey & Denton's D per county between two
// group-count variables over the county's tracts, with a paired-bootstrap
// interval. The county page's "Segregation index" panel reads exactly this
// shape: a statewide 'dissimilarity' analysis whose per-county scores carry
// details.group_pair. Counties with fewer than 3 tracts holding both counts
// are withheld — D over one or two tracts is an artifact, not a measure.
func executeDissimilarity(ctx context.Context, s store.Store, run *store.AnalysisRun) (store.AnalysisResult, []store.AnalysisScore, error) {
	var zero store.AnalysisResult
	p := run.Parameters
	groupVar := stringParam(p, "group_variable", "")
	refVar := stringParam(p, "reference_variable", "")
	pair := stringParam(p, "group_pair", "")
	nBoot := intParam(p, "n_boot", defaultNBoot)
	alpha := floatParam(p, "alpha", defaultAlpha)

	tracts, err := childTracts(ctx, s, run.ScopeLevel, run.ScopeGEOID)
	if err != nil {
		return zero, nil, err
	}
	groups, gVint, err := indicatorVector(ctx, s, tracts, groupVar, run.Vintage)
	if err != nil {
		return zero, nil, err
	}
	refs, rVint, err := indicatorVector(ctx, s, tracts, refVar, "")
	if err != nil {
		return zero, nil, err
	}

	byCounty := make(map[string][]string)
	for _, t := range tracts {
		byCounty[t[:5]] = append(byCounty[t[:5]], t)
	}
	counties := make([]string, 0, len(byCounty))
	for c := range byCounty {
		counties = append(counties, c)
	}
	sort.Strings(counties)

	const minTracts = 3
	scores := make([]store.AnalysisScore, 0, len(counties))
	withheld := map[string]interface{}{}
	dOf := func(gs, rs []float64) float64 {
		pg := make([]*float64, len(gs))
		pr := make([]*float64, len(rs))
		for i := range gs {
			pg[i] = &gs[i]
			pr[i] = &rs[i]
		}
		d, err := stats.DissimilarityIndex(pg, pr)
		if err != nil || d == nil {
			return 0
		}
		return *d
	}

	for _, county := range counties {
		var gs, rs []float64
		for _, t := range byCounty[county] {
			gv, rv := groups[t], refs[t]
			if gv != nil && rv != nil {
				gs = append(gs, *gv)
				rs = append(rs, *rv)
			}
		}
		if len(gs) < minTracts {
			withheld[county] = fmt.Sprintf("only %d tracts carry both counts (need %d)", len(gs), minTracts)
			continue
		}
		d := dOf(gs, rs)
		ci := stats.BootstrapPairs(dOf, gs, rs, nBoot, alpha)
		scores = append(scores, store.AnalysisScore{
			GEOID: county,
			Score: d,
			Details: map[string]interface{}{
				"group_pair": pair,
				"n_tracts":   len(gs),
				"ci_lower":   ci.Lower,
				"ci_upper":   ci.Upper,
			},
		})
	}

	result := store.AnalysisResult{
		Type:       run.RunType,
		ScopeGEOID: run.ScopeGEOID,
		ScopeLevel: run.ScopeLevel,
		Vintage:    run.Vintage,
		Parameters: run.Parameters,
		Results: map[string]interface{}{
			"group_pair":        pair,
			"published":         len(scores),
			"withheld_counties": withheld,
			"group_vintage":     gVint,
			"reference_vintage": rVint,
		},
	}
	return result, scores, nil
}

// executeBootstrapMean routes stats.Bootstrap directly: a CI on the mean of
// one variable over the scope's tracts.
func executeBootstrapMean(ctx context.Context, s store.Store, run *store.AnalysisRun) (store.AnalysisResult, []store.AnalysisScore, error) {
	var zero store.AnalysisResult
	p := run.Parameters
	varID := stringParam(p, "variable_id", "")
	nBoot := intParam(p, "n_boot", defaultNBoot)
	alpha := floatParam(p, "alpha", defaultAlpha)

	tracts, err := childTracts(ctx, s, run.ScopeLevel, run.ScopeGEOID)
	if err != nil {
		return zero, nil, err
	}
	vec, vint, err := indicatorVector(ctx, s, tracts, varID, run.Vintage)
	if err != nil {
		return zero, nil, err
	}
	var vals []float64
	for _, t := range tracts {
		if vec[t] != nil {
			vals = append(vals, *vec[t])
		}
	}
	if len(vals) == 0 {
		return zero, nil, fmt.Errorf("no %s values in scope", varID)
	}

	ci := stats.Bootstrap(func(xs []float64) float64 {
		var sum float64
		for _, v := range xs {
			sum += v
		}
		return sum / float64(len(xs))
	}, vals, nBoot, alpha)

	result := store.AnalysisResult{
		Type:       run.RunType,
		ScopeGEOID: run.ScopeGEOID,
		ScopeLevel: run.ScopeLevel,
		Vintage:    run.Vintage,
		Parameters: run.Parameters,
		Results: map[string]interface{}{
			"mean":     ci.PointEstimate,
			"ci_lower": ci.Lower,
			"ci_upper": ci.Upper,
			"n":        len(vals),
			"vintage":  vint,
		},
	}
	return result, nil, nil
}

// ── parameter helpers ───────────────────────────────────────────────────────

func stringParam(p map[string]interface{}, key, def string) string {
	if v, ok := p[key].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

func requireStringParam(p map[string]interface{}, key string) (string, error) {
	v := stringParam(p, key, "")
	if v == "" {
		return "", fmt.Errorf("parameter %q is required", key)
	}
	return v, nil
}

func floatParam(p map[string]interface{}, key string, def float64) float64 {
	switch v := p[key].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	}
	return def
}

func intParam(p map[string]interface{}, key string, def int) int {
	switch v := p[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}
	return def
}

func stringSliceParam(p map[string]interface{}, key string) []string {
	var out []string
	switch v := p[key].(type) {
	case []string:
		return v
	case []interface{}:
		for _, e := range v {
			if s, ok := e.(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, strings.TrimSpace(s))
			}
		}
	}
	return out
}

func requireStringSliceParam(p map[string]interface{}, key string) ([]string, error) {
	v := stringSliceParam(p, key)
	if len(v) == 0 {
		return nil, fmt.Errorf("parameter %q must be a non-empty string array", key)
	}
	return v, nil
}

// pairParam parses [[0,1],[1,2]]-style index pairs.
func pairParam(p map[string]interface{}, key string) ([][2]int, error) {
	raw, ok := p[key]
	if !ok || raw == nil {
		return nil, nil
	}
	list, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("parameter %q must be an array of [i,j] pairs", key)
	}
	out := make([][2]int, 0, len(list))
	for _, e := range list {
		pair, ok := e.([]interface{})
		if !ok || len(pair) != 2 {
			return nil, fmt.Errorf("parameter %q entries must be [i,j] pairs", key)
		}
		a, aok := pair[0].(float64)
		b, bok := pair[1].(float64)
		if !aok || !bok {
			return nil, fmt.Errorf("parameter %q entries must be numeric pairs", key)
		}
		out = append(out, [2]int{int(a), int(b)})
	}
	return out, nil
}

func toIfaceSlice(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

func pairsToIface(pairs [][2]int) []interface{} {
	out := make([]interface{}, len(pairs))
	for i, p := range pairs {
		out[i] = []interface{}{float64(p[0]), float64(p[1])}
	}
	return out
}
