package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/stats"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// newAnalyzeCmd returns the "pdi analyze" command.
//
// Usage:
//
//	pdi analyze --scope county:55025 --type composite \
//	    --weights "poverty_rate:0.2,median_household_income:0.2,transit_score:0.15,uninsured_rate:0.15,cost_burden_rate:0.15,eviction_rate:0.15"
func newAnalyzeCmd() *cobra.Command {
	var (
		scope      string
		analysisType string
		weights    string
		vintage    string
	)

	cmd := &cobra.Command{
		Use:   "analyze",
		Short: "Run statistical analyses on stored indicators",
		Long: `analyze queries stored indicators for the given geographic scope and runs
one of three analysis types:

  composite   — weighted composite deprivation index (percentile-rank or z-score)
  ols         — ordinary least squares regression
  correlation — pairwise Pearson correlations across indicator columns

Results are written to the analyses and analysis_scores tables.

Scope format: "level:geoid"  (e.g. county:55025, state:55)
Weights format: "variable_id:weight,..."  (e.g. "poverty_rate:0.3,median_hh_income:0.2")`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAnalyze(scope, analysisType, weights, vintage)
		},
	}

	cmd.Flags().StringVar(&scope, "scope", "", "Geographic scope — format: level:geoid (e.g. county:55025) [required]")
	cmd.Flags().StringVar(&analysisType, "type", "composite", "Analysis type: composite, ols, or correlation")
	cmd.Flags().StringVar(&weights, "weights", "", "Variable weights for composite analysis (e.g. poverty_rate:0.3,median_hh_income:0.2)")
	cmd.Flags().StringVar(&vintage, "vintage", "", "Vintage string to filter indicators (e.g. ACS-2023-5yr; empty = latest)")

	_ = cmd.MarkFlagRequired("scope")

	return cmd
}

func runAnalyze(scope, analysisType, weightsFlag, vintage string) error {
	// Parse --scope  (format: "level:geoid")
	level, scopeGEOID, err := parseScope(scope)
	if err != nil {
		return fmt.Errorf("analyze: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	s, err := store.NewPostgresStore(ctx, resolveConnString())
	if err != nil {
		return fmt.Errorf("analyze: connect to store: %w", err)
	}
	defer s.Close()

	// Query indicators for this scope.
	geoQ := store.GeoQuery{Level: geo.Level(level), Limit: 5000}
	geos, err := s.QueryGeographies(ctx, geoQ)
	if err != nil {
		return fmt.Errorf("analyze: query geographies: %w", err)
	}
	if len(geos) == 0 {
		return fmt.Errorf("analyze: no geographies found for scope %q", scope)
	}

	geoids := make([]string, len(geos))
	for i, g := range geos {
		geoids[i] = g.GEOID
	}

	indQ := store.IndicatorQuery{GEOIDs: geoids, LatestOnly: true}
	if vintage != "" {
		indQ.Vintage = vintage
		indQ.LatestOnly = false
	}
	indicators, err := s.QueryIndicators(ctx, indQ)
	if err != nil {
		return fmt.Errorf("analyze: query indicators: %w", err)
	}
	if len(indicators) == 0 {
		return fmt.Errorf("analyze: no indicators found for scope %q", scope)
	}

	// Collect unique variable IDs present in the result set.
	varSet := make(map[string]bool)
	for _, ind := range indicators {
		varSet[ind.VariableID] = true
	}
	varIDs := sortedKeys(varSet)

	// Build the indicator matrix: varIDs × geoids.
	type ikey struct{ geoid, variableID string }
	idx := make(map[ikey]*float64, len(indicators))
	for _, ind := range indicators {
		idx[ikey{ind.GEOID, ind.VariableID}] = ind.Value
	}

	matrix := make([][]*float64, len(varIDs))
	for k, varID := range varIDs {
		col := make([]*float64, len(geoids))
		for i, geoid := range geoids {
			col[i] = idx[ikey{geoid, varID}]
		}
		matrix[k] = col
	}

	var resultID string
	start := time.Now()

	switch analysisType {
	case "composite":
		resultID, err = runCompositeAnalysis(ctx, s, scopeGEOID, string(level), varIDs, geoids, matrix, weightsFlag, vintage)

	case "ols":
		resultID, err = runOLSAnalysis(ctx, s, scopeGEOID, string(level), varIDs, geoids, matrix, vintage)

	case "correlation":
		resultID, err = runCorrelationAnalysis(ctx, s, scopeGEOID, string(level), varIDs, geoids, matrix, vintage)

	default:
		return fmt.Errorf("analyze: unknown type %q (want composite, ols, or correlation)", analysisType)
	}

	if err != nil {
		return fmt.Errorf("analyze: %w", err)
	}

	elapsed := time.Since(start).Round(time.Millisecond)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ANALYSIS ID\tTYPE\tGEOGRAPHIES\tINDICATORS\tDURATION")
	fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\n", resultID, analysisType, len(geoids), len(varIDs), elapsed)
	return w.Flush()
}

// runCompositeAnalysis builds a weighted composite index.
func runCompositeAnalysis(
	ctx context.Context,
	s store.Store,
	scopeGEOID, scopeLevel string,
	varIDs, geoids []string,
	matrix [][]*float64,
	weightsFlag, vintage string,
) (string, error) {
	// Parse optional weights.
	wmap, err := parseWeights(weightsFlag)
	if err != nil {
		return "", fmt.Errorf("composite: parse weights: %w", err)
	}

	wslice := make([]float64, len(varIDs))
	hasWeights := len(wmap) > 0
	if hasWeights {
		for k, varID := range varIDs {
			if w, ok := wmap[varID]; ok {
				wslice[k] = w
			} else {
				wslice[k] = 0
			}
		}
	}

	method := "equal_percentile"
	if hasWeights {
		method = "weighted_zscore"
	}

	scores, err := stats.CompositeIndex(matrix, wslice, method)
	if err != nil {
		return "", fmt.Errorf("composite: %w", err)
	}

	// Standard tiers.
	tiers := []stats.TierDef{
		{Name: "very_high", MinPercentile: 0.80, MaxPercentile: 1.01},
		{Name: "high", MinPercentile: 0.60, MaxPercentile: 0.80},
		{Name: "moderate", MinPercentile: 0.40, MaxPercentile: 0.60},
		{Name: "low", MinPercentile: 0.20, MaxPercentile: 0.40},
		{Name: "minimal", MinPercentile: 0.00, MaxPercentile: 0.20},
	}
	tierAssign := stats.AssignTiers(scores, tiers)

	analysisID := fmt.Sprintf("composite-%s-%s", scopeGEOID, vintage)
	result := store.AnalysisResult{
		ID:         analysisID,
		Type:       "composite_index",
		ScopeGEOID: scopeGEOID,
		ScopeLevel: scopeLevel,
		Parameters: map[string]interface{}{
			"method":    method,
			"variables": varIDs,
			"vintage":   vintage,
		},
		Results: map[string]interface{}{
			"tract_count": len(geoids),
		},
		Vintage: vintage,
	}
	if err := s.PutAnalysis(ctx, result); err != nil {
		return "", fmt.Errorf("composite: PutAnalysis: %w", err)
	}

	analysisScores := make([]store.AnalysisScore, 0, len(geoids))
	for i, geoid := range geoids {
		sc := scores[i]
		scoreVal := 0.0
		if sc != nil {
			scoreVal = *sc
		}
		analysisScores = append(analysisScores, store.AnalysisScore{
			AnalysisID: analysisID,
			GEOID:      geoid,
			Score:      scoreVal,
			Rank:       i + 1,
			Percentile: scoreVal,
			Tier:       tierAssign[i],
		})
	}
	if err := s.PutAnalysisScores(ctx, analysisScores); err != nil {
		return "", fmt.Errorf("composite: PutAnalysisScores: %w", err)
	}

	return analysisID, nil
}

// runOLSAnalysis runs OLS regression using the first variable as the outcome
// and the remaining variables as predictors.
func runOLSAnalysis(
	ctx context.Context,
	s store.Store,
	scopeGEOID, scopeLevel string,
	varIDs, geoids []string,
	matrix [][]*float64,
	vintage string,
) (string, error) {
	if len(varIDs) < 2 {
		return "", fmt.Errorf("OLS requires at least 2 variables (outcome + ≥1 predictor)")
	}

	outcome := varIDs[0]
	predictors := varIDs[1:]

	// Build complete-case vectors.
	type ikey struct{ row, col int }
	var yVals []float64
	var XRows [][]float64

	for i := range geoids {
		y := matrix[0][i]
		if y == nil {
			continue
		}
		row := make([]float64, 1+len(predictors))
		row[0] = 1.0
		complete := true
		for j := range predictors {
			v := matrix[j+1][i]
			if v == nil {
				complete = false
				break
			}
			row[j+1] = *v
		}
		if !complete {
			continue
		}
		yVals = append(yVals, *y)
		XRows = append(XRows, row)
	}

	if len(yVals) < 2 {
		return "", fmt.Errorf("OLS: insufficient complete cases (%d)", len(yVals))
	}

	res, err := stats.OLS(XRows, yVals)
	if err != nil {
		return "", fmt.Errorf("OLS: %w", err)
	}

	betas := map[string]interface{}{"intercept": res.Betas[0]}
	pvalues := map[string]interface{}{"intercept": res.PValues[0]}
	for j, varID := range predictors {
		betas[varID] = res.Betas[j+1]
		pvalues[varID] = res.PValues[j+1]
	}

	analysisID := fmt.Sprintf("ols-%s-%s-%s", outcome, scopeGEOID, vintage)
	result := store.AnalysisResult{
		ID:         analysisID,
		Type:       "ols_regression",
		ScopeGEOID: scopeGEOID,
		ScopeLevel: scopeLevel,
		Parameters: map[string]interface{}{
			"outcome":    outcome,
			"predictors": predictors,
			"vintage":    vintage,
			"n":          len(yVals),
		},
		Results: map[string]interface{}{
			"betas":     betas,
			"pvalues":   pvalues,
			"r_squared": res.RSquared,
			"n":         len(yVals),
		},
		Vintage: vintage,
	}
	if err := s.PutAnalysis(ctx, result); err != nil {
		return "", fmt.Errorf("OLS: PutAnalysis: %w", err)
	}
	return analysisID, nil
}

// runCorrelationAnalysis computes pairwise Pearson correlations.
func runCorrelationAnalysis(
	ctx context.Context,
	s store.Store,
	scopeGEOID, scopeLevel string,
	varIDs, _ []string,
	matrix [][]*float64,
	vintage string,
) (string, error) {
	type corrEntry struct{ a, b string; r float64 }
	var corrs []corrEntry
	for i, a := range varIDs {
		for j, b := range varIDs {
			if j <= i {
				continue
			}
			r := stats.PearsonR(matrix[i], matrix[j])
			corrs = append(corrs, corrEntry{a, b, r})
		}
	}

	corrMap := make(map[string]interface{}, len(varIDs))
	for _, c := range corrs {
		row, ok := corrMap[c.a].(map[string]interface{})
		if !ok {
			row = make(map[string]interface{})
			corrMap[c.a] = row
		}
		row[c.b] = c.r
	}

	analysisID := fmt.Sprintf("corr-%s-%s", scopeGEOID, vintage)
	result := store.AnalysisResult{
		ID:         analysisID,
		Type:       "correlation_matrix",
		ScopeGEOID: scopeGEOID,
		ScopeLevel: scopeLevel,
		Parameters: map[string]interface{}{
			"variables": varIDs,
			"vintage":   vintage,
		},
		Results: map[string]interface{}{
			"correlations": corrMap,
			"pair_count":   len(corrs),
		},
		Vintage: vintage,
	}
	if err := s.PutAnalysis(ctx, result); err != nil {
		return "", fmt.Errorf("correlation: PutAnalysis: %w", err)
	}
	return analysisID, nil
}

// ── helpers ────────────────────────────────────────────────────────────────────

// parseScope splits "level:geoid" into its components.
func parseScope(scope string) (geoLevel, geoid string, err error) {
	parts := strings.SplitN(scope, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid scope %q — expected format level:geoid (e.g. county:55025)", scope)
	}
	return parts[0], parts[1], nil
}

// parseWeights parses "varID:weight,varID:weight" into a map.
func parseWeights(s string) (map[string]float64, error) {
	if s == "" {
		return nil, nil
	}
	m := make(map[string]float64)
	for _, part := range parseCSV(s) {
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid weight entry %q (expected varID:weight)", part)
		}
		w, err := strconv.ParseFloat(strings.TrimSpace(kv[1]), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid weight value for %q: %w", kv[0], err)
		}
		m[strings.TrimSpace(kv[0])] = w
	}
	return m, nil
}

// sortedKeys returns map keys in sorted order.
func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Simple insertion sort — these are short slices.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

// geoLevel wraps a string to satisfy store.GeoQuery.Level (which is geo.Level / string).
type geoLevel = string
