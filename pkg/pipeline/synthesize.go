package pipeline

import (
	"context"
	"fmt"
	"log"
	"sort"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/stats"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// olsPredictors are the independent variables used to predict chronic_absence.
var olsPredictors = []string{
	"pct_poverty",
	"median_household_income",
	"pct_transit_access",
	"pct_unemployment",
	"pct_uninsured",
}

// tippingVariables are the variables on which tipping-point detection is run,
// using chronic_absence as the outcome.
var tippingVariables = []string{
	"pct_poverty",
	"median_household_income",
}

// synthKey is the composite key used to look up indicator values by tract + variable.
type synthKey = struct{ geoid, variableID string }

// SynthesizeStage is Stage 05: it runs multi-variate OLS regression to model
// chronic_absence, performs tipping-point detection on key predictors, and
// computes pairwise Pearson correlations across all composite variables.
// All results are written to the analyses table via store.PutAnalysis.
type SynthesizeStage struct{}

func (sy *SynthesizeStage) Name() string          { return "synthesize" }
func (sy *SynthesizeStage) Dependencies() []string { return []string{"analyze"} }

func (sy *SynthesizeStage) Run(ctx context.Context, s store.Store, cfg *Config) error {
	if cfg.DryRun {
		log.Printf("synthesize: dry-run mode — skipping")
		return nil
	}

	// 1. Load tracts in scope.
	geoQuery := store.GeoQuery{Level: geo.Tract}
	if cfg.CountyFIPS != "" {
		geoQuery.CountyFIPS = cfg.CountyFIPS
		geoQuery.StateFIPS = cfg.StateFIPS
	} else {
		geoQuery.StateFIPS = cfg.StateFIPS
	}

	geographies, err := s.QueryGeographies(ctx, geoQuery)
	if err != nil {
		return fmt.Errorf("synthesize: query geographies: %w", err)
	}
	if len(geographies) == 0 {
		log.Printf("synthesize: no tracts found for scope, skipping")
		return nil
	}

	sort.Slice(geographies, func(i, j int) bool {
		return geographies[i].GEOID < geographies[j].GEOID
	})
	tractGEOIDs := make([]string, len(geographies))
	for i, g := range geographies {
		tractGEOIDs[i] = g.GEOID
	}

	// 2. Query indicators.
	indicators, err := s.QueryIndicators(ctx, store.IndicatorQuery{
		GEOIDs:     tractGEOIDs,
		Vintage:    cfg.Vintage,
		LatestOnly: true,
	})
	if err != nil {
		return fmt.Errorf("synthesize: query indicators: %w", err)
	}

	indicatorIdx := make(map[synthKey]*float64, len(indicators))
	for _, ind := range indicators {
		indicatorIdx[synthKey{ind.GEOID, ind.VariableID}] = ind.Value
	}

	scopeGEOID := cfg.StateFIPS
	if cfg.CountyFIPS != "" {
		scopeGEOID = cfg.StateFIPS + cfg.CountyFIPS
	}

	// 3. OLS: predict chronic_absence from predictor variables.
	if err := sy.runOLS(ctx, s, cfg, tractGEOIDs, indicatorIdx, scopeGEOID); err != nil {
		log.Printf("synthesize: OLS skipped: %v", err)
	}

	// 4. TippingPoint on key predictors vs. chronic_absence.
	if err := sy.runTippingPoints(ctx, s, cfg, tractGEOIDs, indicatorIdx, scopeGEOID); err != nil {
		log.Printf("synthesize: tipping point detection skipped: %v", err)
	}

	// 5. Correlation matrix across composite variables.
	if err := sy.runCorrelations(ctx, s, cfg, tractGEOIDs, indicatorIdx, scopeGEOID); err != nil {
		log.Printf("synthesize: correlation matrix skipped: %v", err)
	}

	return nil
}

func (sy *SynthesizeStage) runOLS(
	ctx context.Context,
	s store.Store,
	cfg *Config,
	tractGEOIDs []string,
	idx map[synthKey]*float64,
	scopeGEOID string,
) error {
	// Build complete-case matrix: only tracts with all values present.
	var yVals []float64
	var XRows [][]float64

	for _, geoid := range tractGEOIDs {
		y := idx[synthKey{geoid, "chronic_absence"}]
		if y == nil {
			continue
		}
		row := make([]float64, 1+len(olsPredictors)) // intercept + predictors
		row[0] = 1.0                                   // intercept term
		complete := true
		for j, varID := range olsPredictors {
			v := idx[synthKey{geoid, varID}]
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

	if len(yVals) <= len(olsPredictors)+1 {
		return fmt.Errorf("OLS: insufficient complete cases (%d); need >%d", len(yVals), len(olsPredictors)+1)
	}

	res, err := stats.OLS(XRows, yVals)
	if err != nil {
		return fmt.Errorf("OLS: %w", err)
	}

	betas := make(map[string]interface{}, len(res.Betas))
	betas["intercept"] = res.Betas[0]
	for j, varID := range olsPredictors {
		betas[varID] = res.Betas[j+1]
	}

	pvalues := make(map[string]interface{}, len(res.PValues))
	pvalues["intercept"] = res.PValues[0]
	for j, varID := range olsPredictors {
		pvalues[varID] = res.PValues[j+1]
	}

	analysisID := fmt.Sprintf("ols-chronic-absence-%s-%s", scopeGEOID, cfg.Vintage)
	result := store.AnalysisResult{
		ID:         analysisID,
		Type:       "ols_regression",
		ScopeGEOID: scopeGEOID,
		ScopeLevel: scopeLevelStr(cfg),
		Parameters: map[string]interface{}{
			"outcome":    "chronic_absence",
			"predictors": olsPredictors,
			"vintage":    cfg.Vintage,
			"n":          len(yVals),
		},
		Results: map[string]interface{}{
			"betas":     betas,
			"pvalues":   pvalues,
			"r_squared": res.RSquared,
			"n":         len(yVals),
		},
		Vintage: cfg.Vintage,
	}

	dbID, err := s.PutAnalysis(ctx, result)
	if err != nil {
		return fmt.Errorf("PutAnalysis OLS: %w", err)
	}
	log.Printf("synthesize: OLS complete — R²=%.4f, n=%d, analysis ID %q", res.RSquared, len(yVals), dbID)
	return nil
}

func (sy *SynthesizeStage) runTippingPoints(
	ctx context.Context,
	s store.Store,
	cfg *Config,
	tractGEOIDs []string,
	idx map[synthKey]*float64,
	scopeGEOID string,
) error {
	var firstErr error
	for _, xVar := range tippingVariables {
		var xVals, yVals []float64
		for _, geoid := range tractGEOIDs {
			xv := idx[synthKey{geoid, xVar}]
			yv := idx[synthKey{geoid, "chronic_absence"}]
			if xv == nil || yv == nil {
				continue
			}
			xVals = append(xVals, *xv)
			yVals = append(yVals, *yv)
		}

		res, err := stats.TippingPoint(xVals, yVals)
		if err != nil {
			log.Printf("synthesize: TippingPoint(%s): %v", xVar, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		analysisID := fmt.Sprintf("tipping-%s-%s-%s", xVar, scopeGEOID, cfg.Vintage)
		ar := store.AnalysisResult{
			ID:         analysisID,
			Type:       "tipping_point",
			ScopeGEOID: scopeGEOID,
			ScopeLevel: scopeLevelStr(cfg),
			Parameters: map[string]interface{}{
				"x_variable": xVar,
				"y_variable": "chronic_absence",
				"vintage":    cfg.Vintage,
				"n":          len(xVals),
			},
			Results: map[string]interface{}{
				"threshold":       res.Threshold,
				"left_slope":      res.LeftSlope,
				"right_slope":     res.RightSlope,
				"left_intercept":  res.LeftIntercept,
				"right_intercept": res.RightIntercept,
				"f_statistic":     res.FStatistic,
			},
			Vintage: cfg.Vintage,
		}
		if _, err := s.PutAnalysis(ctx, ar); err != nil {
			log.Printf("synthesize: PutAnalysis tipping %s: %v", xVar, err)
		} else {
			log.Printf("synthesize: tipping point %q threshold=%.4f, F=%.4f", xVar, res.Threshold, res.FStatistic)
		}
	}
	return firstErr
}

func (sy *SynthesizeStage) runCorrelations(
	ctx context.Context,
	s store.Store,
	cfg *Config,
	tractGEOIDs []string,
	idx map[synthKey]*float64,
	scopeGEOID string,
) error {
	// Build column vectors for all composite variables.
	nTracts := len(tractGEOIDs)
	cols := make(map[string][]*float64, len(compositeVariables))
	for _, varID := range compositeVariables {
		col := make([]*float64, nTracts)
		for i, geoid := range tractGEOIDs {
			col[i] = idx[synthKey{geoid, varID}]
		}
		cols[varID] = col
	}

	// Compute upper-triangle pairwise correlations.
	type corrEntry struct {
		A, B string
		R    float64
	}
	var corrs []corrEntry
	for i, a := range compositeVariables {
		for _, b := range compositeVariables[i+1:] {
			r := stats.PearsonR(cols[a], cols[b])
			corrs = append(corrs, corrEntry{a, b, r})
		}
	}

	// Serialize correlation matrix as map of maps.
	matrix := make(map[string]interface{}, len(compositeVariables))
	for _, c := range corrs {
		row, ok := matrix[c.A].(map[string]interface{})
		if !ok {
			row = make(map[string]interface{})
			matrix[c.A] = row
		}
		row[c.B] = c.R
	}

	analysisID := fmt.Sprintf("corr-matrix-%s-%s", scopeGEOID, cfg.Vintage)
	ar := store.AnalysisResult{
		ID:         analysisID,
		Type:       "correlation_matrix",
		ScopeGEOID: scopeGEOID,
		ScopeLevel: scopeLevelStr(cfg),
		Parameters: map[string]interface{}{
			"variables": compositeVariables,
			"vintage":   cfg.Vintage,
			"n_tracts":  nTracts,
		},
		Results: map[string]interface{}{
			"correlations": matrix,
		},
		Vintage: cfg.Vintage,
	}

	dbID, err := s.PutAnalysis(ctx, ar)
	if err != nil {
		return fmt.Errorf("PutAnalysis correlations: %w", err)
	}
	log.Printf("synthesize: correlation matrix written (%d pairs), analysis ID %q", len(corrs), dbID)
	return nil
}

// scopeLevelStr returns "county" if CountyFIPS is set, otherwise "state".
func scopeLevelStr(cfg *Config) string {
	if cfg.CountyFIPS != "" {
		return string(geo.County)
	}
	return string(geo.State)
}
