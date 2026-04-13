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

// nariTiers defines the standard NARI-style deprivation tiers used for the
// composite index. Tracts in higher tiers face greater structural disadvantage.
var nariTiers = []stats.TierDef{
	{Name: "highest_need", MinPercentile: 0.80, MaxPercentile: 1.01},
	{Name: "high_need", MinPercentile: 0.60, MaxPercentile: 0.80},
	{Name: "moderate_need", MinPercentile: 0.40, MaxPercentile: 0.60},
	{Name: "low_need", MinPercentile: 0.20, MaxPercentile: 0.40},
	{Name: "lowest_need", MinPercentile: 0.00, MaxPercentile: 0.20},
}

// compositeVariables is the ordered list of indicator variable IDs used to
// build the NARI composite index. Order determines column assignment.
var compositeVariables = []string{
	"pct_poverty",
	"median_household_income",
	"pct_poc",
	"pct_no_hs_diploma",
	"pct_unemployment",
	"pct_uninsured",
	"pct_renter_occupied",
	"pct_housing_cost_burdened",
}

// AnalyzeStage is Stage 04: it queries indicators for all tracts in scope,
// builds the indicator matrix, computes the composite index via
// stats.CompositeIndex (equal_percentile method), assigns tiers via
// stats.AssignTiers, and persists the result via store.PutAnalysis and
// store.PutAnalysisScores.
type AnalyzeStage struct{}

func (a *AnalyzeStage) Name() string          { return "analyze" }
func (a *AnalyzeStage) Dependencies() []string { return []string{"enrich"} }

func (a *AnalyzeStage) Run(ctx context.Context, s store.Store, cfg *Config) error {
	if cfg.DryRun {
		log.Printf("analyze: dry-run mode — skipping")
		return nil
	}

	// 1. Load all tract GEOIDs in scope.
	geoQuery := store.GeoQuery{Level: geo.Tract}
	if cfg.CountyFIPS != "" {
		geoQuery.CountyFIPS = cfg.CountyFIPS
		geoQuery.StateFIPS = cfg.StateFIPS
	} else {
		geoQuery.StateFIPS = cfg.StateFIPS
	}

	geographies, err := s.QueryGeographies(ctx, geoQuery)
	if err != nil {
		return fmt.Errorf("analyze: query geographies: %w", err)
	}
	if len(geographies) == 0 {
		log.Printf("analyze: no tracts found for scope, skipping")
		return nil
	}

	// Sort for deterministic column ordering.
	sort.Slice(geographies, func(i, j int) bool {
		return geographies[i].GEOID < geographies[j].GEOID
	})
	tractGEOIDs := make([]string, len(geographies))
	for i, g := range geographies {
		tractGEOIDs[i] = g.GEOID
	}
	nTracts := len(tractGEOIDs)
	log.Printf("analyze: building composite index for %d tracts", nTracts)

	// 2. Query all indicators for the scope.
	indicators, err := s.QueryIndicators(ctx, store.IndicatorQuery{
		GEOIDs:     tractGEOIDs,
		Vintage:    cfg.Vintage,
		LatestOnly: true,
	})
	if err != nil {
		return fmt.Errorf("analyze: query indicators: %w", err)
	}

	// Index indicators by GEOID+VariableID.
	type ikey struct{ geoid, variableID string }
	indicatorIdx := make(map[ikey]*float64, len(indicators))
	for _, ind := range indicators {
		k := ikey{ind.GEOID, ind.VariableID}
		indicatorIdx[k] = ind.Value
	}

	// 3. Build the indicator matrix.
	// indicators[k] is a column of length nTracts for compositeVariables[k].
	matrix := make([][]*float64, len(compositeVariables))
	for k, varID := range compositeVariables {
		col := make([]*float64, nTracts)
		for i, geoid := range tractGEOIDs {
			col[i] = indicatorIdx[ikey{geoid, varID}]
		}
		matrix[k] = col
	}

	// 4. Compute composite index.
	scores, err := stats.CompositeIndex(matrix, nil, "equal_percentile")
	if err != nil {
		return fmt.Errorf("analyze: CompositeIndex: %w", err)
	}

	// 5. Assign tiers.
	tiers := stats.AssignTiers(scores, nariTiers)

	// 6. Build scope GEOID for the analysis record.
	scopeGEOID := cfg.StateFIPS
	scopeLevel := string(geo.State)
	if cfg.CountyFIPS != "" {
		scopeGEOID = cfg.StateFIPS + cfg.CountyFIPS
		scopeLevel = string(geo.County)
	}

	analysisID := fmt.Sprintf("nari-%s-%s", scopeGEOID, cfg.Vintage)

	// Build summary results map.
	var nonNilCount int
	for _, v := range scores {
		if v != nil {
			nonNilCount++
		}
	}

	result := store.AnalysisResult{
		ID:         analysisID,
		Type:       "composite_index",
		ScopeGEOID: scopeGEOID,
		ScopeLevel: scopeLevel,
		Parameters: map[string]interface{}{
			"method":     "equal_percentile",
			"variables":  compositeVariables,
			"vintage":    cfg.Vintage,
			"tract_count": nTracts,
		},
		Results: map[string]interface{}{
			"scored_tracts": nonNilCount,
			"total_tracts":  nTracts,
		},
		Vintage: cfg.Vintage,
	}

	dbID, err := s.PutAnalysis(ctx, result)
	if err != nil {
		return fmt.Errorf("analyze: PutAnalysis: %w", err)
	}

	// 7. Build and persist per-tract scores.
	analysisScores := make([]store.AnalysisScore, 0, nTracts)
	for i, geoid := range tractGEOIDs {
		sc := scores[i]
		tier := tiers[i]

		scoreVal := 0.0
		if sc != nil {
			scoreVal = *sc
		}

		analysisScores = append(analysisScores, store.AnalysisScore{
			AnalysisID: dbID,
			GEOID:      geoid,
			Score:      scoreVal,
			Rank:       i + 1, // placeholder rank; real rank requires sort by score
			Percentile: scoreVal, // for equal_percentile method score IS the percentile
			Tier:       tier,
			Details: map[string]interface{}{
				"method": "equal_percentile",
			},
		})
	}

	// Re-sort by score descending to assign meaningful ranks.
	sort.Slice(analysisScores, func(i, j int) bool {
		return analysisScores[i].Score > analysisScores[j].Score
	})
	for i := range analysisScores {
		analysisScores[i].Rank = i + 1
	}

	if err := s.PutAnalysisScores(ctx, analysisScores); err != nil {
		return fmt.Errorf("analyze: PutAnalysisScores: %w", err)
	}

	log.Printf("analyze: composite index complete — %d/%d tracts scored, analysis ID %q",
		nonNilCount, nTracts, dbID)
	return nil
}
