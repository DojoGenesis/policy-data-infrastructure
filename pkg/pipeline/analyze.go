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

// correlationVariables are the key indicators used for correlation analysis
// and validated feature computation. Replaces the deprecated compositeVariables.
var correlationVariables = []string{
	"poverty_rate",
	"median_household_income",
	"pct_poc",
	"uninsured_rate",
	"pct_cost_burdened",
	"total_population",
}

// AnalyzeStage is Stage 04: queries indicators for all tracts in scope,
// computes validated features (ICE, CV/reliability), and persists results.
// Replaces the former NARI composite index with research-grounded methods.
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

	sort.Slice(geographies, func(i, j int) bool {
		return geographies[i].GEOID < geographies[j].GEOID
	})
	tractGEOIDs := make([]string, len(geographies))
	for i, g := range geographies {
		tractGEOIDs[i] = g.GEOID
	}
	nTracts := len(tractGEOIDs)
	log.Printf("analyze: computing validated features for %d tracts", nTracts)

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

	// 3. Compute ICE (Index of Concentration at the Extremes).
	// ICE = (high_income_white - low_income_poc) / total_population
	// We approximate using available ACS variables.
	totalPop := make([]*float64, nTracts)
	pctPOC := make([]*float64, nTracts)
	poverty := make([]*float64, nTracts)
	for i, geoid := range tractGEOIDs {
		totalPop[i] = indicatorIdx[ikey{geoid, "total_population"}]
		pctPOC[i] = indicatorIdx[ikey{geoid, "pct_poc"}]
		poverty[i] = indicatorIdx[ikey{geoid, "poverty_rate"}]
	}

	// Approximate ICE using available data:
	// privileged ≈ (1 - pct_poc/100) * (1 - poverty_rate/100) * total_pop
	// deprived ≈ (pct_poc/100) * (poverty_rate/100) * total_pop
	privileged := make([]*float64, nTracts)
	deprived := make([]*float64, nTracts)
	for i := 0; i < nTracts; i++ {
		if totalPop[i] != nil && pctPOC[i] != nil && poverty[i] != nil {
			pop := *totalPop[i]
			poc := *pctPOC[i] / 100.0
			pov := *poverty[i] / 100.0
			priv := (1 - poc) * (1 - pov) * pop
			dep := poc * pov * pop
			privileged[i] = &priv
			deprived[i] = &dep
		}
	}

	iceScores, err := stats.ICEIncomeRace(privileged, deprived, totalPop)
	if err != nil {
		return fmt.Errorf("analyze: ICE computation: %w", err)
	}

	// 4. Compute CV and reliability for each indicator.
	// TODO: When MOE data is available in the indicators table, compute
	// stats.CoefficientOfVariation(estimate, moe) and stats.ReliabilityLevel(cv)
	// for each indicator. For now, log placeholder.
	log.Printf("analyze: CV/reliability computation deferred — MOE data not yet ingested")

	// 5. Build scope GEOID for the analysis record.
	scopeGEOID := cfg.StateFIPS
	scopeLevel := string(geo.State)
	if cfg.CountyFIPS != "" {
		scopeGEOID = cfg.StateFIPS + cfg.CountyFIPS
		scopeLevel = string(geo.County)
	}

	analysisID := fmt.Sprintf("validated-%s-%s", scopeGEOID, cfg.Vintage)

	var iceCount int
	for _, v := range iceScores {
		if v != nil {
			iceCount++
		}
	}

	result := store.AnalysisResult{
		ID:         analysisID,
		Type:       "validated_features",
		ScopeGEOID: scopeGEOID,
		ScopeLevel: scopeLevel,
		Parameters: map[string]interface{}{
			"features": []string{"ice_income_race"},
			"vintage":  cfg.Vintage,
		},
		Results: map[string]interface{}{
			"ice_scored_tracts": iceCount,
			"total_tracts":     nTracts,
		},
		Vintage: cfg.Vintage,
	}

	dbID, err := s.PutAnalysis(ctx, result)
	if err != nil {
		return fmt.Errorf("analyze: PutAnalysis: %w", err)
	}

	// 6. Persist per-tract ICE scores as analysis scores.
	// The Score field carries ICE, Tier is empty (no arbitrary cutoffs).
	analysisScores := make([]store.AnalysisScore, 0, nTracts)
	iceRanks := stats.PercentileRank(iceScores)
	for i, geoid := range tractGEOIDs {
		scoreVal := 0.0
		pctVal := 0.0
		if iceScores[i] != nil {
			scoreVal = *iceScores[i]
		}
		if iceRanks[i] != nil {
			pctVal = *iceRanks[i] * 100
		}

		analysisScores = append(analysisScores, store.AnalysisScore{
			AnalysisID: dbID,
			GEOID:      geoid,
			Score:      scoreVal,
			Rank:       i + 1,
			Percentile: pctVal,
			Tier:       "", // No arbitrary tiers — use factor profiles instead
			Details: map[string]interface{}{
				"feature": "ice_income_race",
			},
		})
	}

	// Sort by ICE score descending for meaningful ranks.
	sort.Slice(analysisScores, func(i, j int) bool {
		return analysisScores[i].Score > analysisScores[j].Score
	})
	for i := range analysisScores {
		analysisScores[i].Rank = i + 1
	}

	if err := s.PutAnalysisScores(ctx, analysisScores); err != nil {
		return fmt.Errorf("analyze: PutAnalysisScores: %w", err)
	}

	log.Printf("analyze: validated features complete — %d/%d tracts with ICE scores, analysis ID %q",
		iceCount, nTracts, dbID)
	return nil
}
