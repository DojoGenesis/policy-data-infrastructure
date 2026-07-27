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
		return fmt.Errorf("analyze: no geographies found at tract level — seed geographies before running pipeline")
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
	// Prefer true cross-tabulated B19001 data (b19001_ice_score) when available.
	// Fall back to poverty×race approximation when B19001 has not been ingested.
	totalPop := make([]*float64, nTracts)
	for i, geoid := range tractGEOIDs {
		totalPop[i] = indicatorIdx[ikey{geoid, "total_population"}]
	}

	// Check for true B19001 ICE scores first.
	iceScores := make([]*float64, nTracts)
	trueICECount := 0
	for i, geoid := range tractGEOIDs {
		if v := indicatorIdx[ikey{geoid, "b19001_ice_score"}]; v != nil {
			iceScores[i] = v
			trueICECount++
		}
	}

	if trueICECount > 0 {
		log.Printf("analyze: using true B19001 ICE scores for %d/%d tracts", trueICECount, nTracts)
	} else {
		// Fall back to poverty×race approximation.
		log.Printf("analyze: B19001 not available, using poverty×race ICE approximation")
		pctPOC := make([]*float64, nTracts)
		poverty := make([]*float64, nTracts)
		for i, geoid := range tractGEOIDs {
			pctPOC[i] = indicatorIdx[ikey{geoid, "pct_poc"}]
			poverty[i] = indicatorIdx[ikey{geoid, "poverty_rate"}]
		}

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

		var err error
		iceScores, err = stats.ICEIncomeRace(privileged, deprived, totalPop)
		if err != nil {
			return fmt.Errorf("analyze: ICE computation: %w", err)
		}
	}

	// 4. Compute CV and reliability for each indicator.
	// TODO: When MOE data is available in the indicators table, compute
	// stats.CoefficientOfVariation(estimate, moe) and stats.ReliabilityLevel(cv)
	// for each indicator. For now, log placeholder.
	log.Printf("analyze: CV/reliability computation deferred — MOE data not yet ingested")

	// 4b. Compute Dissimilarity Index (Massey & Denton, 1988) per county.
	// Group tracts by county FIPS (first 5 chars of GEOID).
	countyTracts := make(map[string][]int) // countyFIPS -> tract indices
	for i, geoid := range tractGEOIDs {
		if len(geoid) >= 5 {
			cfips := geoid[:5]
			countyTracts[cfips] = append(countyTracts[cfips], i)
		}
	}
	log.Printf("analyze: computing dissimilarity index across %d counties", len(countyTracts))

	type dissimResult struct {
		countyFIPS     string
		blackWhiteD    *float64
		hispanicWhiteD *float64
	}
	var dissimResults []dissimResult

	for cfips, indices := range countyTracts {
		n := len(indices)
		popBlack := make([]*float64, n)
		popWhite := make([]*float64, n)
		popHispanic := make([]*float64, n)

		for j, idx := range indices {
			geoid := tractGEOIDs[idx]
			popBlack[j] = indicatorIdx[ikey{geoid, "pop_black"}]
			popWhite[j] = indicatorIdx[ikey{geoid, "pop_white_non_hispanic"}]
			popHispanic[j] = indicatorIdx[ikey{geoid, "pop_hispanic_latino"}]
		}

		bwD, err := stats.DissimilarityIndex(popBlack, popWhite)
		if err != nil {
			log.Printf("analyze: DissimilarityIndex error for county %s (Black-White): %v", cfips, err)
		}

		hwD, err := stats.DissimilarityIndex(popHispanic, popWhite)
		if err != nil {
			log.Printf("analyze: DissimilarityIndex error for county %s (Hispanic-White): %v", cfips, err)
		}

		if bwD != nil || hwD != nil {
			dissimResults = append(dissimResults, dissimResult{
				countyFIPS:     cfips,
				blackWhiteD:    bwD,
				hispanicWhiteD: hwD,
			})
		}
	}

	// Persist dissimilarity index results.
	if len(dissimResults) > 0 {
		dissimScopeGEOID := cfg.StateFIPS
		dissimScopeLevel := string(geo.State)
		if cfg.CountyFIPS != "" {
			dissimScopeGEOID = cfg.StateFIPS + cfg.CountyFIPS
			dissimScopeLevel = string(geo.County)
		}

		// Black-White dissimilarity analysis.
		bwAnalysisID := fmt.Sprintf("dissimilarity-bw-%s-%s", dissimScopeGEOID, cfg.Vintage)
		bwResult := store.AnalysisResult{
			ID:         bwAnalysisID,
			Type:       "dissimilarity_index",
			ScopeGEOID: dissimScopeGEOID,
			ScopeLevel: dissimScopeLevel,
			Parameters: map[string]interface{}{
				"metric":    "black_white",
				"algorithm": "massey_denton_1988",
				"vintage":   cfg.Vintage,
			},
			Results: map[string]interface{}{
				"counties_analyzed": len(dissimResults),
			},
			Vintage: cfg.Vintage,
		}

		bwDBID, err := s.PutAnalysis(ctx, bwResult)
		if err != nil {
			return fmt.Errorf("analyze: PutAnalysis (dissimilarity-bw): %w", err)
		}

		bwScores := make([]store.AnalysisScore, 0, len(dissimResults))
		for _, dr := range dissimResults {
			if dr.blackWhiteD != nil {
				bwScores = append(bwScores, store.AnalysisScore{
					AnalysisID: bwDBID,
					GEOID:      dr.countyFIPS,
					Score:      *dr.blackWhiteD,
					Details: map[string]interface{}{
						"metric": "black_white",
					},
				})
			}
		}
		if err := s.PutAnalysisScores(ctx, bwScores); err != nil {
			return fmt.Errorf("analyze: PutAnalysisScores (dissimilarity-bw): %w", err)
		}

		// Hispanic-White dissimilarity analysis.
		hwAnalysisID := fmt.Sprintf("dissimilarity-hw-%s-%s", dissimScopeGEOID, cfg.Vintage)
		hwResult := store.AnalysisResult{
			ID:         hwAnalysisID,
			Type:       "dissimilarity_index",
			ScopeGEOID: dissimScopeGEOID,
			ScopeLevel: dissimScopeLevel,
			Parameters: map[string]interface{}{
				"metric":    "hispanic_white",
				"algorithm": "massey_denton_1988",
				"vintage":   cfg.Vintage,
			},
			Results: map[string]interface{}{
				"counties_analyzed": len(dissimResults),
			},
			Vintage: cfg.Vintage,
		}

		hwDBID, err := s.PutAnalysis(ctx, hwResult)
		if err != nil {
			return fmt.Errorf("analyze: PutAnalysis (dissimilarity-hw): %w", err)
		}

		hwScores := make([]store.AnalysisScore, 0, len(dissimResults))
		for _, dr := range dissimResults {
			if dr.hispanicWhiteD != nil {
				hwScores = append(hwScores, store.AnalysisScore{
					AnalysisID: hwDBID,
					GEOID:      dr.countyFIPS,
					Score:      *dr.hispanicWhiteD,
					Details: map[string]interface{}{
						"metric": "hispanic_white",
					},
				})
			}
		}
		if err := s.PutAnalysisScores(ctx, hwScores); err != nil {
			return fmt.Errorf("analyze: PutAnalysisScores (dissimilarity-hw): %w", err)
		}

		var bwCount, hwCount int
		for _, dr := range dissimResults {
			if dr.blackWhiteD != nil {
				bwCount++
			}
			if dr.hispanicWhiteD != nil {
				hwCount++
			}
		}
		log.Printf("analyze: dissimilarity index complete — %d counties, %d Black-White scores, %d Hispanic-White scores",
			len(dissimResults), bwCount, hwCount)
	}

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
		rank := i + 1

		analysisScores = append(analysisScores, store.AnalysisScore{
			AnalysisID: dbID,
			GEOID:      geoid,
			Score:      scoreVal,
			Rank:       &rank,
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
		r := i + 1
		analysisScores[i].Rank = &r
	}

	if err := s.PutAnalysisScores(ctx, analysisScores); err != nil {
		return fmt.Errorf("analyze: PutAnalysisScores: %w", err)
	}

	log.Printf("analyze: validated features complete — %d/%d tracts with ICE scores, analysis ID %q",
		iceCount, nTracts, dbID)
	return nil
}
