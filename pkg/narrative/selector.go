package narrative

import (
	"context"
	"fmt"
	"sort"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// tierOrder maps tier name → severity rank (lower = more severe / higher priority).
var tierOrder = map[string]int{
	"very_high": 0,
	"critical":  0,
	"high":      1,
	"moderate":  2,
	"low":       3,
	"on_track":  4,
	"minimal":   4,
	"unknown":   5,
}

// tierRank returns the sort key for a tier name, defaulting to 5.
func tierRank(tier string) int {
	if r, ok := tierOrder[tier]; ok {
		return r
	}
	return 5
}

// buildProfile constructs a GeographyProfile from a store.AnalysisScore and its
// Geography, enriching it with indicator values fetched from the store.
func buildProfile(
	ctx context.Context,
	s store.Store,
	score store.AnalysisScore,
	scopeName string,
	scopeLevel string,
) (GeographyProfile, error) {
	g, err := s.GetGeography(ctx, score.GEOID)
	if err != nil {
		return GeographyProfile{}, fmt.Errorf("selector: GetGeography %s: %w", score.GEOID, err)
	}
	if g == nil {
		g = &geo.Geography{GEOID: score.GEOID, Name: score.GEOID}
	}

	p := GeographyProfile{
		GEOID:      score.GEOID,
		Name:       g.Name,
		Level:      string(g.Level),
		Population: g.Population,
		NARITier:   score.Tier,
		ScopeLevel: scopeLevel,
		ScopeName:  scopeName,
	}

	// Percentile is stored as 0–100 in AnalysisScore.
	pct := score.Percentile
	scoreVal := score.Score

	// Populate ICE field (the validated metric replacing NARI).
	p.ICE = &scoreVal

	// Backward compat: keep NARI fields populated until all templates migrate.
	p.NARIPercentile = &pct
	p.NARIScore = &scoreVal
	p.NARITier = score.Tier

	// Pull indicator values for known variable IDs.
	indicators, err := s.QueryIndicators(ctx, store.IndicatorQuery{
		GEOIDs:     []string{score.GEOID},
		LatestOnly: true,
	})
	if err != nil {
		// Non-fatal: return the partial profile.
		return p, nil
	}

	for _, ind := range indicators {
		switch ind.VariableID {
		case "median_household_income", "B19013_001E":
			p.MedianIncome = ind.Value
		case "poverty_rate", "poverty_pct":
			p.PovertyRate = ind.Value
		case "pct_poc", "pct_nonwhite":
			p.PctPOC = ind.Value
		case "uninsured_rate", "uninsured_pct":
			p.UninsuredRate = ind.Value
		case "cost_burden_rate", "cost_burden_pct":
			p.CostBurdenRate = ind.Value
		case "eviction_rate", "eviction_filing_rate":
			p.EvictionRate = ind.Value
		case "transit_score":
			p.TransitScore = ind.Value
		case "chronic_absence_rate", "chronic_absence_pct":
			p.ChronicAbsence = ind.Value
		}
	}

	return p, nil
}

// SelectByTier selects one geography per tier from analysis scores, ordered by
// tier severity (very_high/critical first). Returns up to count profiles.
func SelectByTier(ctx context.Context, s store.Store, analysisID string, count int) ([]GeographyProfile, error) {
	// Fetch all scores with no tier filter.
	scores, err := s.QueryAnalysisScores(ctx, analysisID, "")
	if err != nil {
		return nil, fmt.Errorf("SelectByTier: QueryAnalysisScores: %w", err)
	}

	// Sort by tier severity then by score descending within tier.
	sort.Slice(scores, func(i, j int) bool {
		ri, rj := tierRank(scores[i].Tier), tierRank(scores[j].Tier)
		if ri != rj {
			return ri < rj
		}
		return scores[i].Score > scores[j].Score
	})

	// Pick one per tier, up to count.
	seen := map[string]bool{}
	var selected []store.AnalysisScore
	for _, sc := range scores {
		if len(selected) >= count {
			break
		}
		if !seen[sc.Tier] {
			seen[sc.Tier] = true
			selected = append(selected, sc)
		}
	}
	// If we don't have enough tiers, fill with remaining scores.
	for _, sc := range scores {
		if len(selected) >= count {
			break
		}
		alreadyIncluded := false
		for _, s2 := range selected {
			if s2.GEOID == sc.GEOID {
				alreadyIncluded = true
				break
			}
		}
		if !alreadyIncluded {
			selected = append(selected, sc)
		}
	}

	return scoresToProfiles(ctx, s, selected, "", "county")
}

// SelectOutliers selects geographies that outperform ("positive") or
// underperform ("negative") their structural predictions. direction must be
// "positive" or "negative".
func SelectOutliers(ctx context.Context, s store.Store, analysisID string, direction string, count int) ([]GeographyProfile, error) {
	scores, err := s.QueryAnalysisScores(ctx, analysisID, "")
	if err != nil {
		return nil, fmt.Errorf("SelectOutliers: QueryAnalysisScores: %w", err)
	}

	// Outliers are identified via the "residual" field in Details, if present.
	// Positive outliers: residual < 0 (performing better than predicted).
	// Negative outliers: residual > 0 (performing worse than predicted).
	type scoredOutlier struct {
		sc       store.AnalysisScore
		residual float64
	}
	var outliers []scoredOutlier
	for _, sc := range scores {
		if sc.Details == nil {
			continue
		}
		v, ok := sc.Details["residual"]
		if !ok {
			continue
		}
		r, ok := toFloat64(v)
		if !ok {
			continue
		}
		if direction == "positive" && r < 0 {
			outliers = append(outliers, scoredOutlier{sc, r})
		} else if direction == "negative" && r > 0 {
			outliers = append(outliers, scoredOutlier{sc, r})
		}
	}

	// Sort by absolute residual descending.
	sort.Slice(outliers, func(i, j int) bool {
		ai := outliers[i].residual
		if ai < 0 {
			ai = -ai
		}
		aj := outliers[j].residual
		if aj < 0 {
			aj = -aj
		}
		return ai > aj
	})

	selected := make([]store.AnalysisScore, 0, count)
	for i := 0; i < len(outliers) && len(selected) < count; i++ {
		selected = append(selected, outliers[i].sc)
	}

	return scoresToProfiles(ctx, s, selected, "", "county")
}

// SelectByIndicator selects geographies with extreme values for a given
// indicator. direction must be "highest" or "lowest".
func SelectByIndicator(ctx context.Context, s store.Store, variableID string, direction string, count int) ([]GeographyProfile, error) {
	indicators, err := s.QueryIndicators(ctx, store.IndicatorQuery{
		VariableIDs: []string{variableID},
		LatestOnly:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("SelectByIndicator: QueryIndicators: %w", err)
	}

	// Filter nulls.
	type valued struct {
		geoid string
		val   float64
	}
	var vals []valued
	for _, ind := range indicators {
		if ind.Value != nil {
			vals = append(vals, valued{ind.GEOID, *ind.Value})
		}
	}

	if direction == "highest" {
		sort.Slice(vals, func(i, j int) bool { return vals[i].val > vals[j].val })
	} else {
		sort.Slice(vals, func(i, j int) bool { return vals[i].val < vals[j].val })
	}

	profiles := make([]GeographyProfile, 0, count)
	for i := 0; i < len(vals) && len(profiles) < count; i++ {
		g, err := s.GetGeography(ctx, vals[i].geoid)
		if err != nil || g == nil {
			g = &geo.Geography{GEOID: vals[i].geoid, Name: vals[i].geoid}
		}
		p := GeographyProfile{
			GEOID: vals[i].geoid,
			Name:  g.Name,
			Level: string(g.Level),
		}
		profiles = append(profiles, p)
	}
	return profiles, nil
}

// scoresToProfiles converts a slice of AnalysisScores into GeographyProfiles.
func scoresToProfiles(ctx context.Context, s store.Store, scores []store.AnalysisScore, scopeName, scopeLevel string) ([]GeographyProfile, error) {
	profiles := make([]GeographyProfile, 0, len(scores))
	for _, sc := range scores {
		p, err := buildProfile(ctx, s, sc, scopeName, scopeLevel)
		if err != nil {
			return nil, err
		}
		profiles = append(profiles, p)
	}
	return profiles, nil
}

// toFloat64 converts an interface{} to float64. Returns false if conversion fails.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}
