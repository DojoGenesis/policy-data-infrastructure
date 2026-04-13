package stats

import "fmt"

// TierDef defines a named tier by percentile rank bounds [MinPercentile, MaxPercentile).
// The final tier should set MaxPercentile to 1.0 (inclusive in AssignTiers).
type TierDef struct {
	Name          string
	MinPercentile float64
	MaxPercentile float64
}

// CompositeIndex builds a composite index across a set of indicators.
//
// indicators is a slice of columns: indicators[k][i] is the value for indicator k at tract i.
// weights is length len(indicators); ignored for "equal_percentile" (all equal).
// method is one of:
//   - "equal_percentile": percentile-rank each indicator, average across all indicators
//   - "weighted_zscore":  z-score each indicator, take weighted sum
//
// Returns a []*float64 of length equal to the number of tracts. An element is nil
// if all indicator values for that tract are nil.
func CompositeIndex(indicators [][]*float64, weights []float64, method string) ([]*float64, error) {
	if len(indicators) == 0 {
		return nil, fmt.Errorf("CompositeIndex: no indicators")
	}
	nTracts := len(indicators[0])
	nIndicators := len(indicators)

	switch method {
	case "equal_percentile":
		// percentile-rank each indicator, then average across indicators
		ranked := make([][]*float64, nIndicators)
		for k, col := range indicators {
			ranked[k] = PercentileRank(col)
		}
		result := make([]*float64, nTracts)
		for i := 0; i < nTracts; i++ {
			sum := 0.0
			count := 0
			for k := 0; k < nIndicators; k++ {
				if ranked[k][i] != nil {
					sum += *ranked[k][i]
					count++
				}
			}
			if count > 0 {
				v := sum / float64(count)
				result[i] = &v
			}
		}
		return result, nil

	case "weighted_zscore":
		if len(weights) != nIndicators {
			return nil, fmt.Errorf("CompositeIndex: weights length %d != indicators length %d", len(weights), nIndicators)
		}
		// z-score each indicator, then take weighted sum
		scored := make([][]*float64, nIndicators)
		for k, col := range indicators {
			scored[k] = ZScore(col)
		}
		result := make([]*float64, nTracts)
		for i := 0; i < nTracts; i++ {
			sum := 0.0
			count := 0
			for k := 0; k < nIndicators; k++ {
				if scored[k][i] != nil {
					sum += weights[k] * *scored[k][i]
					count++
				}
			}
			if count > 0 {
				v := sum
				result[i] = &v
			}
		}
		return result, nil

	default:
		return nil, fmt.Errorf("CompositeIndex: unknown method %q (want equal_percentile or weighted_zscore)", method)
	}
}

// AssignTiers maps each composite score to a tier name.
// Scores are compared against each TierDef by percentile rank of the scores themselves.
// Returns "unknown" for nil scores or scores that fall outside all tiers.
func AssignTiers(scores []*float64, tiers []TierDef) []string {
	// compute percentile ranks of the scores to assign tiers
	ranks := PercentileRank(scores)
	result := make([]string, len(scores))
	for i, rank := range ranks {
		if rank == nil {
			result[i] = "unknown"
			continue
		}
		r := *rank
		assigned := "unknown"
		for _, t := range tiers {
			// last tier or upper bound is inclusive
			if r >= t.MinPercentile && (r < t.MaxPercentile || t.MaxPercentile >= 1.0) {
				assigned = t.Name
				break
			}
		}
		result[i] = assigned
	}
	return result
}
