package stats

import "math"

// PercentileRank returns percentile ranks in [0.0, 1.0] for each element.
// nil input values produce nil output values.
// A single non-nil value gets rank 0.0 (rank / max(n-1, 1) with n=1).
func PercentileRank(values []*float64) []*float64 {
	type indexed struct {
		origIdx int
		val     float64
	}

	var valid []indexed
	for i, v := range values {
		if v != nil {
			valid = append(valid, indexed{i, *v})
		}
	}

	// sort ascending by value
	n := len(valid)
	for i := 1; i < n; i++ {
		for j := i; j > 0 && valid[j].val < valid[j-1].val; j-- {
			valid[j], valid[j-1] = valid[j-1], valid[j]
		}
	}

	denom := float64(max1(n-1, 1))
	ranks := make([]*float64, len(values))
	for rank, item := range valid {
		r := float64(rank) / denom
		ranks[item.origIdx] = &r
	}
	return ranks
}

// ZScore normalizes values to z-scores (mean=0, sd=1).
// nil input values produce nil output values.
// Returns a copy of values unchanged if fewer than 2 non-nil values exist.
// Uses population standard deviation (divide by n, matching the Python source).
func ZScore(values []*float64) []*float64 {
	var valid []float64
	for _, v := range values {
		if v != nil {
			valid = append(valid, *v)
		}
	}

	result := make([]*float64, len(values))
	if len(valid) < 2 {
		// return a copy with same nil pattern
		for i, v := range values {
			if v != nil {
				c := *v
				result[i] = &c
			}
		}
		return result
	}

	mu := mean(valid)
	variance := 0.0
	for _, v := range valid {
		d := v - mu
		variance += d * d
	}
	variance /= float64(len(valid))
	sd := 1.0
	if variance > 0 {
		sd = math.Sqrt(variance)
	}

	for i, v := range values {
		if v != nil {
			z := (*v - mu) / sd
			result[i] = &z
		}
	}
	return result
}

func mean(xs []float64) float64 {
	s := 0.0
	for _, v := range xs {
		s += v
	}
	return s / float64(len(xs))
}

func max1(a, b int) int {
	if a > b {
		return a
	}
	return b
}
