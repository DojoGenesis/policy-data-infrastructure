package stats

import "math"

// PearsonR computes the Pearson correlation coefficient between xs and ys.
// Pairs where either value is nil are skipped.
// Returns 0.0 if fewer than 3 complete pairs exist.
func PearsonR(xs, ys []*float64) float64 {
	type pair struct{ x, y float64 }
	var pairs []pair
	n := len(xs)
	if len(ys) < n {
		n = len(ys)
	}
	for i := 0; i < n; i++ {
		if xs[i] != nil && ys[i] != nil {
			pairs = append(pairs, pair{*xs[i], *ys[i]})
		}
	}
	if len(pairs) < 3 {
		return 0.0
	}

	np := float64(len(pairs))
	mx, my := 0.0, 0.0
	for _, p := range pairs {
		mx += p.x
		my += p.y
	}
	mx /= np
	my /= np

	sxy, sxx, syy := 0.0, 0.0, 0.0
	for _, p := range pairs {
		dx := p.x - mx
		dy := p.y - my
		sxy += dx * dy
		sxx += dx * dx
		syy += dy * dy
	}

	denom := math.Sqrt(sxx * syy)
	if denom == 0 {
		return 0.0
	}
	return sxy / denom
}

// SpearmanRho computes the Spearman rank correlation between xs and ys.
// It ranks each slice independently (nil values excluded, nil-keyed rank = nil)
// then computes PearsonR on the ranks.
func SpearmanRho(xs, ys []*float64) float64 {
	rxs := PercentileRank(xs)
	rys := PercentileRank(ys)
	return PearsonR(rxs, rys)
}
