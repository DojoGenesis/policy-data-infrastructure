package stats

import (
	"fmt"
	"math"
	"sort"
)

// TippingResult holds the result of a tipping point detection.
type TippingResult struct {
	// Threshold is the x value of the detected breakpoint (midpoint between split).
	Threshold float64
	// LeftSlope is the slope of the left-segment OLS fit.
	LeftSlope float64
	// RightSlope is the slope of the right-segment OLS fit.
	RightSlope float64
	// LeftIntercept is the intercept of the left-segment OLS fit.
	LeftIntercept float64
	// RightIntercept is the intercept of the right-segment OLS fit.
	RightIntercept float64
	// FStatistic compares the two-segment model against the single-line null.
	FStatistic float64
}

type xypt struct{ x, y float64 }

// TippingPoint finds a breakpoint in (x, y) data via piecewise linear regression.
//
// It evaluates every candidate split point (excluding the 2 smallest and 2 largest x values
// so each segment has at least 3 points), fits simple OLS on each segment, and picks the
// split that minimizes total SSE. Returns an error if there are fewer than 6 observations.
func TippingPoint(x, y []float64) (*TippingResult, error) {
	n := len(x)
	if n != len(y) {
		return nil, fmt.Errorf("TippingPoint: x and y must be same length")
	}
	if n < 6 {
		return nil, fmt.Errorf("TippingPoint: need at least 6 observations, got %d", n)
	}

	pts := make([]xypt, n)
	for i := range pts {
		pts[i] = xypt{x[i], y[i]}
	}
	sort.Slice(pts, func(i, j int) bool { return pts[i].x < pts[j].x })

	bestSSE := math.Inf(1)
	bestSplit := -1

	// split at index s means left = pts[0..s] (len s+1), right = pts[s+1..n-1] (len n-s-1)
	// need s+1 >= 3 → s >= 2 and n-s-1 >= 3 → s <= n-4
	for s := 2; s <= n-4; s++ {
		total := xyptSSE(pts[:s+1]) + xyptSSE(pts[s+1:])
		if total < bestSSE {
			bestSSE = total
			bestSplit = s
		}
	}

	if bestSplit < 0 {
		return nil, fmt.Errorf("TippingPoint: no valid split found")
	}

	lSlope, lIntercept := xyptOLS(pts[:bestSplit+1])
	rSlope, rIntercept := xyptOLS(pts[bestSplit+1:])
	threshold := (pts[bestSplit].x + pts[bestSplit+1].x) / 2.0

	// F-statistic: piecewise model (4 params) vs single line (2 params)
	nullSSE := xyptSSE(pts)
	ssr := nullSSE - bestSSE
	k := 2.0
	dfRes := float64(n - 4)
	fStat := 0.0
	if dfRes > 0 && bestSSE > 0 {
		fStat = (ssr / k) / (bestSSE / dfRes)
	}

	return &TippingResult{
		Threshold:      threshold,
		LeftSlope:      lSlope,
		RightSlope:     rSlope,
		LeftIntercept:  lIntercept,
		RightIntercept: rIntercept,
		FStatistic:     fStat,
	}, nil
}

// xyptOLS fits y = intercept + slope*x on a slice of xypt points.
func xyptOLS(pts []xypt) (slope, intercept float64) {
	n := float64(len(pts))
	if n < 2 {
		return 0, 0
	}
	mx, my := 0.0, 0.0
	for _, p := range pts {
		mx += p.x
		my += p.y
	}
	mx /= n
	my /= n
	sxx, sxy := 0.0, 0.0
	for _, p := range pts {
		dx := p.x - mx
		sxx += dx * dx
		sxy += dx * (p.y - my)
	}
	if sxx == 0 {
		return 0, my
	}
	slope = sxy / sxx
	intercept = my - slope*mx
	return
}

// xyptSSE computes SSE for a simple OLS fit on the given points.
func xyptSSE(pts []xypt) float64 {
	slope, intercept := xyptOLS(pts)
	sse := 0.0
	for _, p := range pts {
		r := p.y - (intercept + slope*p.x)
		sse += r * r
	}
	return sse
}
