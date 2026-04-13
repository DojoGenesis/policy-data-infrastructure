package stats

import (
	"math"
	"testing"
)

func TestPearsonR_perfect_positive(t *testing.T) {
	xs := []*float64{pf(1), pf(2), pf(3), pf(4), pf(5)}
	ys := []*float64{pf(2), pf(4), pf(6), pf(8), pf(10)}
	r := PearsonR(xs, ys)
	if math.Abs(r-1.0) > 1e-9 {
		t.Errorf("expected 1.0 for perfect positive correlation, got %.6f", r)
	}
}

func TestPearsonR_perfect_negative(t *testing.T) {
	xs := []*float64{pf(1), pf(2), pf(3), pf(4), pf(5)}
	ys := []*float64{pf(10), pf(8), pf(6), pf(4), pf(2)}
	r := PearsonR(xs, ys)
	if math.Abs(r+1.0) > 1e-9 {
		t.Errorf("expected -1.0 for perfect negative correlation, got %.6f", r)
	}
}

func TestPearsonR_zero_correlation(t *testing.T) {
	// constant y → zero correlation
	xs := []*float64{pf(1), pf(2), pf(3), pf(4), pf(5)}
	ys := []*float64{pf(3), pf(3), pf(3), pf(3), pf(3)}
	r := PearsonR(xs, ys)
	if math.Abs(r) > 1e-9 {
		t.Errorf("expected 0 correlation with constant y, got %.6f", r)
	}
}

func TestPearsonR_fewer_than_3(t *testing.T) {
	xs := []*float64{pf(1), pf(2)}
	ys := []*float64{pf(1), pf(2)}
	r := PearsonR(xs, ys)
	if r != 0.0 {
		t.Errorf("expected 0.0 for fewer than 3 pairs, got %.6f", r)
	}
}

func TestPearsonR_nil_skip(t *testing.T) {
	xs := []*float64{pf(1), nil, pf(3), pf(4), pf(5)}
	ys := []*float64{pf(2), pf(4), pf(6), pf(8), pf(10)}
	// removing nil pair leaves 4 pairs that are still perfectly correlated
	r := PearsonR(xs, ys)
	if math.Abs(r-1.0) > 1e-9 {
		t.Errorf("expected ~1.0, got %.6f", r)
	}
}

func TestSpearmanRho_monotone(t *testing.T) {
	xs := []*float64{pf(1), pf(2), pf(3), pf(4), pf(5)}
	ys := []*float64{pf(10), pf(20), pf(15), pf(30), pf(25)}
	rho := SpearmanRho(xs, ys)
	// both monotone → rho should be 1.0 only if ranks match perfectly
	// xs are already sorted → ranks 0, 0.25, 0.5, 0.75, 1.0
	// ys sorted: 10,15,20,25,30 → ranks of ys at original indices: 0, 0.5, 0.25, 1.0, 0.75
	// correlation between xs ranks and ys ranks is partial; just verify it's in [-1, 1]
	if rho < -1.0 || rho > 1.0 {
		t.Errorf("SpearmanRho out of range: %.6f", rho)
	}
}

func TestSpearmanRho_perfect_rank(t *testing.T) {
	// same ordering → rho = 1
	xs := []*float64{pf(1), pf(4), pf(9), pf(16), pf(25)}
	ys := []*float64{pf(10), pf(40), pf(90), pf(160), pf(250)}
	rho := SpearmanRho(xs, ys)
	if math.Abs(rho-1.0) > 1e-9 {
		t.Errorf("expected 1.0 for identical rank ordering, got %.6f", rho)
	}
}
