package stats

import (
	"math"
	"testing"
)

// buildGroupX builds an n×2 design matrix [[1, x1], ...] for n observations
// starting from startX with unit increments.
func buildGroupX(startX float64, n int) [][]float64 {
	X := make([][]float64, n)
	for i := 0; i < n; i++ {
		X[i] = []float64{1, startX + float64(i)}
	}
	return X
}

func TestBlinderOaxaca_known_gap(t *testing.T) {
	// Group A: y = 2 + 3x for x = 1..6
	// Group B: y = 1 + 2x for x = 1..6
	// Mean yA = 2 + 3*3.5 = 12.5; mean yB = 1 + 2*3.5 = 8
	// Gap = 4.5
	nA, nB := 6, 6
	xA := buildGroupX(1, nA)
	yA := make([]float64, nA)
	for i := 0; i < nA; i++ {
		yA[i] = 2 + 3*float64(i+1)
	}

	xB := buildGroupX(1, nB)
	yB := make([]float64, nB)
	for i := 0; i < nB; i++ {
		yB[i] = 1 + 2*float64(i+1)
	}

	res, err := BlinderOaxaca(xA, yA, xB, yB)
	if err != nil {
		t.Fatalf("BlinderOaxaca error: %v", err)
	}

	if math.Abs(res.Gap-4.5) > 1e-6 {
		t.Errorf("Gap: want 4.5, got %.6f", res.Gap)
	}
	if math.Abs(res.MeanA-12.5) > 1e-6 {
		t.Errorf("MeanA: want 12.5, got %.6f", res.MeanA)
	}
	if math.Abs(res.MeanB-8.0) > 1e-6 {
		t.Errorf("MeanB: want 8.0, got %.6f", res.MeanB)
	}
}

func TestBlinderOaxaca_decomposition_adds_up(t *testing.T) {
	// Endowment + Coefficients + Interaction should equal Gap
	nA, nB := 8, 8
	xA := buildGroupX(1, nA)
	yA := make([]float64, nA)
	for i := 0; i < nA; i++ {
		yA[i] = 5 + 2*float64(i+1) + 0.1*float64(i%3)
	}

	xB := buildGroupX(3, nB)
	yB := make([]float64, nB)
	for i := 0; i < nB; i++ {
		yB[i] = 2 + 1.5*float64(i+3) + 0.05*float64(i%2)
	}

	res, err := BlinderOaxaca(xA, yA, xB, yB)
	if err != nil {
		t.Fatalf("BlinderOaxaca error: %v", err)
	}

	total := res.Endowment + res.Coefficients + res.Interaction
	if math.Abs(total-res.Gap) > 1e-6 {
		t.Errorf("Endowment+Coefficients+Interaction = %.6f, Gap = %.6f", total, res.Gap)
	}
}

func TestBlinderOaxaca_zero_gap(t *testing.T) {
	// Identical groups → gap = 0, pct should be 0
	n := 6
	x := buildGroupX(1, n)
	y := []float64{3, 5, 7, 9, 11, 13}
	res, err := BlinderOaxaca(x, y, x, y)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if math.Abs(res.Gap) > 1e-9 {
		t.Errorf("expected gap=0, got %.6f", res.Gap)
	}
	if res.EndowmentPct != 0 || res.CoefficientsPct != 0 {
		t.Error("expected zero pcts for zero gap")
	}
}
