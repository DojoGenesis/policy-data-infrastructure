package stats

import (
	"math"
	"testing"
)

func TestInteractionOLS_adds_column(t *testing.T) {
	// X: n×3 [[1, a, b]] with independent a and b values.
	// Interaction of cols 1 and 2 → augmented X is [[1, a, b, a*b]].
	// Use data from TestInteractionOLS_known_interaction (exact fit).
	data := []struct{ x1, x2, y float64 }{
		{1, 1, 10}, {1, 2, 17}, {2, 1, 16}, {2, 2, 27},
		{3, 1, 22}, {3, 2, 37}, {1, 3, 24}, {2, 3, 38},
	}
	n := len(data)
	X := make([][]float64, n)
	y := make([]float64, n)
	for i, d := range data {
		X[i] = []float64{1, d.x1, d.x2}
		y[i] = d.y
	}
	res, err := InteractionOLS(X, y, [][2]int{{1, 2}})
	if err != nil {
		t.Fatalf("InteractionOLS error: %v", err)
	}
	// Should have 4 betas (original 3 + 1 interaction)
	if len(res.Betas) != 4 {
		t.Errorf("expected 4 betas, got %d", len(res.Betas))
	}
}

func TestInteractionOLS_known_interaction(t *testing.T) {
	// y = 1 + 2*x1 + 3*x2 + 4*x1*x2 (exact)
	// Test that OLS with interaction recovers y with R² ≈ 1
	data := []struct{ x1, x2, y float64 }{
		{1, 1, 10},  // 1+2+3+4 = 10
		{1, 2, 17},  // 1+2+6+8 = 17
		{2, 1, 14},  // 1+4+3+8 = 16 ... wait let me be explicit
		{2, 2, 25},  // 1+4+6+16 = 27 ... let me recompute
		{3, 1, 22},  // 1+6+3+12 = 22
		{3, 2, 35},  // 1+6+6+24 = 37 ... let me just use exact values
		{1, 3, 24},  // 1+2+9+12 = 24
		{2, 3, 39},  // 1+4+9+24 = 38 ... let me recompute
	}
	// y = 1 + 2*x1 + 3*x2 + 4*x1*x2 exactly:
	// (1,1)=1+2+3+4=10; (1,2)=1+2+6+8=17; (2,1)=1+4+3+8=16; (2,2)=1+4+6+16=27
	// (3,1)=1+6+3+12=22; (3,2)=1+6+6+24=37; (1,3)=1+2+9+12=24; (2,3)=1+4+9+24=38
	data = []struct{ x1, x2, y float64 }{
		{1, 1, 10}, {1, 2, 17}, {2, 1, 16}, {2, 2, 27},
		{3, 1, 22}, {3, 2, 37}, {1, 3, 24}, {2, 3, 38},
	}
	n := len(data)
	X := make([][]float64, n)
	y := make([]float64, n)
	for i, d := range data {
		X[i] = []float64{1, d.x1, d.x2}
		y[i] = d.y
	}

	res, err := InteractionOLS(X, y, [][2]int{{1, 2}})
	if err != nil {
		t.Fatalf("InteractionOLS error: %v", err)
	}
	if res.RSquared < 0.999 {
		t.Errorf("expected R² ~ 1.0 for exact model, got %.4f", res.RSquared)
	}
}

func TestInteractionOLS_out_of_range_column(t *testing.T) {
	X := [][]float64{{1, 2}, {1, 3}, {1, 4}}
	y := []float64{1, 2, 3}
	_, err := InteractionOLS(X, y, [][2]int{{0, 5}})
	if err == nil {
		t.Error("expected error for out-of-range column index")
	}
}

func TestInteractionOLS_no_interactions(t *testing.T) {
	// With no interaction terms, should behave identically to plain OLS
	xs := []float64{1, 2, 3, 4, 5}
	ys := []float64{3, 5, 7, 9, 11}
	X := buildDesignMatrix(xs)

	resOLS, _ := OLS(X, ys)
	resInt, err := InteractionOLS(X, ys, nil)
	if err != nil {
		t.Fatalf("InteractionOLS error: %v", err)
	}
	for j := range resOLS.Betas {
		if math.Abs(resOLS.Betas[j]-resInt.Betas[j]) > 1e-9 {
			t.Errorf("betas[%d]: OLS=%.6f, InteractionOLS=%.6f", j, resOLS.Betas[j], resInt.Betas[j])
		}
	}
}
