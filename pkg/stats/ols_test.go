package stats

import (
	"math"
	"testing"
)

// buildDesignMatrix builds an n×2 matrix [[1, x0], [1, x1], ...] for simple linear regression.
func buildDesignMatrix(xs []float64) [][]float64 {
	X := make([][]float64, len(xs))
	for i, x := range xs {
		X[i] = []float64{1, x}
	}
	return X
}

func TestOLS_simple_linear(t *testing.T) {
	// y = 1 + 2x (exact, no noise)
	xs := []float64{0, 1, 2, 3, 4, 5}
	ys := make([]float64, len(xs))
	for i, x := range xs {
		ys[i] = 1 + 2*x
	}
	X := buildDesignMatrix(xs)
	res, err := OLS(X, ys)
	if err != nil {
		t.Fatalf("OLS error: %v", err)
	}
	if math.Abs(res.Betas[0]-1.0) > 1e-8 {
		t.Errorf("intercept: want 1.0, got %.6f", res.Betas[0])
	}
	if math.Abs(res.Betas[1]-2.0) > 1e-8 {
		t.Errorf("slope: want 2.0, got %.6f", res.Betas[1])
	}
	if math.Abs(res.RSquared-1.0) > 1e-9 {
		t.Errorf("R²: want 1.0, got %.6f", res.RSquared)
	}
}

func TestOLS_betas_close_to_known(t *testing.T) {
	// y = 1 + 2x + small noise
	xs := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	noise := []float64{0.1, -0.1, 0.05, -0.05, 0.08, -0.08, 0.03, -0.03, 0.07, -0.07}
	ys := make([]float64, len(xs))
	for i, x := range xs {
		ys[i] = 1 + 2*x + noise[i]
	}
	X := buildDesignMatrix(xs)
	res, err := OLS(X, ys)
	if err != nil {
		t.Fatalf("OLS error: %v", err)
	}
	if math.Abs(res.Betas[0]-1.0) > 0.5 {
		t.Errorf("intercept far from 1.0: got %.4f", res.Betas[0])
	}
	if math.Abs(res.Betas[1]-2.0) > 0.1 {
		t.Errorf("slope far from 2.0: got %.4f", res.Betas[1])
	}
	if res.RSquared < 0.99 {
		t.Errorf("R² too low: %.4f", res.RSquared)
	}
}

func TestOLS_residuals_length(t *testing.T) {
	xs := []float64{1, 2, 3, 4, 5}
	ys := []float64{2, 4, 5, 4, 5}
	X := buildDesignMatrix(xs)
	res, err := OLS(X, ys)
	if err != nil {
		t.Fatalf("OLS error: %v", err)
	}
	if len(res.Residuals) != 5 {
		t.Errorf("expected 5 residuals, got %d", len(res.Residuals))
	}
	if len(res.Predictions) != 5 {
		t.Errorf("expected 5 predictions, got %d", len(res.Predictions))
	}
}

func TestOLS_std_errors_positive(t *testing.T) {
	xs := []float64{1, 2, 3, 4, 5, 6, 7}
	ys := []float64{2.1, 3.9, 6.2, 7.8, 10.1, 12.0, 14.2}
	X := buildDesignMatrix(xs)
	res, err := OLS(X, ys)
	if err != nil {
		t.Fatalf("OLS error: %v", err)
	}
	for j, se := range res.StdErrors {
		if se < 0 {
			t.Errorf("StdErrors[%d] < 0: %.6f", j, se)
		}
	}
}

func TestOLS_p_values_range(t *testing.T) {
	xs := []float64{1, 2, 3, 4, 5, 6, 7}
	ys := []float64{2.1, 3.9, 6.2, 7.8, 10.1, 12.0, 14.2}
	X := buildDesignMatrix(xs)
	res, err := OLS(X, ys)
	if err != nil {
		t.Fatalf("OLS error: %v", err)
	}
	for j, p := range res.PValues {
		if p < 0 || p > 1 {
			t.Errorf("PValues[%d] = %.6f out of [0,1]", j, p)
		}
	}
}

func TestOLS_too_few_observations(t *testing.T) {
	X := [][]float64{{1, 1}, {1, 2}}
	y := []float64{1, 2}
	_, err := OLS(X, y)
	if err == nil {
		t.Error("expected error for n <= p")
	}
}

func TestOLS_singular(t *testing.T) {
	// X has two identical columns → X'X is singular
	X := [][]float64{{1, 2, 2}, {1, 3, 3}, {1, 4, 4}, {1, 5, 5}}
	y := []float64{1, 2, 3, 4}
	_, err := OLS(X, y)
	if err == nil {
		t.Error("expected error for singular X'X")
	}
}
