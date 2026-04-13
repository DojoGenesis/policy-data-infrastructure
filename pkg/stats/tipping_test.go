package stats

import (
	"math"
	"testing"
)

func TestTippingPoint_obvious_breakpoint(t *testing.T) {
	// Left segment: y = x for x in [1..6] (slope 1)
	// Right segment: y = 6 + 5*(x-6) for x in [7..12] (slope 5)
	x := make([]float64, 12)
	y := make([]float64, 12)
	for i := 0; i < 6; i++ {
		x[i] = float64(i + 1)
		y[i] = float64(i + 1)
	}
	for i := 6; i < 12; i++ {
		x[i] = float64(i + 1)
		y[i] = 6 + 5*float64(i-5)
	}

	res, err := TippingPoint(x, y)
	if err != nil {
		t.Fatalf("TippingPoint error: %v", err)
	}

	// Threshold should be between 6 and 7 (the midpoint)
	if res.Threshold < 5.5 || res.Threshold > 8.0 {
		t.Errorf("threshold %.2f not near the breakpoint at 6.5", res.Threshold)
	}

	// Left slope should be close to 1, right slope close to 5
	if math.Abs(res.LeftSlope-1.0) > 0.5 {
		t.Errorf("left slope: want ~1.0, got %.4f", res.LeftSlope)
	}
	if math.Abs(res.RightSlope-5.0) > 1.0 {
		t.Errorf("right slope: want ~5.0, got %.4f", res.RightSlope)
	}
}

func TestTippingPoint_too_few(t *testing.T) {
	_, err := TippingPoint([]float64{1, 2, 3}, []float64{1, 2, 3})
	if err == nil {
		t.Error("expected error for fewer than 6 observations")
	}
}

func TestTippingPoint_length_mismatch(t *testing.T) {
	_, err := TippingPoint([]float64{1, 2, 3, 4, 5, 6}, []float64{1, 2, 3})
	if err == nil {
		t.Error("expected error for mismatched lengths")
	}
}

func TestTippingPoint_f_statistic_positive(t *testing.T) {
	x := make([]float64, 20)
	y := make([]float64, 20)
	for i := range x {
		x[i] = float64(i + 1)
		if i < 10 {
			y[i] = float64(i + 1)
		} else {
			y[i] = 10 + 10*float64(i-9)
		}
	}
	res, err := TippingPoint(x, y)
	if err != nil {
		t.Fatalf("TippingPoint error: %v", err)
	}
	if res.FStatistic < 0 {
		t.Errorf("F-statistic should be non-negative, got %.4f", res.FStatistic)
	}
}

func TestTippingPoint_returns_result(t *testing.T) {
	x := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	y := []float64{1, 2, 3, 4, 5, 15, 25, 35, 45, 55}
	res, err := TippingPoint(x, y)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result")
	}
}
