package stats

import (
	"math"
	"testing"
)

func sampleMean(data []float64) float64 {
	s := 0.0
	for _, v := range data {
		s += v
	}
	return s / float64(len(data))
}

func TestBootstrap_CI_contains_true_mean(t *testing.T) {
	// Population mean = 5
	data := []float64{3, 4, 5, 5, 6, 7, 4, 5, 6, 5, 5, 4, 6, 5, 5}
	ci := Bootstrap(sampleMean, data, 2000, 0.05)

	trueMean := 5.0
	if ci.Lower > trueMean || ci.Upper < trueMean {
		t.Errorf("95%% CI [%.4f, %.4f] does not contain true mean %.1f", ci.Lower, ci.Upper, trueMean)
	}
	if ci.Lower >= ci.Upper {
		t.Errorf("CI lower %.4f >= upper %.4f", ci.Lower, ci.Upper)
	}
}

func TestBootstrap_point_estimate(t *testing.T) {
	data := []float64{1, 2, 3, 4, 5}
	ci := Bootstrap(sampleMean, data, 100, 0.05)
	if math.Abs(ci.PointEstimate-3.0) > 1e-9 {
		t.Errorf("point estimate: want 3.0, got %.6f", ci.PointEstimate)
	}
}

func TestBootstrap_empty_data(t *testing.T) {
	ci := Bootstrap(sampleMean, []float64{}, 100, 0.05)
	// Should return NaN or zero — just check it doesn't panic
	_ = ci
}

func TestBootstrap_zero_nboot(t *testing.T) {
	data := []float64{1, 2, 3}
	ci := Bootstrap(sampleMean, data, 0, 0.05)
	if ci.Lower != ci.PointEstimate || ci.Upper != ci.PointEstimate {
		t.Errorf("zero nBoot: expected lower=upper=point, got [%.4f, %.4f] pe=%.4f",
			ci.Lower, ci.Upper, ci.PointEstimate)
	}
}

func TestBootstrap_parallel_deterministic_range(t *testing.T) {
	// Multiple runs should produce CIs that contain the true value
	data := make([]float64, 50)
	for i := range data {
		data[i] = float64(i + 1)
	}
	trueMean := 25.5
	for run := 0; run < 3; run++ {
		ci := Bootstrap(sampleMean, data, 500, 0.05)
		if ci.Lower > trueMean || ci.Upper < trueMean {
			t.Errorf("run %d: CI [%.4f, %.4f] misses true mean %.1f", run, ci.Lower, ci.Upper, trueMean)
		}
	}
}
