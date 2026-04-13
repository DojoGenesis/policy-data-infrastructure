package stats

import (
	"math"
	"testing"
)

func pf(v float64) *float64 { return &v }

func TestZScore_basic(t *testing.T) {
	vals := []*float64{pf(2), pf(4), pf(4), pf(4), pf(5), pf(5), pf(7), pf(9)}
	got := ZScore(vals)
	if len(got) != len(vals) {
		t.Fatalf("length mismatch: want %d, got %d", len(vals), len(got))
	}
	// mean of z-scores should be ~0
	sum := 0.0
	for _, v := range got {
		if v == nil {
			t.Fatal("unexpected nil in output")
		}
		sum += *v
	}
	if math.Abs(sum) > 1e-9 {
		t.Errorf("mean of z-scores should be 0, got %.6f", sum)
	}
}

func TestZScore_nil_passthrough(t *testing.T) {
	vals := []*float64{pf(1), nil, pf(3)}
	got := ZScore(vals)
	if got[1] != nil {
		t.Errorf("expected nil at index 1, got %v", *got[1])
	}
	if got[0] == nil || got[2] == nil {
		t.Error("expected non-nil values at index 0 and 2")
	}
}

func TestZScore_fewer_than_two_non_nil(t *testing.T) {
	v := 42.0
	vals := []*float64{&v, nil, nil}
	got := ZScore(vals)
	// should return copy unchanged
	if got[0] == nil || *got[0] != 42.0 {
		t.Errorf("expected 42.0, got %v", got[0])
	}
	if got[1] != nil || got[2] != nil {
		t.Error("expected nil at indices 1 and 2")
	}
}

func TestZScore_all_same_value(t *testing.T) {
	// all same → variance=0 → sd forced to 1.0 → all z-scores = 0
	vals := []*float64{pf(5), pf(5), pf(5)}
	got := ZScore(vals)
	for i, v := range got {
		if v == nil {
			t.Fatalf("unexpected nil at index %d", i)
		}
		if math.Abs(*v) > 1e-12 {
			t.Errorf("index %d: expected 0, got %.6f", i, *v)
		}
	}
}

func TestPercentileRank_basic(t *testing.T) {
	vals := []*float64{pf(10), pf(20), pf(30)}
	got := PercentileRank(vals)
	// expect 0, 0.5, 1.0
	expected := []float64{0, 0.5, 1.0}
	for i, v := range got {
		if v == nil {
			t.Fatalf("unexpected nil at index %d", i)
		}
		if math.Abs(*v-expected[i]) > 1e-9 {
			t.Errorf("index %d: want %.3f, got %.3f", i, expected[i], *v)
		}
	}
}

func TestPercentileRank_nil_passthrough(t *testing.T) {
	vals := []*float64{pf(1), nil, pf(3)}
	got := PercentileRank(vals)
	if got[1] != nil {
		t.Errorf("expected nil at index 1, got %v", *got[1])
	}
}

func TestPercentileRank_single_value(t *testing.T) {
	vals := []*float64{nil, pf(7), nil}
	got := PercentileRank(vals)
	if got[1] == nil {
		t.Fatal("expected non-nil at index 1")
	}
	if *got[1] != 0.0 {
		t.Errorf("single value should have rank 0.0, got %.4f", *got[1])
	}
}

func TestPercentileRank_order(t *testing.T) {
	vals := []*float64{pf(30), pf(10), pf(20)}
	got := PercentileRank(vals)
	// sorted order: 10, 20, 30 → ranks 0, 0.5, 1.0 at original indices 1, 2, 0
	if math.Abs(*got[0]-1.0) > 1e-9 {
		t.Errorf("index 0 (val=30): want 1.0, got %.4f", *got[0])
	}
	if math.Abs(*got[1]-0.0) > 1e-9 {
		t.Errorf("index 1 (val=10): want 0.0, got %.4f", *got[1])
	}
	if math.Abs(*got[2]-0.5) > 1e-9 {
		t.Errorf("index 2 (val=20): want 0.5, got %.4f", *got[2])
	}
}
