package stats

import (
	"math"
	"testing"
)

func TestCompositeIndex_equal_percentile(t *testing.T) {
	// Two indicators, 4 tracts
	ind1 := []*float64{pf(10), pf(20), pf(30), pf(40)}
	ind2 := []*float64{pf(100), pf(200), pf(300), pf(400)}
	result, err := CompositeIndex([][]*float64{ind1, ind2}, nil, "equal_percentile")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 4 {
		t.Fatalf("expected 4 results, got %d", len(result))
	}
	// Both indicators have ranks [0, 1/3, 2/3, 1] → average = same
	// First tract: rank=0 for both → composite=0; last: rank=1 → composite=1
	if result[0] == nil || math.Abs(*result[0]-0.0) > 1e-9 {
		t.Errorf("tract 0: want 0.0, got %v", result[0])
	}
	if result[3] == nil || math.Abs(*result[3]-1.0) > 1e-9 {
		t.Errorf("tract 3: want 1.0, got %v", result[3])
	}
}

func TestCompositeIndex_weighted_zscore(t *testing.T) {
	// Single indicator, weight=1 → result == z-score
	ind := []*float64{pf(1), pf(2), pf(3), pf(4), pf(5)}
	result, err := CompositeIndex([][]*float64{ind}, []float64{1.0}, "weighted_zscore")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	zs := ZScore(ind)
	for i := range result {
		if result[i] == nil || zs[i] == nil {
			t.Fatalf("nil at index %d", i)
		}
		if math.Abs(*result[i]-*zs[i]) > 1e-9 {
			t.Errorf("index %d: composite=%.6f, zscore=%.6f", i, *result[i], *zs[i])
		}
	}
}

func TestCompositeIndex_nil_handling(t *testing.T) {
	ind1 := []*float64{pf(1), nil, pf(3)}
	ind2 := []*float64{nil, nil, pf(9)}
	result, err := CompositeIndex([][]*float64{ind1, ind2}, nil, "equal_percentile")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// index 1: both nil → result should be nil
	if result[1] != nil {
		t.Errorf("expected nil at index 1, got %.4f", *result[1])
	}
}

func TestCompositeIndex_bad_method(t *testing.T) {
	ind := []*float64{pf(1), pf(2)}
	_, err := CompositeIndex([][]*float64{ind}, nil, "bogus")
	if err == nil {
		t.Error("expected error for unknown method")
	}
}

func TestCompositeIndex_weight_mismatch(t *testing.T) {
	ind1 := []*float64{pf(1), pf(2)}
	ind2 := []*float64{pf(3), pf(4)}
	_, err := CompositeIndex([][]*float64{ind1, ind2}, []float64{1.0}, "weighted_zscore")
	if err == nil {
		t.Error("expected error for weight length mismatch")
	}
}

func TestAssignTiers_basic(t *testing.T) {
	scores := []*float64{pf(10), pf(20), pf(30), pf(40), pf(50)}
	tiers := []TierDef{
		{Name: "low", MinPercentile: 0.0, MaxPercentile: 0.4},
		{Name: "mid", MinPercentile: 0.4, MaxPercentile: 0.8},
		{Name: "high", MinPercentile: 0.8, MaxPercentile: 1.0},
	}
	result := AssignTiers(scores, tiers)
	if len(result) != 5 {
		t.Fatalf("expected 5 results, got %d", len(result))
	}
	// scores rank 0→0.25→0.5→0.75→1.0
	// 0 and 0.25 → low; 0.5 and 0.75 → mid; 1.0 → high
	if result[0] != "low" {
		t.Errorf("index 0: want low, got %s", result[0])
	}
	if result[4] != "high" {
		t.Errorf("index 4: want high, got %s", result[4])
	}
}

func TestAssignTiers_nil_score(t *testing.T) {
	scores := []*float64{nil, pf(10)}
	tiers := []TierDef{{Name: "all", MinPercentile: 0.0, MaxPercentile: 1.0}}
	result := AssignTiers(scores, tiers)
	if result[0] != "unknown" {
		t.Errorf("nil score should yield 'unknown', got %s", result[0])
	}
}
