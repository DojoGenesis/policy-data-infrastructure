package stats

import (
	"math"
	"testing"
)

// ── ComputeWeightedComposite tests ──────────────────────────────────────────

func TestComputeWeightedComposite_Normal(t *testing.T) {
	// Two variables, equal weights, all values present.
	values := map[string]*float64{
		"var_a": pf(4.0),
		"var_b": pf(9.0),
	}
	weights := map[string]float64{
		"var_a": 0.5,
		"var_b": 0.5,
	}

	result, err := ComputeWeightedComposite(values, weights)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Geometric mean: exp(0.5*ln(4) + 0.5*ln(9)) = exp(ln(2) + ln(3)) = 6
	if math.Abs(*result-6.0) > 1e-9 {
		t.Errorf("expected 6.0, got %.9f", *result)
	}
}

func TestComputeWeightedComposite_NilValues(t *testing.T) {
	// One variable has a nil value — should be skipped.
	values := map[string]*float64{
		"var_a": pf(4.0),
		"var_b": nil,
	}
	weights := map[string]float64{
		"var_a": 0.5,
		"var_b": 0.5,
	}

	result, err := ComputeWeightedComposite(values, weights)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Only var_a contributes: exp(0.5*ln(4) / 0.5) = 4.0
	if math.Abs(*result-4.0) > 1e-9 {
		t.Errorf("expected 4.0, got %.9f", *result)
	}
}

func TestComputeWeightedComposite_EmptyIntersection(t *testing.T) {
	// Values has no keys that match weights.
	values := map[string]*float64{
		"var_c": pf(10.0),
	}
	weights := map[string]float64{
		"var_a": 0.5,
		"var_b": 0.5,
	}

	result, err := ComputeWeightedComposite(values, weights)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result for empty intersection, got %.9f", *result)
	}
}

func TestComputeWeightedComposite_EmptyWeights(t *testing.T) {
	values := map[string]*float64{
		"var_a": pf(4.0),
	}
	weights := map[string]float64{}

	result, err := ComputeWeightedComposite(values, weights)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result for empty weights, got %.9f", *result)
	}
}

func TestComputeWeightedComposite_ZeroValue(t *testing.T) {
	// Geometric mean requires positive values — zero should error.
	values := map[string]*float64{
		"var_a": pf(0.0),
	}
	weights := map[string]float64{
		"var_a": 1.0,
	}

	_, err := ComputeWeightedComposite(values, weights)
	if err == nil {
		t.Error("expected error for zero value")
	}
}

func TestComputeWeightedComposite_NegativeValue(t *testing.T) {
	values := map[string]*float64{
		"var_a": pf(-1.0),
	}
	weights := map[string]float64{
		"var_a": 1.0,
	}

	_, err := ComputeWeightedComposite(values, weights)
	if err == nil {
		t.Error("expected error for negative value")
	}
}

func TestComputeWeightedComposite_SingleVariable(t *testing.T) {
	values := map[string]*float64{
		"var_a": pf(42.0),
	}
	weights := map[string]float64{
		"var_a": 1.0,
	}

	result, err := ComputeWeightedComposite(values, weights)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if math.Abs(*result-42.0) > 1e-9 {
		t.Errorf("expected 42.0, got %.9f", *result)
	}
}

func TestComputeWeightedComposite_SingleVariableNonUnitWeight(t *testing.T) {
	// Weight is not 1.0 — should still work since weights get normalised.
	values := map[string]*float64{
		"var_a": pf(100.0),
	}
	weights := map[string]float64{
		"var_a": 7.0, // non-unit weight
	}

	result, err := ComputeWeightedComposite(values, weights)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	// exp(7*ln(100) / 7) = 100
	if math.Abs(*result-100.0) > 1e-9 {
		t.Errorf("expected 100.0, got %.9f", *result)
	}
}

func TestComputeWeightedComposite_UneqWeightSums(t *testing.T) {
	// Weights don't sum to 1 — function uses weightSum normalisation.
	values := map[string]*float64{
		"var_a": pf(2.0),
		"var_b": pf(8.0),
	}
	weights := map[string]float64{
		"var_a": 3.0,
		"var_b": 1.0, // total sum = 4, not 1
	}

	result, err := ComputeWeightedComposite(values, weights)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// exp((3*ln(2) + 1*ln(8)) / 4) = exp((3*0.693147... + 2.07944...) / 4)
	// = exp((2.07944 + 2.07944) / 4) = exp(4.15888 / 4) = exp(1.03972) ≈ 2.82843
	expected := math.Pow(2.0, 3.0/4.0) * math.Pow(8.0, 1.0/4.0)
	if math.Abs(*result-expected) > 1e-9 {
		t.Errorf("expected %.9f, got %.9f", expected, *result)
	}
}

func TestComputeWeightedComposite_AllNilValues(t *testing.T) {
	values := map[string]*float64{
		"var_a": nil,
		"var_b": nil,
	}
	weights := map[string]float64{
		"var_a": 0.5,
		"var_b": 0.5,
	}

	result, err := ComputeWeightedComposite(values, weights)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result when all values are nil, got %.9f", *result)
	}
}

func TestComputeWeightedComposite_KeyNotInValues(t *testing.T) {
	// Weight references a key not present in values map.
	values := map[string]*float64{
		"var_a": pf(5.0),
	}
	weights := map[string]float64{
		"var_a": 0.5,
		"var_b": 0.5,
	}

	result, err := ComputeWeightedComposite(values, weights)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	// Only var_a contributes: weightSum = 0.5
	// exp(0.5*ln(5) / 0.5) = 5.0
	if math.Abs(*result-5.0) > 1e-9 {
		t.Errorf("expected 5.0, got %.9f", *result)
	}
}

// ── CompositeSensitivity tests ──────────────────────────────────────────────

func TestCompositeSensitivity_Normal(t *testing.T) {
	inputs := []CompositeInput{
		{
			GEOID: "55025",
			Values: map[string]*float64{
				"var_a": pf(10.0),
				"var_b": pf(20.0),
			},
		},
		{
			GEOID: "55079",
			Values: map[string]*float64{
				"var_a": pf(3.0),
				"var_b": pf(7.0),
			},
		},
	}
	weights := map[string]float64{
		"var_a": 0.5,
		"var_b": 0.5,
	}

	result, err := CompositeSensitivity(inputs, weights, 0.20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Check base scores.
	if len(result.BaseScores) != 2 {
		t.Fatalf("expected 2 base scores, got %d", len(result.BaseScores))
	}
	if result.BaseScores[0].GEOID != "55025" {
		t.Errorf("expected GEOID 55025, got %q", result.BaseScores[0].GEOID)
	}
	if result.BaseScores[0].Score == nil {
		t.Error("expected non-nil base score for 55025")
	}

	// Check contributing/missing variables.
	if len(result.BaseScores[0].ContribVars) != 2 {
		t.Errorf("expected 2 contributing vars, got %d", len(result.BaseScores[0].ContribVars))
	}
	if len(result.BaseScores[0].MissingVars) != 0 {
		t.Errorf("expected 0 missing vars, got %d", len(result.BaseScores[0].MissingVars))
	}

	// Perturbation should be preserved.
	if math.Abs(result.Perturbation-0.20) > 1e-9 {
		t.Errorf("expected perturbation 0.20, got %.9f", result.Perturbation)
	}

	// Should have scenarios: 2 variables × 2 directions = 4.
	if len(result.Scenarios) != 4 {
		t.Errorf("expected 4 scenarios, got %d", len(result.Scenarios))
	}

	// Stability should be computed for both GEOIDs.
	if len(result.Stability) != 2 {
		t.Errorf("expected 2 stability entries, got %d", len(result.Stability))
	}
	for _, geoid := range []string{"55025", "55079"} {
		s, ok := result.Stability[geoid]
		if !ok {
			t.Errorf("missing stability for GEOID %q", geoid)
		}
		if s < 0 || s > 1 {
			t.Errorf("stability for %q out of range: %.4f", geoid, s)
		}
	}
}

func TestCompositeSensitivity_EmptyInputs(t *testing.T) {
	weights := map[string]float64{"var_a": 1.0}
	_, err := CompositeSensitivity(nil, weights, 0.20)
	if err == nil {
		t.Error("expected error for nil geoid inputs")
	}
}

func TestCompositeSensitivity_EmptyWeights(t *testing.T) {
	inputs := []CompositeInput{
		{GEOID: "55025", Values: map[string]*float64{"var_a": pf(1.0)}},
	}
	_, err := CompositeSensitivity(inputs, map[string]float64{}, 0.20)
	if err == nil {
		t.Error("expected error for empty weights")
	}
}

func TestCompositeSensitivity_InvalidPerturbation(t *testing.T) {
	inputs := []CompositeInput{
		{GEOID: "55025", Values: map[string]*float64{"var_a": pf(1.0)}},
	}
	weights := map[string]float64{"var_a": 1.0}

	// Zero perturbation.
	_, err := CompositeSensitivity(inputs, weights, 0)
	if err == nil {
		t.Error("expected error for zero perturbation")
	}

	// Negative perturbation.
	_, err = CompositeSensitivity(inputs, weights, -0.1)
	if err == nil {
		t.Error("expected error for negative perturbation")
	}

	// Perturbation >= 1.
	_, err = CompositeSensitivity(inputs, weights, 1.0)
	if err == nil {
		t.Error("expected error for perturbation >= 1")
	}

	_, err = CompositeSensitivity(inputs, weights, 1.5)
	if err == nil {
		t.Error("expected error for perturbation > 1")
	}
}

func TestCompositeSensitivity_NilScoresGraceful(t *testing.T) {
	// When all values for a GEOID are nil, ComputeWeightedComposite returns nil.
	// Sensitivity should handle this.
	inputs := []CompositeInput{
		{
			GEOID: "nil_geoid",
			Values: map[string]*float64{
				"var_a": nil,
				"var_b": nil,
			},
		},
		{
			GEOID: "ok_geoid",
			Values: map[string]*float64{
				"var_a": pf(5.0),
				"var_b": pf(10.0),
			},
		},
	}
	weights := map[string]float64{
		"var_a": 0.5,
		"var_b": 0.5,
	}

	result, err := CompositeSensitivity(inputs, weights, 0.10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// nil_geoid should have nil score.
	for _, cs := range result.BaseScores {
		if cs.GEOID == "nil_geoid" {
			if cs.Score != nil {
				t.Errorf("expected nil score for nil_geoid, got %.9f", *cs.Score)
			}
			if len(cs.MissingVars) != 2 {
				t.Errorf("expected 2 missing vars for nil_geoid, got %d", len(cs.MissingVars))
			}
		}
		if cs.GEOID == "ok_geoid" {
			if cs.Score == nil {
				t.Error("expected non-nil score for ok_geoid")
			}
		}
	}
}

func TestCompositeSensitivity_SingleGeography(t *testing.T) {
	inputs := []CompositeInput{
		{
			GEOID: "55025",
			Values: map[string]*float64{
				"var_a": pf(10.0),
				"var_b": pf(20.0),
			},
		},
	}
	weights := map[string]float64{
		"var_a": 0.5,
		"var_b": 0.5,
	}

	result, err := CompositeSensitivity(inputs, weights, 0.20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.BaseScores) != 1 {
		t.Errorf("expected 1 base score, got %d", len(result.BaseScores))
	}
	if len(result.Stability) != 1 {
		t.Errorf("expected 1 stability entry, got %d", len(result.Stability))
	}
}

func TestCompositeSensitivity_ScenarioDirections(t *testing.T) {
	inputs := []CompositeInput{
		{
			GEOID: "55025",
			Values: map[string]*float64{
				"var_a": pf(10.0),
				"var_b": pf(20.0),
			},
		},
	}
	weights := map[string]float64{
		"var_a": 0.5,
		"var_b": 0.5,
	}

	result, err := CompositeSensitivity(inputs, weights, 0.20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify we have both directions for each variable.
	directions := map[string]map[string]bool{
		"var_a": {"up": false, "down": false},
		"var_b": {"up": false, "down": false},
	}
	for _, sc := range result.Scenarios {
		directions[sc.PerturbedVar][sc.Direction] = true
		if len(sc.Scores) != 1 {
			t.Errorf("expected 1 score per scenario, got %d", len(sc.Scores))
		}
	}
	for varID, dirs := range directions {
		for dir, found := range dirs {
			if !found {
				t.Errorf("missing scenario: %q %s", varID, dir)
			}
		}
	}
}

func TestCompositeSensitivity_UnequalWeights(t *testing.T) {
	inputs := []CompositeInput{
		{
			GEOID: "55025",
			Values: map[string]*float64{
				"var_a": pf(10.0),
				"var_b": pf(20.0),
				"var_c": pf(5.0),
			},
		},
	}
	// Weights not summing to 1.
	weights := map[string]float64{
		"var_a": 3.0,
		"var_b": 2.0,
		"var_c": 1.0,
	}

	result, err := CompositeSensitivity(inputs, weights, 0.15)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Scenarios) != 6 { // 3 variables × 2 directions
		t.Errorf("expected 6 scenarios, got %d", len(result.Scenarios))
	}
	if result.BaseScores[0].Score == nil {
		t.Error("expected non-nil base score")
	}
}

func TestCompositeSensitivity_NegativeValueErrors(t *testing.T) {
	inputs := []CompositeInput{
		{
			GEOID: "55025",
			Values: map[string]*float64{
				"var_a": pf(-1.0),
			},
		},
	}
	weights := map[string]float64{"var_a": 1.0}

	_, err := CompositeSensitivity(inputs, weights, 0.20)
	if err == nil {
		t.Error("expected error for negative value in sensitivity analysis")
	}
}
