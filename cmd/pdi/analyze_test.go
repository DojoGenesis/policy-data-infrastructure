package main

import (
	"context"
	"strings"
	"testing"
)

// The two silent-default defects fixed per the 2026-08-02 handoff (ADR-014 F5):
// composite --weights silently zeroed unlisted variables, and ols took its
// outcome from alphabetical sort order. These tests pin the loud behavior.

func TestCompositeWeightsMustCoverEveryScopeVariable(t *testing.T) {
	varIDs := []string{"cdc_obesity", "poverty_rate", "uninsured_rate"}
	geoids := []string{"55025000100"}
	matrix := [][]*float64{{nil}, {nil}, {nil}}

	// Missing weights must error, naming the uncovered variables.
	_, err := runCompositeAnalysis(context.Background(), nil, "55025", "county",
		varIDs, geoids, matrix, "poverty_rate:0.5", "ACS-2024-5yr")
	if err == nil {
		t.Fatal("expected error for weights covering 1 of 3 variables, got nil")
	}
	for _, want := range []string{"cdc_obesity", "uninsured_rate"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should name uncovered variable %q; got: %v", want, err)
		}
	}
}

func TestCompositeWeightsRejectUnknownVariables(t *testing.T) {
	varIDs := []string{"poverty_rate"}
	geoids := []string{"55025000100"}
	matrix := [][]*float64{{nil}}

	_, err := runCompositeAnalysis(context.Background(), nil, "55025", "county",
		varIDs, geoids, matrix, "poverty_rate:0.5,not_a_variable:0.5", "ACS-2024-5yr")
	if err == nil {
		t.Fatal("expected error for weight naming an out-of-scope variable, got nil")
	}
	if !strings.Contains(err.Error(), "not_a_variable") {
		t.Errorf("error should name the unknown variable; got: %v", err)
	}
}

func TestParseWeightsRejectsMalformedEntries(t *testing.T) {
	if _, err := parseWeights("poverty_rate"); err == nil {
		t.Error("expected error for entry with no colon")
	}
	if _, err := parseWeights("poverty_rate:abc"); err == nil {
		t.Error("expected error for non-numeric weight")
	}
	m, err := parseWeights("a:0.5, b:0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m["a"] != 0.5 || m["b"] != 0 {
		t.Errorf("parsed weights wrong: %#v", m)
	}
}
