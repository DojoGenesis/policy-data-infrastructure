package datasource

import (
	"strings"
	"testing"
)

// TestFCCBroadbandNewSource validates the adapter's identity metadata.
func TestFCCBroadbandNewSource(t *testing.T) {
	s := NewFCCBroadbandSource(FCCBroadbandConfig{Year: 2024})
	if s.Name() != "fcc-broadband" {
		t.Errorf("Name(): want fcc-broadband, got %q", s.Name())
	}
	if s.Category() != "infrastructure" {
		t.Errorf("Category(): want infrastructure, got %q", s.Category())
	}
	if s.Vintage() != "FCC-BROADBAND-2024" {
		t.Errorf("Vintage(): want FCC-BROADBAND-2024, got %q", s.Vintage())
	}
}

// TestFCCBroadbandNewSource_DefaultYear validates defaults when Year is zero.
func TestFCCBroadbandNewSource_DefaultYear(t *testing.T) {
	s := NewFCCBroadbandSource(FCCBroadbandConfig{})
	if s.Vintage() != "FCC-BROADBAND-2024" {
		t.Errorf("Vintage() default year: want FCC-BROADBAND-2024, got %q", s.Vintage())
	}
}

// TestFCCBroadbandSchema verifies Schema() returns expected variable definitions.
func TestFCCBroadbandSchema(t *testing.T) {
	s := NewFCCBroadbandSource(FCCBroadbandConfig{Year: 2024})
	schema := s.Schema()

	expectedIDs := []string{
		"fcc_broadband_access_pct",
		"fcc_multiple_providers_pct",
	}

	if len(schema) != len(expectedIDs) {
		t.Errorf("Schema() length: want %d, got %d", len(expectedIDs), len(schema))
	}

	byID := make(map[string]VariableDef)
	for _, def := range schema {
		byID[def.ID] = def
	}

	for _, id := range expectedIDs {
		def, ok := byID[id]
		if !ok {
			t.Errorf("Schema() missing variable %q", id)
			continue
		}
		if def.Name == "" {
			t.Errorf("VariableDef %q has empty Name", id)
		}
		if def.Description == "" {
			t.Errorf("VariableDef %q has empty Description", id)
		}
		if def.Unit == "" {
			t.Errorf("VariableDef %q has empty Unit", id)
		}
		if def.Direction == "" {
			t.Errorf("VariableDef %q has empty Direction", id)
		}
	}

	// Verify expected directions.
	dirChecks := map[string]string{
		"fcc_broadband_access_pct":   "higher_better",
		"fcc_multiple_providers_pct": "higher_better",
	}
	for id, want := range dirChecks {
		if def, ok := byID[id]; ok && def.Direction != want {
			t.Errorf("%s Direction: want %q, got %q", id, want, def.Direction)
		}
	}
}

// TestFCCBroadbandInterface verifies FCCBroadbandSource satisfies DataSource at compile time.
func TestFCCBroadbandInterface(t *testing.T) {
	var _ DataSource = NewFCCBroadbandSource(FCCBroadbandConfig{Year: 2024})
}

// TestFCCBroadbandFetchCounty_NotImplemented verifies FetchCounty returns an
// explicit HTTP 501 not-implemented error and a nil slice, rather than the
// silent nil, nil success that let a source which cannot load look
// identical to a source that legitimately found no rows.
func TestFCCBroadbandFetchCounty_NotImplemented(t *testing.T) {
	s := NewFCCBroadbandSource(FCCBroadbandConfig{Year: 2024})

	indicators, err := s.FetchCounty(nil, "55", "025")
	if err == nil {
		t.Fatal("FetchCounty: want error (HTTP 501 not implemented), got nil")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("FetchCounty: error should say %q, got: %v", "not implemented", err)
	}
	if indicators != nil {
		t.Errorf("FetchCounty: want nil slice, got %d indicators", len(indicators))
	}
}

// TestFCCBroadbandFetchState_NotImplemented verifies FetchState returns an
// explicit HTTP 501 not-implemented error and a nil slice. Same rationale as
// TestFCCBroadbandFetchCounty_NotImplemented.
func TestFCCBroadbandFetchState_NotImplemented(t *testing.T) {
	s := NewFCCBroadbandSource(FCCBroadbandConfig{Year: 2024})

	indicators, err := s.FetchState(nil, "55")
	if err == nil {
		t.Fatal("FetchState: want error (HTTP 501 not implemented), got nil")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("FetchState: error should say %q, got: %v", "not implemented", err)
	}
	if indicators != nil {
		t.Errorf("FetchState: want nil slice, got %d indicators", len(indicators))
	}
}
