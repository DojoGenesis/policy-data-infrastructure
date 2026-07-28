package datasource

import "testing"

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

// TestFCCBroadbandFetch_Noop verifies FetchCounty and FetchState return nil, nil
// (the Go adapter delegates to the Python ingest path).
func TestFCCBroadbandFetch_Noop(t *testing.T) {
	s := NewFCCBroadbandSource(FCCBroadbandConfig{Year: 2024})

	indicators, err := s.FetchCounty(nil, "55", "025")
	if err != nil {
		t.Errorf("FetchCounty: unexpected error: %v", err)
	}
	if indicators != nil {
		t.Errorf("FetchCounty: want nil, got %d indicators", len(indicators))
	}

	indicators, err = s.FetchState(nil, "55")
	if err != nil {
		t.Errorf("FetchState: unexpected error: %v", err)
	}
	if indicators != nil {
		t.Errorf("FetchState: want nil, got %d indicators", len(indicators))
	}
}
