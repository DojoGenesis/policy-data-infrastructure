package datasource

import (
	"testing"
)

// ---- TestNewCDCSVISource ------------------------------------------------------

func TestNewCDCSVISource_Defaults(t *testing.T) {
	s := NewCDCSVISource(CDCSVIConfig{Year: 2022})

	if s.Name() != "cdc-svi" {
		t.Errorf("Name(): want cdc-svi, got %q", s.Name())
	}
	if s.Category() != "health" {
		t.Errorf("Category(): want health, got %q", s.Category())
	}
	if s.Vintage() != "CDC-SVI-2022" {
		t.Errorf("Vintage(): want CDC-SVI-2022, got %q", s.Vintage())
	}
	if s.cfg.HTTPClient == nil {
		t.Error("HTTPClient must not be nil after NewCDCSVISource")
	}
}

func TestNewCDCSVISource_ZeroYear(t *testing.T) {
	s := NewCDCSVISource(CDCSVIConfig{})
	if s.Vintage() != "CDC-SVI" {
		t.Errorf("Vintage() with zero Year: want CDC-SVI, got %q", s.Vintage())
	}
}

func TestCDCSVISource_InterfaceSatisfied(t *testing.T) {
	// Compile-time check: cdcSVISource must satisfy DataSource.
	var _ DataSource = NewCDCSVISource(CDCSVIConfig{Year: 2022})
}

// ---- TestCDCSVISchema ---------------------------------------------------------

func TestCDCSVISchema_VariableCount(t *testing.T) {
	s := NewCDCSVISource(CDCSVIConfig{Year: 2022})
	schema := s.Schema()
	if len(schema) != 5 {
		t.Errorf("Schema() length: want 5, got %d", len(schema))
	}
}

func TestCDCSVISchema_RequiredFields(t *testing.T) {
	s := NewCDCSVISource(CDCSVIConfig{Year: 2022})
	for _, def := range s.Schema() {
		if def.ID == "" {
			t.Errorf("VariableDef missing ID: %+v", def)
		}
		if def.Name == "" {
			t.Errorf("VariableDef %q missing Name", def.ID)
		}
		if def.Unit == "" {
			t.Errorf("VariableDef %q missing Unit", def.ID)
		}
		if def.Direction == "" {
			t.Errorf("VariableDef %q missing Direction", def.ID)
		}
	}
}

func TestCDCSVISchema_ExpectedVariableIDs(t *testing.T) {
	s := NewCDCSVISource(CDCSVIConfig{Year: 2022})
	wantIDs := map[string]bool{
		"cdc_svi_overall":           false,
		"cdc_svi_socioeconomic":     false,
		"cdc_svi_household":         false,
		"cdc_svi_racial_ethnic":     false,
		"cdc_svi_housing_transport": false,
	}
	for _, def := range s.Schema() {
		if _, ok := wantIDs[def.ID]; ok {
			wantIDs[def.ID] = true
		}
	}
	for id, found := range wantIDs {
		if !found {
			t.Errorf("Schema() missing expected variable ID %q", id)
		}
	}
}

func TestCDCSVISchema_DirectionIsLowerBetter(t *testing.T) {
	s := NewCDCSVISource(CDCSVIConfig{Year: 2022})
	for _, def := range s.Schema() {
		if def.Direction != "lower_better" {
			t.Errorf("VariableDef %q: Direction expected lower_better, got %q", def.ID, def.Direction)
		}
	}
}

// ---- TestFetchReturnsNotImplemented -------------------------------------------

func TestCDCSVISource_FetchCounty_NotImplemented(t *testing.T) {
	s := NewCDCSVISource(CDCSVIConfig{Year: 2022})
	_, err := s.FetchCounty(nil, "55", "025")
	if err == nil {
		t.Error("FetchCounty should return error since bulk CSV is used instead")
	}
}

func TestCDCSVISource_FetchState_NotImplemented(t *testing.T) {
	s := NewCDCSVISource(CDCSVIConfig{Year: 2022})
	_, err := s.FetchState(nil, "55")
	if err == nil {
		t.Error("FetchState should return error since bulk CSV is used instead")
	}
}