package datasource

import "testing"

// TestNewFBINIBRSSource validates the adapter's identity metadata and defaults.
func TestNewFBINIBRSSource(t *testing.T) {
	s := NewFBINIBRSSource(FBINIBRSConfig{Year: 2023})

	if s.Name() != "fbi-nibrs" {
		t.Errorf("Name(): want fbi-nibrs, got %q", s.Name())
	}
	if s.Category() != "crime" {
		t.Errorf("Category(): want crime, got %q", s.Category())
	}
	if s.Vintage() != "FBI-NIBRS-2023" {
		t.Errorf("Vintage(): want FBI-NIBRS-2023, got %q", s.Vintage())
	}
}

// TestNewFBINIBRSSource_NoYear validates vintage without a year.
func TestNewFBINIBRSSource_NoYear(t *testing.T) {
	s := NewFBINIBRSSource(FBINIBRSConfig{})
	if s.Vintage() != "FBI-NIBRS" {
		t.Errorf("Vintage() without year: want FBI-NIBRS, got %q", s.Vintage())
	}
}

// TestFBINIBRSSchema verifies Schema() returns both expected variable definitions.
func TestFBINIBRSSchema(t *testing.T) {
	s := NewFBINIBRSSource(FBINIBRSConfig{Year: 2023})
	schema := s.Schema()

	expectedIDs := []string{
		"fbi_violent_crime_rate",
		"fbi_property_crime_rate",
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
		"fbi_violent_crime_rate":  "lower_better",
		"fbi_property_crime_rate": "lower_better",
	}
	for id, want := range dirChecks {
		if def, ok := byID[id]; ok && def.Direction != want {
			t.Errorf("%s Direction: want %q, got %q", id, want, def.Direction)
		}
	}
}

// TestFBINIBRSInterface verifies FBINIBRSSource satisfies DataSource at compile time.
func TestFBINIBRSInterface(t *testing.T) {
	var _ DataSource = NewFBINIBRSSource(FBINIBRSConfig{Year: 2023})
}

// TestFBINIBRSFetchCounty_BadFIPS verifies FIPS validation errors.
func TestFBINIBRSFetchCounty_BadFIPS(t *testing.T) {
	s := NewFBINIBRSSource(FBINIBRSConfig{Year: 2023})

	// Bad state FIPS (1 digit).
	_, err := s.FetchCounty(t.Context(), "5", "025")
	if err == nil {
		t.Error("FetchCounty bad state FIPS: want error, got nil")
	}

	// Bad county FIPS (2 digits).
	_, err = s.FetchCounty(t.Context(), "55", "25")
	if err == nil {
		t.Error("FetchCounty bad county FIPS: want error, got nil")
	}
}

// TestFBINIBRSRowToIndicators tests the row-to-indicator conversion.
func TestFBINIBRSRowToIndicators(t *testing.T) {
	s := NewFBINIBRSSource(FBINIBRSConfig{Year: 2023})

	t.Run("valid row", func(t *testing.T) {
		row := fbiNIBRSCountyRow{
			CountyFIPS:        "55025",
			CountyName:        "Dane County",
			StateAbbr:         "WI",
			Year:              2023,
			ViolentCrimeRate:  "280.5",
			PropertyCrimeRate: "1850.3",
			Population:        "561504",
		}
		indicators := s.rowToIndicators(row)
		if len(indicators) != 2 {
			t.Fatalf("want 2 indicators, got %d", len(indicators))
		}

		byVar := make(map[string]*float64)
		for _, ind := range indicators {
			if ind.GEOID != "55025" {
				t.Errorf("GEOID: want 55025, got %q", ind.GEOID)
			}
			if ind.Vintage != "FBI-NIBRS-2023" {
				t.Errorf("Vintage: want FBI-NIBRS-2023, got %q", ind.Vintage)
			}
			byVar[ind.VariableID] = ind.Value
		}

		if v := byVar["fbi_violent_crime_rate"]; v == nil || *v != 280.5 {
			t.Errorf("violent_crime_rate: want 280.5, got %v", v)
		}
		if v := byVar["fbi_property_crime_rate"]; v == nil || *v != 1850.3 {
			t.Errorf("property_crime_rate: want 1850.3, got %v", v)
		}
	})

	t.Run("suppressed values (-1)", func(t *testing.T) {
		row := fbiNIBRSCountyRow{
			CountyFIPS:        "55003",
			CountyName:        "Ashland County",
			StateAbbr:         "WI",
			Year:              2023,
			ViolentCrimeRate:  "-1",
			PropertyCrimeRate: "950.0",
			Population:        "15800",
		}
		indicators := s.rowToIndicators(row)
		if len(indicators) != 2 {
			t.Fatalf("want 2 indicators, got %d", len(indicators))
		}

		for _, ind := range indicators {
			if ind.VariableID == "fbi_violent_crime_rate" {
				if ind.Value != nil {
					t.Errorf("violent_crime_rate: want nil (suppressed), got %v", *ind.Value)
				}
			}
			if ind.VariableID == "fbi_property_crime_rate" {
				if ind.Value == nil || *ind.Value != 950.0 {
					t.Errorf("property_crime_rate: want 950.0, got %v", ind.Value)
				}
			}
		}
	})

	t.Run("suppressed values (null string)", func(t *testing.T) {
		row := fbiNIBRSCountyRow{
			CountyFIPS:        "55007",
			CountyName:        "Bayfield County",
			StateAbbr:         "WI",
			Year:              2023,
			ViolentCrimeRate:  "null",
			PropertyCrimeRate: "",
			Population:        "15000",
		}
		indicators := s.rowToIndicators(row)
		if len(indicators) != 2 {
			t.Fatalf("want 2 indicators, got %d", len(indicators))
		}
		for _, ind := range indicators {
			if ind.Value != nil {
				t.Errorf("%s: want nil (suppressed), got %v", ind.VariableID, *ind.Value)
			}
		}
	})

	t.Run("bad geoid length", func(t *testing.T) {
		row := fbiNIBRSCountyRow{
			CountyFIPS:        "5502",
			ViolentCrimeRate:  "100.0",
			PropertyCrimeRate: "500.0",
		}
		indicators := s.rowToIndicators(row)
		if len(indicators) != 0 {
			t.Errorf("want 0 indicators for bad GEOID, got %d", len(indicators))
		}
	})
}

// TestFBINIBRSFetchCounty_NoAPIKey verifies the no-key error is clear.
func TestFBINIBRSFetchCounty_NoAPIKey(t *testing.T) {
	// Explicitly pass empty key (not from env).
	s := NewFBINIBRSSource(FBINIBRSConfig{Year: 2023, APIKey: ""})
	_, err := s.FetchCounty(t.Context(), "55", "025")
	if err == nil {
		t.Error("FetchCounty without API key: want error, got nil")
	}
}
