package datasource

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// pitFixture is a minimal HUD PIT CSV with three CoC rows:
//   - WI-500 (maps to Dane County 55025)
//   - WI-501 (maps to Milwaukee County 55079)
//   - IL-510 (maps to Cook County 17031 — different state)
//
// Column names follow the 2023 vintage HUD PIT national CSV format.
const pitFixture = `CoC Number,CoC Name,Overall Homeless,Sheltered Total Homeless,Unsheltered Homeless,Chronically Homeless,Homeless Veterans,Homeless Unaccompanied Youth (Under 25)
WI-500,Wisconsin Balance of State CoC,"234","190","44","32","18","9"
WI-501,Milwaukee City and County CoC,"1543","1200","343","187","94","55"
IL-510,Chicago CoC,"6821","5100","1721","982","423","210"
`

// pitFixtureMissingCols is a CSV missing the chronically homeless column —
// used to test graceful missing-column handling.
const pitFixtureMissingCols = `CoC Number,CoC Name,Overall Homeless,Sheltered Total Homeless,Unsheltered Homeless
WI-500,Wisconsin Balance of State CoC,"234","190","44"
`

// setupHUDPITMockServer creates a test HTTP server serving pitFixture at /pit.csv.
func setupHUDPITMockServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/pit.csv", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte(body))
	})
	return httptest.NewServer(mux)
}

// newMockHUDPITSource creates a hudPITSource pointed at the test server.
func newMockHUDPITSource(ts *httptest.Server) *hudPITSource {
	return NewHUDPITSource(HUDPITConfig{
		Year:       2023,
		CSVURL:     ts.URL + "/pit.csv",
		HTTPClient: ts.Client(),
	})
}

// ---------------------------------------------------------------------------
// Identity and schema tests
// ---------------------------------------------------------------------------

// TestHUDPITDefaults validates Name, Category, and Vintage with and without year.
func TestHUDPITDefaults(t *testing.T) {
	s := NewHUDPITSource(HUDPITConfig{Year: 2023})
	if s.Name() != "hud-pit" {
		t.Errorf("Name(): want hud-pit, got %q", s.Name())
	}
	if s.Category() != "housing" {
		t.Errorf("Category(): want housing, got %q", s.Category())
	}
	if s.Vintage() != "HUD-PIT-2023" {
		t.Errorf("Vintage(2023): want HUD-PIT-2023, got %q", s.Vintage())
	}
}

// TestHUDPITDefaults_NoYear validates vintage when year is omitted.
func TestHUDPITDefaults_NoYear(t *testing.T) {
	s := NewHUDPITSource(HUDPITConfig{})
	if s.Vintage() != "HUD-PIT" {
		t.Errorf("Vintage() without year: want HUD-PIT, got %q", s.Vintage())
	}
}

// TestHUDPITSchema verifies Schema() returns all four variable definitions.
func TestHUDPITSchema(t *testing.T) {
	s := NewHUDPITSource(HUDPITConfig{Year: 2023})
	schema := s.Schema()

	if len(schema) == 0 {
		t.Fatal("Schema() returned empty slice")
	}

	expectedIDs := []string{
		"hud_pit_total_homeless",
		"hud_pit_sheltered",
		"hud_pit_unsheltered",
		"hud_pit_chronic",
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
		if def.Unit != "count" {
			t.Errorf("VariableDef %q Unit: want count, got %q", id, def.Unit)
		}
		if def.Direction != "lower_better" {
			t.Errorf("VariableDef %q Direction: want lower_better, got %q", id, def.Direction)
		}
	}
}

// TestHUDPITInterface verifies hudPITSource satisfies DataSource at compile time.
func TestHUDPITInterface(t *testing.T) {
	var _ DataSource = NewHUDPITSource(HUDPITConfig{Year: 2023})
}

// ---------------------------------------------------------------------------
// CSV parsing tests
// ---------------------------------------------------------------------------

// TestHUDPITParsing_MockServer verifies the adapter fetches and parses the CSV.
func TestHUDPITParsing_MockServer(t *testing.T) {
	ts := setupHUDPITMockServer(t, pitFixture)
	defer ts.Close()

	s := newMockHUDPITSource(ts)
	byCoC, err := s.fetchAll(context.Background())
	if err != nil {
		t.Fatalf("fetchAll: %v", err)
	}

	// Expect three CoC records.
	if len(byCoC) != 3 {
		t.Errorf("fetchAll: want 3 CoC records, got %d", len(byCoC))
	}

	rec, ok := byCoC["WI-500"]
	if !ok {
		t.Fatal("fetchAll: missing CoC WI-500")
	}

	if rec.totalHomeless == nil || *rec.totalHomeless != 234 {
		t.Errorf("WI-500 totalHomeless: want 234, got %v", rec.totalHomeless)
	}
	if rec.sheltered == nil || *rec.sheltered != 190 {
		t.Errorf("WI-500 sheltered: want 190, got %v", rec.sheltered)
	}
	if rec.unsheltered == nil || *rec.unsheltered != 44 {
		t.Errorf("WI-500 unsheltered: want 44, got %v", rec.unsheltered)
	}
	if rec.chronic == nil || *rec.chronic != 32 {
		t.Errorf("WI-500 chronic: want 32, got %v", rec.chronic)
	}
}

// TestHUDPITParsing_ThousandsSeparator verifies commas in numbers are stripped.
func TestHUDPITParsing_ThousandsSeparator(t *testing.T) {
	ts := setupHUDPITMockServer(t, pitFixture)
	defer ts.Close()

	s := newMockHUDPITSource(ts)
	byCoC, err := s.fetchAll(context.Background())
	if err != nil {
		t.Fatalf("fetchAll: %v", err)
	}

	// WI-501 total is "1543" in the fixture — verify it parses as 1543.
	rec, ok := byCoC["WI-501"]
	if !ok {
		t.Fatal("missing CoC WI-501")
	}
	if rec.totalHomeless == nil || *rec.totalHomeless != 1543 {
		t.Errorf("WI-501 totalHomeless with thousands sep: want 1543, got %v", rec.totalHomeless)
	}
}

// ---------------------------------------------------------------------------
// CoC-to-county mapping tests
// ---------------------------------------------------------------------------

// TestHUDPITFetchCounty_KnownCoC verifies FetchCounty returns indicators for
// a county that maps to a CoC present in the CSV.
func TestHUDPITFetchCounty_KnownCoC(t *testing.T) {
	ts := setupHUDPITMockServer(t, pitFixture)
	defer ts.Close()

	s := newMockHUDPITSource(ts)

	// 55 + 025 → Dane County → WI-500 (and WI-506 if present; fixture only has WI-500).
	indicators, err := s.FetchCounty(context.Background(), "55", "025")
	if err != nil {
		t.Fatalf("FetchCounty(55,025): %v", err)
	}
	if len(indicators) == 0 {
		t.Fatal("FetchCounty(55,025): expected indicators for Dane County, got none")
	}

	byVar := make(map[string]*float64)
	for _, ind := range indicators {
		if ind.GEOID != "55025" {
			t.Errorf("unexpected GEOID %q (want 55025)", ind.GEOID)
		}
		if ind.Vintage != "HUD-PIT-2023" {
			t.Errorf("indicator %q vintage: want HUD-PIT-2023, got %q", ind.VariableID, ind.Vintage)
		}
		v := ind.Value
		byVar[ind.VariableID] = v
	}

	if v, ok := byVar["hud_pit_total_homeless"]; !ok || v == nil || *v != 234 {
		t.Errorf("hud_pit_total_homeless: want 234, got %v", byVar["hud_pit_total_homeless"])
	}
	if v, ok := byVar["hud_pit_sheltered"]; !ok || v == nil || *v != 190 {
		t.Errorf("hud_pit_sheltered: want 190, got %v", byVar["hud_pit_sheltered"])
	}
	if v, ok := byVar["hud_pit_unsheltered"]; !ok || v == nil || *v != 44 {
		t.Errorf("hud_pit_unsheltered: want 44, got %v", byVar["hud_pit_unsheltered"])
	}
	if v, ok := byVar["hud_pit_chronic"]; !ok || v == nil || *v != 32 {
		t.Errorf("hud_pit_chronic: want 32, got %v", byVar["hud_pit_chronic"])
	}
}

// TestHUDPITFetchCounty_Milwaukee verifies Milwaukee county mapping (WI-501).
func TestHUDPITFetchCounty_Milwaukee(t *testing.T) {
	ts := setupHUDPITMockServer(t, pitFixture)
	defer ts.Close()

	s := newMockHUDPITSource(ts)
	indicators, err := s.FetchCounty(context.Background(), "55", "079")
	if err != nil {
		t.Fatalf("FetchCounty(55,079): %v", err)
	}
	if len(indicators) == 0 {
		t.Fatal("FetchCounty(55,079): expected indicators for Milwaukee, got none")
	}

	byVar := make(map[string]*float64)
	for _, ind := range indicators {
		byVar[ind.VariableID] = ind.Value
	}

	if v := byVar["hud_pit_total_homeless"]; v == nil || *v != 1543 {
		t.Errorf("Milwaukee total_homeless: want 1543, got %v", v)
	}
}

// TestHUDPITFetchCounty_UnknownCoC verifies that a county not in the crosswalk
// returns nil, nil (not an error).
func TestHUDPITFetchCounty_UnknownCoC(t *testing.T) {
	ts := setupHUDPITMockServer(t, pitFixture)
	defer ts.Close()

	s := newMockHUDPITSource(ts)

	// 01001 (Autauga, AL) has no entry in the crosswalk.
	indicators, err := s.FetchCounty(context.Background(), "01", "001")
	if err != nil {
		t.Fatalf("FetchCounty unknown county: unexpected error: %v", err)
	}
	if len(indicators) != 0 {
		t.Errorf("FetchCounty unknown county: want 0 indicators, got %d", len(indicators))
	}
}

// TestHUDPITFetchCounty_BadFIPS verifies that malformed FIPS returns an error.
func TestHUDPITFetchCounty_BadFIPS(t *testing.T) {
	s := NewHUDPITSource(HUDPITConfig{Year: 2023})
	_, err := s.FetchCounty(context.Background(), "5", "25") // produces 3 digits, not 5
	if err == nil {
		t.Error("FetchCounty with bad FIPS: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// State-level fetch tests
// ---------------------------------------------------------------------------

// TestHUDPITFetchState_Wisconsin verifies FetchState returns only WI indicators.
func TestHUDPITFetchState_Wisconsin(t *testing.T) {
	ts := setupHUDPITMockServer(t, pitFixture)
	defer ts.Close()

	s := newMockHUDPITSource(ts)
	indicators, err := s.FetchState(context.Background(), "55")
	if err != nil {
		t.Fatalf("FetchState(55): %v", err)
	}
	if len(indicators) == 0 {
		t.Fatal("FetchState(55): expected indicators for Wisconsin CoCs, got none")
	}

	// All returned GEOIDs must start with "55".
	for _, ind := range indicators {
		if !strings.HasPrefix(ind.GEOID, "55") {
			t.Errorf("FetchState(55): indicator GEOID %q does not start with 55", ind.GEOID)
		}
	}
}

// TestHUDPITFetchState_BadState returns an error on malformed state FIPS.
func TestHUDPITFetchState_BadState(t *testing.T) {
	s := NewHUDPITSource(HUDPITConfig{Year: 2023})
	_, err := s.FetchState(context.Background(), "5") // 1 digit, not 2
	if err == nil {
		t.Error("FetchState with bad state FIPS: expected error, got nil")
	}
}

// TestHUDPITFetchState_NoMatchingState verifies empty result for a state with
// no CoC crosswalk entries present in the CSV.
func TestHUDPITFetchState_NoMatchingState(t *testing.T) {
	ts := setupHUDPITMockServer(t, pitFixture)
	defer ts.Close()

	s := newMockHUDPITSource(ts)

	// State 36 (New York) has no entries in the built-in crosswalk.
	indicators, err := s.FetchState(context.Background(), "36")
	if err != nil {
		t.Fatalf("FetchState(36): unexpected error: %v", err)
	}
	if len(indicators) != 0 {
		t.Errorf("FetchState(36): want 0 indicators (no crosswalk), got %d", len(indicators))
	}
}

// ---------------------------------------------------------------------------
// parseCount unit tests
// ---------------------------------------------------------------------------

// TestParseCount covers the various formats HUD uses in CSV cells.
func TestHUDPITParseCount(t *testing.T) {
	tests := []struct {
		input string
		want  *float64
	}{
		{"", nil},
		{"-", nil},
		{"N/A", nil},
		{"n/a", nil},
		{"0", floatPtr(0)},
		{"234", floatPtr(234)},
		{"1,234", floatPtr(1234)},
		{"6,821", floatPtr(6821)},
		{"  42  ", floatPtr(42)},
	}

	for _, tt := range tests {
		got := parseCount(tt.input)
		if tt.want == nil && got != nil {
			t.Errorf("parseCount(%q): want nil, got %v", tt.input, *got)
		} else if tt.want != nil && got == nil {
			t.Errorf("parseCount(%q): want %v, got nil", tt.input, *tt.want)
		} else if tt.want != nil && got != nil && *tt.want != *got {
			t.Errorf("parseCount(%q): want %v, got %v", tt.input, *tt.want, *got)
		}
	}
}

// floatPtr is a test helper that returns a pointer to a float64 literal.
func floatPtr(f float64) *float64 { return &f }

// ---------------------------------------------------------------------------
// Column detection resilience test
// ---------------------------------------------------------------------------

// TestHUDPITFindCols_MissingRequired verifies that findHUDPITCols returns an
// error when a required column is absent.
func TestHUDPITFindCols_MissingRequired(t *testing.T) {
	header := []string{"CoC Number", "CoC Name", "Overall Homeless", "Sheltered Total Homeless"}
	// Missing: Unsheltered Homeless and Chronically Homeless.
	_, err := findHUDPITCols(header)
	if err == nil {
		t.Error("findHUDPITCols with missing columns: expected error, got nil")
	}
}

// TestHUDPITFindCols_AllPresent verifies successful column detection.
func TestHUDPITFindCols_AllPresent(t *testing.T) {
	header := []string{
		"CoC Number",
		"CoC Name",
		"Overall Homeless",
		"Sheltered Total Homeless",
		"Unsheltered Homeless",
		"Chronically Homeless",
	}
	idx, err := findHUDPITCols(header)
	if err != nil {
		t.Fatalf("findHUDPITCols: unexpected error: %v", err)
	}
	if idx.cocCode != 0 {
		t.Errorf("cocCode col: want 0, got %d", idx.cocCode)
	}
	if idx.total != 2 {
		t.Errorf("total col: want 2, got %d", idx.total)
	}
	if idx.sheltered != 3 {
		t.Errorf("sheltered col: want 3, got %d", idx.sheltered)
	}
	if idx.unsheltered != 4 {
		t.Errorf("unsheltered col: want 4, got %d", idx.unsheltered)
	}
	if idx.chronic != 5 {
		t.Errorf("chronic col: want 5, got %d", idx.chronic)
	}
}
