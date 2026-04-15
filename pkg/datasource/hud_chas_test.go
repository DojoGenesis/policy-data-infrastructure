package datasource

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// chasCSVFixture is a minimal CHAS tract-level CSV fixture.
// It contains two Wisconsin tracts (one in Dane County 55025, one in
// Milwaukee County 55079) and one Alabama tract (01001) to exercise
// state and county filtering.
//
// Column names match the stable CHAS Table export schema.
// Values are chosen so the derived percentages are easy to verify:
//
//	Tract 55025000100 (Dane / WI):
//	  totalHH=100, problemHH=40, overcrowdedHH=5
//	  totalRenters=60, eliRenters=18
//	  renterCB30=10, renterCB50=8, ownerCB30=5, ownerCB50=4
//	  cost_burden_30pct = (10+8+5+4)/100 * 100 = 27.0
//	  cost_burden_50pct = (8+4)/100 * 100       = 12.0
//	  housing_problems  = 40/100 * 100           = 40.0
//	  eli_renters       = 18/60  * 100           = 30.0
//	  overcrowded       = 5/100  * 100           =  5.0
//
//	Tract 55079000100 (Milwaukee / WI):
//	  totalHH=200, problemHH=80, overcrowdedHH=10
//	  totalRenters=120, eliRenters=36
//	  renterCB30=20, renterCB50=16, ownerCB30=10, ownerCB50=8
//
//	Tract 01001020100 (Autauga / AL):
//	  totalHH=50, all indicators 0
const chasCSVFixture = `geoid,T1_est1,T1_est4,T1_est11,T4_est2,T4_est3,T8_est10,T8_est11,T9_est10,T9_est11
55025000100,100,40,5,60,18,10,8,5,4
55079000100,200,80,10,120,36,20,16,10,8
01001020100,50,0,0,30,0,0,0,0,0
`

// buildCHASZip wraps chasCSVFixture in an in-memory ZIP archive.
// The file inside the ZIP is named "Table8.csv" to match the HUD CHAS ZIP format.
func buildCHASZip(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	fw, err := zw.Create("Table8.csv")
	if err != nil {
		t.Fatalf("buildCHASZip: create entry: %v", err)
	}
	if _, err := fw.Write([]byte(chasCSVFixture)); err != nil {
		t.Fatalf("buildCHASZip: write entry: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("buildCHASZip: close: %v", err)
	}
	return buf.Bytes()
}

// setupCHASMockServer creates a test HTTP server that serves the CHAS ZIP at
// any path (the adapter uses a single configurable URL).
func setupCHASMockServer(t *testing.T) *httptest.Server {
	t.Helper()
	zipBody := buildCHASZip(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(zipBody)
	})
	return httptest.NewServer(mux)
}

// newMockCHASSource creates a hudCHASSource wired to ts.
func newMockCHASSource(ts *httptest.Server) *hudCHASSource {
	s := NewHUDCHASSource(HUDCHASConfig{
		Year:       2020,
		HTTPClient: ts.Client(),
	})
	// Point the source at the test server URL so the download goes there.
	s.zipURL = ts.URL + "/chas.zip"
	return s
}

// TestNewHUDCHASSource validates default construction.
func TestNewHUDCHASSource(t *testing.T) {
	s := NewHUDCHASSource(HUDCHASConfig{})
	if s.Name() != "hud-chas" {
		t.Errorf("Name(): want hud-chas, got %q", s.Name())
	}
	if s.Category() != "housing" {
		t.Errorf("Category(): want housing, got %q", s.Category())
	}
	if s.Vintage() != "CHAS-2020" {
		t.Errorf("Vintage(): want CHAS-2020, got %q", s.Vintage())
	}
	if s.cfg.Year != 2020 {
		t.Errorf("default Year: want 2020, got %d", s.cfg.Year)
	}
}

// TestNewHUDCHASSource_CustomYear validates construction with an explicit year.
func TestNewHUDCHASSource_CustomYear(t *testing.T) {
	s := NewHUDCHASSource(HUDCHASConfig{Year: 2019})
	if s.Vintage() != "CHAS-2019" {
		t.Errorf("Vintage(): want CHAS-2019, got %q", s.Vintage())
	}
	// URL should embed 2015–2019.
	if !strings.Contains(s.zipURL, "2015thru2019") {
		t.Errorf("zipURL %q should contain 2015thru2019", s.zipURL)
	}
}

// TestHUDCHASSchema verifies Schema() returns all expected variable definitions.
func TestHUDCHASSchema(t *testing.T) {
	s := NewHUDCHASSource(HUDCHASConfig{Year: 2020})
	schema := s.Schema()

	expectedIDs := []string{
		"hud_cost_burden_30pct",
		"hud_cost_burden_50pct",
		"hud_housing_problems",
		"hud_eli_renters",
		"hud_overcrowded",
	}

	if len(schema) != len(expectedIDs) {
		t.Errorf("Schema(): want %d variables, got %d", len(expectedIDs), len(schema))
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
}

// TestHUDCHASInterface verifies hudCHASSource satisfies DataSource at compile time.
func TestHUDCHASInterface(t *testing.T) {
	var _ DataSource = NewHUDCHASSource(HUDCHASConfig{Year: 2020})
}

// TestHUDCHASParseCSV tests parsing fixture CSV rows into indicators.
func TestHUDCHASParseCSV(t *testing.T) {
	s := NewHUDCHASSource(HUDCHASConfig{Year: 2020})

	rows := parseCSVString(t, chasCSVFixture)
	indicators, err := s.parseRows(rows, "55025", true)
	if err != nil {
		t.Fatalf("parseRows: %v", err)
	}

	if len(indicators) == 0 {
		t.Fatal("parseRows: no indicators returned")
	}

	// Group by variableID for assertions.
	byVar := make(map[string]float64)
	byVarNil := make(map[string]bool)
	for _, ind := range indicators {
		if ind.GEOID != "55025000100" {
			t.Errorf("unexpected GEOID %q (expected 55025000100 for county prefix 55025)", ind.GEOID)
		}
		if ind.Vintage != "CHAS-2020" {
			t.Errorf("Vintage: want CHAS-2020, got %q", ind.Vintage)
		}
		if ind.Value == nil {
			byVarNil[ind.VariableID] = true
		} else {
			byVar[ind.VariableID] = *ind.Value
		}
	}

	tests := []struct {
		varID string
		want  float64
	}{
		{"hud_cost_burden_30pct", 27.0},
		{"hud_cost_burden_50pct", 12.0},
		{"hud_housing_problems", 40.0},
		{"hud_eli_renters", 30.0},
		{"hud_overcrowded", 5.0},
	}

	for _, tc := range tests {
		got, ok := byVar[tc.varID]
		if !ok {
			if byVarNil[tc.varID] {
				t.Errorf("%s: got nil value, want %.1f", tc.varID, tc.want)
			} else {
				t.Errorf("%s: missing from output", tc.varID)
			}
			continue
		}
		// Allow 0.01 tolerance for floating-point arithmetic.
		if got < tc.want-0.01 || got > tc.want+0.01 {
			t.Errorf("%s: want %.1f, got %.4f", tc.varID, tc.want, got)
		}
	}

	// Verify RawValue is populated when Value is non-nil.
	for _, ind := range indicators {
		if ind.Value != nil && ind.RawValue == "" {
			t.Errorf("%s GEOID %s: Value is non-nil but RawValue is empty", ind.VariableID, ind.GEOID)
		}
	}
}

// TestHUDCHASCountyFilter verifies FetchCounty returns only tracts with the
// matching county FIPS prefix, using a mock HTTP server.
func TestHUDCHASCountyFilter(t *testing.T) {
	ts := setupCHASMockServer(t)
	defer ts.Close()

	s := newMockCHASSource(ts)

	indicators, err := s.FetchCounty(context.Background(), "55", "025")
	if err != nil {
		t.Fatalf("FetchCounty: %v", err)
	}
	if len(indicators) == 0 {
		t.Fatal("FetchCounty: no indicators returned")
	}

	for _, ind := range indicators {
		if !strings.HasPrefix(ind.GEOID, "55025") {
			t.Errorf("FetchCounty(55,025): indicator GEOID %q does not start with 55025", ind.GEOID)
		}
	}

	// Milwaukee (55079) and Alabama (01001) tracts must be absent.
	for _, ind := range indicators {
		if strings.HasPrefix(ind.GEOID, "55079") || strings.HasPrefix(ind.GEOID, "01001") {
			t.Errorf("FetchCounty(55,025): unexpected GEOID %q leaked through filter", ind.GEOID)
		}
	}
}

// TestHUDCHASStateFilter verifies FetchState returns only tracts in the given state.
func TestHUDCHASStateFilter(t *testing.T) {
	ts := setupCHASMockServer(t)
	defer ts.Close()

	s := newMockCHASSource(ts)

	indicators, err := s.FetchState(context.Background(), "55")
	if err != nil {
		t.Fatalf("FetchState: %v", err)
	}
	if len(indicators) == 0 {
		t.Fatal("FetchState(55): no indicators returned")
	}

	for _, ind := range indicators {
		if !strings.HasPrefix(ind.GEOID, "55") {
			t.Errorf("FetchState(55): indicator GEOID %q does not start with 55", ind.GEOID)
		}
	}

	// Alabama (01) tracts must be absent.
	for _, ind := range indicators {
		if strings.HasPrefix(ind.GEOID, "01") {
			t.Errorf("FetchState(55): unexpected Alabama GEOID %q leaked through filter", ind.GEOID)
		}
	}

	// Both WI tracts should be represented.
	geoids := make(map[string]bool)
	for _, ind := range indicators {
		geoids[ind.GEOID] = true
	}
	if !geoids["55025000100"] {
		t.Error("FetchState(55): missing tract 55025000100 (Dane County)")
	}
	if !geoids["55079000100"] {
		t.Error("FetchState(55): missing tract 55079000100 (Milwaukee County)")
	}
}

// TestHUDCHASFetchCounty_UnknownCounty verifies that a county with no matching
// tracts returns nil, nil (not an error).
func TestHUDCHASFetchCounty_UnknownCounty(t *testing.T) {
	ts := setupCHASMockServer(t)
	defer ts.Close()

	s := newMockCHASSource(ts)

	// County 99001 is not in the fixture.
	indicators, err := s.FetchCounty(context.Background(), "99", "001")
	if err != nil {
		t.Fatalf("FetchCounty unknown county: unexpected error: %v", err)
	}
	if len(indicators) != 0 {
		t.Errorf("FetchCounty unknown county: want 0 indicators, got %d", len(indicators))
	}
}

// TestHUDCHASFetchCounty_BadFIPS verifies that a malformed FIPS returns an error.
func TestHUDCHASFetchCounty_BadFIPS(t *testing.T) {
	s := NewHUDCHASSource(HUDCHASConfig{Year: 2020})
	_, err := s.FetchCounty(context.Background(), "5", "25") // 3-digit result, not 5
	if err == nil {
		t.Error("FetchCounty with bad FIPS: expected error, got nil")
	}
}

// TestHUDCHASInMemoryCache verifies the ZIP is downloaded only once even when
// FetchCounty and FetchState are called multiple times.
func TestHUDCHASInMemoryCache(t *testing.T) {
	downloadCount := 0
	mux := http.NewServeMux()
	zipBody := buildCHASZip(t)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		downloadCount++
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(zipBody)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	s := newMockCHASSource(ts)

	ctx := context.Background()
	if _, err := s.FetchCounty(ctx, "55", "025"); err != nil {
		t.Fatalf("first FetchCounty: %v", err)
	}
	if _, err := s.FetchCounty(ctx, "55", "079"); err != nil {
		t.Fatalf("second FetchCounty: %v", err)
	}
	if _, err := s.FetchState(ctx, "55"); err != nil {
		t.Fatalf("FetchState: %v", err)
	}

	if downloadCount != 1 {
		t.Errorf("expected exactly 1 HTTP download, got %d", downloadCount)
	}
}

// TestHUDCHASZeroTotalHH verifies that a tract with totalHH=0 produces nil values
// (no division by zero).
func TestHUDCHASZeroTotalHH(t *testing.T) {
	s := NewHUDCHASSource(HUDCHASConfig{Year: 2020})

	// Single row with 0 total households.
	rows := [][]string{
		{"geoid", "T1_est1", "T1_est4", "T1_est11", "T4_est2", "T4_est3",
			"T8_est10", "T8_est11", "T9_est10", "T9_est11"},
		{"55025999900", "0", "0", "0", "0", "0", "0", "0", "0", "0"},
	}

	indicators, err := s.parseRows(rows, "55025", true)
	if err != nil {
		t.Fatalf("parseRows: %v", err)
	}
	for _, ind := range indicators {
		if ind.Value != nil {
			t.Errorf("%s: expected nil value for zero-totalHH tract, got %v", ind.VariableID, *ind.Value)
		}
	}
}

// parseCSVString is a test helper that parses a raw CSV string into rows.
func parseCSVString(t *testing.T, s string) [][]string {
	t.Helper()
	r := strings.NewReader(s)
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true
	rows, err := cr.ReadAll()
	if err != nil {
		t.Fatalf("parseCSVString: %v", err)
	}
	return rows
}
