package datasource

import (
	"archive/zip"
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// --------------------------------------------------------------------------
// Fixtures
// --------------------------------------------------------------------------

// usdaCSVFixture is a minimal USDA FARA CSV with 4 tracts:
//   - 55025000100 (Dane County, WI) — food desert, has grocery
//   - 55025000200 (Dane County, WI) — not food desert
//   - 55071000100 (Manitowoc County, WI) — food desert
//   - 17031010100 (Cook County, IL)  — different state, should be filtered out
const usdaCSVFixture = `CensusTract,State,County,LILATracts_1And10,LAPOP1_10,LAPOP10_10,TractSNAP,TractSuper,Urban,PovertyRate,Pop2010
55025000100,WI,Dane,1,350,0,120,2,1,12.5,4500
55025000200,WI,Dane,0,0,0,80,1,1,8.0,3200
55071000100,WI,Manitowoc,1,500,200,200,0,0,18.0,2100
17031010100,IL,Cook,1,800,0,300,3,1,22.0,6000
`

// usdaCSVFixtureAlt uses alternate column names from a different ERS vintage.
const usdaCSVFixtureAlt = `GEOID,LILATracts_halfAnd10,LAPOP1,LAPOP10,SNAP_1,SuperCount
55025000100,1,400,0,150,2
55025000200,0,50,0,90,1
`

// buildZIP wraps a CSV body into a valid ZIP archive.
func buildZIP(t *testing.T, csvBody string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("FoodAccessResearchAtlasData2019.csv")
	if err != nil {
		t.Fatalf("buildZIP: create: %v", err)
	}
	if _, err := w.Write([]byte(csvBody)); err != nil {
		t.Fatalf("buildZIP: write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("buildZIP: close: %v", err)
	}
	return buf.Bytes()
}

// newUSDAMockServer starts a test HTTP server that serves the USDA CSV
// (optionally wrapped in a ZIP) at the root path "/".
func newUSDAMockServer(t *testing.T, body []byte, contentType string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
}

// newUSDA404Server starts a test HTTP server that always returns 404.
func newUSDA404Server(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
}

// --------------------------------------------------------------------------
// Interface / identity tests
// --------------------------------------------------------------------------

// TestNewUSDAFoodSource validates Name, Category, Vintage with year.
func TestNewUSDAFoodSource(t *testing.T) {
	s := NewUSDAFoodSource(USDAFoodConfig{Year: 2019})
	if s.Name() != "usda-foodaccess" {
		t.Errorf("Name(): want usda-foodaccess, got %q", s.Name())
	}
	if s.Category() != "food" {
		t.Errorf("Category(): want food, got %q", s.Category())
	}
	if s.Vintage() != "USDA-FARA-2019" {
		t.Errorf("Vintage(): want USDA-FARA-2019, got %q", s.Vintage())
	}
}

// TestNewUSDAFoodSource_NoYear validates vintage without a year.
func TestNewUSDAFoodSource_NoYear(t *testing.T) {
	s := NewUSDAFoodSource(USDAFoodConfig{})
	if s.Vintage() != "USDA-FARA" {
		t.Errorf("Vintage() without year: want USDA-FARA, got %q", s.Vintage())
	}
}

// TestUSDAFoodInterface verifies USDAFoodSource satisfies DataSource at compile time.
func TestUSDAFoodInterface(t *testing.T) {
	var _ DataSource = NewUSDAFoodSource(USDAFoodConfig{Year: 2019})
}

// --------------------------------------------------------------------------
// Schema tests
// --------------------------------------------------------------------------

// TestUSDAFoodSchema verifies Schema() returns all expected variable definitions.
func TestUSDAFoodSchema(t *testing.T) {
	s := NewUSDAFoodSource(USDAFoodConfig{Year: 2019})
	schema := s.Schema()

	if len(schema) == 0 {
		t.Fatal("Schema() returned empty slice")
	}

	expectedIDs := []string{
		"usda_food_desert",
		"usda_food_low_access_1mi",
		"usda_food_low_access_10mi",
		"usda_food_snap_count",
		"usda_food_grocery_count",
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

// TestUSDAFoodSchema_ReturnsCopy verifies mutations to the returned slice do
// not affect subsequent calls (defensive copy).
func TestUSDAFoodSchema_ReturnsCopy(t *testing.T) {
	s := NewUSDAFoodSource(USDAFoodConfig{Year: 2019})
	s1 := s.Schema()
	s1[0].ID = "mutated"
	s2 := s.Schema()
	if s2[0].ID == "mutated" {
		t.Error("Schema() returned same backing slice — defensive copy required")
	}
}

// --------------------------------------------------------------------------
// CSV parsing tests (unit, no HTTP)
// --------------------------------------------------------------------------

// TestParseUSDACSV_BasicColumns validates parseUSDACSV with the primary fixture.
func TestParseUSDACSV_BasicColumns(t *testing.T) {
	byGEOID, err := parseUSDACSV(usdaCSVFixture)
	if err != nil {
		t.Fatalf("parseUSDACSV: %v", err)
	}

	// Should have exactly 4 records (one per row).
	if len(byGEOID) != 4 {
		t.Errorf("parseUSDACSV: want 4 records, got %d", len(byGEOID))
	}

	rec, ok := byGEOID["55025000100"]
	if !ok {
		t.Fatal("parseUSDACSV: missing tract 55025000100")
	}

	// Food desert flag should be "1".
	if v := rec.values[usdaColFoodDesert]; v != "1" {
		t.Errorf("55025000100 food_desert: want %q, got %q", "1", v)
	}
	// Low access 1mi should be "350".
	if v := rec.values[usdaColLowAcc1Mi]; v != "350" {
		t.Errorf("55025000100 low_access_1mi: want %q, got %q", "350", v)
	}
	// Grocery count should be "2".
	if v := rec.values[usdaColGrocery]; v != "2" {
		t.Errorf("55025000100 grocery_count: want %q, got %q", "2", v)
	}
}

// TestParseUSDACSV_AltColumns validates the alternate column name set.
func TestParseUSDACSV_AltColumns(t *testing.T) {
	byGEOID, err := parseUSDACSV(usdaCSVFixtureAlt)
	if err != nil {
		t.Fatalf("parseUSDACSV alt: %v", err)
	}

	if len(byGEOID) != 2 {
		t.Errorf("parseUSDACSV alt: want 2 records, got %d", len(byGEOID))
	}

	rec, ok := byGEOID["55025000100"]
	if !ok {
		t.Fatal("parseUSDACSV alt: missing tract 55025000100")
	}
	if v := rec.values[usdaColFoodDesert]; v != "1" {
		t.Errorf("alt 55025000100 food_desert: want 1, got %q", v)
	}
	if v := rec.values[usdaColLowAcc1Mi]; v != "400" {
		t.Errorf("alt 55025000100 low_access_1mi: want 400, got %q", v)
	}
}

// TestParseUSDACSV_GEOIDPadding verifies short GEOIDs are zero-padded to 11 digits.
func TestParseUSDACSV_GEOIDPadding(t *testing.T) {
	csvText := "CensusTract,LILATracts_1And10\n55025001,1\n"
	byGEOID, err := parseUSDACSV(csvText)
	if err != nil {
		t.Fatalf("parseUSDACSV: %v", err)
	}
	if _, ok := byGEOID["55025001"]; ok {
		t.Error("8-char GEOID should have been padded to 11 digits")
	}
	if _, ok := byGEOID["00055025001"]; !ok {
		t.Error("8-char GEOID should be zero-padded to 00055025001")
	}
}

// TestParseUSDACSV_MissingGEOIDColumn verifies an error is returned when the
// CSV has no recognizable GEOID column.
func TestParseUSDACSV_MissingGEOIDColumn(t *testing.T) {
	csvText := "NotAGEOID,LILATracts_1And10\n12345678901,1\n"
	_, err := parseUSDACSV(csvText)
	if err == nil {
		t.Error("parseUSDACSV: expected error for missing GEOID column, got nil")
	}
}

// TestParseUSDACSV_BlankOrNAValues verifies blank and N/A values are stored
// as empty (nil Value after tractToIndicators).
func TestParseUSDACSV_BlankOrNAValues(t *testing.T) {
	csvText := "CensusTract,LILATracts_1And10,LAPOP1_10\n55025000100,N/A,-\n"
	byGEOID, err := parseUSDACSV(csvText)
	if err != nil {
		t.Fatalf("parseUSDACSV: %v", err)
	}
	rec, ok := byGEOID["55025000100"]
	if !ok {
		t.Fatal("missing tract 55025000100")
	}
	if v, present := rec.values[usdaColFoodDesert]; present && v != "" {
		t.Errorf("N/A should be omitted from values map, got %q", v)
	}
}

// --------------------------------------------------------------------------
// ZIP extraction tests
// --------------------------------------------------------------------------

// TestExtractLargestCSVFromZIP verifies the ZIP extractor returns the largest CSV.
func TestExtractLargestCSVFromZIP(t *testing.T) {
	zipData := buildZIP(t, usdaCSVFixture)
	csvBytes, err := extractLargestCSVFromZIP(zipData)
	if err != nil {
		t.Fatalf("extractLargestCSVFromZIP: %v", err)
	}
	if !strings.Contains(string(csvBytes), "55025000100") {
		t.Error("extracted CSV should contain tract 55025000100")
	}
}

// TestExtractLargestCSVFromZIP_EmptyZIP verifies an error is returned when the
// ZIP contains no CSV files.
func TestExtractLargestCSVFromZIP_EmptyZIP(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	// Add a non-CSV file.
	w, _ := zw.Create("readme.txt")
	_, _ = w.Write([]byte("not a csv"))
	_ = zw.Close()

	_, err := extractLargestCSVFromZIP(buf.Bytes())
	if err == nil {
		t.Error("expected error for ZIP with no CSVs, got nil")
	}
}

// --------------------------------------------------------------------------
// HTTP integration tests (mock server)
// --------------------------------------------------------------------------

// TestUSDAFoodFetchState_CSVDirect exercises FetchState with a plain CSV response.
func TestUSDAFoodFetchState_CSVDirect(t *testing.T) {
	ts := newUSDAMockServer(t, []byte(usdaCSVFixture), "text/csv")
	defer ts.Close()

	s := NewUSDAFoodSource(USDAFoodConfig{
		Year:       2019,
		DataURL:    ts.URL + "/",
		HTTPClient: ts.Client(),
	})
	// Disable the rate-limit delay in tests by using an already-populated cache
	// (populated on first call below — rate delay fires once, which is acceptable
	// in test; for strict zero-delay tests, replace the delay constant).

	indicators, err := s.FetchState(context.Background(), "55")
	if err != nil {
		t.Fatalf("FetchState: %v", err)
	}

	// Should return indicators for WI tracts only (3 tracts × 5 variables = 15).
	const wantRecords = 3 * 5
	if len(indicators) != wantRecords {
		t.Errorf("FetchState(55): want %d indicators, got %d", wantRecords, len(indicators))
	}

	// All GEOIDs must begin with "55".
	for _, ind := range indicators {
		if !strings.HasPrefix(ind.GEOID, "55") {
			t.Errorf("FetchState(55): indicator GEOID %q does not start with 55", ind.GEOID)
		}
	}
}

// TestUSDAFoodFetchState_ZIPResponse exercises FetchState with a ZIP-wrapped CSV.
func TestUSDAFoodFetchState_ZIPResponse(t *testing.T) {
	zipData := buildZIP(t, usdaCSVFixture)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/zip")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(zipData)
	}))
	defer ts.Close()

	// Use a URL that ends in .zip so the adapter detects the ZIP format.
	s := NewUSDAFoodSource(USDAFoodConfig{
		Year:       2019,
		DataURL:    ts.URL + "/data.zip",
		HTTPClient: ts.Client(),
	})

	indicators, err := s.FetchState(context.Background(), "55")
	if err != nil {
		t.Fatalf("FetchState (ZIP): %v", err)
	}

	if len(indicators) == 0 {
		t.Fatal("FetchState (ZIP): got no indicators")
	}
}

// TestUSDAFoodFetchCounty_FiltersByPrefix verifies FetchCounty returns only the
// requested county's tracts.
func TestUSDAFoodFetchCounty_FiltersByPrefix(t *testing.T) {
	ts := newUSDAMockServer(t, []byte(usdaCSVFixture), "text/csv")
	defer ts.Close()

	s := NewUSDAFoodSource(USDAFoodConfig{
		Year:       2019,
		DataURL:    ts.URL + "/",
		HTTPClient: ts.Client(),
	})

	// Fetch Dane County, WI (55025) — should get 2 tracts × 5 variables = 10.
	indicators, err := s.FetchCounty(context.Background(), "55", "025")
	if err != nil {
		t.Fatalf("FetchCounty: %v", err)
	}

	const wantRecords = 2 * 5
	if len(indicators) != wantRecords {
		t.Errorf("FetchCounty(55,025): want %d indicators, got %d", wantRecords, len(indicators))
	}

	for _, ind := range indicators {
		if !strings.HasPrefix(ind.GEOID, "55025") {
			t.Errorf("FetchCounty(55,025): GEOID %q does not start with 55025", ind.GEOID)
		}
	}
}

// TestUSDAFoodFetchCounty_UnknownCounty verifies that a county with no tracts
// in the data returns nil, nil (not an error).
func TestUSDAFoodFetchCounty_UnknownCounty(t *testing.T) {
	ts := newUSDAMockServer(t, []byte(usdaCSVFixture), "text/csv")
	defer ts.Close()

	s := NewUSDAFoodSource(USDAFoodConfig{
		Year:       2019,
		DataURL:    ts.URL + "/",
		HTTPClient: ts.Client(),
	})

	// Alabama county 001 has no rows in the fixture.
	indicators, err := s.FetchCounty(context.Background(), "01", "001")
	if err != nil {
		t.Fatalf("FetchCounty unknown county: unexpected error: %v", err)
	}
	if len(indicators) != 0 {
		t.Errorf("FetchCounty unknown county: want 0 indicators, got %d", len(indicators))
	}
}

// TestUSDAFoodFetchState_OtherState verifies FetchState filters out other states.
func TestUSDAFoodFetchState_OtherState(t *testing.T) {
	ts := newUSDAMockServer(t, []byte(usdaCSVFixture), "text/csv")
	defer ts.Close()

	s := NewUSDAFoodSource(USDAFoodConfig{
		Year:       2019,
		DataURL:    ts.URL + "/",
		HTTPClient: ts.Client(),
	})

	// State 99 has no rows in the fixture.
	indicators, err := s.FetchState(context.Background(), "99")
	if err != nil {
		t.Fatalf("FetchState(99): unexpected error: %v", err)
	}
	if len(indicators) != 0 {
		t.Errorf("FetchState(99): want 0 indicators, got %d", len(indicators))
	}
}

// --------------------------------------------------------------------------
// Indicator value tests
// --------------------------------------------------------------------------

// TestUSDAFoodIndicatorValues validates that specific indicator values match
// the fixture data for tract 55025000100.
func TestUSDAFoodIndicatorValues(t *testing.T) {
	ts := newUSDAMockServer(t, []byte(usdaCSVFixture), "text/csv")
	defer ts.Close()

	s := NewUSDAFoodSource(USDAFoodConfig{
		Year:       2019,
		DataURL:    ts.URL + "/",
		HTTPClient: ts.Client(),
	})

	indicators, err := s.FetchCounty(context.Background(), "55", "025")
	if err != nil {
		t.Fatalf("FetchCounty: %v", err)
	}

	byVar := make(map[string]map[string]*float64) // GEOID → varID → value
	for _, ind := range indicators {
		if byVar[ind.GEOID] == nil {
			byVar[ind.GEOID] = make(map[string]*float64)
		}
		byVar[ind.GEOID][ind.VariableID] = ind.Value
		if ind.Vintage != "USDA-FARA-2019" {
			t.Errorf("vintage: want USDA-FARA-2019, got %q", ind.Vintage)
		}
	}

	// Tract 55025000100: food desert = 1, low_access_1mi = 350, grocery = 2.
	t1 := byVar["55025000100"]
	if t1 == nil {
		t.Fatal("no indicators for 55025000100")
	}
	checkFloat(t, "55025000100", "usda_food_desert", t1, 1)
	checkFloat(t, "55025000100", "usda_food_low_access_1mi", t1, 350)
	checkFloat(t, "55025000100", "usda_food_grocery_count", t1, 2)

	// Tract 55025000200: food desert = 0, no low access.
	t2 := byVar["55025000200"]
	if t2 == nil {
		t.Fatal("no indicators for 55025000200")
	}
	checkFloat(t, "55025000200", "usda_food_desert", t2, 0)
	checkFloat(t, "55025000200", "usda_food_grocery_count", t2, 1)
}

// checkFloat is a helper that asserts a specific float value in the map.
func checkFloat(t *testing.T, geoid, varID string, m map[string]*float64, want float64) {
	t.Helper()
	val, ok := m[varID]
	if !ok {
		t.Errorf("%s/%s: variable not found in indicator set", geoid, varID)
		return
	}
	if val == nil {
		t.Errorf("%s/%s: value is nil, want %v", geoid, varID, want)
		return
	}
	if *val != want {
		t.Errorf("%s/%s: want %v, got %v", geoid, varID, want, *val)
	}
}

// TestUSDAFoodIndicator_NilValueForMissingData verifies that N/A raw values
// produce nil Value pointers.
func TestUSDAFoodIndicator_NilValueForMissingData(t *testing.T) {
	csvText := "CensusTract,LILATracts_1And10,LAPOP1_10,LAPOP10_10,TractSNAP,TractSuper\n" +
		"55025000100,N/A,,-,.,\n"
	ts := newUSDAMockServer(t, []byte(csvText), "text/csv")
	defer ts.Close()

	s := NewUSDAFoodSource(USDAFoodConfig{
		Year:       2019,
		DataURL:    ts.URL + "/",
		HTTPClient: ts.Client(),
	})

	indicators, err := s.FetchState(context.Background(), "55")
	if err != nil {
		t.Fatalf("FetchState: %v", err)
	}

	for _, ind := range indicators {
		if ind.Value != nil {
			t.Errorf("expected nil Value for missing data column %q, got %v", ind.VariableID, *ind.Value)
		}
	}
}

// --------------------------------------------------------------------------
// Bad FIPS tests
// --------------------------------------------------------------------------

// TestUSDAFoodFetchCounty_BadStateFIPS verifies that a malformed state FIPS
// returns an error without making an HTTP request.
func TestUSDAFoodFetchCounty_BadStateFIPS(t *testing.T) {
	s := NewUSDAFoodSource(USDAFoodConfig{Year: 2019})
	_, err := s.FetchCounty(context.Background(), "5", "025") // 1 digit → too short
	if err == nil {
		t.Error("FetchCounty bad state FIPS: expected error, got nil")
	}
}

// TestUSDAFoodFetchCounty_BadCountyFIPS verifies that a malformed county FIPS
// returns an error.
func TestUSDAFoodFetchCounty_BadCountyFIPS(t *testing.T) {
	s := NewUSDAFoodSource(USDAFoodConfig{Year: 2019})
	_, err := s.FetchCounty(context.Background(), "55", "25") // 2 digits → too short
	if err == nil {
		t.Error("FetchCounty bad county FIPS: expected error, got nil")
	}
}

// TestUSDAFoodFetchState_BadFIPS verifies that a malformed state FIPS returns
// an error.
func TestUSDAFoodFetchState_BadFIPS(t *testing.T) {
	s := NewUSDAFoodSource(USDAFoodConfig{Year: 2019})
	_, err := s.FetchState(context.Background(), "5") // 1 digit → too short
	if err == nil {
		t.Error("FetchState bad FIPS: expected error, got nil")
	}
}

// --------------------------------------------------------------------------
// HTTP error tests
// --------------------------------------------------------------------------

// TestUSDAFoodFetchState_HTTP404 verifies that a 404 from the ERS server
// returns an error.
func TestUSDAFoodFetchState_HTTP404(t *testing.T) {
	ts := newUSDA404Server(t)
	defer ts.Close()

	s := NewUSDAFoodSource(USDAFoodConfig{
		Year:       2019,
		DataURL:    ts.URL + "/",
		HTTPClient: ts.Client(),
	})

	_, err := s.FetchState(context.Background(), "55")
	if err == nil {
		t.Error("FetchState 404: expected error, got nil")
	}
}

// --------------------------------------------------------------------------
// Cache test
// --------------------------------------------------------------------------

// TestUSDAFoodCache verifies that the parsed data is cached: the mock server
// records request count, and a second FetchState should not trigger a second
// HTTP request.
func TestUSDAFoodCache(t *testing.T) {
	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "text/csv")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(usdaCSVFixture))
	}))
	defer ts.Close()

	s := NewUSDAFoodSource(USDAFoodConfig{
		Year:       2019,
		DataURL:    ts.URL + "/",
		HTTPClient: ts.Client(),
	})

	if _, err := s.FetchState(context.Background(), "55"); err != nil {
		t.Fatalf("FetchState first call: %v", err)
	}
	if _, err := s.FetchState(context.Background(), "55"); err != nil {
		t.Fatalf("FetchState second call: %v", err)
	}

	if requestCount != 1 {
		t.Errorf("cache: expected 1 HTTP request, got %d", requestCount)
	}
}
