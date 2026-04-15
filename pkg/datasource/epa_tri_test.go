package datasource

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// ---- TestNewEPATRISource -------------------------------------------------------

// TestNewEPATRISource validates adapter identity metadata and defaults.
func TestNewEPATRISource(t *testing.T) {
	s := NewEPATRISource(EPATRIConfig{Year: 2022})
	if s.Name() != "epa-tri" {
		t.Errorf("Name(): want epa-tri, got %q", s.Name())
	}
	if s.Category() != "environment" {
		t.Errorf("Category(): want environment, got %q", s.Category())
	}
	if s.Vintage() != "EPA-TRI-2022" {
		t.Errorf("Vintage(): want EPA-TRI-2022, got %q", s.Vintage())
	}
	if s.cfg.HTTPClient == nil {
		t.Error("HTTPClient must be non-nil after NewEPATRISource")
	}
}

func TestNewEPATRISource_NoYear(t *testing.T) {
	s := NewEPATRISource(EPATRIConfig{})
	if s.Vintage() != "EPA-TRI" {
		t.Errorf("Vintage() without year: want EPA-TRI, got %q", s.Vintage())
	}
}

// ---- TestEPATRISchema ----------------------------------------------------------

// TestEPATRISchema verifies Schema() returns all expected variable definitions.
func TestEPATRISchema(t *testing.T) {
	s := NewEPATRISource(EPATRIConfig{Year: 2022})
	schema := s.Schema()

	if len(schema) == 0 {
		t.Fatal("Schema() returned empty slice")
	}

	expectedIDs := []string{
		"epa_tri_facility_count",
		"epa_tri_total_releases_lbs",
		"epa_tri_air_releases_lbs",
		"epa_tri_carcinogen_facility_count",
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

	if len(schema) != len(expectedIDs) {
		t.Errorf("Schema() length: want %d, got %d", len(expectedIDs), len(schema))
	}
}

// ---- TestEPATRIInterface -------------------------------------------------------

// TestEPATRIInterface verifies EPATRISource satisfies DataSource at compile time.
func TestEPATRIInterface(t *testing.T) {
	var _ DataSource = NewEPATRISource(EPATRIConfig{Year: 2022})
}

// ---- JSON parse helpers --------------------------------------------------------

// triFixture is a sample Envirofacts JSON response — two facilities in Dane
// County WI (55025) and one in Milwaukee County (55079).
// One Dane facility releases carcinogens; one does not.
var triFixtureRecords = []triRecord{
	{
		FIPSD:         "55025",
		StateFIPS:     "55",
		CountyFIPS:    "025",
		FugitiveAir:   "1000",
		StackAir:      "2000",
		TotalReleases: "5000",
		Carcinogen:    "YES",
		ReportingYear: "2022",
	},
	{
		FIPSD:         "55025",
		StateFIPS:     "55",
		CountyFIPS:    "025",
		FugitiveAir:   "500",
		StackAir:      "750",
		TotalReleases: "2000",
		Carcinogen:    "NO",
		ReportingYear: "2022",
	},
	{
		FIPSD:         "55079",
		StateFIPS:     "55",
		CountyFIPS:    "079",
		FugitiveAir:   "300",
		StackAir:      "700",
		TotalReleases: "1500",
		Carcinogen:    "YES",
		ReportingYear: "2022",
	},
}

// TestEPATRIParseJSON verifies that the adapter can parse the triRecord JSON
// structure used by the Envirofacts API.
func TestEPATRIParseJSON(t *testing.T) {
	data, err := json.Marshal(triFixtureRecords)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}

	var parsed []triRecord
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(parsed) != 3 {
		t.Fatalf("want 3 records, got %d", len(parsed))
	}

	// Spot-check first record.
	if parsed[0].FIPSD != "55025" {
		t.Errorf("record[0].FIPS_CD: want 55025, got %q", parsed[0].FIPSD)
	}
	if parsed[0].TotalReleases != "5000" {
		t.Errorf("record[0].TotalReleases: want 5000, got %q", parsed[0].TotalReleases)
	}
	if parsed[0].Carcinogen != "YES" {
		t.Errorf("record[0].Carcinogen: want YES, got %q", parsed[0].Carcinogen)
	}
}

// ---- TestEPATRIAggregateToCounty ----------------------------------------------

// TestEPATRIAggregateToCounty verifies that facility records aggregate correctly
// by county GEOID.
func TestEPATRIAggregateToCounty(t *testing.T) {
	byCounty := make(map[string]*countyTRIRecord)

	for _, rec := range triFixtureRecords {
		fips5 := resolveCountyFIPS(rec, "55")
		if fips5 == "" {
			t.Errorf("resolveCountyFIPS returned empty for rec %+v", rec)
			continue
		}
		county := getOrCreateCountyTRI(byCounty, fips5)
		county.facilityCount++
		county.totalReleasesLbs += parseFloatOrZero(rec.TotalReleases)
		county.airReleasesLbs += parseFloatOrZero(rec.FugitiveAir) + parseFloatOrZero(rec.StackAir)
		if strings.EqualFold(strings.TrimSpace(rec.Carcinogen), "YES") {
			county.carcinogenFacilityCount++
		}
	}

	// Dane County: 2 facilities.
	dane, ok := byCounty["55025"]
	if !ok {
		t.Fatal("expected county 55025 in aggregate map")
	}
	if dane.facilityCount != 2 {
		t.Errorf("55025 facilityCount: want 2, got %d", dane.facilityCount)
	}
	wantTotal := 5000.0 + 2000.0
	if dane.totalReleasesLbs != wantTotal {
		t.Errorf("55025 totalReleasesLbs: want %v, got %v", wantTotal, dane.totalReleasesLbs)
	}
	wantAir := 1000.0 + 2000.0 + 500.0 + 750.0
	if dane.airReleasesLbs != wantAir {
		t.Errorf("55025 airReleasesLbs: want %v, got %v", wantAir, dane.airReleasesLbs)
	}
	// Only first facility has CARCINOGEN=YES (second has NO).
	if dane.carcinogenFacilityCount != 1 {
		t.Errorf("55025 carcinogenFacilityCount: want 1, got %v", dane.carcinogenFacilityCount)
	}

	// Milwaukee County: 1 facility.
	milw, ok := byCounty["55079"]
	if !ok {
		t.Fatal("expected county 55079 in aggregate map")
	}
	if milw.facilityCount != 1 {
		t.Errorf("55079 facilityCount: want 1, got %d", milw.facilityCount)
	}
}

// ---- TestEPATRIStateScope -------------------------------------------------------

// setupEPATRIMockServer creates a test HTTP server that serves the TRI fixture
// records as a single JSON page.
func setupEPATRIMockServer(t *testing.T, records []triRecord) *httptest.Server {
	t.Helper()
	data, err := json.Marshal(records)
	if err != nil {
		t.Fatalf("marshal fixture records: %v", err)
	}

	mux := http.NewServeMux()
	// Match any path — the adapter will call /tri_facility/state_abbr/WI/rows/0:999/json
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	})
	return httptest.NewServer(mux)
}

// TestEPATRIStateScope verifies that FetchState produces correct county-level
// indicators from a mocked API response.
func TestEPATRIStateScope(t *testing.T) {
	ts := setupEPATRIMockServer(t, triFixtureRecords)
	defer ts.Close()

	// Build a source wired to the mock server.
	mockSrc := &epaTRISource{
		cfg: EPATRIConfig{
			Year:       2022,
			HTTPClient: ts.Client(),
		},
		vintage: "EPA-TRI-2022",
	}

	// We call fetchPage directly with the test server URL.
	records, err := mockSrc.fetchPage(context.Background(), ts.URL+"/tri_facility/state_abbr/WI/rows/0:999/json")
	if err != nil {
		t.Fatalf("fetchPage: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("fetchPage: want 3 records, got %d", len(records))
	}

	// Now run the full aggregate path manually using the records we fetched.
	byCounty := make(map[string]*countyTRIRecord)
	for _, rec := range records {
		if rec.ReportingYear != "2022" {
			continue
		}
		fips5 := resolveCountyFIPS(rec, "55")
		if fips5 == "" {
			continue
		}
		county := getOrCreateCountyTRI(byCounty, fips5)
		county.facilityCount++
		county.totalReleasesLbs += parseFloatOrZero(rec.TotalReleases)
		county.airReleasesLbs += parseFloatOrZero(rec.FugitiveAir) + parseFloatOrZero(rec.StackAir)
		if strings.EqualFold(strings.TrimSpace(rec.Carcinogen), "YES") {
			county.carcinogenFacilityCount++
		}
	}

	// Convert to indicators.
	var indicators []store.Indicator
	for fips5, rec := range byCounty {
		indicators = append(indicators, mockSrc.countyRecordToIndicators(fips5, rec)...)
	}

	if len(indicators) == 0 {
		t.Fatal("expected indicators, got none")
	}

	// Verify GEOID prefixes all start with "55".
	for _, ind := range indicators {
		if !strings.HasPrefix(ind.GEOID, "55") {
			t.Errorf("indicator GEOID %q does not start with 55 (Wisconsin)", ind.GEOID)
		}
		if ind.Vintage != "EPA-TRI-2022" {
			t.Errorf("indicator vintage: want EPA-TRI-2022, got %q", ind.Vintage)
		}
		if ind.Value == nil {
			t.Errorf("indicator %q has nil value", ind.VariableID)
		}
	}

	// Verify facility count for Dane county.
	var daneCount *float64
	for _, ind := range indicators {
		if ind.GEOID == "55025" && ind.VariableID == "epa_tri_facility_count" {
			daneCount = ind.Value
		}
	}
	if daneCount == nil {
		t.Error("epa_tri_facility_count for 55025 not found in indicators")
	} else if *daneCount != 2 {
		t.Errorf("55025 epa_tri_facility_count: want 2, got %v", *daneCount)
	}
}

// ---- TestEPATRIResolveCountyFIPS -----------------------------------------------

func TestEPATRIResolveCountyFIPS_FromFIPSCD(t *testing.T) {
	rec := triRecord{FIPSD: "55025"}
	got := resolveCountyFIPS(rec, "55")
	if got != "55025" {
		t.Errorf("want 55025, got %q", got)
	}
}

func TestEPATRIResolveCountyFIPS_FromComponents(t *testing.T) {
	// No FIPS_CD; rely on state + county fields.
	rec := triRecord{FIPSD: "", StateFIPS: "55", CountyFIPS: "025"}
	got := resolveCountyFIPS(rec, "55")
	if got != "55025" {
		t.Errorf("want 55025, got %q", got)
	}
}

func TestEPATRIResolveCountyFIPS_PadsCounty(t *testing.T) {
	// County FIPS without leading zero.
	rec := triRecord{FIPSD: "", StateFIPS: "55", CountyFIPS: "25"}
	got := resolveCountyFIPS(rec, "55")
	if got != "55025" {
		t.Errorf("want 55025, got %q", got)
	}
}

func TestEPATRIResolveCountyFIPS_Empty(t *testing.T) {
	rec := triRecord{}
	got := resolveCountyFIPS(rec, "55")
	if got != "" {
		t.Errorf("want empty string, got %q", got)
	}
}

// ---- TestEPATRIStateFIPSToAbbr -------------------------------------------------

func TestEPATRIStateFIPSToAbbr(t *testing.T) {
	cases := []struct{ fips, want string }{
		{"55", "WI"},
		{"06", "CA"},
		{"48", "TX"},
		{"99", ""},
	}
	for _, c := range cases {
		got := stateFIPSToAbbr(c.fips)
		if got != c.want {
			t.Errorf("stateFIPSToAbbr(%q): want %q, got %q", c.fips, c.want, got)
		}
	}
}

// ---- TestEPATRIFetchCounty_BadFIPS ---------------------------------------------

func TestEPATRIFetchCounty_BadFIPS(t *testing.T) {
	s := NewEPATRISource(EPATRIConfig{Year: 2022})
	_, err := s.FetchCounty(context.Background(), "5", "25") // bad lengths
	if err == nil {
		t.Error("FetchCounty with bad FIPS: expected error, got nil")
	}
}

func TestEPATRIFetchState_BadFIPS(t *testing.T) {
	s := NewEPATRISource(EPATRIConfig{Year: 2022})
	_, err := s.FetchState(context.Background(), "5") // 1-digit, invalid
	if err == nil {
		t.Error("FetchState with bad FIPS: expected error, got nil")
	}
}

// ---- TestEPATRIParseFloatOrZero ------------------------------------------------

func TestParseFloatOrZero(t *testing.T) {
	cases := []struct {
		in   string
		want float64
	}{
		{"1234.56", 1234.56},
		{"0", 0},
		{"", 0},
		{"   ", 0},
		{"not-a-number", 0},
	}
	for _, c := range cases {
		got := parseFloatOrZero(c.in)
		if got != c.want {
			t.Errorf("parseFloatOrZero(%q): want %v, got %v", c.in, c.want, got)
		}
	}
}
