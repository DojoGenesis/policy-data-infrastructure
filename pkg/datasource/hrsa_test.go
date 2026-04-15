package datasource

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// hpsaFixturePC is a minimal primary care HPSA CSV fixture.
// Two counties: 55025 (Dane, WI) with an active designation score 18,
// and 55071 (Manitowoc, WI) with a withdrawn designation.
const hpsaFixturePC = `HPSA Name,HPSA ID,Designation Type,HPSA Discipline Class,HPSA Score,Primary State Abbreviation,HPSA Status,HPSA Designation Date,HPSA Designation Last Update Date,Metropolitan Indicator,HPSA Geography Identification Number,HPSA Degree of Shortage,Withdrawn Date,HPSA FTE,HPSA Designation Population,% of Population Below 100% Poverty,HPSA Formal Ratio,HPSA Population Type,Rural Status,Longitude,Latitude,BHCMIS Organization Identification Number,Break in Designation,Common County Name,Common Postal Code,Common Region Name,Common State Abbreviation,Common State County FIPS Code,Common State FIPS Code,Common State Name,County Equivalent Name,County or County Equivalent Federal Information Processing Standard Code,Discipline Class Number,HPSA Address,HPSA City,HPSA Component Name,HPSA Component Source Identification Number,HPSA Component State Abbreviation,HPSA Component Type Code,HPSA Component Type Description,HPSA Designation Population Type Description,HPSA Estimated Served Population,HPSA Estimated Underserved Population,HPSA Metropolitan Indicator Code,HPSA Population Type Code,HPSA Postal Code,HPSA Provider Ratio Goal,HPSA Resident Civilian Population,HPSA Shortage,HPSA Status Code,HPSA Type Code,HPSA Withdrawn Date String,Primary State FIPS Code,Primary State Name,Provider Type,Rural Status Code,State Abbreviation,State and County Federal Information Processing Standard Code,State FIPS Code,State Name,U.S. - Mexico Border 100 Kilometer Indicator,U.S. - Mexico Border County Indicator,Data Warehouse Record Create Date,Data Warehouse Record Create Date Text,
"Dane County PC HPSA","1234567890","Geographic HPSA","Primary Care","18","WI","Designated","01/01/2020","01/01/2024","Metropolitan","55025","Not applicable","","2.0000","50000.0","15.0","2000:1","Geographic Population","Urban","-89.4","43.0","","N","Dane, WI","53701","Region 5","WI","55025","55","Wisconsin","Dane","025","1","","Madison","Dane County","","WI","SCTY","Single County","Geographic Population","40000","10000","M","TRC","53701","3500:1","50000","14.3","D","Hpsa Geo","","55","Wisconsin","Not Applicable","U","WI","55025","55","Wisconsin","N","N","04/14/2026","2026/04/14"
"Manitowoc County PC HPSA","9876543210","Geographic HPSA","Primary Care","12","WI","Withdrawn","01/01/2010","01/01/2012","Non-Metropolitan","55071","Not applicable","01/01/2012","1.5000","80000.0","20.0","3000:1","Geographic Population","Rural","-87.7","44.1","","N","Manitowoc, WI","54220","Region 5","WI","55071","55","Wisconsin","Manitowoc","071","1","","Manitowoc","Manitowoc County","","WI","SCTY","Single County","Geographic Population","60000","20000","N","TRC","54220","3500:1","80000","26.7","W","Hpsa Geo","2012/01/01","55","Wisconsin","Not Applicable","R","WI","55071","55","Wisconsin","N","N","04/14/2026","2026/04/14"
`

// hpsaFixtureDH is a minimal dental HPSA CSV fixture for county 55025 (score 10).
const hpsaFixtureDH = `HPSA Name,HPSA ID,Designation Type,HPSA Discipline Class,HPSA Score,Primary State Abbreviation,HPSA Status,HPSA Designation Date,HPSA Designation Last Update Date,Metropolitan Indicator,HPSA Geography Identification Number,HPSA Degree of Shortage,Withdrawn Date,HPSA FTE,HPSA Designation Population,% of Population Below 100% Poverty,HPSA Formal Ratio,HPSA Population Type,Rural Status,Longitude,Latitude,BHCMIS Organization Identification Number,Break in Designation,Common County Name,Common Postal Code,Common Region Name,Common State Abbreviation,Common State County FIPS Code,Common State FIPS Code,Common State Name,County Equivalent Name,County or County Equivalent Federal Information Processing Standard Code,Discipline Class Number,HPSA Address,HPSA City,HPSA Component Name,HPSA Component Source Identification Number,HPSA Component State Abbreviation,HPSA Component Type Code,HPSA Component Type Description,HPSA Designation Population Type Description,HPSA Estimated Served Population,HPSA Estimated Underserved Population,HPSA Metropolitan Indicator Code,HPSA Population Type Code,HPSA Postal Code,HPSA Provider Ratio Goal,HPSA Resident Civilian Population,HPSA Shortage,HPSA Status Code,HPSA Type Code,HPSA Withdrawn Date String,Primary State FIPS Code,Primary State Name,Provider Type,Rural Status Code,State Abbreviation,State and County Federal Information Processing Standard Code,State FIPS Code,State Name,U.S. - Mexico Border 100 Kilometer Indicator,U.S. - Mexico Border County Indicator,Data Warehouse Record Create Date,Data Warehouse Record Create Date Text,
"Dane County DH HPSA","1234567891","Geographic HPSA","Dental Health","10","WI","Designated","01/01/2020","01/01/2024","Metropolitan","55025","Not applicable","","1.0000","50000.0","15.0","4000:1","Geographic Population","Urban","-89.4","43.0","","N","Dane, WI","53701","Region 5","WI","55025","55","Wisconsin","Dane","025","6","","Madison","Dane County","","WI","SCTY","Single County","Geographic Population","40000","10000","M","TRC","53701","5000:1","50000","12.5","D","Hpsa Geo","","55","Wisconsin","Not Applicable","U","WI","55025","55","Wisconsin","N","N","04/14/2026","2026/04/14"
`

// hpsaFixtureMH is a minimal mental health HPSA CSV — empty (no rows for 55025).
const hpsaFixtureMH = `HPSA Name,HPSA ID,Designation Type,HPSA Discipline Class,HPSA Score,Primary State Abbreviation,HPSA Status,HPSA Designation Date,HPSA Designation Last Update Date,Metropolitan Indicator,HPSA Geography Identification Number,HPSA Degree of Shortage,Withdrawn Date,HPSA FTE,HPSA Designation Population,% of Population Below 100% Poverty,HPSA Formal Ratio,HPSA Population Type,Rural Status,Longitude,Latitude,BHCMIS Organization Identification Number,Break in Designation,Common County Name,Common Postal Code,Common Region Name,Common State Abbreviation,Common State County FIPS Code,Common State FIPS Code,Common State Name,County Equivalent Name,County or County Equivalent Federal Information Processing Standard Code,Discipline Class Number,HPSA Address,HPSA City,HPSA Component Name,HPSA Component Source Identification Number,HPSA Component State Abbreviation,HPSA Component Type Code,HPSA Component Type Description,HPSA Designation Population Type Description,HPSA Estimated Served Population,HPSA Estimated Underserved Population,HPSA Metropolitan Indicator Code,HPSA Population Type Code,HPSA Postal Code,HPSA Provider Ratio Goal,HPSA Resident Civilian Population,HPSA Shortage,HPSA Status Code,HPSA Type Code,HPSA Withdrawn Date String,Primary State FIPS Code,Primary State Name,Provider Type,Rural Status Code,State Abbreviation,State and County Federal Information Processing Standard Code,State FIPS Code,State Name,U.S. - Mexico Border 100 Kilometer Indicator,U.S. - Mexico Border County Indicator,Data Warehouse Record Create Date,Data Warehouse Record Create Date Text,
`

// fqhcFixture is a minimal FQHC CSV with 2 active sites in 55025 and 1 inactive.
const fqhcFixture = `Health Center Type,Health Center Number,BHCMIS Organization Identification Number,BPHC Assigned Number,Site Name,Site Address,Site City,Site State Abbreviation,Site Postal Code,Site Telephone Number,Site Web Address,Operating Hours per Week,Health Center Location Setting Identification Number,Health Center Service Delivery Site Location Setting Description,Health Center Status Identification Number,Site Status Description,FQHC Site Medicare Billing Number,FQHC Site NPI Number,Health Center Location Identification Number,Health Center Location Type Description,Health Center Type Identification Number,Health Center Type Description,Health Center Operator Identification Number,Health Center Operator Description,Health Center Operating Schedule Identification Number,Health Center Operational Schedule Description,Health Center Operating Calendar Surrogate Key,Health Center Operating Calendar,Site Added to Scope this Date,Health Center Name,Health Center Organization Street Address,Health Center Organization City,Health Center Organization State,Health Center Organization ZIP Code,Grantee Organization Type Description,Geocoding Artifact Address Primary X Coordinate,Geocoding Artifact Address Primary Y Coordinate,U.S. - Mexico Border 100 Kilometer Indicator,U.S. - Mexico Border County Indicator,State and County Federal Information Processing Standard Code,Complete County Name,County Equivalent Name,County Description,HHS Region Code,HHS Region Name,State FIPS Code,State Name,State FIPS and Congressional District Number Code,Congressional District Number,Congressional District Name,Congressional District Code,U.S. Congressional Representative Name,Name of U.S. Senator Number One,Name of U.S. Senator Number Two,Data Warehouse Record Create Date,
"Federally Qualified Health Center (FQHC)","H80CS00001","111111","BPS-H80-000001","Dane Health Site A","100 Main St","Madison","WI","53703","608-555-0001","","40.00","1","Community Health Center","1","Active","","","1","Permanent","2","Service Delivery Site","1","Health Center/Applicant","1","Full-Time","1","Year-Round","01/01/2010","Dane Health","100 Main St","Madison","WI","53703","Corporate Entity, Federal Tax Exempt","-89.4","43.0","N","N","55025","Dane County","Dane","County","05","Region 5","55","Wisconsin","5501","01","Wisconsin District 01","WI-01","Rep A","Sen B","Sen C","04/14/2026"
"Federally Qualified Health Center (FQHC)","H80CS00002","111112","BPS-H80-000002","Dane Health Site B","200 Main St","Madison","WI","53703","608-555-0002","","40.00","1","Community Health Center","1","Active","","","2","Permanent","2","Service Delivery Site","1","Health Center/Applicant","1","Full-Time","1","Year-Round","01/01/2012","Dane Health","100 Main St","Madison","WI","53703","Corporate Entity, Federal Tax Exempt","-89.4","43.0","N","N","55025","Dane County","Dane","County","05","Region 5","55","Wisconsin","5501","01","Wisconsin District 01","WI-01","Rep A","Sen B","Sen C","04/14/2026"
"Federally Qualified Health Center (FQHC)","H80CS00003","111113","BPS-H80-000003","Dane Health Site C (Closed)","300 Main St","Madison","WI","53703","608-555-0003","","0.00","1","Community Health Center","2","Inactive","","","3","Permanent","2","Service Delivery Site","1","Health Center/Applicant","1","Full-Time","1","Year-Round","01/01/2005","Dane Health","100 Main St","Madison","WI","53703","Corporate Entity, Federal Tax Exempt","-89.4","43.0","N","N","55025","Dane County","Dane","County","05","Region 5","55","Wisconsin","5501","01","Wisconsin District 01","WI-01","Rep A","Sen B","Sen C","04/14/2026"
`

// newMockHRSASource creates an hrsaSource wired to a test server that serves
// the four CSV fixtures at the paths the adapter expects.
func newMockHRSASource(ts *httptest.Server) *hrsaSource {
	s := NewHRSASource(HRSAConfig{
		Year:       2024,
		HTTPClient: ts.Client(),
	})
	// Override the cached URLs to point at the test server.
	s.cache[hrsaHPSAPCURL] = nil // ensure fetch is attempted
	return s
}

// setupHRSAMockServer creates a test HTTP server that routes the four CSV paths.
func setupHRSAMockServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	// Map the CSV filenames to their fixture content.
	routes := map[string]string{
		"/DataDownload/DD_Files/BCD_HPSA_FCT_DET_PC.csv":                              hpsaFixturePC,
		"/DataDownload/DD_Files/BCD_HPSA_FCT_DET_DH.csv":                              hpsaFixtureDH,
		"/DataDownload/DD_Files/BCD_HPSA_FCT_DET_MH.csv":                              hpsaFixtureMH,
		"/DataDownload/DD_Files/Health_Center_Service_Delivery_and_LookAlike_Sites.csv": fqhcFixture,
	}

	for path, body := range routes {
		body := body // capture
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/csv")
			_, _ = w.Write([]byte(body))
		})
	}

	return httptest.NewServer(mux)
}

// patchHRSAURLs replaces the package-level URL constants in the source by
// using the in-process cache to inject pre-parsed rows from the test server
// fixtures. This avoids the need for a real HTTP call to data.hrsa.gov.
func patchHRSAURLs(s *hrsaSource, ts *httptest.Server) {
	ctx := context.Background()

	// Override the HTTP client to use the test server's client.
	s.cfg.HTTPClient = ts.Client()

	// Pre-populate cache with rows fetched from the mock server (using
	// test-server-relative URLs that match the real URL path suffixes).
	for realURL, path := range map[string]string{
		hrsaHPSAPCURL: "/DataDownload/DD_Files/BCD_HPSA_FCT_DET_PC.csv",
		hrsaHPSADHURL: "/DataDownload/DD_Files/BCD_HPSA_FCT_DET_DH.csv",
		hrsaHPSAMHURL: "/DataDownload/DD_Files/BCD_HPSA_FCT_DET_MH.csv",
		hrsaFQHCURL:   "/DataDownload/DD_Files/Health_Center_Service_Delivery_and_LookAlike_Sites.csv",
	} {
		testURL := ts.URL + path
		rows, err := s.csvRows(ctx, testURL)
		if err == nil {
			s.cacheMu.Lock()
			s.cache[realURL] = rows
			s.cacheMu.Unlock()
		}
	}
}

// TestNewHRSASource validates the adapter's identity metadata.
func TestNewHRSASource(t *testing.T) {
	s := NewHRSASource(HRSAConfig{Year: 2024})
	if s.Name() != "hrsa" {
		t.Errorf("Name(): want hrsa, got %q", s.Name())
	}
	if s.Category() != "health" {
		t.Errorf("Category(): want health, got %q", s.Category())
	}
	if s.Vintage() != "HRSA-2024" {
		t.Errorf("Vintage(): want HRSA-2024, got %q", s.Vintage())
	}
}

// TestNewHRSASource_NoYear validates vintage without a year.
func TestNewHRSASource_NoYear(t *testing.T) {
	s := NewHRSASource(HRSAConfig{})
	if s.Vintage() != "HRSA" {
		t.Errorf("Vintage() without year: want HRSA, got %q", s.Vintage())
	}
}

// TestHRSASchema verifies Schema() returns all expected variable definitions.
func TestHRSASchema(t *testing.T) {
	s := NewHRSASource(HRSAConfig{Year: 2024})
	schema := s.Schema()

	if len(schema) == 0 {
		t.Fatal("Schema() returned empty slice")
	}

	expectedIDs := []string{
		"hrsa_hpsa_primary_care",
		"hrsa_hpsa_dental",
		"hrsa_hpsa_mental_health",
		"hrsa_hpsa_designation",
		"hrsa_fqhc_count",
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

// TestHRSAInterface verifies HRSASource satisfies DataSource at compile time.
func TestHRSAInterface(t *testing.T) {
	var _ DataSource = NewHRSASource(HRSAConfig{Year: 2024})
}

// TestHRSARowsToIndicators_County tests the full row-to-indicator conversion
// using a mock HTTP server serving the fixture CSVs.
func TestHRSARowsToIndicators_County(t *testing.T) {
	ts := setupHRSAMockServer(t)
	defer ts.Close()

	s := NewHRSASource(HRSAConfig{Year: 2024})
	patchHRSAURLs(s, ts)

	byCounty, err := s.fetchAll(context.Background())
	if err != nil {
		t.Fatalf("fetchAll: %v", err)
	}

	// County 55025 (Dane, WI) should have data from all sources.
	rec, ok := byCounty["55025"]
	if !ok {
		t.Fatal("fetchAll: missing county 55025")
	}

	// Primary care score should be 18.
	if rec.primaryCareScore == nil {
		t.Error("primaryCareScore: want 18, got nil")
	} else if *rec.primaryCareScore != 18 {
		t.Errorf("primaryCareScore: want 18, got %v", *rec.primaryCareScore)
	}

	// Dental score should be 10.
	if rec.dentalScore == nil {
		t.Error("dentalScore: want 10, got nil")
	} else if *rec.dentalScore != 10 {
		t.Errorf("dentalScore: want 10, got %v", *rec.dentalScore)
	}

	// Mental health score should be nil (no rows in fixture).
	if rec.mentalHealthScore != nil {
		t.Errorf("mentalHealthScore: want nil (no MH rows), got %v", *rec.mentalHealthScore)
	}

	// hasAnyDesignation must be true.
	if !rec.hasAnyDesignation {
		t.Error("hasAnyDesignation: want true, got false")
	}

	// FQHC count: 2 active sites (1 inactive excluded).
	if rec.fqhcCount != 2 {
		t.Errorf("fqhcCount: want 2, got %d", rec.fqhcCount)
	}

	// County 55071 (Manitowoc) had only a Withdrawn HPSA — should have no record
	// (since the source only processes active designations).
	if _, found := byCounty["55071"]; found {
		t.Logf("county 55071 present in map (may have had FQHC sites) — checking no HPSA")
		if rec71 := byCounty["55071"]; rec71.primaryCareScore != nil {
			t.Errorf("55071 should have nil primaryCareScore (withdrawn), got %v", *rec71.primaryCareScore)
		}
	}
}

// TestHRSARowsToIndicators_Indicators tests that countyDataToIndicators
// produces correct store.Indicator values.
func TestHRSARowsToIndicators_Indicators(t *testing.T) {
	ts := setupHRSAMockServer(t)
	defer ts.Close()

	s := NewHRSASource(HRSAConfig{Year: 2024})
	patchHRSAURLs(s, ts)

	indicators, err := s.FetchCounty(context.Background(), "55", "025")
	if err != nil {
		t.Fatalf("FetchCounty: %v", err)
	}
	if len(indicators) == 0 {
		t.Fatal("FetchCounty: got no indicators")
	}

	byVar := make(map[string]float64)
	byVarNil := make(map[string]bool)
	for _, ind := range indicators {
		if ind.GEOID != "55025" {
			t.Errorf("unexpected GEOID %q (want 55025)", ind.GEOID)
		}
		if ind.Vintage != "HRSA-2024" {
			t.Errorf("indicator %q vintage: want HRSA-2024, got %q", ind.VariableID, ind.Vintage)
		}
		if ind.Value == nil {
			byVarNil[ind.VariableID] = true
		} else {
			byVar[ind.VariableID] = *ind.Value
		}
	}

	if v, ok := byVar["hrsa_hpsa_primary_care"]; !ok || v != 18 {
		t.Errorf("hrsa_hpsa_primary_care: want 18, got %v (nil=%v)", v, byVarNil["hrsa_hpsa_primary_care"])
	}
	if v, ok := byVar["hrsa_hpsa_dental"]; !ok || v != 10 {
		t.Errorf("hrsa_hpsa_dental: want 10, got %v (nil=%v)", v, byVarNil["hrsa_hpsa_dental"])
	}
	// Mental health is nil in fixture — the indicator should be present but value nil.
	if !byVarNil["hrsa_hpsa_mental_health"] {
		t.Errorf("hrsa_hpsa_mental_health: want nil value (no MH designation in fixture)")
	}
	if v, ok := byVar["hrsa_hpsa_designation"]; !ok || v != 1 {
		t.Errorf("hrsa_hpsa_designation: want 1, got %v (nil=%v)", v, byVarNil["hrsa_hpsa_designation"])
	}
	if v, ok := byVar["hrsa_fqhc_count"]; !ok || v != 2 {
		t.Errorf("hrsa_fqhc_count: want 2, got %v (nil=%v)", v, byVarNil["hrsa_fqhc_count"])
	}
}

// TestHRSAFetchCounty_UnknownCounty verifies that a county with no HRSA
// records returns nil, nil (not an error).
func TestHRSAFetchCounty_UnknownCounty(t *testing.T) {
	ts := setupHRSAMockServer(t)
	defer ts.Close()

	s := NewHRSASource(HRSAConfig{Year: 2024})
	patchHRSAURLs(s, ts)

	// County 01001 (Autauga, AL) has no rows in the fixture.
	indicators, err := s.FetchCounty(context.Background(), "01", "001")
	if err != nil {
		t.Fatalf("FetchCounty unknown county: unexpected error: %v", err)
	}
	if len(indicators) != 0 {
		t.Errorf("FetchCounty unknown county: want 0 indicators, got %d", len(indicators))
	}
}

// TestHRSAFetchState returns results only for counties in the requested state.
func TestHRSAFetchState(t *testing.T) {
	ts := setupHRSAMockServer(t)
	defer ts.Close()

	s := NewHRSASource(HRSAConfig{Year: 2024})
	patchHRSAURLs(s, ts)

	indicators, err := s.FetchState(context.Background(), "55")
	if err != nil {
		t.Fatalf("FetchState: %v", err)
	}
	if len(indicators) == 0 {
		t.Fatal("FetchState(55): expected indicators for Wisconsin, got none")
	}
	for _, ind := range indicators {
		if !strings.HasPrefix(ind.GEOID, "55") {
			t.Errorf("FetchState(55): indicator GEOID %q does not start with 55", ind.GEOID)
		}
	}
}

// TestHRSAFetchState_WrongState verifies FetchState filters out other states.
func TestHRSAFetchState_WrongState(t *testing.T) {
	ts := setupHRSAMockServer(t)
	defer ts.Close()

	s := NewHRSASource(HRSAConfig{Year: 2024})
	patchHRSAURLs(s, ts)

	// State 01 (Alabama) has no rows in the fixture.
	indicators, err := s.FetchState(context.Background(), "01")
	if err != nil {
		t.Fatalf("FetchState(01): unexpected error: %v", err)
	}
	if len(indicators) != 0 {
		t.Errorf("FetchState(01): want 0 indicators for Alabama (not in fixture), got %d", len(indicators))
	}
}

// TestHRSAAggregateHPSA_WithdrawnIgnored verifies that withdrawn HPSA rows
// are excluded from aggregation.
func TestHRSAAggregateHPSA_WithdrawnIgnored(t *testing.T) {
	s := NewHRSASource(HRSAConfig{Year: 2024})
	byCounty := make(map[string]*countyRecord)

	// Parse the primary care fixture directly.
	rows, err := s.csvRows(context.Background(), hrsaHPSAPCURL)
	if err == nil && rows != nil {
		// This shouldn't succeed without a mock server; fall through to manual test.
		_ = rows
	}

	// Manually inject rows that mirror the fixture:
	// row 1: county 55025, score 18, status D (Designated)
	// row 2: county 55071, score 12, status W (Withdrawn)
	manualRows := [][]string{
		// header
		{"HPSA Name", "HPSA Score", "HPSA Status Code",
			"State and County Federal Information Processing Standard Code"},
		// designated
		{"Dane HPSA", "18", "D", "55025"},
		// withdrawn — must be ignored
		{"Manitowoc HPSA", "12", "W", "55071"},
	}

	s.aggregateHPSA(byCounty, manualRows, "primary_care")

	if _, ok := byCounty["55025"]; !ok {
		t.Error("aggregateHPSA: 55025 should be present (designated)")
	}
	if _, ok := byCounty["55071"]; ok {
		t.Error("aggregateHPSA: 55071 should NOT be present (withdrawn)")
	}
}

// TestHRSAAggregateHPSA_MaxScore verifies that when multiple rows exist for
// the same county, the highest score is retained.
func TestHRSAAggregateHPSA_MaxScore(t *testing.T) {
	s := NewHRSASource(HRSAConfig{Year: 2024})
	byCounty := make(map[string]*countyRecord)

	rows := [][]string{
		{"HPSA Name", "HPSA Score", "HPSA Status Code",
			"State and County Federal Information Processing Standard Code"},
		{"Low Shortage", "5", "D", "55025"},
		{"High Shortage", "22", "D", "55025"},
		{"Medium Shortage", "14", "D", "55025"},
	}
	s.aggregateHPSA(byCounty, rows, "primary_care")

	rec, ok := byCounty["55025"]
	if !ok {
		t.Fatal("county 55025 not found")
	}
	if rec.primaryCareScore == nil {
		t.Fatal("primaryCareScore is nil")
	}
	if *rec.primaryCareScore != 22 {
		t.Errorf("max score: want 22, got %v", *rec.primaryCareScore)
	}
}

// TestHRSAAggregateFQHC_InactiveIgnored verifies that inactive FQHC sites
// are not counted.
func TestHRSAAggregateFQHC_InactiveIgnored(t *testing.T) {
	s := NewHRSASource(HRSAConfig{Year: 2024})
	byCounty := make(map[string]*countyRecord)

	rows := [][]string{
		{"Site Name", "Site Status Description",
			"State and County Federal Information Processing Standard Code"},
		{"Active Site 1", "Active", "55025"},
		{"Active Site 2", "Active", "55025"},
		{"Inactive Site", "Inactive", "55025"},
		{"Closed Site", "Closed", "55025"},
	}
	s.aggregateFQHC(byCounty, rows)

	rec, ok := byCounty["55025"]
	if !ok {
		t.Fatal("county 55025 not found")
	}
	if rec.fqhcCount != 2 {
		t.Errorf("fqhcCount: want 2 active, got %d", rec.fqhcCount)
	}
}

// TestHRSADesignationFlag_FalseWhenNoDesignation verifies the designation
// flag is 0 for counties with only FQHC sites and no HPSA.
func TestHRSADesignationFlag_FalseWhenNoDesignation(t *testing.T) {
	s := NewHRSASource(HRSAConfig{Year: 2024})
	byCounty := make(map[string]*countyRecord)

	// Only inject an FQHC — no HPSA.
	fqhcRows := [][]string{
		{"Site Name", "Site Status Description",
			"State and County Federal Information Processing Standard Code"},
		{"Health Center A", "Active", "55025"},
	}
	s.aggregateFQHC(byCounty, fqhcRows)

	rec := byCounty["55025"]
	indicators := s.countyDataToIndicators("55025", rec)

	var desigVal *float64
	for _, ind := range indicators {
		if ind.VariableID == "hrsa_hpsa_designation" {
			desigVal = ind.Value
		}
	}
	if desigVal == nil {
		t.Fatal("hrsa_hpsa_designation: expected non-nil value (even when 0)")
	}
	if *desigVal != 0 {
		t.Errorf("hrsa_hpsa_designation: want 0 (no HPSA), got %v", *desigVal)
	}
}

// TestHRSAFetchCounty_BadFIPS verifies that malformed FIPS returns an error.
func TestHRSAFetchCounty_BadFIPS(t *testing.T) {
	s := NewHRSASource(HRSAConfig{Year: 2024})
	_, err := s.FetchCounty(context.Background(), "5", "25") // wrong lengths → 3 digits
	if err == nil {
		t.Error("FetchCounty with bad FIPS: expected error, got nil")
	}
}
