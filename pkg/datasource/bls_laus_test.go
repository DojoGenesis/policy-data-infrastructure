package datasource

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ---- Helpers ----------------------------------------------------------------

// blsJSONResponse builds a minimal BLS API JSON response for the given series data.
// seriesData maps seriesID → []blsDataEntry.
func blsJSONResponse(status string, seriesData map[string][]blsDataEntry) []byte {
	type apiSeries struct {
		SeriesID string         `json:"seriesID"`
		Data     []blsDataEntry `json:"data"`
	}
	type results struct {
		Series []apiSeries `json:"series"`
	}
	type apiResp struct {
		Status  string   `json:"status"`
		Message []string `json:"message"`
		Results results  `json:"Results"`
	}

	var series []apiSeries
	for sid, data := range seriesData {
		series = append(series, apiSeries{SeriesID: sid, Data: data})
	}

	resp := apiResp{
		Status:  status,
		Message: []string{},
		Results: results{Series: series},
	}
	b, _ := json.Marshal(resp)
	return b
}

// newBLSMockServer creates a test HTTP server that routes:
//
//   - POST /publicAPI/v2/timeseries/data/ → BLS API mock
//   - GET  /data/{year}/acs/acs5         → Census county list mock
//
// handler receives the POST body ([]byte) and returns a BLS response.
// countyListJSON is the raw JSON for the Census county list endpoint.
func newBLSMockServer(t *testing.T, blsHandler func(body []byte) []byte, countyListJSON []byte) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/publicAPI/v2/timeseries/data/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body []byte
		if r.Body != nil {
			buf := make([]byte, 65536)
			n, _ := r.Body.Read(buf)
			body = buf[:n]
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(blsHandler(body))
	})

	// Census county list: match /data/*/acs/acs5
	mux.HandleFunc("/data/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(countyListJSON)
	})

	return httptest.NewServer(mux)
}

// newBLSSourceWithServer creates a BLSLAUSSource pointed at ts instead of the
// real BLS API. The BLS API URL is overridden by replacing the host in postBLS.
// Because the blsAPIURL constant is package-level, we test through the mock
// server by constructing the source with a custom HTTPClient that redirects all
// requests to ts.
func newBLSSourceWithServer(ts *httptest.Server, year int) *blsLAUSSource {
	s := NewBLSLAUSSource(BLSLAUSConfig{
		Year:       year,
		HTTPClient: ts.Client(),
	})
	// Patch the internal URL targets so that all HTTP calls hit the test server.
	// We do this by wrapping the http.Client with a transport that rewrites the host.
	s.cfg.HTTPClient = &http.Client{
		Transport: &blsHostRewriter{real: ts.URL, delegate: ts.Client().Transport},
	}
	return s
}

// blsHostRewriter is an http.RoundTripper that rewrites all outgoing requests
// to point at a single test server base URL, preserving the path.
type blsHostRewriter struct {
	real     string // test server base URL (e.g. "http://127.0.0.1:PORT")
	delegate http.RoundTripper
}

func (r *blsHostRewriter) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = "http"
	// Strip scheme+host from real and replace.
	realURL := r.real
	if strings.HasPrefix(realURL, "http://") {
		realURL = realURL[7:]
	}
	clone.URL.Host = realURL
	if r.delegate != nil {
		return r.delegate.RoundTrip(clone)
	}
	return http.DefaultTransport.RoundTrip(clone)
}

// countyListFixture returns Census-style JSON for a minimal county list.
// Wisconsin (55) has 2 counties in this fixture: 001 and 025.
var countyListFixtureWI = func() []byte {
	rows := [][]string{
		{"NAME", "state", "county"},
		{"Adams County, Wisconsin", "55", "001"},
		{"Dane County, Wisconsin", "55", "025"},
	}
	b, _ := json.Marshal(rows)
	return b
}()

// monthlyEntries produces 12 monthly BLS entries (M01-M12) for a given year+value.
func monthlyEntries(year int, value float64) []blsDataEntry {
	yearStr := fmt.Sprintf("%d", year)
	entries := make([]blsDataEntry, 12)
	for i := 0; i < 12; i++ {
		entries[i] = blsDataEntry{
			Year:   yearStr,
			Period: fmt.Sprintf("M%02d", i+1),
			Value:  fmt.Sprintf("%.1f", value),
		}
	}
	return entries
}

// ---- Tests ------------------------------------------------------------------

// TestNewBLSLAUSSource validates the adapter's identity metadata.
func TestBLSLAUSNewSource(t *testing.T) {
	s := NewBLSLAUSSource(BLSLAUSConfig{Year: 2023})
	if s.Name() != "bls-laus" {
		t.Errorf("Name(): want bls-laus, got %q", s.Name())
	}
	if s.Category() != "economic" {
		t.Errorf("Category(): want economic, got %q", s.Category())
	}
	if s.Vintage() != "BLS-LAUS-2023" {
		t.Errorf("Vintage(): want BLS-LAUS-2023, got %q", s.Vintage())
	}
}

// TestBLSLAUSNewSource_NoYear validates vintage without a year.
func TestBLSLAUSNewSource_NoYear(t *testing.T) {
	s := NewBLSLAUSSource(BLSLAUSConfig{})
	if s.Vintage() != "BLS-LAUS" {
		t.Errorf("Vintage() without year: want BLS-LAUS, got %q", s.Vintage())
	}
}

// TestBLSLAUSSchema verifies Schema() returns all four expected variable definitions.
func TestBLSLAUSSchema(t *testing.T) {
	s := NewBLSLAUSSource(BLSLAUSConfig{Year: 2023})
	schema := s.Schema()

	expectedIDs := []string{
		"bls_laus_unemployment_rate",
		"bls_laus_unemployment_count",
		"bls_laus_employment_count",
		"bls_laus_labor_force",
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
		"bls_laus_unemployment_rate":  "lower_better",
		"bls_laus_unemployment_count": "lower_better",
		"bls_laus_employment_count":   "higher_better",
		"bls_laus_labor_force":        "neutral",
	}
	for id, want := range dirChecks {
		if def, ok := byID[id]; ok && def.Direction != want {
			t.Errorf("%s Direction: want %q, got %q", id, want, def.Direction)
		}
	}
}

// TestBLSLAUSInterface verifies BLSLAUSSource satisfies DataSource at compile time.
func TestBLSLAUSInterface(t *testing.T) {
	var _ DataSource = NewBLSLAUSSource(BLSLAUSConfig{Year: 2023})
}

// TestBLSLAUSSeriesID_FillZeros is the critical test: series IDs must contain
// exactly 8 fill zeros, not 7.
func TestBLSLAUSSeriesID_FillZeros(t *testing.T) {
	cases := []struct {
		state, county, measure string
		want                   string
	}{
		// Wisconsin Dane County unemployment rate: LAUCN(5)+55(2)+025(3)+00000000(8)+03(2) = 20 chars
		{"55", "025", "03", "LAUCN550250000000003"},
		// Wisconsin Dane County labor force
		{"55", "025", "06", "LAUCN550250000000006"},
		// Alabama Autauga County employed
		{"01", "001", "05", "LAUCN010010000000005"},
		// California Los Angeles County unemployed count
		{"06", "037", "04", "LAUCN060370000000004"},
	}

	for _, tc := range cases {
		got := blsSeriesID(tc.state, tc.county, tc.measure)
		if got != tc.want {
			t.Errorf("blsSeriesID(%q,%q,%q) = %q; want %q", tc.state, tc.county, tc.measure, got, tc.want)
		}
		// Verify the length: LAUCN(5) + state(2) + county(3) + zeros(8) + measure(2) = 20 chars
		if len(got) != 20 {
			t.Errorf("blsSeriesID %q has length %d (want 20)", got, len(got))
		}
		// Verify the fill zeros are exactly 8 (positions 10-17 inclusive).
		zeros := got[10:18] // characters at index 10,11,12,13,14,15,16,17
		if zeros != "00000000" {
			t.Errorf("blsSeriesID %q fill zeros %q at [10:18] want 00000000", got, zeros)
		}
	}
}

// TestBLSLAUSSeriesID_AllFour verifies that blsSeriesIDs returns 4 series IDs
// for one county, each with the correct measure code.
func TestBLSLAUSSeriesID_AllFour(t *testing.T) {
	ids := blsSeriesIDs("55", "025")
	if len(ids) != 4 {
		t.Fatalf("blsSeriesIDs: want 4, got %d", len(ids))
	}

	measureCodes := map[string]bool{
		blsMeasureUnemploymentRate: false,
		blsMeasureUnemployedCount:  false,
		blsMeasureEmployedCount:    false,
		blsMeasureLaborForce:       false,
	}
	for _, id := range ids {
		// Last 2 chars are the measure code.
		code := id[len(id)-2:]
		if _, ok := measureCodes[code]; ok {
			measureCodes[code] = true
		}
	}
	for code, seen := range measureCodes {
		if !seen {
			t.Errorf("measure code %q not present in blsSeriesIDs output", code)
		}
	}
}

// TestBLSLAUSBatchSize verifies the batch size logic.
func TestBLSLAUSBatchSize(t *testing.T) {
	noKey := NewBLSLAUSSource(BLSLAUSConfig{Year: 2023})
	if noKey.batchSize() != blsBatchSizeUnregistered {
		t.Errorf("batchSize without key: want %d, got %d", blsBatchSizeUnregistered, noKey.batchSize())
	}

	withKey := NewBLSLAUSSource(BLSLAUSConfig{Year: 2023, APIKey: "test-key"})
	if withKey.batchSize() != blsBatchSizeRegistered {
		t.Errorf("batchSize with key: want %d, got %d", blsBatchSizeRegistered, withKey.batchSize())
	}
}

// TestBLSLAUSChunkStrings verifies the batch-chunking helper function.
func TestBLSLAUSChunkStrings(t *testing.T) {
	ss := make([]string, 10)
	for i := range ss {
		ss[i] = fmt.Sprintf("S%02d", i)
	}

	chunks := chunkStrings(ss, 3)
	if len(chunks) != 4 {
		t.Fatalf("chunkStrings(10,3): want 4 chunks, got %d", len(chunks))
	}
	if len(chunks[0]) != 3 {
		t.Errorf("chunk[0] len: want 3, got %d", len(chunks[0]))
	}
	if len(chunks[3]) != 1 { // 10 mod 3 = 1
		t.Errorf("chunk[3] len: want 1, got %d", len(chunks[3]))
	}

	// Exact chunk, no remainder.
	exact := chunkStrings(ss, 5)
	if len(exact) != 2 {
		t.Errorf("chunkStrings(10,5): want 2 chunks, got %d", len(exact))
	}
	for i, c := range exact {
		if len(c) != 5 {
			t.Errorf("exact chunk[%d] len: want 5, got %d", i, len(c))
		}
	}

	// Edge: chunk size >= slice.
	all := chunkStrings(ss, 25)
	if len(all) != 1 || len(all[0]) != 10 {
		t.Errorf("chunkStrings(10,25): want 1 chunk of 10, got %v", all)
	}

	// Edge: empty slice.
	empty := chunkStrings(nil, 5)
	if len(empty) != 0 {
		t.Errorf("chunkStrings(nil,5): want empty, got %v", empty)
	}
}

// TestBLSLAUSChunkStrings_WiFull verifies that 288 WI series (72 counties × 4
// measures) batch correctly at 25 series/request without a key.
func TestBLSLAUSChunkStrings_WiFull(t *testing.T) {
	const wiCounties = 72
	const measures = 4
	total := wiCounties * measures // 288

	series := make([]string, total)
	for i := range series {
		series[i] = fmt.Sprintf("SID%03d", i)
	}

	chunks := chunkStrings(series, blsBatchSizeUnregistered) // 25 per chunk
	want := (total + blsBatchSizeUnregistered - 1) / blsBatchSizeUnregistered // ceil(288/25) = 12
	if len(chunks) != want {
		t.Errorf("288 series / 25 per batch: want %d chunks, got %d", want, len(chunks))
	}

	// Every chunk must be ≤ 25 series.
	for i, c := range chunks {
		if len(c) > blsBatchSizeUnregistered {
			t.Errorf("chunk[%d] has %d series (exceeds limit %d)", i, len(c), blsBatchSizeUnregistered)
		}
	}
}

// TestBLSLAUSExtractAnnual_M13 verifies that M13 (official annual average)
// is preferred over computed monthly average.
func TestBLSLAUSExtractAnnual_M13(t *testing.T) {
	sr := blsSeriesResult{
		SeriesID: "LAUCN550250000000003",
		Data: []blsDataEntry{
			{Year: "2023", Period: "M01", Value: "3.0"},
			{Year: "2023", Period: "M02", Value: "3.2"},
			{Year: "2023", Period: "M13", Value: "3.1"}, // official annual average
		},
	}

	val := blsExtractAnnual(sr, 2023)
	if val == nil {
		t.Fatal("blsExtractAnnual: want non-nil, got nil")
	}
	if *val != 3.1 {
		t.Errorf("blsExtractAnnual with M13: want 3.1, got %v", *val)
	}
}

// TestBLSLAUSExtractAnnual_MonthlyComputed verifies client-side averaging from
// 12 monthly values when M13 is absent.
func TestBLSLAUSExtractAnnual_MonthlyComputed(t *testing.T) {
	// All months equal 4.0 → annual average = 4.0
	sr := blsSeriesResult{
		SeriesID: "LAUCN550250000000003",
		Data:     monthlyEntries(2023, 4.0),
	}

	val := blsExtractAnnual(sr, 2023)
	if val == nil {
		t.Fatal("blsExtractAnnual: want non-nil, got nil")
	}
	if *val != 4.0 {
		t.Errorf("blsExtractAnnual (monthly avg): want 4.0, got %v", *val)
	}
}

// TestBLSLAUSExtractAnnual_MonthlyRounding verifies the 1-decimal rounding.
func TestBLSLAUSExtractAnnual_MonthlyRounding(t *testing.T) {
	// Construct entries that average to 3.1666..., expecting 3.2 after round(1dp).
	values := []float64{3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 4.0, 4.0}
	yearStr := "2023"
	entries := make([]blsDataEntry, len(values))
	for i, v := range values {
		entries[i] = blsDataEntry{Year: yearStr, Period: fmt.Sprintf("M%02d", i+1), Value: fmt.Sprintf("%.1f", v)}
	}
	// sum = 10*3.0 + 2*4.0 = 38.0; avg = 38/12 = 3.1666...; rounded = 3.2
	sr := blsSeriesResult{SeriesID: "LAUCN550250000000003", Data: entries}
	val := blsExtractAnnual(sr, 2023)
	if val == nil {
		t.Fatal("blsExtractAnnual: want non-nil, got nil")
	}
	if *val != 3.2 {
		t.Errorf("blsExtractAnnual rounding: want 3.2, got %v", *val)
	}
}

// TestBLSLAUSExtractAnnual_NoData verifies nil return when no data for the year.
func TestBLSLAUSExtractAnnual_NoData(t *testing.T) {
	sr := blsSeriesResult{
		SeriesID: "LAUCN550250000000003",
		Data: []blsDataEntry{
			// Data for a different year.
			{Year: "2022", Period: "M01", Value: "3.0"},
		},
	}
	val := blsExtractAnnual(sr, 2023)
	if val != nil {
		t.Errorf("blsExtractAnnual no 2023 data: want nil, got %v", *val)
	}
}

// TestBLSLAUSExtractAnnual_PartialYear verifies that partial monthly data still
// produces an average (BLS may suppress some months).
func TestBLSLAUSExtractAnnual_PartialYear(t *testing.T) {
	// Only 6 months available.
	sr := blsSeriesResult{
		SeriesID: "LAUCN550250000000003",
		Data: []blsDataEntry{
			{Year: "2023", Period: "M01", Value: "4.0"},
			{Year: "2023", Period: "M02", Value: "4.0"},
			{Year: "2023", Period: "M03", Value: "4.0"},
			{Year: "2023", Period: "M04", Value: "4.0"},
			{Year: "2023", Period: "M05", Value: "4.0"},
			{Year: "2023", Period: "M06", Value: "4.0"},
		},
	}
	val := blsExtractAnnual(sr, 2023)
	if val == nil {
		t.Fatal("blsExtractAnnual partial year: want non-nil, got nil")
	}
	if *val != 4.0 {
		t.Errorf("blsExtractAnnual partial year: want 4.0, got %v", *val)
	}
}

// TestBLSLAUSExtractAnnual_InvalidValue verifies graceful handling of non-numeric BLS values.
func TestBLSLAUSExtractAnnual_InvalidValue(t *testing.T) {
	sr := blsSeriesResult{
		SeriesID: "LAUCN550250000000003",
		Data: []blsDataEntry{
			{Year: "2023", Period: "M01", Value: "N.A."}, // BLS suppression marker
			{Year: "2023", Period: "M02", Value: "3.0"},
		},
	}
	val := blsExtractAnnual(sr, 2023)
	// Only 1 valid month — should still compute average from 1 value.
	if val == nil {
		t.Fatal("blsExtractAnnual with 1 valid month: want non-nil, got nil")
	}
	if *val != 3.0 {
		t.Errorf("blsExtractAnnual 1 valid month: want 3.0, got %v", *val)
	}
}

// TestBLSLAUSFetchCounty_BadFIPS verifies FIPS validation errors.
func TestBLSLAUSFetchCounty_BadFIPS(t *testing.T) {
	s := NewBLSLAUSSource(BLSLAUSConfig{Year: 2023})

	// Bad state FIPS (1 digit).
	_, err := s.FetchCounty(context.Background(), "5", "025")
	if err == nil {
		t.Error("FetchCounty bad state FIPS: want error, got nil")
	}

	// Bad county FIPS (2 digits).
	_, err = s.FetchCounty(context.Background(), "55", "25")
	if err == nil {
		t.Error("FetchCounty bad county FIPS: want error, got nil")
	}
}

// TestBLSLAUSFetchState_BadFIPS verifies FIPS validation for FetchState.
func TestBLSLAUSFetchState_BadFIPS(t *testing.T) {
	s := NewBLSLAUSSource(BLSLAUSConfig{Year: 2023})

	_, err := s.FetchState(context.Background(), "5") // 1-digit state
	if err == nil {
		t.Error("FetchState bad state FIPS: want error, got nil")
	}
}

// TestBLSLAUSFetchCounty parses a mock BLS API response for a single county.
func TestBLSLAUSFetchCounty(t *testing.T) {
	// Build a response for Dane County WI (55025) with all 4 measures.
	// Use M13 (official annual average) for clean values.
	seriesData := map[string][]blsDataEntry{
		"LAUCN550250000000003": {{Year: "2023", Period: "M13", Value: "3.2"}}, // rate
		"LAUCN550250000000004": {{Year: "2023", Period: "M13", Value: "15000"}}, // unemployed
		"LAUCN550250000000005": {{Year: "2023", Period: "M13", Value: "350000"}}, // employed
		"LAUCN550250000000006": {{Year: "2023", Period: "M13", Value: "365000"}}, // labor force
	}

	ts := newBLSMockServer(t,
		func(_ []byte) []byte {
			return blsJSONResponse("REQUEST_SUCCEEDED", seriesData)
		},
		countyListFixtureWI, // not used in FetchCounty, but server needs to serve it
	)
	defer ts.Close()

	s := newBLSSourceWithServer(ts, 2023)

	indicators, err := s.FetchCounty(context.Background(), "55", "025")
	if err != nil {
		t.Fatalf("FetchCounty: %v", err)
	}
	if len(indicators) != 4 {
		t.Fatalf("FetchCounty: want 4 indicators, got %d", len(indicators))
	}

	byVar := make(map[string]float64)
	for _, ind := range indicators {
		if ind.GEOID != "55025" {
			t.Errorf("indicator GEOID: want 55025, got %q", ind.GEOID)
		}
		if ind.Vintage != "BLS-LAUS-2023" {
			t.Errorf("indicator %q vintage: want BLS-LAUS-2023, got %q", ind.VariableID, ind.Vintage)
		}
		if ind.Value == nil {
			t.Errorf("indicator %q: value is nil", ind.VariableID)
			continue
		}
		byVar[ind.VariableID] = *ind.Value
	}

	if v, ok := byVar["bls_laus_unemployment_rate"]; !ok || v != 3.2 {
		t.Errorf("bls_unemployment_rate: want 3.2, got %v (ok=%v)", v, ok)
	}
	if v, ok := byVar["bls_laus_unemployment_count"]; !ok || v != 15000 {
		t.Errorf("bls_unemployment_count: want 15000, got %v (ok=%v)", v, ok)
	}
	if v, ok := byVar["bls_laus_employment_count"]; !ok || v != 350000 {
		t.Errorf("bls_employment_count: want 350000, got %v (ok=%v)", v, ok)
	}
	if v, ok := byVar["bls_laus_labor_force"]; !ok || v != 365000 {
		t.Errorf("bls_labor_force: want 365000, got %v (ok=%v)", v, ok)
	}
}

// TestBLSLAUSFetchCounty_MonthlyComputed verifies that FetchCounty uses monthly
// averaging when M13 is absent from the API response.
func TestBLSLAUSFetchCounty_MonthlyComputed(t *testing.T) {
	// Use 12 monthly entries for the rate series, no M13.
	seriesData := map[string][]blsDataEntry{
		"LAUCN550250000000003": monthlyEntries(2023, 4.0),
		// Other series have M13 for simplicity.
		"LAUCN550250000000004": {{Year: "2023", Period: "M13", Value: "18000"}},
		"LAUCN550250000000005": {{Year: "2023", Period: "M13", Value: "430000"}},
		"LAUCN550250000000006": {{Year: "2023", Period: "M13", Value: "448000"}},
	}

	ts := newBLSMockServer(t,
		func(_ []byte) []byte {
			return blsJSONResponse("REQUEST_SUCCEEDED", seriesData)
		},
		countyListFixtureWI,
	)
	defer ts.Close()

	s := newBLSSourceWithServer(ts, 2023)

	indicators, err := s.FetchCounty(context.Background(), "55", "025")
	if err != nil {
		t.Fatalf("FetchCounty monthly: %v", err)
	}

	for _, ind := range indicators {
		if ind.VariableID == "bls_laus_unemployment_rate" {
			if ind.Value == nil {
				t.Fatal("bls_unemployment_rate: nil value")
			}
			if *ind.Value != 4.0 {
				t.Errorf("bls_unemployment_rate (monthly avg): want 4.0, got %v", *ind.Value)
			}
		}
	}
}

// TestBLSLAUSFetchCounty_MissingData verifies nil values for series with no data.
func TestBLSLAUSFetchCounty_MissingData(t *testing.T) {
	// Only the unemployment rate series has data — others return empty.
	seriesData := map[string][]blsDataEntry{
		"LAUCN550250000000003": {{Year: "2023", Period: "M13", Value: "5.0"}},
		// No data for 04, 05, 06.
	}

	ts := newBLSMockServer(t,
		func(_ []byte) []byte {
			return blsJSONResponse("REQUEST_SUCCEEDED", seriesData)
		},
		countyListFixtureWI,
	)
	defer ts.Close()

	s := newBLSSourceWithServer(ts, 2023)

	indicators, err := s.FetchCounty(context.Background(), "55", "025")
	if err != nil {
		t.Fatalf("FetchCounty missing data: %v", err)
	}
	if len(indicators) != 4 {
		t.Fatalf("FetchCounty: want 4 indicators, got %d", len(indicators))
	}

	byVar := make(map[string]*float64)
	for _, ind := range indicators {
		v := ind.Value
		byVar[ind.VariableID] = v
	}

	if byVar["bls_laus_unemployment_rate"] == nil || *byVar["bls_laus_unemployment_rate"] != 5.0 {
		t.Errorf("bls_unemployment_rate: want 5.0")
	}
	if byVar["bls_laus_unemployment_count"] != nil {
		t.Error("bls_unemployment_count: want nil (no data), got non-nil")
	}
	if byVar["bls_laus_employment_count"] != nil {
		t.Error("bls_employment_count: want nil (no data), got non-nil")
	}
	if byVar["bls_laus_labor_force"] != nil {
		t.Error("bls_labor_force: want nil (no data), got non-nil")
	}
}

// TestBLSLAUSFetchCounty_RawValuePopulated verifies that RawValue is set when
// a value is present, and empty when nil.
func TestBLSLAUSFetchCounty_RawValuePopulated(t *testing.T) {
	seriesData := map[string][]blsDataEntry{
		"LAUCN550250000000003": {{Year: "2023", Period: "M13", Value: "3.5"}},
		// Others missing.
	}

	ts := newBLSMockServer(t,
		func(_ []byte) []byte {
			return blsJSONResponse("REQUEST_SUCCEEDED", seriesData)
		},
		countyListFixtureWI,
	)
	defer ts.Close()

	s := newBLSSourceWithServer(ts, 2023)
	indicators, err := s.FetchCounty(context.Background(), "55", "025")
	if err != nil {
		t.Fatalf("FetchCounty: %v", err)
	}

	for _, ind := range indicators {
		if ind.Value != nil && ind.RawValue == "" {
			t.Errorf("%s: value non-nil but RawValue empty", ind.VariableID)
		}
		if ind.Value == nil && ind.RawValue != "" {
			t.Errorf("%s: value nil but RawValue non-empty: %q", ind.VariableID, ind.RawValue)
		}
	}
}

// TestBLSLAUSOverLimitError verifies that REQUEST_FAILED_OVER_LIMIT returns an error.
func TestBLSLAUSOverLimitError(t *testing.T) {
	ts := newBLSMockServer(t,
		func(_ []byte) []byte {
			return blsJSONResponse("REQUEST_FAILED_OVER_LIMIT", nil)
		},
		countyListFixtureWI,
	)
	defer ts.Close()

	s := newBLSSourceWithServer(ts, 2023)

	_, err := s.FetchCounty(context.Background(), "55", "025")
	if err == nil {
		t.Error("FetchCounty over-limit: want error, got nil")
	}
	if !strings.Contains(err.Error(), "REQUEST_FAILED_OVER_LIMIT") {
		t.Errorf("FetchCounty over-limit: error should mention REQUEST_FAILED_OVER_LIMIT, got: %v", err)
	}
}

// TestBLSLAUSFetchState verifies FetchState returns indicators for all
// counties in the fixture county list (55001 and 55025).
func TestBLSLAUSFetchState(t *testing.T) {
	// Build responses for both counties in the fixture.
	seriesData := map[string][]blsDataEntry{
		// County 55001 (Adams)
		"LAUCN550010000000003": {{Year: "2023", Period: "M13", Value: "4.5"}},
		"LAUCN550010000000004": {{Year: "2023", Period: "M13", Value: "800"}},
		"LAUCN550010000000005": {{Year: "2023", Period: "M13", Value: "17000"}},
		"LAUCN550010000000006": {{Year: "2023", Period: "M13", Value: "17800"}},
		// County 55025 (Dane)
		"LAUCN550250000000003": {{Year: "2023", Period: "M13", Value: "2.8"}},
		"LAUCN550250000000004": {{Year: "2023", Period: "M13", Value: "15000"}},
		"LAUCN550250000000005": {{Year: "2023", Period: "M13", Value: "520000"}},
		"LAUCN550250000000006": {{Year: "2023", Period: "M13", Value: "535000"}},
	}

	ts := newBLSMockServer(t,
		func(_ []byte) []byte {
			return blsJSONResponse("REQUEST_SUCCEEDED", seriesData)
		},
		countyListFixtureWI,
	)
	defer ts.Close()

	s := newBLSSourceWithServer(ts, 2023)

	indicators, err := s.FetchState(context.Background(), "55")
	if err != nil {
		t.Fatalf("FetchState: %v", err)
	}

	// 2 counties × 4 measures = 8 indicators.
	if len(indicators) != 8 {
		t.Errorf("FetchState: want 8 indicators (2 counties × 4 measures), got %d", len(indicators))
	}

	// All GEOIDs must start with "55".
	geoids := make(map[string]bool)
	for _, ind := range indicators {
		if !strings.HasPrefix(ind.GEOID, "55") {
			t.Errorf("FetchState(55): indicator GEOID %q does not start with 55", ind.GEOID)
		}
		geoids[ind.GEOID] = true
	}

	// Both counties must be present.
	if !geoids["55001"] {
		t.Error("FetchState(55): missing indicators for county 55001")
	}
	if !geoids["55025"] {
		t.Error("FetchState(55): missing indicators for county 55025")
	}
}

// TestBLSLAUSNoBatchDelay_SingleBatch verifies that a single-batch request
// does not wait for the rate-limit delay (context would expire otherwise).
func TestBLSLAUSNoBatchDelay_SingleBatch(t *testing.T) {
	// 4 series = 1 batch (well under the 25-series limit).
	seriesData := map[string][]blsDataEntry{
		"LAUCN550250000000003": {{Year: "2023", Period: "M13", Value: "3.0"}},
		"LAUCN550250000000004": {{Year: "2023", Period: "M13", Value: "12000"}},
		"LAUCN550250000000005": {{Year: "2023", Period: "M13", Value: "400000"}},
		"LAUCN550250000000006": {{Year: "2023", Period: "M13", Value: "412000"}},
	}

	ts := newBLSMockServer(t,
		func(_ []byte) []byte {
			return blsJSONResponse("REQUEST_SUCCEEDED", seriesData)
		},
		countyListFixtureWI,
	)
	defer ts.Close()

	s := newBLSSourceWithServer(ts, 2023)

	// Use a short-lived context; if the adapter incorrectly sleeps between
	// batches even for a single batch, this will time out.
	ctx, cancel := context.WithTimeout(context.Background(), 5*blsRateDelay)
	defer cancel()

	_, err := s.FetchCounty(ctx, "55", "025")
	if err != nil {
		t.Fatalf("FetchCounty single batch: unexpected error: %v", err)
	}
}
