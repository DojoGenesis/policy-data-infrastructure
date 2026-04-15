package datasource

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// hmdaFixture is a minimal loan-level HMDA CSV fixture.
//
// Tracts used:
//
//	55025001100 (Dane County, WI) — 3 originated, 2 denied (1 minority denied)
//	55025001200 (Dane County, WI) — 0 originated, 1 denied minority applicant
//	17031010100 (Cook County, IL) — 1 originated (should be excluded from county 55025 queries)
//
// Loan amounts: 200000, 300000, 250000 for originated in 55025001100.
// CLTV: 80, 90, 85 for those loans.
// Minority applicants in 55025001100: 2 (1 denied, 1 originated).
const hmdaFixture = `census_tract,action_taken,loan_amount,combined_loan_to_value_ratio,derived_race,derived_ethnicity
55025001100,1,200000,80,White,Not Hispanic or Latino
55025001100,1,300000,90,Black or African American,Not Hispanic or Latino
55025001100,1,250000,85,White,Not Hispanic or Latino
55025001100,3,175000,NA,Black or African American,Not Hispanic or Latino
55025001100,3,220000,NA,White,Not Hispanic or Latino
55025001200,3,180000,NA,Asian,Not Hispanic or Latino
17031010100,1,400000,75,White,Not Hispanic or Latino
`

// hmdaFixtureNoTract is a CSV with a row that has an invalid/missing census tract.
const hmdaFixtureNoTract = `census_tract,action_taken,loan_amount,combined_loan_to_value_ratio,derived_race,derived_ethnicity
NA,1,200000,80,White,Not Hispanic or Latino
55025001100,1,250000,85,White,Not Hispanic or Latino
`

// hmdaFixtureMissingCols is a CSV missing the required census_tract column.
const hmdaFixtureMissingCols = `action_taken,loan_amount
1,200000
3,175000
`

// setupHMDAMockServer creates a test HTTP server that serves hmdaFixture for
// any request path.
func setupHMDAMockServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte(body))
	})
	return httptest.NewServer(mux)
}

// newMockHMDASource creates an hmdaSource configured to use the test server.
func newMockHMDASource(ts *httptest.Server, year int) *hmdaSource {
	s := NewHMDASource(HMDAConfig{
		Year:       year,
		HTTPClient: ts.Client(),
	})
	// Override the base URL constant by patching the source's fetch logic
	// via a custom HTTP client that redirects all requests to the test server.
	// We accomplish this by replacing the transport to rewrite the host.
	s.cfg.HTTPClient = &http.Client{
		Transport: &hostRewriter{base: ts.URL, inner: ts.Client().Transport},
	}
	return s
}

// hostRewriter is a test RoundTripper that redirects all requests to a fixed
// base URL, preserving path and query.
type hostRewriter struct {
	base  string
	inner http.RoundTripper
}

func (h *hostRewriter) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	cloned.URL.Scheme = "http"
	cloned.URL.Host = strings.TrimPrefix(h.base, "http://")
	if h.inner != nil {
		return h.inner.RoundTrip(cloned)
	}
	return http.DefaultTransport.RoundTrip(cloned)
}

// --------------------------------------------------------------------------
// Identity tests
// --------------------------------------------------------------------------

// TestNewHMDASource validates the adapter's default values.
func TestNewHMDASource(t *testing.T) {
	s := NewHMDASource(HMDAConfig{Year: 2023})
	if s.Name() != "hmda" {
		t.Errorf("Name(): want hmda, got %q", s.Name())
	}
	if s.Category() != "housing" {
		t.Errorf("Category(): want housing, got %q", s.Category())
	}
	if s.Vintage() != "HMDA-2023" {
		t.Errorf("Vintage(): want HMDA-2023, got %q", s.Vintage())
	}
}

// TestNewHMDASource_DefaultYear validates that a zero Year defaults to 2023.
func TestNewHMDASource_DefaultYear(t *testing.T) {
	s := NewHMDASource(HMDAConfig{})
	if s.cfg.Year != 2023 {
		t.Errorf("default year: want 2023, got %d", s.cfg.Year)
	}
	if s.Vintage() != "HMDA-2023" {
		t.Errorf("Vintage() default year: want HMDA-2023, got %q", s.Vintage())
	}
}

// --------------------------------------------------------------------------
// Schema test
// --------------------------------------------------------------------------

// TestHMDASchema verifies Schema() returns all expected variable definitions.
func TestHMDASchema(t *testing.T) {
	s := NewHMDASource(HMDAConfig{Year: 2023})
	schema := s.Schema()

	if len(schema) == 0 {
		t.Fatal("Schema() returned empty slice")
	}

	expectedIDs := []string{
		"hmda_loan_count",
		"hmda_denial_rate",
		"hmda_median_loan_amount",
		"hmda_minority_denial_rate",
		"hmda_ltv_ratio",
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

// --------------------------------------------------------------------------
// Interface satisfaction
// --------------------------------------------------------------------------

// TestHMDAInterface verifies hmdaSource satisfies DataSource at compile time.
func TestHMDAInterface(t *testing.T) {
	var _ DataSource = NewHMDASource(HMDAConfig{Year: 2023})
}

// --------------------------------------------------------------------------
// CSV parsing tests (unit-level, no HTTP)
// --------------------------------------------------------------------------

// TestHMDAParseCSV verifies that aggregateHMDARows correctly groups loan-level
// rows into per-tract aggregates using the fixture data.
func TestHMDAParseCSV(t *testing.T) {
	rows := csvStringToRows(hmdaFixture)
	byTract := aggregateHMDARows(rows)

	// Tract 55025001100: 3 originated, 2 denied = 5 total applications.
	td, ok := byTract["55025001100"]
	if !ok {
		t.Fatal("aggregateHMDARows: missing tract 55025001100")
	}
	if td.totalApplications != 5 {
		t.Errorf("55025001100 totalApplications: want 5, got %d", td.totalApplications)
	}
	if td.denials != 2 {
		t.Errorf("55025001100 denials: want 2, got %d", td.denials)
	}
	if len(td.loanAmounts) != 3 {
		t.Errorf("55025001100 loanAmounts: want 3 entries, got %d", len(td.loanAmounts))
	}
	if len(td.cltvRatios) != 3 {
		t.Errorf("55025001100 cltvRatios: want 3 entries, got %d", len(td.cltvRatios))
	}

	// Tract 55025001200: 0 originated, 1 denied.
	td2, ok := byTract["55025001200"]
	if !ok {
		t.Fatal("aggregateHMDARows: missing tract 55025001200")
	}
	if td2.totalApplications != 1 {
		t.Errorf("55025001200 totalApplications: want 1, got %d", td2.totalApplications)
	}
	if td2.denials != 1 {
		t.Errorf("55025001200 denials: want 1, got %d", td2.denials)
	}

	// Cook County tract must be present (no filter applied at aggregate stage).
	if _, ok := byTract["17031010100"]; !ok {
		t.Error("aggregateHMDARows: missing Cook County tract 17031010100")
	}
}

// TestHMDAParseCSV_InvalidTractSkipped verifies that rows with NA/missing tract
// are skipped without error.
func TestHMDAParseCSV_InvalidTractSkipped(t *testing.T) {
	rows := csvStringToRows(hmdaFixtureNoTract)
	byTract := aggregateHMDARows(rows)

	// Only the valid 11-digit tract should be present.
	if _, ok := byTract["55025001100"]; !ok {
		t.Error("expected tract 55025001100 in result")
	}
	// NA tract must not appear.
	for k := range byTract {
		if strings.Contains(k, "NA") || len(k) != 11 {
			t.Errorf("unexpected tract key %q (expected 11-digit FIPS only)", k)
		}
	}
}

// TestHMDAParseCSV_MissingRequiredCol verifies that a CSV missing census_tract
// returns an empty map rather than panicking.
func TestHMDAParseCSV_MissingRequiredCol(t *testing.T) {
	rows := csvStringToRows(hmdaFixtureMissingCols)
	byTract := aggregateHMDARows(rows)
	if len(byTract) != 0 {
		t.Errorf("expected empty map for CSV missing census_tract, got %d entries", len(byTract))
	}
}

// --------------------------------------------------------------------------
// Denial rate calculation
// --------------------------------------------------------------------------

// TestHMDADenialRate verifies the denial rate formula:
// denials / (originated + denied) * 100
func TestHMDADenialRate(t *testing.T) {
	rows := csvStringToRows(hmdaFixture)
	byTract := aggregateHMDARows(rows)

	s := NewHMDASource(HMDAConfig{Year: 2023})

	// Tract 55025001100: 2 denied / 5 total = 40%
	td := byTract["55025001100"]
	indicators := s.tractToIndicators("55025001100", td)

	byVar := indicatorsByVar(indicators)

	denialInd, ok := byVar["hmda_denial_rate"]
	if !ok {
		t.Fatal("hmda_denial_rate indicator missing for 55025001100")
	}
	if denialInd.Value == nil {
		t.Fatal("hmda_denial_rate: value is nil")
	}
	want := 40.0 // 2/5 * 100
	if *denialInd.Value != want {
		t.Errorf("hmda_denial_rate: want %.2f, got %.2f", want, *denialInd.Value)
	}

	// Tract 55025001200: 1 denied / 1 total = 100%
	td2 := byTract["55025001200"]
	indicators2 := s.tractToIndicators("55025001200", td2)
	byVar2 := indicatorsByVar(indicators2)
	denial2, ok := byVar2["hmda_denial_rate"]
	if !ok || denial2.Value == nil {
		t.Fatal("hmda_denial_rate missing or nil for 55025001200")
	}
	if *denial2.Value != 100.0 {
		t.Errorf("55025001200 denial rate: want 100, got %.2f", *denial2.Value)
	}
}

// TestHMDADenialRate_ZeroApplications verifies that a tract with no
// applications produces a nil denial rate (not a divide-by-zero panic).
func TestHMDADenialRate_ZeroApplications(t *testing.T) {
	s := NewHMDASource(HMDAConfig{Year: 2023})
	td := &hmdaTractData{} // zero value: no applications
	indicators := s.tractToIndicators("55025001100", td)

	byVar := indicatorsByVar(indicators)
	denial, ok := byVar["hmda_denial_rate"]
	if !ok {
		t.Fatal("hmda_denial_rate indicator missing")
	}
	if denial.Value != nil {
		t.Errorf("hmda_denial_rate with 0 applications: want nil, got %v", *denial.Value)
	}
}

// --------------------------------------------------------------------------
// Minority denial rate
// --------------------------------------------------------------------------

// TestHMDAMinorityDenialRate verifies minority denial rate calculation.
// In the fixture for 55025001100:
//   - 2 minority applicants: 1 originated (Black), 1 denied (Black)
//   - minority denial rate = 1/2 * 100 = 50%
func TestHMDAMinorityDenialRate(t *testing.T) {
	rows := csvStringToRows(hmdaFixture)
	byTract := aggregateHMDARows(rows)
	s := NewHMDASource(HMDAConfig{Year: 2023})

	td := byTract["55025001100"]
	// Verify counts from aggregation.
	if td.minorityApplications != 2 {
		t.Errorf("55025001100 minorityApplications: want 2, got %d", td.minorityApplications)
	}
	if td.minorityDenials != 1 {
		t.Errorf("55025001100 minorityDenials: want 1, got %d", td.minorityDenials)
	}

	indicators := s.tractToIndicators("55025001100", td)
	byVar := indicatorsByVar(indicators)
	mdr, ok := byVar["hmda_minority_denial_rate"]
	if !ok || mdr.Value == nil {
		t.Fatal("hmda_minority_denial_rate missing or nil")
	}
	want := 50.0
	if *mdr.Value != want {
		t.Errorf("minority denial rate: want %.1f, got %.2f", want, *mdr.Value)
	}
}

// --------------------------------------------------------------------------
// Median loan amount and LTV
// --------------------------------------------------------------------------

// TestHMDAMedianLoanAmount verifies the median calculation.
// Amounts: 200000, 300000, 250000 → sorted: 200000, 250000, 300000 → median 250000.
func TestHMDAMedianLoanAmount(t *testing.T) {
	rows := csvStringToRows(hmdaFixture)
	byTract := aggregateHMDARows(rows)
	s := NewHMDASource(HMDAConfig{Year: 2023})

	td := byTract["55025001100"]
	indicators := s.tractToIndicators("55025001100", td)
	byVar := indicatorsByVar(indicators)

	mla, ok := byVar["hmda_median_loan_amount"]
	if !ok || mla.Value == nil {
		t.Fatal("hmda_median_loan_amount missing or nil")
	}
	want := 250000.0
	if *mla.Value != want {
		t.Errorf("median loan amount: want %.0f, got %.2f", want, *mla.Value)
	}
}

// TestHMDAMedianLTV verifies the median LTV calculation.
// CLTVs: 80, 90, 85 → sorted: 80, 85, 90 → median 85.
func TestHMDAMedianLTV(t *testing.T) {
	rows := csvStringToRows(hmdaFixture)
	byTract := aggregateHMDARows(rows)
	s := NewHMDASource(HMDAConfig{Year: 2023})

	td := byTract["55025001100"]
	indicators := s.tractToIndicators("55025001100", td)
	byVar := indicatorsByVar(indicators)

	ltv, ok := byVar["hmda_ltv_ratio"]
	if !ok || ltv.Value == nil {
		t.Fatal("hmda_ltv_ratio missing or nil")
	}
	want := 85.0
	if *ltv.Value != want {
		t.Errorf("median LTV: want %.1f, got %.2f", want, *ltv.Value)
	}
}

// --------------------------------------------------------------------------
// medianFloat64 helper
// --------------------------------------------------------------------------

func TestMedianFloat64(t *testing.T) {
	cases := []struct {
		input []float64
		want  float64
		isNil bool
	}{
		{nil, 0, true},
		{[]float64{}, 0, true},
		{[]float64{5}, 5, false},
		{[]float64{1, 3}, 2, false},
		{[]float64{3, 1, 2}, 2, false},
		{[]float64{1, 2, 3, 4}, 2.5, false},
	}
	for _, tc := range cases {
		got := medianFloat64(tc.input)
		if tc.isNil {
			if got != nil {
				t.Errorf("medianFloat64(%v): want nil, got %v", tc.input, *got)
			}
			continue
		}
		if got == nil {
			t.Errorf("medianFloat64(%v): want %v, got nil", tc.input, tc.want)
			continue
		}
		if *got != tc.want {
			t.Errorf("medianFloat64(%v): want %v, got %v", tc.input, tc.want, *got)
		}
	}
}

// --------------------------------------------------------------------------
// County scope filter
// --------------------------------------------------------------------------

// TestHMDACountyScope verifies that tractDataToIndicators with a county filter
// only returns tracts that belong to that county.
func TestHMDACountyScope(t *testing.T) {
	rows := csvStringToRows(hmdaFixture)
	byTract := aggregateHMDARows(rows)
	s := NewHMDASource(HMDAConfig{Year: 2023})

	// Filter to county 55025 (Dane County, WI).
	indicators := s.tractDataToIndicators(byTract, "55025", "")

	for _, ind := range indicators {
		if !strings.HasPrefix(ind.GEOID, "55025") {
			t.Errorf("indicator GEOID %q does not start with 55025 (county filter failed)", ind.GEOID)
		}
	}

	// Should have excluded Cook County tract 17031010100.
	for _, ind := range indicators {
		if strings.HasPrefix(ind.GEOID, "17031") {
			t.Errorf("Cook County tract %q leaked through county filter", ind.GEOID)
		}
	}
}

// TestHMDACountyScope_StateFilter verifies state-level filtering.
func TestHMDACountyScope_StateFilter(t *testing.T) {
	rows := csvStringToRows(hmdaFixture)
	byTract := aggregateHMDARows(rows)
	s := NewHMDASource(HMDAConfig{Year: 2023})

	// State filter "55" should include Dane County tracts, exclude Cook County.
	indicators := s.tractDataToIndicators(byTract, "", "55")
	for _, ind := range indicators {
		if !strings.HasPrefix(ind.GEOID, "55") {
			t.Errorf("indicator GEOID %q does not start with 55 (state filter failed)", ind.GEOID)
		}
	}
}

// --------------------------------------------------------------------------
// FetchCounty FIPS validation
// --------------------------------------------------------------------------

// TestHMDAFetchCounty_BadStateFIPS verifies that a malformed state FIPS returns error.
func TestHMDAFetchCounty_BadStateFIPS(t *testing.T) {
	s := NewHMDASource(HMDAConfig{Year: 2023})
	_, err := s.FetchCounty(context.Background(), "5", "025") // 1-digit state
	if err == nil {
		t.Error("FetchCounty bad state FIPS: expected error, got nil")
	}
}

// TestHMDAFetchCounty_BadCountyFIPS verifies that a malformed county FIPS returns error.
func TestHMDAFetchCounty_BadCountyFIPS(t *testing.T) {
	s := NewHMDASource(HMDAConfig{Year: 2023})
	_, err := s.FetchCounty(context.Background(), "55", "25") // 2-digit county
	if err == nil {
		t.Error("FetchCounty bad county FIPS: expected error, got nil")
	}
}

// TestHMDAFetchState_BadStateFIPS verifies that a malformed state FIPS returns error.
func TestHMDAFetchState_BadStateFIPS(t *testing.T) {
	s := NewHMDASource(HMDAConfig{Year: 2023})
	_, err := s.FetchState(context.Background(), "5") // 1-digit
	if err == nil {
		t.Error("FetchState bad state FIPS: expected error, got nil")
	}
}

// --------------------------------------------------------------------------
// FetchCounty via mock server
// --------------------------------------------------------------------------

// TestHMDAFetchCounty_MockServer verifies FetchCounty using a mock HTTP server.
func TestHMDAFetchCounty_MockServer(t *testing.T) {
	ts := setupHMDAMockServer(t, hmdaFixture)
	defer ts.Close()

	s := newMockHMDASource(ts, 2023)
	indicators, err := s.FetchCounty(context.Background(), "55", "025")
	if err != nil {
		t.Fatalf("FetchCounty: %v", err)
	}
	if len(indicators) == 0 {
		t.Fatal("FetchCounty: expected indicators, got none")
	}
	for _, ind := range indicators {
		if !strings.HasPrefix(ind.GEOID, "55025") {
			t.Errorf("indicator GEOID %q should start with 55025", ind.GEOID)
		}
		if ind.Vintage != "HMDA-2023" {
			t.Errorf("indicator vintage: want HMDA-2023, got %q", ind.Vintage)
		}
	}
}

// TestHMDAFetchState_MockServer verifies FetchState returns indicators filtered
// to the requested state.
func TestHMDAFetchState_MockServer(t *testing.T) {
	ts := setupHMDAMockServer(t, hmdaFixture)
	defer ts.Close()

	s := newMockHMDASource(ts, 2023)
	indicators, err := s.FetchState(context.Background(), "55")
	if err != nil {
		t.Fatalf("FetchState: %v", err)
	}
	if len(indicators) == 0 {
		t.Fatal("FetchState: expected indicators, got none")
	}
	for _, ind := range indicators {
		if !strings.HasPrefix(ind.GEOID, "55") {
			t.Errorf("FetchState(55): indicator GEOID %q does not start with 55", ind.GEOID)
		}
	}
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

// csvStringToRows parses a CSV string into [][]string (header + data rows).
func csvStringToRows(s string) [][]string {
	var rows [][]string
	lines := strings.Split(strings.TrimSpace(s), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		fields := strings.Split(line, ",")
		rows = append(rows, fields)
	}
	return rows
}

// indicatorsByVar indexes a slice of Indicators by VariableID.
func indicatorsByVar(inds []store.Indicator) map[string]store.Indicator {
	m := make(map[string]store.Indicator, len(inds))
	for _, ind := range inds {
		m[ind.VariableID] = ind
	}
	return m
}
