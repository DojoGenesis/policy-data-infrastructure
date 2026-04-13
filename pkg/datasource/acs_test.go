package datasource

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// censusFixture returns a Census-format JSON response with two tracts.
// The first tract has a real income value; the second has a suppressed value.
const censusFixture = `[
  ["B19013_001E","B19013_001M","B03002_001E","B03002_003E","B03002_004E","B03002_012E","S1701_C03_001E","B01001_001E","S2701_C05_001E","B25106_001E","B25106_006E","B25106_010E","B25106_014E","B25106_018E","B25106_022E","B25106_024E","B25106_028E","B25106_032E","B25106_036E","B25106_040E","state","county","tract"],
  ["52000","2500","4800","3200","900","400","18.5","4850","12.3","1900","45","30","20","10","5","80","60","40","25","15","55","025","000100"],
  ["*","*","3100","2000","500","350","*","3200","8.7","1200","20","15","10","5","3","50","40","30","20","10","55","025","000201"]
]`

// newMockACSSource creates an ACSSource pointed at the given test server URL.
func newMockACSSource(ts *httptest.Server) *acsSource {
	s := &acsSource{
		cfg: ACSConfig{
			Year:       2024,
			HTTPClient: ts.Client(),
		},
		vintage: "ACS-2024-5yr",
		ticker:  nil, // disable rate limiting in tests
	}
	return s
}

func TestACSParseResponse_GEOIDConstruction(t *testing.T) {
	s := &acsSource{vintage: "ACS-2024-5yr", ticker: nil}
	indicators, err := s.parseResponse(strings.NewReader(censusFixture))
	if err != nil {
		t.Fatalf("parseResponse returned error: %v", err)
	}
	if len(indicators) == 0 {
		t.Fatal("expected indicators, got none")
	}

	// Verify GEOID construction for the first tract.
	// state="55", county="025", tract="000100" → "55025000100"
	var found bool
	for _, ind := range indicators {
		if ind.GEOID == "55025000100" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected GEOID 55025000100 in results; got GEOIDs:")
		seen := make(map[string]bool)
		for _, ind := range indicators {
			if !seen[ind.GEOID] {
				t.Logf("  %s", ind.GEOID)
				seen[ind.GEOID] = true
			}
		}
	}

	// Verify second tract GEOID.
	// state="55", county="025", tract="000201" → "55025000201"
	var found2 bool
	for _, ind := range indicators {
		if ind.GEOID == "55025000201" {
			found2 = true
			break
		}
	}
	if !found2 {
		t.Errorf("expected GEOID 55025000201 in results")
	}
}

func TestACSParseResponse_SuppressedValuesAreNil(t *testing.T) {
	s := &acsSource{vintage: "ACS-2024-5yr", ticker: nil}
	indicators, err := s.parseResponse(strings.NewReader(censusFixture))
	if err != nil {
		t.Fatalf("parseResponse returned error: %v", err)
	}

	// For tract 55025000201, median_household_income is "*" — must be nil.
	for _, ind := range indicators {
		if ind.GEOID == "55025000201" && ind.VariableID == "median_household_income" {
			if ind.Value != nil {
				t.Errorf("suppressed income value: want nil, got %v", *ind.Value)
			}
			if ind.RawValue != "*" {
				t.Errorf("suppressed income raw value: want *, got %q", ind.RawValue)
			}
			return
		}
	}
	t.Error("did not find median_household_income for tract 55025000201")
}

func TestACSParseResponse_ValidValue(t *testing.T) {
	s := &acsSource{vintage: "ACS-2024-5yr", ticker: nil}
	indicators, err := s.parseResponse(strings.NewReader(censusFixture))
	if err != nil {
		t.Fatalf("parseResponse returned error: %v", err)
	}

	// Tract 55025000100 has income 52000.
	for _, ind := range indicators {
		if ind.GEOID == "55025000100" && ind.VariableID == "median_household_income" {
			if ind.Value == nil {
				t.Fatal("expected non-nil value for median_household_income")
			}
			if *ind.Value != 52000 {
				t.Errorf("median_household_income: want 52000, got %v", *ind.Value)
			}
			if ind.Vintage != "ACS-2024-5yr" {
				t.Errorf("vintage: want ACS-2024-5yr, got %q", ind.Vintage)
			}
			return
		}
	}
	t.Error("did not find median_household_income for tract 55025000100")
}

func TestACSParseResponse_MOEAttached(t *testing.T) {
	s := &acsSource{vintage: "ACS-2024-5yr", ticker: nil}
	indicators, err := s.parseResponse(strings.NewReader(censusFixture))
	if err != nil {
		t.Fatalf("parseResponse returned error: %v", err)
	}

	// Tract 55025000100: income MOE is 2500.
	for _, ind := range indicators {
		if ind.GEOID == "55025000100" && ind.VariableID == "median_household_income" {
			if ind.MarginOfError == nil {
				t.Fatal("expected non-nil MarginOfError for median_household_income")
			}
			if *ind.MarginOfError != 2500 {
				t.Errorf("margin of error: want 2500, got %v", *ind.MarginOfError)
			}
			return
		}
	}
	t.Error("did not find median_household_income for tract 55025000100")
}

func TestACSParseResponse_SuppressedMOEIsNil(t *testing.T) {
	s := &acsSource{vintage: "ACS-2024-5yr", ticker: nil}
	indicators, err := s.parseResponse(strings.NewReader(censusFixture))
	if err != nil {
		t.Fatalf("parseResponse returned error: %v", err)
	}

	// Tract 55025000201: income is "*", MOE is "*" — MarginOfError must be nil.
	for _, ind := range indicators {
		if ind.GEOID == "55025000201" && ind.VariableID == "median_household_income" {
			if ind.MarginOfError != nil {
				t.Errorf("suppressed MOE: want nil, got %v", *ind.MarginOfError)
			}
			return
		}
	}
	t.Error("did not find median_household_income for tract 55025000201")
}

func TestACSParseResponse_NoMOESuffixInOutput(t *testing.T) {
	s := &acsSource{vintage: "ACS-2024-5yr", ticker: nil}
	indicators, err := s.parseResponse(strings.NewReader(censusFixture))
	if err != nil {
		t.Fatalf("parseResponse returned error: %v", err)
	}

	// No Indicator should have a VariableID ending in "_moe" — those are merged.
	for _, ind := range indicators {
		if strings.HasSuffix(ind.VariableID, "_moe") {
			t.Errorf("unexpected _moe indicator in output: GEOID=%s VariableID=%s", ind.GEOID, ind.VariableID)
		}
	}
}

func TestACSFetchCounty_HTTPIntegration(t *testing.T) {
	// Track whether the handler was called with the correct query params.
	var gotFor, gotGet string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotFor = r.URL.Query().Get("for")
		gotGet = r.URL.Query().Get("get")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(censusFixture))
	}))
	defer ts.Close()

	s := newMockACSSource(ts)

	// Build a URL that points at the test server instead of api.census.gov.
	// We call fetch() directly with the test-server URL so the HTTP round-trip
	// exercises the full request/parse pipeline.
	vars := s.variableList()
	url := fmt.Sprintf("%s?get=%s&for=tract:*&in=state:55+county:025", ts.URL, vars)

	ctx := context.Background()
	indicators, err := s.fetch(ctx, url)
	if err != nil {
		t.Fatalf("fetch returned error: %v", err)
	}
	if len(indicators) == 0 {
		t.Error("expected indicators from HTTP round-trip, got none")
	}
	if gotFor == "" {
		t.Error("handler: missing 'for' query param")
	}
	if !strings.Contains(gotFor, "tract") {
		t.Errorf("handler: expected 'for' to contain 'tract', got %q", gotFor)
	}
	if gotGet == "" {
		t.Errorf("handler: missing 'get' query param; built URL: %s", url)
	}
}

func TestParseValue_VariousSuppressedForms(t *testing.T) {
	suppressed := []string{"*", "**", "-", "-1", "-9", "null", "N", "(X)", ""}
	for _, raw := range suppressed {
		val, _ := parseValue(raw)
		if val != nil {
			t.Errorf("parseValue(%q): want nil, got %v", raw, *val)
		}
	}
}

func TestParseValue_ValidNumbers(t *testing.T) {
	cases := []struct {
		raw  string
		want float64
	}{
		{"0", 0},
		{"52000", 52000},
		{"18.5", 18.5},
		{"-0.5", -0.5},
		{"  42  ", 42},
	}
	for _, tc := range cases {
		val, _ := parseValue(tc.raw)
		if val == nil {
			t.Errorf("parseValue(%q): want %v, got nil", tc.raw, tc.want)
			continue
		}
		if *val != tc.want {
			t.Errorf("parseValue(%q): want %v, got %v", tc.raw, tc.want, *val)
		}
	}
}

func TestACSSource_Interface(t *testing.T) {
	// Verify ACSSource satisfies the DataSource interface at compile time.
	var _ DataSource = NewACSSource(ACSConfig{Year: 2024})
}

func TestACSSource_Schema(t *testing.T) {
	s := NewACSSource(ACSConfig{Year: 2024})
	schema := s.Schema()
	if len(schema) == 0 {
		t.Fatal("Schema() returned empty slice")
	}
	// Every VariableDef must have an ID, ACSTable, Unit, and Direction.
	for _, def := range schema {
		if def.ID == "" {
			t.Errorf("VariableDef missing ID: %+v", def)
		}
		if def.ACSTable == "" {
			t.Errorf("VariableDef %q missing ACSTable", def.ID)
		}
		if def.Unit == "" {
			t.Errorf("VariableDef %q missing Unit", def.ID)
		}
		if def.Direction == "" {
			t.Errorf("VariableDef %q missing Direction", def.ID)
		}
	}
}

func TestACSSource_VintageAndCategory(t *testing.T) {
	s := NewACSSource(ACSConfig{Year: 2023})
	if s.Name() != "acs-5yr" {
		t.Errorf("Name(): want acs-5yr, got %q", s.Name())
	}
	if s.Category() != "demographic" {
		t.Errorf("Category(): want demographic, got %q", s.Category())
	}
	if s.Vintage() != "ACS-2023-5yr" {
		t.Errorf("Vintage(): want ACS-2023-5yr, got %q", s.Vintage())
	}
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()
	s := NewACSSource(ACSConfig{Year: 2024})
	r.Register(s)

	got, ok := r.Get("acs-5yr")
	if !ok {
		t.Fatal("Get(acs-5yr) returned false")
	}
	if got.Name() != "acs-5yr" {
		t.Errorf("got name %q, want acs-5yr", got.Name())
	}

	_, ok = r.Get("nonexistent")
	if ok {
		t.Error("Get(nonexistent) should return false")
	}
}

func TestRegistry_All(t *testing.T) {
	r := NewRegistry()
	r.Register(NewACSSource(ACSConfig{Year: 2024}))
	all := r.All()
	if len(all) != 1 {
		t.Errorf("All() len: want 1, got %d", len(all))
	}
}
