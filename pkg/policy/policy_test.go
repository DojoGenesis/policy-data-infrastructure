package policy

import (
	"testing"
)

const (
	testCSVPath       = "../../data/policies/francesca_hong_2026.csv"
	testCrosswalkPath = "../../data/crosswalks/wi_equity_crosswalk.json"
)

// TestLoadPoliciesFromCSV verifies that the francesca_hong_2026.csv file loads correctly
// and contains the expected number of records.
func TestLoadPoliciesFromCSV(t *testing.T) {
	policies, err := LoadPoliciesFromCSV(testCSVPath)
	if err != nil {
		t.Fatalf("LoadPoliciesFromCSV: unexpected error: %v", err)
	}

	const wantCount = 70
	if len(policies) != wantCount {
		t.Errorf("record count: got %d, want %d", len(policies), wantCount)
	}

	// Spot-check first record
	first := policies[0]
	if first.ID != "FH-EDU-001" {
		t.Errorf("first record ID: got %q, want %q", first.ID, "FH-EDU-001")
	}
	if first.Candidate != "Francesca Hong" {
		t.Errorf("first record Candidate: got %q, want %q", first.Candidate, "Francesca Hong")
	}
	if first.State != "WI" {
		t.Errorf("first record State: got %q, want %q", first.State, "WI")
	}
	if first.EquityDimension != "food_access" {
		t.Errorf("first record EquityDimension: got %q, want %q", first.EquityDimension, "food_access")
	}

	// Spot-check last record
	last := policies[len(policies)-1]
	if last.ID != "FH-SMBUS-002" {
		t.Errorf("last record ID: got %q, want %q", last.ID, "FH-SMBUS-002")
	}
}

// TestFilterByCategory verifies filtering by the "Schools" category returns exactly 6 records.
func TestFilterByCategory(t *testing.T) {
	policies, err := LoadPoliciesFromCSV(testCSVPath)
	if err != nil {
		t.Fatalf("LoadPoliciesFromCSV: %v", err)
	}

	schools := FilterByCategory(policies, "Schools")
	const wantCount = 6
	if len(schools) != wantCount {
		t.Errorf("FilterByCategory(Schools): got %d records, want %d", len(schools), wantCount)
	}
	for _, p := range schools {
		if p.Category != "Schools" {
			t.Errorf("unexpected category %q in Schools filter result (ID: %s)", p.Category, p.ID)
		}
	}

	// Empty result for unknown category
	none := FilterByCategory(policies, "Nonexistent")
	if len(none) != 0 {
		t.Errorf("FilterByCategory(Nonexistent): got %d records, want 0", len(none))
	}
}

// TestCategories verifies that distinct categories are returned and include expected values.
func TestCategories(t *testing.T) {
	policies, err := LoadPoliciesFromCSV(testCSVPath)
	if err != nil {
		t.Fatalf("LoadPoliciesFromCSV: %v", err)
	}

	cats := Categories(policies)
	if len(cats) == 0 {
		t.Fatal("Categories: returned empty slice")
	}

	// Verify known categories are present
	wantCategories := []string{
		"Agriculture",
		"Bodily Autonomy",
		"Cannabis",
		"Care Economy",
		"Cities Towns Transit",
		"Criminal Legal Reform",
		"Disability",
		"Economic Justice",
		"Environment",
		"Gun Safety",
		"Healthcare",
		"Housing",
		"Immigration",
		"Labor",
		"Schools",
		"Small Businesses",
		"Taxes",
		"Workers",
	}

	catSet := make(map[string]bool, len(cats))
	for _, c := range cats {
		catSet[c] = true
	}

	for _, want := range wantCategories {
		if !catSet[want] {
			t.Errorf("Categories: missing expected category %q", want)
		}
	}

	// Verify the returned slice is sorted
	for i := 1; i < len(cats); i++ {
		if cats[i] < cats[i-1] {
			t.Errorf("Categories: not sorted at index %d: %q > %q", i, cats[i-1], cats[i])
		}
	}

	// Verify no duplicates
	seen := make(map[string]bool)
	for _, c := range cats {
		if seen[c] {
			t.Errorf("Categories: duplicate entry %q", c)
		}
		seen[c] = true
	}
}

// TestSummarize verifies the summary structure contains expected values.
func TestSummarize(t *testing.T) {
	policies, err := LoadPoliciesFromCSV(testCSVPath)
	if err != nil {
		t.Fatalf("LoadPoliciesFromCSV: %v", err)
	}

	s := Summarize(policies)

	if s.TotalPolicies != 70 {
		t.Errorf("Summarize.TotalPolicies: got %d, want 70", s.TotalPolicies)
	}
	if s.Candidate != "Francesca Hong" {
		t.Errorf("Summarize.Candidate: got %q, want %q", s.Candidate, "Francesca Hong")
	}
	if len(s.Categories) == 0 {
		t.Error("Summarize.Categories: empty map")
	}
	if len(s.EquityDimensions) == 0 {
		t.Error("Summarize.EquityDimensions: empty map")
	}
	if s.BillCount <= 0 {
		t.Errorf("Summarize.BillCount: got %d, want > 0", s.BillCount)
	}

	// Verify Schools count in categories
	if s.Categories["Schools"] != 6 {
		t.Errorf("Summarize.Categories[Schools]: got %d, want 6", s.Categories["Schools"])
	}

	// Verify labor_rights count (should be 7 based on CSV)
	if s.EquityDimensions["labor_rights"] != 7 {
		t.Errorf("Summarize.EquityDimensions[labor_rights]: got %d, want 7", s.EquityDimensions["labor_rights"])
	}

	// Verify total of all category counts equals total policies
	catTotal := 0
	for _, v := range s.Categories {
		catTotal += v
	}
	if catTotal != s.TotalPolicies {
		t.Errorf("sum of category counts %d != TotalPolicies %d", catTotal, s.TotalPolicies)
	}
}

// TestCrosswalkIndicators verifies that a known equity dimension maps to expected indicators.
func TestCrosswalkIndicators(t *testing.T) {
	cw, err := LoadCrosswalkFromJSON(testCrosswalkPath)
	if err != nil {
		t.Fatalf("LoadCrosswalkFromJSON: %v", err)
	}

	if len(cw.Dimensions) == 0 {
		t.Fatal("crosswalk has no dimensions")
	}

	// housing_affordability should include pct_cost_burdened
	dm, ok := cw.Dimensions["housing_affordability"]
	if !ok {
		t.Fatal("crosswalk missing 'housing_affordability' dimension")
	}
	if dm.Label == "" {
		t.Error("housing_affordability: empty label")
	}
	if dm.Priority != "P1" {
		t.Errorf("housing_affordability priority: got %q, want P1", dm.Priority)
	}

	wantIndicator := "pct_cost_burdened"
	found := false
	for _, ind := range dm.Indicators {
		if ind == wantIndicator {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("housing_affordability: missing expected indicator %q; got %v", wantIndicator, dm.Indicators)
	}

	// criminal_justice should include pct_non_hispanic_black
	cjDm, ok := cw.Dimensions["criminal_justice"]
	if !ok {
		t.Fatal("crosswalk missing 'criminal_justice' dimension")
	}
	wantCJIndicator := "pct_non_hispanic_black"
	foundCJ := false
	for _, ind := range cjDm.Indicators {
		if ind == wantCJIndicator {
			foundCJ = true
			break
		}
	}
	if !foundCJ {
		t.Errorf("criminal_justice: missing expected indicator %q; got %v", wantCJIndicator, cjDm.Indicators)
	}
}

// TestCrosswalkForPolicy loads both the CSV and the crosswalk, then verifies that a known
// policy maps to expected indicators via IndicatorsForPolicy.
func TestCrosswalkForPolicy(t *testing.T) {
	policies, err := LoadPoliciesFromCSV(testCSVPath)
	if err != nil {
		t.Fatalf("LoadPoliciesFromCSV: %v", err)
	}
	cw, err := LoadCrosswalkFromJSON(testCrosswalkPath)
	if err != nil {
		t.Fatalf("LoadCrosswalkFromJSON: %v", err)
	}

	// FH-HSG-004 is "Protecting renters" with equity_dimension = housing_stability
	var renters *PolicyRecord
	for i := range policies {
		if policies[i].ID == "FH-HSG-004" {
			renters = &policies[i]
			break
		}
	}
	if renters == nil {
		t.Fatal("policy FH-HSG-004 not found in CSV")
	}
	if renters.EquityDimension != "housing_stability" {
		t.Fatalf("FH-HSG-004 equity_dimension: got %q, want %q", renters.EquityDimension, "housing_stability")
	}

	indicators := cw.IndicatorsForPolicy(*renters)
	if len(indicators) == 0 {
		t.Fatal("IndicatorsForPolicy(FH-HSG-004): returned no indicators")
	}

	wantIndicator := "pct_cost_burdened"
	found := false
	for _, ind := range indicators {
		if ind == wantIndicator {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("IndicatorsForPolicy(FH-HSG-004): missing %q; got %v", wantIndicator, indicators)
	}

	// Verify methods are returned for this policy
	methods := cw.MethodsForPolicy(*renters)
	if len(methods) == 0 {
		t.Error("MethodsForPolicy(FH-HSG-004): returned no methods")
	}

	// Verify an unmapped dimension returns nil
	unmapped := PolicyRecord{EquityDimension: "nonexistent_dimension"}
	if got := cw.IndicatorsForPolicy(unmapped); got != nil {
		t.Errorf("IndicatorsForPolicy(unmapped): got %v, want nil", got)
	}

	// Verify FilterByHighRelevance includes housing_stability (P1)
	highRelevance := FilterByHighRelevance(policies, cw)
	if len(highRelevance) == 0 {
		t.Error("FilterByHighRelevance: returned no policies")
	}
	for _, p := range highRelevance {
		dm, ok := cw.Dimensions[p.EquityDimension]
		if !ok || dm.Priority != "P1" {
			t.Errorf("FilterByHighRelevance: included non-P1 policy %s (dimension %q, priority %q)",
				p.ID, p.EquityDimension, dm.Priority)
		}
	}
}
