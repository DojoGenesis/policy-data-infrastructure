package grounding

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── fixture ────────────────────────────────────────────────────────────
// A tiny bundle with the same shape as the real one. Deliberately includes a
// suppressed value (nil) — the case that turns into a silent zero if any layer
// is sloppy about the difference between "missing" and "0".

func writeFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	write := func(name string, v any) {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	write("indicators.json", map[string]any{
		"indicators": []map[string]any{
			{"id": "poverty_rate", "label": "Poverty rate", "unit": "percent",
				"format": "percent", "direction": "lower_better", "table": "S1701"},
			{"id": "median_hh_income", "label": "Median household income", "unit": "dollars",
				"format": "currency", "direction": "higher_better", "table": "B19013"},
		},
	})
	write("manifest.json", map[string]any{
		"sources": []map[string]any{
			{"name": "American Community Survey 5-Year Estimates",
				"publisher": "U.S. Census Bureau", "vintage": "2020-2024",
				"used_for": "indicator values"},
		},
	})

	county := func(geoid, name string, pov any, inc any) map[string]any {
		return map[string]any{"properties": map[string]any{
			"GEOID": geoid, "county_name": name,
			"poverty_rate": pov, "median_hh_income": inc,
		}}
	}
	write("counties.geojson", map[string]any{"features": []map[string]any{
		county("55025", "Dane County", 10.5, 89975.0),
		county("55079", "Milwaukee County", 19.2, 62000.0),
		county("55009", "Brown County", 9.1, 71500.0),
		county("55078", "Menominee County", nil, nil), // suppressed
	}})
	write("tracts.geojson", map[string]any{"features": []map[string]any{
		{"properties": map[string]any{
			"GEOID": "55025000100", "tract_name": "Census Tract 1", "county_name": "Dane County",
			"poverty_rate": 22.4, "median_hh_income": 41000.0}},
		{"properties": map[string]any{
			"GEOID": "55025000201", "tract_name": "Census Tract 2.01", "county_name": "Dane County",
			"poverty_rate": 4.2, "median_hh_income": 120000.0}},
	}})
	write("representation.json", map[string]any{
		"tracts": map[string]any{
			"55025000100": map[string]any{
				"us_house": map[string]any{
					"district": "2", "districtLabel": "U.S. House district 2",
					"official": map[string]any{"name": "Mark Pocan", "party": "Democrat"},
				},
			},
		},
	})
	return dir
}

func load(t *testing.T) *Dataset {
	t.Helper()
	ds, err := Load(writeFixture(t))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return ds
}

// ── dataset ────────────────────────────────────────────────────────────

func TestLoadAndResolve(t *testing.T) {
	ds := load(t)
	if got := ds.Count(LevelCounty); got != 4 {
		t.Errorf("county count = %d, want 4", got)
	}
	if ds.Vintage != "2020-2024" {
		t.Errorf("vintage = %q, want 2020-2024", ds.Vintage)
	}
	for _, name := range []string{"Dane", "Dane County", "dane county", "55025"} {
		if g, ok := ds.ResolvePlace(name, LevelCounty); !ok || g != "55025" {
			t.Errorf("ResolvePlace(%q) = %q,%v; want 55025,true", name, g, ok)
		}
	}
	if _, ok := ds.ResolvePlace("Cook County", LevelCounty); ok {
		t.Error("resolved a county that is not in the dataset")
	}
}

// A suppressed estimate must stay distinguishable from zero at every layer.
func TestSuppressedIsNotZero(t *testing.T) {
	ds := load(t)
	v, ok := ds.Value("55078", LevelCounty, "poverty_rate")
	if !ok {
		t.Fatal("Menominee should exist in the dataset")
	}
	if v != nil {
		t.Fatalf("suppressed value = %v, want nil", *v)
	}

	in := &Intent{Operation: OpLookup, Indicator: "poverty_rate", Places: []string{"Menominee"}}
	if err := in.Validate(ds); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	res, err := Execute(in, ds)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.Values[0].Missing {
		t.Error("lookup did not mark a suppressed value as missing")
	}
	if strings.Contains(res.Facts, "0.0%") || strings.Contains(res.Facts, ": 0") {
		t.Errorf("suppressed value rendered as zero: %q", res.Facts)
	}
	if !strings.Contains(res.Facts, "suppress") {
		t.Errorf("facts should explain suppression, got: %q", res.Facts)
	}
}

// A place that is not in the dataset must be refused, never silently dropped —
// a dropped place produces an answer that looks complete and is not.
func TestUnknownPlaceIsRefused(t *testing.T) {
	ds := load(t)
	in := &Intent{Operation: OpCompare, Indicator: "poverty_rate",
		Places: []string{"Dane", "Cook County"}}
	err := in.Validate(ds)
	if err == nil {
		t.Fatal("expected an error for an unknown place")
	}
	if !strings.Contains(err.Error(), "Cook County") {
		t.Errorf("error should name the unknown place, got: %v", err)
	}
}

func TestUnknownIndicatorListsVocabulary(t *testing.T) {
	ds := load(t)
	in := &Intent{Operation: OpLookup, Indicator: "vibes", Places: []string{"Dane"}}
	err := in.Validate(ds)
	if err == nil {
		t.Fatal("expected an error for an unknown indicator")
	}
	if !strings.Contains(err.Error(), "poverty_rate") {
		t.Errorf("refusal should list the real vocabulary, got: %v", err)
	}
}

func TestMeanIsUnavailable(t *testing.T) {
	ds := load(t)
	in := &Intent{Operation: OpAggregate, Indicator: "median_hh_income", Aggregate: "mean"}
	err := in.Validate(ds)
	if err == nil {
		t.Fatal("mean should be refused: averaging medians is not a median")
	}
	if !strings.Contains(err.Error(), "median") {
		t.Errorf("refusal should explain why, got: %v", err)
	}
}

// ── execution ──────────────────────────────────────────────────────────

func TestRankExcludesMissingAndCountsThem(t *testing.T) {
	ds := load(t)
	in := &Intent{Operation: OpRank, Indicator: "poverty_rate", Direction: DirHighest, Limit: 2}
	if err := in.Validate(ds); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	res, err := Execute(in, ds)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(res.Values) != 2 {
		t.Fatalf("got %d values, want 2", len(res.Values))
	}
	if res.Values[0].Name != "Milwaukee County" || *res.Values[0].Value != 19.2 {
		t.Errorf("top = %s %v, want Milwaukee County 19.2",
			res.Values[0].Name, *res.Values[0].Value)
	}
	if res.TotalCount != 3 {
		t.Errorf("TotalCount = %d, want 3 (the suppressed county is excluded)", res.TotalCount)
	}
	if res.Missing != 1 {
		t.Errorf("Missing = %d, want 1", res.Missing)
	}
}

func TestAggregateMedian(t *testing.T) {
	ds := load(t)
	in := &Intent{Operation: OpAggregate, Indicator: "poverty_rate", Aggregate: AggMedian}
	if err := in.Validate(ds); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	res, err := Execute(in, ds)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// 9.1, 10.5, 19.2 -> median 10.5
	if res.Scalar == nil || *res.Scalar != 10.5 {
		t.Errorf("median = %v, want 10.5", res.Scalar)
	}
}

func TestThreshold(t *testing.T) {
	ds := load(t)
	th := 10.0
	in := &Intent{Operation: OpThreshold, Indicator: "poverty_rate",
		Threshold: &th, Comparator: CmpAbove}
	if err := in.Validate(ds); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	res, err := Execute(in, ds)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.TotalCount != 2 {
		t.Errorf("TotalCount = %d, want 2 (Dane 10.5, Milwaukee 19.2)", res.TotalCount)
	}
}

func TestExecuteIsDeterministic(t *testing.T) {
	ds := load(t)
	run := func() string {
		in := &Intent{Operation: OpRank, Indicator: "median_hh_income", Limit: 3}
		if err := in.Validate(ds); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		res, err := Execute(in, ds)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		return res.Facts
	}
	if a, b := run(), run(); a != b {
		t.Errorf("Execute is not deterministic:\n%q\n%q", a, b)
	}
}

func TestFactsCarryACitation(t *testing.T) {
	ds := load(t)
	in := &Intent{Operation: OpLookup, Indicator: "median_hh_income", Places: []string{"Dane"}}
	if err := in.Validate(ds); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	res, _ := Execute(in, ds)
	if !strings.Contains(res.Facts, "Source:") {
		t.Errorf("facts should cite a source, got: %q", res.Facts)
	}
	if !strings.Contains(res.Facts, "2020-2024") {
		t.Errorf("facts should name the vintage, got: %q", res.Facts)
	}
	// Currency must render without a "$" sigil, matching the site-wide rule.
	if strings.Contains(res.Facts, "$") {
		t.Errorf("currency should not carry a $ sigil, got: %q", res.Facts)
	}
}

func TestRepresentation(t *testing.T) {
	ds := load(t)
	in := &Intent{Operation: OpRepresentation, Level: LevelTract,
		Places: []string{"55025000100"}}
	if err := in.Validate(ds); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	res, err := Execute(in, ds)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(res.Seats) != 1 || res.Seats[0].Official != "Mark Pocan" {
		t.Fatalf("seats = %+v, want one seat held by Mark Pocan", res.Seats)
	}
}

// ── verification: the guarantee ────────────────────────────────────────

func resultForVerify(t *testing.T) *Result {
	t.Helper()
	ds := load(t)
	in := &Intent{Operation: OpLookup, Indicator: "poverty_rate", Places: []string{"Dane"}}
	if err := in.Validate(ds); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	res, err := Execute(in, ds)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return res
}

func TestVerifyAcceptsGroundedProse(t *testing.T) {
	res := resultForVerify(t)
	prose := "Dane County's poverty rate is 10.5 percent, from the 2020-2024 ACS estimates."
	if v := Verify(prose, res); len(v) != 0 {
		t.Errorf("grounded prose was rejected: %v", v)
	}
}

// The exact failure the requirement names: a plausible number no query returned.
func TestVerifyCatchesAConfabulatedNumber(t *testing.T) {
	res := resultForVerify(t)
	prose := "Dane County's poverty rate is 10.5 percent, up from 7.2 in the previous vintage."
	v := Verify(prose, res)
	if len(v) == 0 {
		t.Fatal("verification let an invented figure through")
	}
	found := false
	for _, x := range v {
		if x.Number == "7.2" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 7.2 to be flagged, got %v", v)
	}
}

func TestVerifyCatchesAWrongRestatement(t *testing.T) {
	res := resultForVerify(t)
	// Transposed digits — the most dangerous kind, because it reads right.
	if v := Verify("Dane County's poverty rate is 15.0 percent.", res); len(v) == 0 {
		t.Error("verification let a transposed figure through")
	}
}

func TestVerifyAllowsRanksAndTotals(t *testing.T) {
	ds := load(t)
	in := &Intent{Operation: OpRank, Indicator: "poverty_rate", Direction: DirHighest, Limit: 3}
	if err := in.Validate(ds); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	res, _ := Execute(in, ds)
	prose := "Of 3 counties with a published rate, the highest is Milwaukee County at 19.2 percent, " +
		"then Dane County at 10.5 and Brown County at 9.1. 1 county has no published value."
	if v := Verify(prose, res); len(v) != 0 {
		t.Errorf("ranks and totals should be allowed: %v", v)
	}
}

// Rounding a real figure to a rounder one is still a claim the data does not
// make, so it must not pass.
func TestVerifyRejectsInventedRounding(t *testing.T) {
	ds := load(t)
	in := &Intent{Operation: OpLookup, Indicator: "median_hh_income", Places: []string{"Dane"}}
	if err := in.Validate(ds); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	res, _ := Execute(in, ds)
	if v := Verify("Median household income in Dane County is about 90,000 dollars.", res); len(v) == 0 {
		t.Error("a rounded-off figure the data does not contain should be flagged")
	}
	if v := Verify("Median household income in Dane County is 89,975 dollars.", res); len(v) != 0 {
		t.Errorf("the exact figure should pass: %v", v)
	}
}

func TestVerifyAllowsComparisonDifference(t *testing.T) {
	ds := load(t)
	in := &Intent{Operation: OpCompare, Indicator: "poverty_rate",
		Places: []string{"Dane", "Milwaukee"}}
	if err := in.Validate(ds); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	res, _ := Execute(in, ds)
	// 19.2 - 10.5 = 8.7, computed by this package, so a draft may restate it.
	if v := Verify("Milwaukee County is 8.7 points higher than Dane County.", res); len(v) != 0 {
		t.Errorf("a computed difference should be allowed: %v", v)
	}
}

// The deterministic rendering must itself pass verification — if the floor
// answer failed its own check the pipeline would have no safe output at all.
func TestRenderedFactsAlwaysVerify(t *testing.T) {
	ds := load(t)
	cases := []*Intent{
		{Operation: OpLookup, Indicator: "poverty_rate", Places: []string{"Dane"}},
		{Operation: OpLookup, Indicator: "poverty_rate", Places: []string{"Menominee"}},
		{Operation: OpCompare, Indicator: "poverty_rate", Places: []string{"Dane", "Milwaukee"}},
		{Operation: OpRank, Indicator: "poverty_rate", Direction: DirHighest, Limit: 3},
		{Operation: OpRank, Indicator: "median_hh_income", Direction: DirLowest, Limit: 2},
		{Operation: OpAggregate, Indicator: "poverty_rate", Aggregate: AggMedian},
		{Operation: OpAggregate, Indicator: "poverty_rate", Aggregate: AggCount},
		{Operation: OpAggregate, Indicator: "median_hh_income", Aggregate: AggMax},
	}
	for _, in := range cases {
		if err := in.Validate(ds); err != nil {
			t.Fatalf("Validate %+v: %v", in, err)
		}
		res, err := Execute(in, ds)
		if err != nil {
			t.Fatalf("Execute %+v: %v", in, err)
		}
		if v := Verify(res.Facts, res); len(v) != 0 {
			t.Errorf("rendered facts failed their own verification\nintent: %s/%s\nfacts: %q\nviolations: %v",
				in.Operation, in.Indicator, res.Facts, v)
		}
	}
}

func TestThresholdFactsVerify(t *testing.T) {
	ds := load(t)
	th := 10.0
	in := &Intent{Operation: OpThreshold, Indicator: "poverty_rate",
		Threshold: &th, Comparator: CmpAbove}
	if err := in.Validate(ds); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	res, _ := Execute(in, ds)
	if v := Verify(res.Facts, res); len(v) != 0 {
		t.Errorf("threshold facts failed verification: %v\n%s", v, res.Facts)
	}
}

// Regression: every tract name contains a number ("Census Tract 16.03"), so a
// verifier that does not allow place-name digits rejects every tract answer —
// a systematic false positive rather than a caught fabrication. Found by
// running the real pipeline, not by reading the code.
func TestVerifyAllowsNumbersInPlaceNames(t *testing.T) {
	ds := load(t)
	in := &Intent{Operation: OpLookup, Indicator: "poverty_rate",
		Level: LevelTract, Places: []string{"55025000201"}}
	if err := in.Validate(ds); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	res, err := Execute(in, ds)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	prose := "Census Tract 2.01, Dane County has a poverty rate of 4.2 percent."
	if v := Verify(prose, res); len(v) != 0 {
		t.Errorf("a tract's own name should not read as a fabricated figure: %v", v)
	}
	// The guard must still bite on a real invention in the same sentence.
	if v := Verify(prose+" It was 9.9 percent in 2019.", res); len(v) == 0 {
		t.Error("allowing place-name digits must not disable the guard")
	}
}

// A definitional constant from the indicator's own description is grounded.
func TestVerifyAllowsIndicatorDefinitionNumbers(t *testing.T) {
	ds := &Dataset{
		Indicators: []IndicatorMeta{{
			ID: "pct_cost_burdened", Label: "Cost-burdened households",
			Format: "percent", Direction: "lower_better",
			Description: "Share of households spending 30% or more of income on housing.",
		}},
		Vintage: "2020-2024",
	}
	v := 41.0
	res := &Result{
		Operation: OpLookup,
		Values:    []Value{{GeoID: "55025", Name: "Dane County", Value: &v}},
		Citations: []Citation{ds.citation(ds.Indicators[0], LevelCounty)},
	}
	prose := "Dane County is at 41.0 percent, counting households spending 30% or more of income on housing."
	if got := Verify(prose, res); len(got) != 0 {
		t.Errorf("the indicator's own definition should be quotable: %v", got)
	}
}

// A GEOID is an identifier, not a measurement. An answer that names the tract
// it was asked about must not be rejected for doing so.
func TestVerifyAllowsQueriedGeoIDs(t *testing.T) {
	ds := load(t)
	in := &Intent{Operation: OpRepresentation, Level: LevelTract,
		Places: []string{"55025000100"}}
	if err := in.Validate(ds); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	res, err := Execute(in, ds)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	prose := "Census tract 55025000100 is represented by Mark Pocan in U.S. House district 2."
	if v := Verify(prose, res); len(v) != 0 {
		t.Errorf("the queried GEOID should not read as a fabricated figure: %v", v)
	}
	if v := Verify(res.Facts, res); len(v) != 0 {
		t.Errorf("representation facts failed their own verification: %v", v)
	}
}
