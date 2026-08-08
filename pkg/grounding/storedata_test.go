package grounding

import (
	"context"
	"testing"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

type fakeStoreSource struct {
	vars []store.VariableMeta
	inds []store.Indicator
	geos map[geo.Level][]geo.Geography
}

func (f *fakeStoreSource) QueryVariables(_ context.Context) ([]store.VariableMeta, error) {
	return f.vars, nil
}
func (f *fakeStoreSource) QueryIndicators(_ context.Context, _ store.IndicatorQuery) ([]store.Indicator, error) {
	return f.inds, nil
}
func (f *fakeStoreSource) QueryGeographies(_ context.Context, q store.GeoQuery) ([]geo.Geography, error) {
	return f.geos[q.Level], nil
}

func fv(v float64) *float64 { return &v }

func fixtureSource() *fakeStoreSource {
	return &fakeStoreSource{
		vars: []store.VariableMeta{
			{VariableID: "poverty_rate", SourceID: "acs-5yr", SourceName: "American Community Survey 5-Year", Name: "Poverty Rate", Unit: "percent", Direction: "lower_better"},
			{VariableID: "cdc_svi_overall", SourceID: "cdc-svi", SourceName: "CDC/ATSDR SVI", Name: "Overall Vulnerability", Unit: "percentile", Direction: "lower_better"},
			{VariableID: "fbi_violent_crime_rate", SourceID: "fbi-nibrs", SourceName: "FBI NIBRS", Name: "Violent Crime Rate", Unit: "rate", Direction: "lower_better"},
		},
		inds: []store.Indicator{
			{GEOID: "55025", VariableID: "poverty_rate", Vintage: "ACS-2024-5yr", Value: fv(10.6)},
			{GEOID: "55025000100", VariableID: "poverty_rate", Vintage: "ACS-2024-5yr", Value: fv(12.3)},
			{GEOID: "55025000100", VariableID: "cdc_svi_overall", Vintage: "2022", Value: fv(0.87)},
			// Suppressed value: place exists, value nil.
			{GEOID: "55025000200", VariableID: "cdc_svi_overall", Vintage: "2022", Value: nil},
			// A 12-digit block group must be ignored, not crash.
			{GEOID: "550250001001", VariableID: "poverty_rate", Vintage: "ACS-2024-5yr", Value: fv(1)},
		},
		geos: map[geo.Level][]geo.Geography{
			geo.County: {{GEOID: "55025", Name: "Dane County"}},
			geo.Tract:  {{GEOID: "55025000100", Name: "Tract 1, Dane County"}, {GEOID: "55025000200", Name: "Tract 2, Dane County"}},
		},
	}
}

func TestLoadFromStoreBuildsDataset(t *testing.T) {
	ds, err := LoadFromStore(context.Background(), fixtureSource())
	if err != nil {
		t.Fatalf("LoadFromStore: %v", err)
	}

	// fbi_violent_crime_rate has zero rows → excluded from the vocabulary.
	if _, ok := ds.Indicator("fbi_violent_crime_rate"); ok {
		t.Error("zero-row variable must not enter the planner vocabulary")
	}
	if len(ds.Indicators) != 2 {
		t.Fatalf("indicators = %d, want 2", len(ds.Indicators))
	}

	// Per-indicator vintage and source survive to the metadata.
	pov, _ := ds.Indicator("poverty_rate")
	if pov.Vintage != "ACS-2024-5yr" || pov.SourceName != "American Community Survey 5-Year" {
		t.Errorf("poverty_rate provenance wrong: %+v", pov)
	}
	svi, _ := ds.Indicator("cdc_svi_overall")
	if svi.Vintage != "2022" {
		t.Errorf("svi vintage = %q, want 2022", svi.Vintage)
	}
	// 0–1 percentile must render as a decimal, not a thousands int or a percent.
	if svi.Format != "decimal" {
		t.Errorf("svi format = %q, want decimal", svi.Format)
	}
	if got := formatValue(0.87, svi); got != "0.87" {
		t.Errorf("formatValue(0.87) = %q, want 0.87", got)
	}

	// Citations carry the indicator's own vintage, not a dataset-wide one.
	cit := ds.citation(svi, LevelTract)
	if cit.Vintage != "2022" || cit.Source != "CDC/ATSDR SVI" {
		t.Errorf("citation = %+v", cit)
	}

	// Values at both levels; suppressed stays nil-with-presence.
	if v, ok := ds.Value("55025", LevelCounty, "poverty_rate"); !ok || v == nil || *v != 10.6 {
		t.Errorf("county value wrong: %v %v", v, ok)
	}
	if v, ok := ds.Value("55025000200", LevelTract, "cdc_svi_overall"); !ok || v != nil {
		t.Errorf("suppressed value must be present-and-nil, got %v ok=%v", v, ok)
	}
	// The block-group row was ignored.
	if _, ok := ds.Value("550250001001", LevelTract, "poverty_rate"); ok {
		t.Error("block-group GEOID must not appear at tract level")
	}

	// Name resolution incl. the bare-county convenience.
	if g, ok := ds.ResolvePlace("dane", LevelCounty); !ok || g != "55025" {
		t.Errorf("ResolvePlace(dane) = %q %v", g, ok)
	}

	// A tract present in geographies but with no indicator rows still
	// resolves by name — and reports every indicator as suppressed rather
	// than unknown. (Tract 2 has one nil row here, exercising backfill.)
	if v, ok := ds.Value("55025000200", LevelTract, "poverty_rate"); !ok || v != nil {
		t.Errorf("backfilled indicator must be present-and-nil, got %v ok=%v", v, ok)
	}
}

func TestLoadFromStoreRefusesEmptyStore(t *testing.T) {
	src := fixtureSource()
	src.inds = nil
	if _, err := LoadFromStore(context.Background(), src); err == nil {
		t.Fatal("want error for a store with no indicator data")
	}
}
