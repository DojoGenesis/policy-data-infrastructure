package grounding

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Dataset is the Atlas bundle in memory: the same files the static site ships,
// read once at startup. Loading the shipped artefact rather than querying
// PostGIS means the chat can never answer from a different dataset than the one
// a reader is looking at on the page — the two cannot drift.
type Dataset struct {
	Vintage    string
	Indicators []IndicatorMeta

	// Values are indexed [level][geoid][indicatorID].
	values map[Level]map[string]map[string]*float64
	names  map[Level]map[string]string // geoid -> display name
	byName map[Level]map[string]string // lowercased name -> geoid

	// Representation is optional; nil when PIP-78's outputs are absent.
	Representation map[string]TractRepresentation

	Sources []SourceMeta
}

// IndicatorMeta is the subset of indicators.json this package needs.
type IndicatorMeta struct {
	ID        string `json:"id"`
	Label     string `json:"label"`
	LabelEs   string `json:"labelEs"`
	Unit      string `json:"unit"`
	Format    string `json:"format"`
	Direction string `json:"direction"`
	Table     string `json:"table"`
	// Description carries the indicator's own definition, e.g. "spending 30% or
	// more of income on housing". Verification needs it: the 30 in that sentence
	// is a dataset fact, and a draft that explains the definition would
	// otherwise be rejected for quoting it.
	Description string `json:"descriptionEn"`
	// Vintage is this indicator's own data vintage. The store-backed dataset
	// spans sources whose vintages differ (ACS-2024-5yr beside 2022 beside
	// USDA-FARA-2019); a citation that named one dataset-wide vintage for all
	// of them would be provenance-shaped fiction. Empty means "use the
	// dataset-wide vintage" (the atlas bundle's single-vintage world).
	Vintage string `json:"vintage,omitempty"`
	// SourceName names the publishing source for citations. Empty falls back
	// to the dataset-wide source heuristic.
	SourceName string `json:"source,omitempty"`
}

// SourceMeta is a citable provenance record.
type SourceMeta struct {
	Name      string `json:"name"`
	Publisher string `json:"publisher"`
	Vintage   string `json:"vintage"`
	URL       string `json:"url"`
	UsedFor   string `json:"used_for"`
}

// District is one seat covering a place.
type District struct {
	District      string `json:"district"`
	DistrictLabel string `json:"districtLabel"`
	Official      *struct {
		Name  string `json:"name"`
		Party string `json:"party"`
		URL   string `json:"url"`
	} `json:"official"`
}

// TractRepresentation is every seat covering one tract.
type TractRepresentation map[string]District

type featureCollection struct {
	Features []struct {
		Properties map[string]any `json:"properties"`
	} `json:"features"`
}

// Load reads an Atlas bundle directory (analysis/output/atlas).
func Load(dir string) (*Dataset, error) {
	ds := &Dataset{
		values: map[Level]map[string]map[string]*float64{},
		names:  map[Level]map[string]string{},
		byName: map[Level]map[string]string{},
	}

	var indFile struct {
		Indicators []IndicatorMeta `json:"indicators"`
	}
	if err := readJSON(filepath.Join(dir, "indicators.json"), &indFile); err != nil {
		return nil, fmt.Errorf("indicators: %w", err)
	}
	ds.Indicators = indFile.Indicators

	var manifest struct {
		Sources []SourceMeta `json:"sources"`
	}
	if err := readJSON(filepath.Join(dir, "manifest.json"), &manifest); err == nil {
		ds.Sources = manifest.Sources
		for _, s := range manifest.Sources {
			if s.Vintage != "" && ds.Vintage == "" {
				ds.Vintage = s.Vintage
			}
		}
	}

	for level, file := range map[Level]string{
		LevelCounty: "counties.geojson",
		LevelTract:  "tracts.geojson",
	} {
		var fc featureCollection
		if err := readJSON(filepath.Join(dir, file), &fc); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		ds.values[level] = map[string]map[string]*float64{}
		ds.names[level] = map[string]string{}
		ds.byName[level] = map[string]string{}

		for _, f := range fc.Features {
			geoid, _ := f.Properties["GEOID"].(string)
			if geoid == "" {
				continue
			}
			name := displayName(level, f.Properties)
			ds.names[level][geoid] = name
			// Index by both the full name and, for counties, the bare name —
			// people say "Dane", not "Dane County".
			ds.byName[level][strings.ToLower(name)] = geoid
			if level == LevelCounty {
				bare := strings.TrimSuffix(name, " County")
				ds.byName[level][strings.ToLower(bare)] = geoid
			}

			vals := map[string]*float64{}
			for _, ind := range ds.Indicators {
				switch v := f.Properties[ind.ID].(type) {
				case float64:
					x := v
					vals[ind.ID] = &x
				default:
					vals[ind.ID] = nil // explicit: suppressed, not zero
				}
			}
			ds.values[level][geoid] = vals
		}
	}

	// Optional representation layer.
	var rep struct {
		Tracts map[string]TractRepresentation `json:"tracts"`
	}
	if err := readJSON(filepath.Join(dir, "representation.json"), &rep); err == nil {
		ds.Representation = rep.Tracts
	}

	return ds, nil
}

func displayName(level Level, props map[string]any) string {
	if level == LevelCounty {
		if n, ok := props["county_name"].(string); ok && n != "" {
			return n
		}
	}
	if n, ok := props["tract_name"].(string); ok && n != "" {
		if c, ok := props["county_name"].(string); ok && c != "" {
			return n + ", " + c
		}
		return n
	}
	if n, ok := props["NAME"].(string); ok {
		return n
	}
	if g, ok := props["GEOID"].(string); ok {
		return g
	}
	return ""
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// Indicator returns metadata for an indicator id.
func (ds *Dataset) Indicator(id string) (IndicatorMeta, bool) {
	for _, ind := range ds.Indicators {
		if ind.ID == id {
			return ind, true
		}
	}
	return IndicatorMeta{}, false
}

// IndicatorIDs returns every indicator id, sorted — used in refusal messages so
// a rejected question comes back with the actual vocabulary.
func (ds *Dataset) IndicatorIDs() []string {
	out := make([]string, 0, len(ds.Indicators))
	for _, ind := range ds.Indicators {
		out = append(out, ind.ID)
	}
	sort.Strings(out)
	return out
}

// ResolvePlace maps a name or GEOID to a GEOID at the given level.
func (ds *Dataset) ResolvePlace(s string, level Level) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	if _, ok := ds.values[level][s]; ok {
		return s, true
	}
	if g, ok := ds.byName[level][strings.ToLower(s)]; ok {
		return g, true
	}
	return "", false
}

// Name returns the display name for a GEOID.
func (ds *Dataset) Name(geoid string, level Level) string {
	if n, ok := ds.names[level][geoid]; ok {
		return n
	}
	return geoid
}

// Value returns the indicator value for a place. The second return distinguishes
// "this place is not in the dataset" from "this place has no value" — collapsing
// those two is how a suppressed estimate becomes a zero.
func (ds *Dataset) Value(geoid string, level Level, indicator string) (*float64, bool) {
	byGeo, ok := ds.values[level]
	if !ok {
		return nil, false
	}
	vals, ok := byGeo[geoid]
	if !ok {
		return nil, false
	}
	v, ok := vals[indicator]
	return v, ok
}

// GeoIDs returns every GEOID at a level, sorted for deterministic output.
func (ds *Dataset) GeoIDs(level Level) []string {
	out := make([]string, 0, len(ds.values[level]))
	for g := range ds.values[level] {
		out = append(out, g)
	}
	sort.Strings(out)
	return out
}

// Count returns how many places exist at a level.
func (ds *Dataset) Count(level Level) int { return len(ds.values[level]) }
