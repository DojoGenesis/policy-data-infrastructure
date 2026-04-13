package datasource

// TIGERSource is an adapter for Census Bureau TIGER/Line geographic boundary
// files. It supports two download paths:
//
//  1. Full TIGER/Line shapefiles (ZIP, requires shapefile parsing — TODO):
//     https://www2.census.gov/geo/tiger/TIGER{year}/TRACT/tl_{year}_{statefips}_tract.zip
//
//  2. Cartographic boundary files pre-converted to GeoJSON-within-ZIP (smaller,
//     500k generalisation) — implemented via FetchBoundariesGeoJSON:
//     https://www2.census.gov/geo/tiger/GENZ{year}/shp/cb_{year}_{statefips}_tract_500k.zip
//
// Only URL construction and HTTP download are implemented; shapefile (.dbf /
// .shp / .prj) parsing is deferred and marked TODO below.

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// TIGERSource fetches Census geographic boundary data from the TIGER/Line
// file distribution server.
type TIGERSource struct {
	// Year is the TIGER vintage year, e.g. 2024.
	Year int
	// HTTPClient is used for all outbound requests. If nil, http.DefaultClient
	// is used.
	HTTPClient *http.Client
}

// NewTIGERSource returns a TIGERSource for the given vintage year.
func NewTIGERSource(year int) *TIGERSource {
	return &TIGERSource{Year: year, HTTPClient: http.DefaultClient}
}

// tractShapefileURL returns the TIGER/Line shapefile ZIP URL for census tracts
// in the given state.
//
// Example (year=2024, stateFIPS="55"):
//
//	https://www2.census.gov/geo/tiger/TIGER2024/TRACT/tl_2024_55_tract.zip
func (t *TIGERSource) tractShapefileURL(stateFIPS string) string {
	return fmt.Sprintf(
		"https://www2.census.gov/geo/tiger/TIGER%d/TRACT/tl_%d_%s_tract.zip",
		t.Year, t.Year, stateFIPS,
	)
}

// cartographicBoundaryURL returns the cartographic boundary file ZIP URL for
// census tracts at the 500k generalisation level.
//
// Example (year=2024, stateFIPS="55"):
//
//	https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_55_tract_500k.zip
func (t *TIGERSource) cartographicBoundaryURL(stateFIPS string) string {
	return fmt.Sprintf(
		"https://www2.census.gov/geo/tiger/GENZ%d/shp/cb_%d_%s_tract_500k.zip",
		t.Year, t.Year, stateFIPS,
	)
}

// FetchBoundariesGeoJSON downloads the cartographic boundary ZIP for the given
// state and extracts the GeoJSON file from within it. The Census Bureau
// cartographic boundary ZIPs for recent vintages contain a .json / .geojson
// file alongside the traditional .shp files.
//
// Returns the raw GeoJSON bytes. Callers are responsible for parsing and
// loading the geometry into the store.
func (t *TIGERSource) FetchBoundariesGeoJSON(ctx context.Context, stateFIPS string) ([]byte, error) {
	url := t.cartographicBoundaryURL(stateFIPS)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("tiger: build request: %w", err)
	}

	client := t.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tiger: http get %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("tiger: server returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	zipBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("tiger: read response body: %w", err)
	}

	zr, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, fmt.Errorf("tiger: open zip: %w", err)
	}

	// Look for a GeoJSON file inside the archive. Census boundary ZIPs may
	// contain .json or .geojson files in recent vintages. Fall back to any
	// JSON-like file if no dedicated .geojson is found.
	var geojsonFile *zip.File
	for _, f := range zr.File {
		name := strings.ToLower(f.Name)
		if strings.HasSuffix(name, ".geojson") {
			geojsonFile = f
			break
		}
		if strings.HasSuffix(name, ".json") && geojsonFile == nil {
			geojsonFile = f
		}
	}
	if geojsonFile == nil {
		// List available files for diagnostics.
		var names []string
		for _, f := range zr.File {
			names = append(names, f.Name)
		}
		return nil, fmt.Errorf("tiger: no .geojson or .json file found in ZIP (files: %s)", strings.Join(names, ", "))
	}

	rc, err := geojsonFile.Open()
	if err != nil {
		return nil, fmt.Errorf("tiger: open %s in zip: %w", geojsonFile.Name, err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("tiger: read %s from zip: %w", geojsonFile.Name, err)
	}
	return data, nil
}

// FetchShapefile downloads the full TIGER/Line shapefile ZIP for the given
// state and returns the raw ZIP bytes. Shapefile parsing (.shp / .dbf) is not
// yet implemented — callers may use a dedicated shapefile library.
//
// TODO: implement shapefile parsing and return []geo.Geography with geometry.
func (t *TIGERSource) FetchShapefile(ctx context.Context, stateFIPS string) ([]byte, error) {
	url := t.tractShapefileURL(stateFIPS)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("tiger: build request: %w", err)
	}

	client := t.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tiger: http get %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("tiger: server returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return io.ReadAll(resp.Body)
}

// Name returns the source identifier.
func (t *TIGERSource) Name() string { return "tiger" }

// Category returns the data category.
func (t *TIGERSource) Category() string { return "boundaries" }

// Vintage returns the data vintage string.
func (t *TIGERSource) Vintage() string { return fmt.Sprintf("TIGER-%d", t.Year) }

// Schema returns an empty slice — TIGER provides geometries, not tabular
// indicators, so there are no VariableDefs.
func (t *TIGERSource) Schema() []VariableDef { return nil }

// FetchCounty returns 501 Not Implemented. TIGER/Line provides geometry, not
// tabular indicators; use FetchBoundariesGeoJSON or FetchShapefile instead.
func (t *TIGERSource) FetchCounty(_ context.Context, _, _ string) ([]store.Indicator, error) {
	return nil, fmt.Errorf("tiger: FetchCounty not implemented — use FetchBoundariesGeoJSON or FetchShapefile")
}

// FetchState returns 501 Not Implemented. TIGER/Line provides geometry, not
// tabular indicators; use FetchBoundariesGeoJSON or FetchShapefile instead.
func (t *TIGERSource) FetchState(_ context.Context, _ string) ([]store.Indicator, error) {
	return nil, fmt.Errorf("tiger: FetchState not implemented — use FetchBoundariesGeoJSON or FetchShapefile")
}
