package htmlcraft

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// DeliverableOpts controls which optional sections are rendered in the
// generated HTML deliverable.
type DeliverableOpts struct {
	// Title is the document <title> and header text.
	Title string

	// IncludeMap embeds a Leaflet choropleth map when true.
	IncludeMap bool

	// IncludeCharts enables Chart.js visualisation sections when true.
	IncludeCharts bool

	// MapCenter is the initial [lat, lon] centre of the Leaflet map.
	MapCenter [2]float64

	// MapZoom is the initial Leaflet zoom level.
	MapZoom int

	// TileLayer selects the basemap style: "dark", "light", or "satellite".
	TileLayer string

	// Components lists which built-in Web Components to inline.
	// Valid values: "data-table", "chart-bar", "metric-card", "stat-callout".
	Components []string
}

// Bridge converts policy data from the Store into self-contained HTML
// deliverables. It combines a narrative HTML fragment with embedded data,
// inlined Web Components, and optional Leaflet/Chart.js integrations.
type Bridge struct {
	store store.Store
}

// NewBridge creates a Bridge backed by the given Store.
func NewBridge(s store.Store) *Bridge {
	return &Bridge{store: s}
}

// BuildDeliverable produces a complete single-file HTML document from a
// narrative HTML fragment and scope geography identifier. It:
//  1. Takes the narrative HTML fragment as the document body
//  2. Embeds indicator data as window.__DATA__.indicators
//  3. Embeds GeoJSON boundaries as window.__DATA__.geojson (when IncludeMap)
//  4. Inlines the requested Web Components
//  5. Adds Leaflet.js + Chart.js CDN links
//  6. Wraps everything in a full HTML5 document with print styles
func (b *Bridge) BuildDeliverable(ctx context.Context, narrativeHTML string, scopeGEOID string, opts DeliverableOpts) (string, error) {
	if opts.Title == "" {
		opts.Title = "Policy Data Brief"
	}
	if opts.MapZoom == 0 {
		opts.MapZoom = 10
	}

	// Load geography metadata for the scope.
	scopeGeo, err := b.store.GetGeography(ctx, scopeGEOID)
	if err != nil {
		return "", fmt.Errorf("htmlcraft: get scope geography %s: %w", scopeGEOID, err)
	}

	// Determine child level so we can embed child indicator data.
	childLevel, hasChildren := geo.ChildLevel(scopeGeo.Level)

	// Query indicators for the scope geography and its children.
	geoIDs := []string{scopeGEOID}
	if hasChildren {
		children, err := b.store.QueryGeographies(ctx, store.GeoQuery{
			Level:       childLevel,
			ParentGEOID: scopeGEOID,
			Limit:       500,
		})
		if err != nil {
			return "", fmt.Errorf("htmlcraft: query children for %s: %w", scopeGEOID, err)
		}
		for _, c := range children {
			geoIDs = append(geoIDs, c.GEOID)
		}
	}

	indicators, err := b.store.QueryIndicators(ctx, store.IndicatorQuery{
		GEOIDs:     geoIDs,
		LatestOnly: true,
	})
	if err != nil {
		return "", fmt.Errorf("htmlcraft: query indicators for %s: %w", scopeGEOID, err)
	}

	// Convert indicators to a JSON-serialisable slice.
	indRows := make([]map[string]interface{}, 0, len(indicators))
	for _, ind := range indicators {
		row := map[string]interface{}{
			"geoid":       ind.GEOID,
			"variable_id": ind.VariableID,
			"vintage":     ind.Vintage,
			"raw_value":   ind.RawValue,
		}
		if ind.Value != nil {
			row["value"] = *ind.Value
		} else {
			row["value"] = nil
		}
		if ind.MarginOfError != nil {
			row["margin_of_error"] = *ind.MarginOfError
		} else {
			row["margin_of_error"] = nil
		}
		indRows = append(indRows, row)
	}

	datasets := []DataSet{
		{Key: "indicators", Data: indRows},
		{Key: "scope", Data: scopeGeo},
	}

	// Build GeoJSON embed for Leaflet when requested.
	var mapScript string
	if opts.IncludeMap {
		geos, err := b.store.QueryGeographies(ctx, store.GeoQuery{
			Level:       scopeGeo.Level,
			ParentGEOID: scopeGeo.ParentGEOID,
			Limit:       2000,
		})
		if err != nil {
			return "", fmt.Errorf("htmlcraft: query geographies for map: %w", err)
		}

		// Encode geographies as a lightweight GeoJSON-compatible structure.
		// Full PostGIS GeoJSON is not available through the Store interface,
		// so we embed the geography metadata and synthesise point features.
		type pointFeature struct {
			Type       string                 `json:"type"`
			Properties map[string]interface{} `json:"properties"`
			Geometry   map[string]interface{} `json:"geometry"`
		}
		type featureCollection struct {
			Type     string         `json:"type"`
			Features []pointFeature `json:"features"`
		}

		fc := featureCollection{Type: "FeatureCollection", Features: make([]pointFeature, 0, len(geos))}
		for _, g := range geos {
			fc.Features = append(fc.Features, pointFeature{
				Type: "Feature",
				Properties: map[string]interface{}{
					"geoid": g.GEOID,
					"name":  g.Name,
					"level": string(g.Level),
				},
				Geometry: map[string]interface{}{
					"type":        "Point",
					"coordinates": []float64{g.Lon, g.Lat},
				},
			})
		}
		datasets = append(datasets, DataSet{Key: "geojson", Data: fc})

		mapScript = b.buildMapScript(opts, scopeGeo)
	}

	// Embed all datasets as window.__DATA__.
	dataEmbed, err := EmbedData(datasets)
	if err != nil {
		return "", fmt.Errorf("htmlcraft: embed data: %w", err)
	}

	// Inline the requested Web Components.
	componentScripts := InlineComponents(opts.Components)

	// Build the head block: data embed + component scripts.
	headParts := []string{dataEmbed}
	head := strings.Join(headParts, "\n")

	// Build scripts block: components + optional map initialisation.
	scriptParts := []string{componentScripts}
	if mapScript != "" {
		scriptParts = append(scriptParts, mapScript)
	}
	scripts := strings.Join(scriptParts, "\n")

	// Wrap narrative HTML in a page layout.
	body := b.buildBody(opts.Title, scopeGeo, narrativeHTML, opts)

	return RenderFull(opts.Title, head, body, scripts), nil
}

// buildBody wraps the narrative HTML in the standard page layout shell.
func (b *Bridge) buildBody(title string, scope *geo.Geography, narrativeHTML string, opts DeliverableOpts) string {
	var sb strings.Builder

	sb.WriteString(`<header class="pdi-header">`)
	sb.WriteString(fmt.Sprintf(`<div><h1>%s</h1>`, htmlEscape(title)))
	if scope != nil {
		sb.WriteString(fmt.Sprintf(`<div class="pdi-subtitle">%s · %s</div>`, htmlEscape(scope.Name), htmlEscape(string(scope.Level))))
	}
	sb.WriteString(`</div></header>`)
	sb.WriteString(`<div class="pdi-container">`)

	if opts.IncludeMap {
		sb.WriteString(`<div class="pdi-section"><h2>Map</h2><div id="pdi-map" class="pdi-map"></div></div>`)
	}

	sb.WriteString(`<div class="pdi-section">`)
	sb.WriteString(narrativeHTML)
	sb.WriteString(`</div>`)

	sb.WriteString(`</div>`)
	return sb.String()
}

// buildMapScript generates the Leaflet initialisation script.
func (b *Bridge) buildMapScript(opts DeliverableOpts, scope *geo.Geography) string {
	lat := opts.MapCenter[0]
	lon := opts.MapCenter[1]
	if lat == 0 && lon == 0 && scope != nil {
		lat = scope.Lat
		lon = scope.Lon
	}
	if lat == 0 && lon == 0 {
		lat = 39.5
		lon = -98.35 // continental US centre
	}
	zoom := opts.MapZoom
	if zoom == 0 {
		zoom = 10
	}

	tileURL := "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
	tileAttrib := `&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors`
	switch opts.TileLayer {
	case "dark":
		tileURL = "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
		tileAttrib = `&copy; <a href="https://carto.com/">CARTO</a>`
	case "satellite":
		tileURL = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
		tileAttrib = `Tiles &copy; Esri`
	}

	geojsonJSON, _ := json.Marshal((map[string]interface{}{"key": "geojson"}))
	_ = geojsonJSON

	return fmt.Sprintf(`<script>
(function() {
  var map = L.map('pdi-map').setView([%f, %f], %d);
  L.tileLayer('%s', { attribution: '%s', maxZoom: 19 }).addTo(map);
  var gj = (window.__DATA__ || {}).geojson;
  if (gj) {
    L.geoJSON(gj, {
      pointToLayer: function(feature, latlng) {
        return L.circleMarker(latlng, { radius: 6, fillColor: '#1b4a7a', color: '#fff', weight: 1, opacity: 1, fillOpacity: 0.85 });
      },
      onEachFeature: function(feature, layer) {
        if (feature.properties) {
          layer.bindPopup('<strong>' + (feature.properties.name || feature.properties.geoid) + '</strong>');
        }
      }
    }).addTo(map);
  }
})();
</script>`, lat, lon, zoom, tileURL, tileAttrib)
}

// htmlEscape escapes the five special HTML characters. A minimal
// implementation to avoid importing html/template for a single use.
func htmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, `"`, "&#34;")
	s = strings.ReplaceAll(s, "'", "&#39;")
	return s
}
