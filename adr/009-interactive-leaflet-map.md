# ADR-009: Interactive Leaflet Map with LISA Cluster Visualization

**Status:** Accepted
**Date:** 2026-07-27
**Deciders:** Cruz Morales, Hermes Agent

## Context

ADR-007 defined a LISA Cluster Map page (`map.html`) using hand-rolled SVG choropleth via `choropleth.js`. The initial implementation rendered a static SVG overlay of Wisconsin tracts colored by LISA cluster classification. It worked but lacked interactivity: no zoom, no pan, no click-to-inspect, no hover labels.

The project had vendored Leaflet 1.9.4 in `cmd/pdi/frontend/leaflet/` (CSS, JS, marker images) from a previous exploration but was not using it. The `tracts.geojson` (1,542 features, 1.8MB) and `counties.geojson` (72 features, 120KB) were already generated and embedded as static assets. The LISA analysis API (`GET /v1/policy/analyses/:id/scores`) was live with 6 analyses returning cluster classifications per tract.

The original ADR-004 Wave 4E proposed "Map visualization (Leaflet, choropleth by indicator)" as a post-grant task. With the infrastructure now in place, this could ship in the current cycle.

## Decision

Upgrade `map.html` from static SVG choropleth to an interactive Leaflet map using only vendored dependencies. No external tile providers, no new npm packages, no CDN.

### Architecture

1. **Base map:** Dark background (#0a0a0f) with no external tile layer — the map is a policy data visualization tool, not a general-purpose map. GeoJSON overlays provide all spatial context.

2. **Tract layer:** `L.geoJSON()` loading `/static/tracts.geojson` with 1,542 Wisconsin census tracts. Each feature styled by LISA cluster classification fetched from the live API.

3. **County overlay:** `/static/counties.geojson` rendered as boundary-only lines with hover labels, providing spatial reference without visual noise.

4. **LISA coloring:** Tracts colored by cluster tier:
   - HH (High-High): amber `#f59e0b` — concentrated disadvantage
   - LL (Low-Low): teal `#14b8a6` — concentrated advantage
   - HL (High-Low): purple `#a855f7` — outlier (worse than neighbors)
   - LH (Low-High): blue `#3b82f6` — outlier (better than neighbors)
   - NS (Not Significant): muted gray `#4a4a5a`

5. **Interactivity:** Click a tract → popup with tract name, GEOID, LISA cluster classification, p-value, and key ACS indicators (poverty rate, median income, POC%, uninsured rate). Link to county profile page.

6. **Variable selector:** Dropdown listing available LISA analyses grouped by variable_id (poverty_rate, pct_poc, pct_cost_burdened, etc.). Switching re-fetches scores and re-colors the map.

7. **State management:** Alpine.js handles loading, error, and empty states. The page initializes by fetching available analyses, loading GeoJSON, then applying cluster colors.

## Consequences

- **Positive:** Interactive zoom/pan makes the map usable for exploring spatial patterns at multiple scales. Click-to-inspect replaces the need for a separate table view.
- **Positive:** Zero new dependencies — Leaflet was already vendored. No external tile provider means no API keys, no rate limits, no third-party dependency.
- **Positive:** The variable selector makes all 6 LISA analyses explorable from one page.
- **Negative:** No basemap means users can't see roads, cities, or other geographic context. Acceptable for a policy data tool focused on statistical patterns, but limits orientation.
- **Negative:** 1.8MB GeoJSON loaded on page open — acceptable for broadband but heavy on mobile. Future: consider vector tiles (Martin) or TopoJSON simplification.
- **Negative:** The map is Wisconsin-only. National scale would require ~73,000 tract features (~85MB GeoJSON), which would need tiling.

## Comparison to Martin Vector Tiles

Martin (maplibre/martin, Rust, 3.8k★) was evaluated as an alternative. It would serve MVT tiles on-the-fly from PostGIS, enabling national-scale maps with progressive loading. However:
- VPS PostGIS `boundary` column is NULL for all geographies (TIGER geometry never loaded)
- Loading TIGER shapefiles into PostGIS is a separate multi-hour task
- The vendored Leaflet + GeoJSON approach ships immediately with zero new infrastructure

ADR-009 does not preclude future Martin integration — the GeoJSON overlay approach can be replaced with a vector tile source when PostGIS geometry is populated.
