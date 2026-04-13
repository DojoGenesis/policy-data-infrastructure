# policy-data-infrastructure

Open-source national policy data platform. Ingests census, health, housing, environmental, and education data at the census tract level. Runs statistical analyses (OLS, Blinder-Oaxaca decomposition, composite indices). Generates data-driven narratives and single-file HTML deliverables.

## Quick Start

```bash
# Start PostgreSQL + PostGIS
docker compose up -d

# Run database migrations
go run ./cmd/pdi migrate up

# Fetch Dane County, WI data (as a test)
go run ./cmd/pdi fetch --state 55 --county 025 --year 2023

# Run the full pipeline
go run ./cmd/pdi pipeline run --state 55 --county 025

# Query a tract
go run ./cmd/pdi query indicators --geoid 55025000100

# Generate a narrative
go run ./cmd/pdi generate narrative --template five_mornings --scope county:55025

# Start the API server
go run ./cmd/pdi serve --port 8340
```

## Architecture

```
                  HTMLCraft Deliverables
                  Single-file HTML with maps, charts, narratives
                         |
                  Narrative Engine (Go)
                  Template-driven story generation
                         |
                  Analysis Engine (Go)
                  OLS, Blinder-Oaxaca, NARI composites, bootstrap CIs
                         |
                  PostgreSQL + PostGIS
                  Spatial indexes, concurrent writes, 85K+ tracts
                         |
                  Data Ingest (Python + Go)
                  Census API, TIGER, CDC PLACES, EPA, HUD, USDA
                         |
                  Gateway Plugin (Go)
                  REST API + SSE on VPS
```

## Data Sources

| Source | Category | Variables | Geo Level |
|--------|----------|-----------|-----------|
| Census ACS 5-Year | Demographic | Income, poverty, race, education, insurance | Tract, Block Group |
| TIGER/Line | Geographic | Boundaries at all levels | All |
| CDC PLACES | Health | 8 measures: BP, diabetes, obesity, mental health | Tract |
| EPA EJScreen | Environment | 12 EJ indicators: PM2.5, lead, superfund | Tract |
| HUD CHAS | Housing | Cost burden, crowding by income/race | Tract |
| USDA Food Access | Food | Food desert flags, supermarket distance | Tract |
| HOLC Redlining | Historical | Grades A-D for ~200 cities | Custom polygons |
| HRSA HPSA | Health | Health professional shortage areas | County |
| Eviction Lab | Housing | Filing rates, eviction rates | Tract |
| BLS LAUS | Employment | Unemployment, labor force | County |
| GTFS | Transit | Frequency, stop density | Point/Route |
| NCES | Education | School characteristics, Title I, FRL | School |

## Statistical Engine

Ported from a proven 125-tract Dane County pipeline and extended for national scale:

- **Descriptive**: z-score normalization, percentile ranks
- **Correlation**: Pearson r, Spearman rho
- **Regression**: OLS with standard errors, t-stats, p-values
- **Decomposition**: Blinder-Oaxaca (generalized two-group)
- **Composite indices**: NARI-style configurable weights with tier assignment
- **Bootstrap**: Parallel confidence intervals via goroutines
- **Tipping points**: Piecewise linear breakpoint detection
- **Interaction effects**: Automated interaction term construction

## Narrative Templates

Generate data-driven policy documents mechanically from any geography:

- **five_mornings** -- Multi-chapter narrative showing how structural conditions shape daily life across neighborhoods (generalized from "Five Mornings in Madison")
- **equity_profile** -- Single-geography deep profile with all indicators
- **comparison_brief** -- Side-by-side comparison of two geographies

## API

Start with `pdi serve --port 8340`. All routes under `/v1/policy/`:

```
GET  /geographies              List geographic levels and counts
GET  /geographies/:geoid       Full profile for a geography
GET  /geographies/:geoid/children   Drill down to child geographies
GET  /geographies/:geoid/indicators All indicators for a geography
POST /query                    Flexible filter/aggregate/compare
POST /compare                  Side-by-side comparison
POST /generate/narrative       Generate narrative HTML
POST /generate/deliverable     Generate full HTMLCraft deliverable
POST /pipeline/run             Trigger pipeline execution
GET  /pipeline/events          SSE stream for pipeline progress
GET  /sources                  List data sources with metadata
```

## National Scale

```bash
# Fetch all 50 states + DC (parallel, rate-limit aware)
pdi fetch --scope national --sources acs-5yr --parallel 5

# Run national pipeline
pdi pipeline run --scope national --year 2023
```

## Deployment

```bash
# Build Docker image
docker compose build

# Deploy on VPS
docker compose up -d

# Or use goreleaser for binary releases
goreleaser release --clean
```

See `deploy/` for nginx config, backup scripts, and VPS setup.

## Project Structure

```
cmd/pdi/          CLI (cobra): migrate, fetch, analyze, query, generate, serve, pipeline
pkg/geo/          GEOID parsing, geographic hierarchy, GeoJSON R/W
pkg/stats/        Statistical engine (OLS, decomposition, bootstrap, composites)
pkg/store/        PostgreSQL + PostGIS (pgx/v5, COPY bulk ingest, spatial queries)
pkg/datasource/   Data source adapters (Census, CDC, EPA, FIPS tables)
pkg/pipeline/     6-stage DAG engine (fetch → process → enrich → analyze → synthesize → deliver)
pkg/narrative/    Template engine with typed slots, geography selection, story assembly
pkg/htmlcraft/    Single-file HTML deliverables (Leaflet maps, Chart.js, Web Components)
pkg/gateway/      Gin REST API plugin (12 endpoints, SSE streaming)
ingest/           Python scripts for data acquisition + Postgres bulk loading
```

## Requirements

- Go 1.22+
- PostgreSQL 16 with PostGIS 3.4
- Python 3.10+ (for ingest scripts, optional)
- Census API key (optional, increases rate limit from 45 to 500 req/min)

## License

Apache-2.0
