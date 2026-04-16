# Policy Data Infrastructure

Open-source national policy data platform. Ingests census, health, housing, environmental, and education data at the census tract level for all 85,000+ US tracts. Runs validated statistical analyses grounded in peer-reviewed methodology. Generates data-driven narratives and single-file HTML deliverables for policy audiences.

**Raw data is the foundation and a complex viewer is the deliverable: live at policydatainfrastructure.com and api.policydatainfrastructure.com!**

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

PDI uses a five-layer statistical architecture where each layer builds on the one below. Composites exist only at the top — computed at query time, never stored as truth.

```
Layer 5: Composite Views       Query-time only, geometric mean, sensitivity analysis
           |
Layer 4: Spatial Analysis      LISA clusters, GWR coefficients, multilevel variance
           |
Layer 3: Factor Scores         EFA-derived, named by loading profile, documented
           |
Layer 2: Validated Features    ICE (Krieger 2016), Dissimilarity (Massey & Denton 1988)
           |
Layer 1: Raw Indicators        Stored in PostGIS, CV-flagged for reliability
```

### Pipeline

Data moves from public sources to policy-ready deliverables through a six-stage DAG:

```
fetch → process → enrich → analyze → synthesize → deliver
  |         |         |         |          |           |
  |    normalize   join to   ICE +      OLS +     HTMLCraft
  |    + clean    geography  validated  tipping   deliverable
  |                          features   points
  |
  Census ACS, TIGER, CDC PLACES, EPA, BLS, USDA, HUD, HRSA
```

Stages run in concurrent waves using topological sort, bounded by parallelism settings.

## Data Sources

| Source | Category | Status | Variables | Geo Level |
|--------|----------|--------|-----------|-----------|
| Census ACS 5-Year | Demographic | Live | Income, poverty, race, education, insurance | Tract, Block Group |
| TIGER/Line | Geographic | Live | Boundaries at all levels | All |
| CDC PLACES | Health | Live | 8 measures: BP, diabetes, obesity, mental health | Tract |
| EPA EJScreen | Environment | Live | 12 EJ indicators: PM2.5, lead, superfund | Tract |
| USDA Food Access | Food | Live | Food desert flags, supermarket distance | Tract |
| BLS LAUS | Employment | Live | Unemployment, labor force | County |
| WI DPI | Education | Live | Attendance, chronic absence by district | District |
| HUD CHAS | Housing | Planned | Cost burden, crowding by income/race | Tract |
| HOLC Redlining | Historical | Planned | Grades A-D for ~200 cities | Custom polygons |
| HRSA HPSA | Health | Planned | Health professional shortage areas | County |
| Eviction Lab | Housing | Planned | Filing rates, eviction rates | Tract |
| GTFS | Transit | Planned | Frequency, stop density | Point/Route |

## Statistical Engine

Grounded in peer-reviewed methodology (30 sources documented in `research/references.csv`):

### Validated Features (Layer 2)
- **ICE**: Index of Concentration at the Extremes (Krieger et al. 2016) — spatial polarization without collapsing race and income
- **Dissimilarity Index**: D = 0.5 x Sum|bi/B - wi/W| (Massey & Denton 1988) — residential segregation
- **Isolation Index**: P* (Lieberson 1981) — group-specific exposure
- **Coefficient of Variation**: CV = SE/estimate for ACS reliability flagging (Census Bureau thresholds)

### Core Methods (Layers 3-4)
- **Factor Analysis**: EFA with oblimin rotation for indicator dimensionality reduction (Kolak et al. 2020)
- **Regression**: OLS with standard errors, t-stats, p-values
- **Decomposition**: Blinder-Oaxaca (generalized two-group equity gap decomposition)
- **Threshold Detection**: Piecewise linear breakpoint detection (segmented regression)
- **Bootstrap**: Parallel confidence intervals via goroutines
- **Interaction Effects**: Automated interaction term construction

### What We Don't Do
- No unvalidated composite indices. The CDC SVI predicts only 38.9% of outcome variability (PMC 2022). The ADI is 98.8% explained by 2 of 17 variables when unstandardized (Health Affairs Scholar 2023).
- No arbitrary tier cutoffs. LISA cluster classification replaces percentile-based tiers.
- No stored composites. Any composite is computed at query time with mandatory sensitivity analysis.

## Narrative Templates

Generate data-driven policy documents mechanically from any geography:

- **five_mornings** — Multi-chapter narrative showing how structural conditions shape daily life across neighborhoods. Generalized from "Five Mornings in Madison" (Common Wealth Development, 2026).
- **equity_profile** — Single-geography deep profile with all indicators, factor scores, and validated features.
- **comparison_brief** — Side-by-side comparison of two geographies with divergence analysis.

## API

Start with `pdi serve --port 8340`. All routes under `/v1/policy/`:

```
GET  /geographies                  List geographic levels and counts
GET  /geographies/:geoid           Full profile for a geography
GET  /geographies/:geoid/children  Drill down to child geographies
GET  /geographies/:geoid/indicators All indicators for a geography
POST /query                        Flexible filter/aggregate/compare
POST /compare                      Side-by-side comparison
POST /generate/narrative           Generate narrative HTML
POST /generate/deliverable         Generate full HTMLCraft deliverable
POST /pipeline/run                 Trigger pipeline execution
GET  /pipeline/events              SSE stream for pipeline progress
GET  /sources                      List data sources with metadata
```

## National Scale

```bash
# Fetch all 50 states + DC (parallel, rate-limit aware)
pdi fetch --scope national --sources acs-5yr --parallel 5

# Run national pipeline
pdi pipeline run --scope national --year 2023
```

## Project Structure

```
cmd/pdi/          CLI (cobra): migrate, fetch, analyze, query, generate, serve, pipeline
pkg/stats/        Statistical engine: validated features, OLS, decomposition, bootstrap
pkg/geo/          GEOID parsing, geographic hierarchy, GeoJSON R/W
pkg/store/        PostgreSQL + PostGIS (pgx/v5, COPY bulk ingest, spatial queries)
pkg/datasource/   Data source adapters (Census, CDC, EPA, BLS, USDA, FIPS tables)
pkg/pipeline/     6-stage DAG engine (fetch -> process -> enrich -> analyze -> synthesize -> deliver)
pkg/narrative/    Template engine with typed slots, geography selection, story assembly
pkg/htmlcraft/    Single-file HTML deliverables (Leaflet maps, Chart.js, Web Components)
pkg/gateway/      Gin REST API plugin (12 endpoints, SSE streaming)
pkg/policy/       Policy record models, equity dimension crosswalk
ingest/           Python scripts for data acquisition + Postgres bulk loading
analysis/         Python analysis scripts (factor analysis, evidence cards)
research/         Statistical methodology research (30 peer-reviewed sources)
data/             Static manifests: sources.toml, crosswalks, policy CSVs
schemas/          JSON Schema definitions for core data types
deploy/           VPS deployment scripts (setup.sh, backup.sh, nginx.conf)
```

## Requirements

- Go 1.24+
- PostgreSQL 16 with PostGIS 3.4
- Python 3.10+ (for ingest and analysis scripts)
- Census API key (optional, increases rate limit from 45 to 500 req/min)

## Research

The statistical architecture is grounded in 30 peer-reviewed and technical sources, documented in `research/`:

| File | Content |
|------|---------|
| `01_composite_index_methodology.md` | CDC SVI, EPA EJScreen, HDI, ADI, Opportunity Atlas — when composites work and when they fail |
| `02_cross_tabulation_methods.md` | ICE, disaggregated analysis, threshold detection, quantile regression, small-area estimation |
| `03_platform_landscape.md` | Census Reporter, COI 3.0, National Equity Atlas, Opportunity Insights — where PDI fits |
| `04_spatial_statistics.md` | LISA, factor analysis, ScaGWR, multilevel models, SKATER — all benchmarked at 85K tracts |
| `05_refactor_plan.md` | 6-phase implementation plan for raw-data-first architecture |
| `06_recommendations.md` | 20 prioritized next steps |
| `references.csv` | Full reference list with URLs and key findings |

## Origin

PDI began as the Madison Equity Atlas — a 22-layer GIS platform analyzing 125 census tracts in Dane County, WI for Common Wealth Development. The Atlas produced "Five Mornings in Madison" (a narrative document profiling five households shaped by structural conditions), a founding partnership proposal, and 70 evidence cards mapping policy positions to tract-level data. PDI generalizes the Atlas methodology to national scale with a research-grounded statistical architecture.

## License

Apache-2.0
