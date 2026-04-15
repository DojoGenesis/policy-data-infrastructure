# CHANGELOG — policy-data-infrastructure

> Format: `## YYYY-MM-DD` sections, newest first. Update after every work session.
> Include row counts for data loads and root causes for fixes.

## 2026-04-15

### Scheduled Pipeline Verification
- All Apr 14 handoff targets confirmed complete — no new data loads required:
  - BLS LAUS: 72/72 WI counties, 0% null on unemployment_rate (rate limit cleared)
  - WI DPI attendance: 449 district rows, 0% null on chronic_absence_rate
  - Evidence cards: 70 cards generated (target was 50+; confirmed via `evidence_cards.json`)
  - VPS PostGIS geographies: 72 counties + 1,652 tracts loaded
  - VPS PostGIS indicators: 1,368 ACS, 12,200 CDC PLACES, 8,009 USDA (all above floor)
  - Gateway: `dojo-gateway.service` active since 2026-04-14, `/v1/models` and `/chat` both return 200
- Added `.wrangler/` to `.gitignore`
- Removed completed P1 BLS re-fetch item from TODO.md

## 2026-04-14

### BLS LAUS Fix
- Fixed series ID format: 8 fill zeros, not 7 — root cause of all-null data since Apr 14 — `1976e97`
  - LAUS county series IDs are 20 chars; script produced 19-char IDs
  - BLS accepted requests but returned empty data for non-existent series (silent drop)
  - Fixed registered batch size from 500 to 50 (actual BLS v2 limit)
  - 72/72 WI counties now return data: unemployment rate [2.1%, 5.8%], 0% null

### Factor Analysis
- New `analysis/factor_analysis.py`: EFA on 1,265 WI tracts with 12 features — `7739423`
  - 2 factors, 66.5% variance explained, KMO=0.833
  - Factor 1: Mental Health / Economic Deprivation (38.4%) — poverty, MHLTH, ICE, ACCESS2
  - Factor 2: Cardiovascular / Metabolic (28.1%) — BPHIGH, DIABETES, PHLTH, OBESITY
  - Outputs: factor_loadings.csv, factor_scores.csv (1,265 tracts)

### ACS B19001 ICE Ingest
- New `ingest/fetch_acs_b19001.py`: true ICE from cross-tabulated income-by-race data — `ba16b27`
  - 1,542 WI tracts, 1,524 with ICE scores (98.8%), range [-0.65, +0.82]
  - Replaces poverty×race approximation with Krieger 2016 methodology
- `analyze.go`: prefers true B19001 ICE scores when available, falls back to approximation

### Narrative Chain Fix
- Fixed 4 NARI→ICE rendering bugs blocking document generation — `d732c2b`
  - `selector.go`: now populates `p.ICE` field (was only setting deprecated NARI fields)
  - `engine.go`: "NARI Percentile" → "Equity Index Percentile", prose references ICE
  - 3 templates (`five_mornings`, `equity_profile`, `comparison_brief`): all user-facing NARI text replaced
  - Root cause: statistical refactor replaced NARI with ICE in pipeline but never updated narrative layer

### Health Audit & Infrastructure
- First comprehensive health audit completed — overall grade B-
- Rescued 6 research files from stale `policy-data-infrastructure/` clone
- Deleted stale clone — canonical directory is `policy-data-infra/`
- Created CLAUDE.md (300 lines, rewritten for Sonnet agents)
- Created TODO.md + CHANGELOG.md
- Added `.github/workflows/ci.yml` — go build + go vet + go test on push/PR

### P0 Fixes (all 7 resolved)
- `.gitignore`: added root `.venv/` and `analysis/output/` — `4b17097`
- Untracked 8 committed analysis output artifacts — `9fada5c`
- Makefile: ldflags PKG → `internal/version` (was `cmd/pdi`, silently broken) — `9fada5c`
- `go.mod`: version corrected via `go mod tidy` — `9fada5c`
- Pipeline: replaced deprecated NARI composite with ICE metric (Krieger et al. 2016) — `9fada5c`
- `analyze.go`: Percentile now uses `stats.PercentileRank()` instead of raw score — `9fada5c`
- `analyze.go`: removed `-1.0` sentinel, uses proper `*float64` nil — `9fada5c`

### P1 Fixes (8 of 9 resolved, 1 deferred)
- Gateway XSS: escape geography names with `html.EscapeString` — `f67d4dc`
- Gateway: log `LoadEmbeddedTemplates` errors instead of silent discard — `f67d4dc`
- Gateway: `errors.Is(err, pgx.ErrNoRows)` replaces string matching — `f67d4dc`
- Python `census.py`: `_clean_sentinel` handles float-string sentinel — `f67d4dc`
- `sources.toml`: cdc-places `api_key_env` → `CDC_PLACES_APP_TOKEN` — `9fada5c`
- `schemas/geography.schema.json`: county_fips 3-digit, `geo_level` → `level` — `9fada5c`
- Store: deleted dead `export.go`/`import.go`, promoted `PutIndicatorsBatch` to interface — `f67d4dc`
- **DEFERRED**: BLS LAUS re-run (rate limit, wait for UTC midnight reset)

### P2 Fixes (all 9 resolved)
- Gateway tests: 23 httptest handler tests (coverage: 0% → 23 tests) — `fc3a3e9`
- HTMLCraft tests: 41 tests across 6 groups (coverage: 0% → 41 tests) — `fc3a3e9`
- CI workflow: `.github/workflows/ci.yml` (build + vet + test) — `f67d4dc`
- Dead code: removed `buildURL()`, `buildStateURL()` from acs.go — `f67d4dc`
- Dead code: removed `geoLevelDisplay()` from query.go — `f67d4dc`
- CDC PLACES: 650ms rate limiting between paginated requests — `f67d4dc`
- README: marked 8 unimplemented sources as (planned), fixed Go version — `f67d4dc`
- `PutIndicatorsBatch` promoted to Store interface — `f67d4dc`

### P3 Fixes (2 of 5 resolved)
- Narrative: 12 magic numbers extracted to named consts with cited sources — `fc3a3e9`
- Narrative tests: 16 new tests (17 → 33 total) with table-driven + boundary — `fc3a3e9`

### New Features (from parallel orchestrator)
- `pkg/stats/features.go`: ICEIncomeRace + CoefficientOfVariation + ReliabilityLevel
- `pkg/stats/features_test.go`: test coverage for new stat functions
- `pkg/store/migrations/007`: cv, reliability columns + factor_scores + validated_features tables
- `pkg/narrative/slot.go`: FactorScores, FactorPercentiles, ICE, Reliability fields
- `pkg/narrative/template.go`: factor-based template helpers (factorLabel, factorColor, etc.)

### Session Stats
- 9 Sonnet agents dispatched across 2 waves (3+3+3)
- Total test count: 17 → 97 (+80 new tests across 3 packages)
- TODO items closed: 25 of 29 P0-P2 items (86%)
- Seed planted: Orchestrator Blindspot — stash don't revert parallel session work

### Data Pipeline
- Expanded evidence cards to 70 (all 70 policies, 0 skipped) — `32f5032`
- Added WI DPI attendance fetcher (449 districts, chronic_absence_rate 0% null)
- Fixed BLS LAUS script (startyear/endyear params) — data still null due to daily rate limit
- VPS PostGIS state: 72 counties + 1,652 tracts + 1,368 ACS + 12,200 CDC PLACES + 8,009 USDA = 22,949 indicator rows

## 2026-04-13

### Gateway & Narrative
- Wired narrative engine to gateway routes — `4edeebd`
- Fixed ACS FetchCounty: split detail/subject tables + SafeFloat string sentinel — `e205c08`

### Code Quality
- Deep code audit found 10 bugs across stats, pipeline, store — `3961a2d`
- Audit-driven sweep: 9 findings across 8 files — `b421cff`
- Fixed PostGIS-optional store + working analyze + narrative pipeline — `fc7dee6`

## 2026-04-12

### Analysis
- Multi-source evidence cards: CDC PLACES + USDA food access — `b110cb5`
- Fixed 5 data source fetchers, added WI output CSVs — `f3a1d61`
- Multi-source data ingest, idempotent migrations, county-level ACS fetch — `9b8ec01`

### Documentation
- Added README with architecture, data sources, API, and usage guide — `92311a9`

## 2026-04-11

### Infrastructure
- Phases 5+6: VPS deployment, national-scale fetch, CDC PLACES, EPA EJScreen — `8db0054`

## 2026-04-10

### Core Development
- v0.1 proof of concept: policy-to-evidence pipeline for Wisconsin — `f4b6ee7`
- Phases 3+4: pipeline engine, narrative generator, HTMLCraft bridge, gateway API, CLI wiring — `82d716a`

## 2026-04-09

### Foundation
- Policy record schema + Francesca Hong 2026 gubernatorial positions — `a724099`
- Phase 2 data ingest: Census API client, Python scripts, Store CRUD — `89a437d`
- Phase 1 foundation: pkg/geo, pkg/stats, pkg/store, CLI, PostGIS schema — `7680cfa`

---

## Update Conventions
- Update this file after every work session
- Use conventional commit categories: feat, fix, chore, docs, data
- Include commit hashes for traceability
- Include row counts and data state for pipeline changes
- For fixes: include the root cause, not just the symptom
- Agent sessions: append to the current date section or create a new one
