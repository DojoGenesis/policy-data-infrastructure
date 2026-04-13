# policy-data-infrastructure

## Project Overview
National and worldwide open-source policy data platform. Ingests Census, TIGER, HUD, EPA, HMDA,
BLS, FEMA, CDC, USDA, OpenStreetMap, and other open datasets; stores indicators at multiple
geographic levels in PostGIS; runs a statistical analysis engine; generates narrative summaries;
and exports HTMLCraft deliverables for policy audiences.

Stack: Go 1.25+ (pgx/v5, cobra), PostgreSQL 16 + PostGIS 3.4, Python (ingest scripts).
Module path: `github.com/DojoGenesis/policy-data-infrastructure`

## Key Commands
```
docker compose up -d              # start Postgres + PostGIS
go run ./cmd/pdi migrate up       # run pending migrations
go build ./...                    # compile all packages
go test ./...                     # run full test suite
go test ./... -short              # skip slow integration tests
make serve                        # go run ./cmd/pdi serve
```

## Directory Structure
```
cmd/pdi/          CLI entrypoint (cobra root + subcommands: migrate, fetch, analyze, serve)
pkg/geo/          Geography models, FIPS parsing, PostGIS query helpers
pkg/stats/        Statistical engine: percentile ranks, z-scores, change detection
pkg/store/        Database layer: pgx connection pool, query builders, transaction helpers
pkg/datasource/   Source adapters: Census API, TIGER loader, HUD, EPA, HMDA, BLS, etc.
pkg/pipeline/     Ingest orchestration: fetch → normalize → upsert → validate
pkg/narrative/    Narrative generation: template engine, indicator summarizer
pkg/htmlcraft/    HTMLCraft deliverable builder: renders policy briefs as single-file HTML
pkg/gateway/      HTTP API server: REST endpoints for geography, indicators, analysis, exports
ingest/           Python helper scripts for large file downloads (TIGER shapefiles, FEMA NFHL)
data/             Static manifests: sources.toml, FIPS reference files
schemas/          JSON Schema definitions for core data types
migrations/       Embedded SQL migrations (run in filename order via go:embed)
```

## Go Conventions
- Run `go build ./...` and `go test ./...` after any Go code change before marking done
- Fix all type mismatches and compilation errors before moving on
- Walk up from the edited file to find go.mod root; run build from there
- Test files (*_test.go): run `go test ./...` not just build

## GEOID / FIPS Conventions
- All GEOIDs are FIPS codes
  - 2-digit  → state (e.g., `"55"` = Wisconsin)
  - 5-digit  → county (e.g., `"55025"` = Dane County WI)
  - 11-digit → census tract (e.g., `"55025000100"`)
  - 12-digit → block group (e.g., `"550250001001"`)
- GEOID strings are always zero-padded to their canonical length
- Never truncate or parse as integers — leading zeros are significant

## Indicator Value Convention
- Use `*float64` (pointer) for all indicator values — `nil` means data is missing or suppressed
- Never use sentinel values (−1, −9999, 0 as missing, etc.) in Go code
- The database stores `NULL` for missing; Go nil maps to SQL NULL via pgx

## Database Connection
- Connection string via `--db` flag or `PDI_DATABASE_URL` environment variable
- Default: `postgres://pdi:pdi@localhost:5432/pdi?sslmode=disable`
- All queries use `pgx/v5` pool; never open raw `database/sql` connections in this repo

## Migrations
- SQL migration files live in `migrations/` and are embedded via `//go:embed migrations/*.sql`
- Filenames must sort lexicographically in execution order: `0001_init.sql`, `0002_geographies.sql`, etc.
- Each migration is idempotent (uses `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`)
- `go run ./cmd/pdi migrate up` applies all pending migrations; `migrate down` rolls back one

## API Key / Credentials Conventions
- Never hardcode API keys in source files
- Census API key: `CENSUS_API_KEY` env var (500 req/min with key, 45 without)
- All other source-specific keys listed in `data/sources.toml` under `api_key_env`

## Architecture Patterns
- Parallel agent work: define interface contracts first, then dispatch
- Return HTTP 501 for unimplemented interface methods — makes gaps visible, not silent
- SSE-driven reactive stores: use rAF batching to prevent UI hangs
- Compilation gate: all parallel tracks must pass `go build ./...` before integration
