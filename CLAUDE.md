# policy-data-infrastructure

## Project Overview
National open-source policy data platform. Ingests Census ACS, TIGER, BLS LAUS, CDC PLACES, EPA EJScreen, USDA Food Access, WI DPI attendance, and other open datasets; stores indicators at multiple geographic levels in PostGIS; runs a statistical analysis engine; generates narrative summaries and evidence cards; exports HTMLCraft deliverables for policy audiences.

Stack: Go 1.24+ (pgx/v5, cobra, gin), PostgreSQL 16 + PostGIS 3.4, Python 3.11+ (ingest/analysis scripts).
Module path: `github.com/DojoGenesis/policy-data-infrastructure`
Remote: `DojoGenesis/policy-data-infrastructure` — push with `gh auth switch --user DojoGenesis` first.

## Key Commands
```
docker compose up -d              # start Postgres + PostGIS
go run ./cmd/pdi migrate up       # run pending migrations
go build ./...                    # compile all packages (REQUIRED before commit)
go test ./... -short              # fast test gate (skip DB integration)
go test ./... -v                  # full test suite with verbose output
go vet ./...                      # static analysis
make build                        # equivalent to go build ./...
make test-short                   # fast CI gate
make serve                        # go run ./cmd/pdi serve
```

## Directory Structure
```
cmd/pdi/          CLI entrypoint (cobra: migrate, fetch, analyze, serve, query, generate, pipeline)
pkg/geo/          Geography models, FIPS parsing, PostGIS query helpers
pkg/stats/        Statistical engine: z-scores, percentile ranks, OLS, bootstrap, tipping points
pkg/store/        Database layer: pgx pool, Store interface, query builders, export/import
pkg/datasource/   Source adapters: Census ACS, TIGER, CDC PLACES, EPA EJScreen, BLS, USDA
pkg/pipeline/     Ingest orchestration: DAG-based fetch → normalize → upsert → validate
pkg/narrative/    Narrative generation: template engine, indicator summarizer
pkg/htmlcraft/    HTMLCraft deliverable builder: renders policy briefs as single-file HTML
pkg/policy/       Policy record models, equity dimension crosswalk
pkg/gateway/      HTTP API server: REST endpoints for geography, indicators, analysis, exports
internal/version/ Build version injection (ldflags target)
ingest/           Python ingest scripts for data sources (each script is standalone CLI)
ingest/lib/       Shared Python libraries: census.py, db.py, geo.py
analysis/         Python analysis scripts (evidence cards, county fetching)
analysis/output/  Generated CSVs, JSON, HTML (DO NOT commit — in .gitignore)
data/             Static manifests: sources.toml, crosswalks, policy CSVs
schemas/          JSON Schema definitions for core data types
deploy/           VPS deployment scripts (setup.sh, backup.sh)
research/         Research notes and methodology documentation
```

## Go Development
- Run `go build ./...` and `go test ./... -short` after EVERY Go code change before marking done
- Fix ALL type mismatches and compilation errors before moving on — never skip
- Walk up from the edited file to find go.mod root; run build from there
- Test files (*_test.go): run `go test ./...` not just build
- Module root: `/Users/alfonsomorales/ZenflowProjects/policy-data-infra/`

### Adding a new datasource
1. Copy `pkg/datasource/cdc_places.go` as template
2. Implement the `DataSource` interface (`Fetch(ctx, store) error`)
3. Register in `pkg/datasource/registry.go`
4. Add companion `*_test.go` with at minimum a `TestNew` that validates defaults
5. Add entry to `data/sources.toml`

### Adding a migration
- New files: `migrations/00N_description.sql`
- Each migration MUST be idempotent (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`)
- Test: `go run ./cmd/pdi migrate up` then `go run ./cmd/pdi migrate down`
- Migrations are embedded via `//go:embed migrations/*.sql` — filenames must sort lexicographically

### Known Makefile bug
The Makefile ldflags target `cmd/pdi.Version` is wrong — version vars live in `internal/version/version.go`. The `.goreleaser.yml` is correct. Fix when touching version injection.

## Python Development
### Structure conventions
- All ingest scripts live in `ingest/`; analysis scripts in `analysis/`
- Each script is a standalone CLI with `argparse` — never import across scripts
- Shared client libraries: `ingest/lib/census.py`, `ingest/lib/db.py`, `ingest/lib/geo.py`
- Output CSVs go to `analysis/output/` — these are generated artifacts, NOT committed
- Virtual environment: `pip install -r ingest/requirements.txt`

### Python development gates
- After modifying `ingest/lib/*.py`, test with `--dry-run` against affected fetchers
- No Python test suite exists yet — use `--dry-run` + `null_audit()` output as verification
- When writing new ingest scripts, follow the pattern from `ingest/fetch_bls_laus.py`:
  `--dry-run` → `--year` → `--load` flags; print_plan → fetch_data → write_csv → null_audit → load_to_db
- First run of ANY new script MUST use `--dry-run`

### Environment variables
- `PDI_DATABASE_URL` — PostgreSQL connection (default: postgres://pdi:pdi@localhost:5432/pdi)
- `CENSUS_API_KEY` — Census Bureau (500 req/min with key, 45 without)
- `BLS_API_KEY` — BLS (500 series/req with key, 25 without)
- `CDC_PLACES_APP_TOKEN` — CDC PLACES / Socrata app token
- Never hardcode API keys. Never commit `.env` files.

### Null audit requirement
- Every ingest script MUST call `null_audit(records)` before `load_to_db()`
- A fresh run with >30% null for the primary indicator is a FAILURE — do not load
- 0% null is suspicious for government data (means suppression handling was missed)

## Data Pipeline Debugging

### Config-before-code rule
Before debugging a fetch script, verify in order:
1. `echo $PDI_DATABASE_URL` — correct connection string?
2. `docker compose ps` — database running?
3. API key env vars set? (check `data/sources.toml` for `api_key_env`)
4. `git log --oneline ingest/` — script version current?

### Government API silent data drops (CRITICAL)
Government batch APIs silently drop data with certain parameter combinations:
- **BLS LAUS**: `annualaverage=true` silently drops ALL data when combined with year range + unregistered key. Compute annual averages client-side from M01-M12 monthly values.
- **BLS LAUS**: Unregistered limit is 25 series per request, NOT 50. BLS truncates silently at 25 with no HTTP error.
- **Census API**: Null sentinel is `-666666666`, not `null` or `0`. Always convert via `safe_int()`/`safe_float()` from `ingest/lib/census.py`.
- **CDC PLACES Socrata**: Filter with `datavaluetypeid='CrdPrv'` (crude prevalence), NOT `data_value_type='CrudePrev'` — column name changed between PLACES versions.

### Rate limit handling
- **BLS**: 25 queries/day unregistered. Symptom: `status=REQUEST_FAILED_OVER_LIMIT` in JSON body (HTTP 200!). Wait until next UTC midnight — do NOT retry same day.
- **Census**: 45 req/min without key. `RATE_LIMIT_DELAY = 1.5` in `lib/census.py` is the safe floor.
- **Socrata**: 100 req/min with app token. Set `CDC_PLACES_APP_TOKEN` env var.

### FK dependency order for data loads
Before loading indicators, geographies MUST exist. Chain:
`geographies` → `indicator_sources` → `indicator_meta` → `indicators`
Loading out of order = FK constraint violations. Correct order: (1) seed geographies, (2) register source, (3) register indicator_meta, (4) load indicators.

### Single-series diagnostic
When a full fetch returns all nulls: run ONE series manually first.
- BLS: fetch one county × one measure before blaming the script
- Census: fetch B19013_001E for one known tract before debugging derived indicators

### Materialized view refresh
After loading new indicator data:
```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY indicators_latest;
```
Without this, queries to `indicators_latest` return stale data silently.

### Null data is not always a bug
Before debugging null values:
1. Check if the API has data for that geography/vintage (some counties are suppressed)
2. Check the null sentinel conversion (Census: `-666666666`)
3. Check if the variable exists for the requested year (not all ACS vars exist pre-2015)

## GEOID / FIPS Conventions
- All GEOIDs are FIPS codes — always strings, always zero-padded
  - 2-digit → state (e.g., `"55"` = Wisconsin)
  - 5-digit → county (e.g., `"55025"` = Dane County WI)
  - 11-digit → census tract (e.g., `"55025000100"`)
  - 12-digit → block group (e.g., `"550250001001"`)
- Never truncate or parse as integers — leading zeros are significant
- GEOID validation regex: `^\d{2}$|^\d{5}$|^\d{11}$|^\d{12}$` (exact lengths only)

## Indicator Value Convention
- Use `*float64` (pointer) for all indicator values — `nil` means data missing or suppressed
- NEVER use sentinel values (−1, −9999, 0 as missing) in Go code
- Database stores `NULL` for missing; Go nil maps to SQL NULL via pgx

## Database Connection
- Connection string via `--db` flag or `PDI_DATABASE_URL` environment variable
- Default: `postgres://pdi:pdi@localhost:5432/pdi?sslmode=disable`
- All queries use `pgx/v5` pool; never open raw `database/sql` connections
- VPS PostGIS: `psql -h 5.161.84.125 -U pdi -d pdi` (key: dojo_deploy_ed25519)

## Data Health and Observability

### Row count benchmarks (VPS PostGIS)
Expected minimums — if a query returns far below, the load failed silently:
| Table | Level | Expected Rows |
|-------|-------|--------------|
| geographies | county | 72 (WI) |
| geographies | tract | ~1,929 (WI) |
| indicators | ACS | ~1,368 |
| indicators | CDC PLACES | ~12,200 |
| indicators | USDA | ~8,009 |
| indicator_sources | all | ≥4 |
| indicator_meta | all | ≥20 |

### Health check queries
```sql
SELECT level, COUNT(*) FROM geographies GROUP BY level ORDER BY level;
SELECT variable_id, COUNT(*), COUNT(value) as non_null FROM indicators GROUP BY variable_id ORDER BY variable_id;
```

### PostGIS health check
```bash
docker compose ps                    # containers running?
psql $PDI_DATABASE_URL -c '\dt'     # tables exist?
psql $PDI_DATABASE_URL -c 'SELECT COUNT(*) FROM geographies;'
psql $PDI_DATABASE_URL -c 'SELECT COUNT(*) FROM indicators;'
```

### Evidence card health
```bash
python3 analysis/evidence_cards.py --dry-run  # check plan first
python3 analysis/evidence_cards.py             # generate
# Expected: 70 policies processed, ≥50 evidence cards
```

## Code Reading Conventions
- For files >50KB, use targeted reads with line ranges (`offset`/`limit`)
- Never read files >200KB in one shot — use Grep to find relevant sections first
- PDI-specific large files to watch:
  - `analysis/output/*.csv` — 1-50MB. NEVER read whole. Use `wc -l` first.
  - `analysis/output/evidence_cards.json` — ~500KB when fully populated
  - `data/sources.toml` — small (<12KB), safe to read whole
  - All migration SQL files — <5KB each, safe to read whole

### Quick CSV null diagnostic
```bash
python3 -c "import csv; rows=list(csv.DictReader(open('FILE.csv'))); print(len(rows), 'rows'); print({k: sum(1 for r in rows if r[k]=='') for k in rows[0]})"
```

## Agent Dispatching
- After parallel agent dispatches, verify EACH agent's output independently
- Do NOT trust agent self-reports — check actual file contents and test results
- Always specify `model: "sonnet"` or `model: "opus"` — never inherit default

### Model assignment for PDI tasks
- **Sonnet**: ingest script modifications, null audit debugging, CSV processing, evidence card expansion, SQL queries, migration authoring, test writing, health checks
- **Opus**: architecture decisions (new source adapters, schema changes, pipeline stage design), grant deliverable strategy, cross-cutting refactors

### After Python ingest agent completes — verify:
```bash
python3 analysis/evidence_cards.py --dry-run
ls -la analysis/output/
wc -l analysis/output/*.csv
```

### After Go code change agent completes — verify:
```bash
go build ./...
go test ./... -short
```

### Wave ordering for pipeline tasks
- Wave 1: fetch data in parallel (CSV output only, no DB writes)
- Wave 2: seed geographies, then indicator_sources + indicator_meta (FK deps)
- Wave 3: load indicators in parallel (CDC PLACES + USDA can be concurrent)

## Research Workflow
- Evidence cards: `analysis/output/evidence_cards.json` + `evidence_summary.md`
- Policy positions: `data/policies/` (canonical source)
- Equity crosswalk: `data/crosswalks/wi_equity_crosswalk.json` (38 dimensions → ACS vars)
- Research notes: `research/` directory
- Before adding a new indicator, check `data/sources.toml` AND `analysis/evidence_cards.py` → `DIMENSION_CONFIG`

### Adding a new policy data source (checklist)
1. Add entry to `data/sources.toml` with all fields
2. Write `ingest/fetch_SOURCENAME.py` following dry-run/fetch/write/audit/load pattern
3. First run: `--dry-run` before any live fetch
4. Register source in `indicator_sources` table
5. Register `indicator_meta` entries
6. Add equity dimensions to `DIMENSION_CONFIG` in `analysis/evidence_cards.py`
7. Re-run evidence cards and verify card count increases

### Grant context
- Arnold Ventures: $591K application, decision ~May 2026
- MCF LOI: $40-50K, due Jun 3 2026
- For grant-facing analysis: always cite ACS vintage year and geography level

## Architecture Patterns
- Parallel agent work: define interface contracts first, then dispatch
- Return HTTP 501 for unimplemented interface methods — makes gaps visible
- Compilation gate: all parallel tracks must pass `go build ./...` before integration
- Store interface: all callers use `store.Store`, not `*PostgresStore` directly
- Pipeline DAG: stages declare dependencies; execution is topological

## Git Rules
- Remote: `DojoGenesis/policy-data-infrastructure` — verify with `git remote -v`
- Before pushing: `gh auth switch --user DojoGenesis`
- `go build ./...` before committing any staged Go files
- Conventional commits: `feat:`, `fix:`, `chore:`, `docs:`, `data:`
  - Use `data:` prefix for data loads, CSV updates, migration changes
- Never force-push main

### Files that must NEVER be committed
- `analysis/output/*.csv`, `*.json`, `*.html` (generated, regenerable)
- `.env`, `*.env` (secrets)
- `.venv/` (virtual environment)
- `__pycache__/` (Python bytecode)

## TODO.md and CHANGELOG.md Conventions

### TODO.md
- Lives in repo root, updated after every work session
- Organized by priority track (P0 Critical → P1 High → P2 Medium → P3 Low)
- Format: `- [ ] description [source: audit/handoff/session] [blocked-by: X]`
- Completed items move to CHANGELOG.md, not deleted
- Agent sessions: check TODO.md at start, update at end

### CHANGELOG.md
- Lives in repo root, updated with each commit or session
- Format: `## YYYY-MM-DD` header, bulleted list under categories
- Include row counts for data loads: `Loaded 12,200 CDC PLACES rows`
- Include root causes for fixes: `Fixed BLS LAUS: annualaverage param drops data`
- Committed with `docs:` prefix separately from code changes

## Subagent Protocol

Subagents dispatched to this repo MUST follow these rules. You start cold — no session
context, no memory. Read this section, then STATUS.md, then the relevant ADR.

### Before Starting
1. Read `STATUS.md` — current HEAD, test count, what's live, what's not
2. Read `TODO.md` — find your assigned item(s)
3. Read relevant `adr/*.md` — understand the decision context
4. Run `go build ./...` to verify the repo compiles before making changes

### While Working
- `go build ./...` after every file save — do NOT accumulate errors
- Run `go test ./pkg/PACKAGE/... -v` for each package you touch
- Store interface: all callers use `store.Store`, not `*PostgresStore` directly
- New Store methods: add to interface in `store.go` + implement in `postgres.go` + add to ALL mock stores in test files (grep for `mockStore`, `stubStore`, `mockValidateStore`)
- New API endpoints: register in `pkg/gateway/plugin.go` RegisterRoutes()
- New migrations: `pkg/store/migrations/00N_description.up.sql` + `.down.sql` — MUST be idempotent

### Before Reporting Done
1. `go build ./...` — MUST pass
2. `go test ./... -short` — MUST pass (all 380+ tests)
3. Update `TODO.md` — mark completed items with `[x]`, add new items discovered
4. Update `CHANGELOG.md` — append to today's date section with commit hash
5. Update `STATUS.md` — update HEAD, test count, any changed coverage or service state
6. Do NOT commit — the orchestrator commits after verification

### File Ownership (avoid conflicts with parallel agents)
When assigned a track, you own ONLY the files listed in your dispatch. Do not modify
files outside your manifest. If you discover a dependency on another file, report it
in your completion summary — do not edit it.

### Key Paths
- API handlers: `pkg/gateway/handlers.go` (add new handlers here)
- Route registration: `pkg/gateway/plugin.go` (register new routes here)
- Store interface: `pkg/store/store.go` (add new methods here)
- Store implementation: `pkg/store/postgres.go`
- Migrations: `pkg/store/migrations/`
- Pipeline stages: `pkg/pipeline/`
- CLI commands: `cmd/pdi/`
- ADRs: `adr/`
- Policy CSVs: `data/policies/`
- Deploy: `deploy/`

### Live Infrastructure
- API: `https://api.policydatainfrastructure.com` (VPS, port 8340, Caddy + Cloudflare)
- PostGIS: `postgres://pdi:pdi@localhost:5432/pdi` on VPS
- Service: `pdi.service` (systemd, user=dojo)
- Static site: `https://policydatainfrastructure.com` (GitHub Pages, `docs/`)

## Security
- Never hardcode API keys — all keys via environment variables
- Census API key: `CENSUS_API_KEY` env var
- All source-specific keys listed in `data/sources.toml` under `api_key_env`
- Default dev DB credentials (`pdi:pdi`) are acceptable for local development only
- Escape user-supplied strings before embedding in HTML (`pkg/htmlcraft/bridge.go` has `htmlEscape()`)
- Use `errors.Is(err, pgx.ErrNoRows)` — never match error strings
