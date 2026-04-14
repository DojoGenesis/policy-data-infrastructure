# Agent Handoff Package

**From:** Opus orchestrator (Apr 13 session)
**To:** Opus-level orchestrator with Sonnet-level agents
**Date:** 2026-04-14
**Subject:** PDI Data Pipeline Continuation — 10 next-step items from Apr 13 session

---

## 1. Objective

> Continue the Wisconsin Policy Data Infrastructure proof of concept by completing 10 specific data pipeline, PostGIS loading, and Gateway integration tasks identified during the Apr 13 session.

---

## 2. Required Context

**Core Repository:**
- `DojoGenesis/policy-data-infrastructure` — HEAD: `b110cb5` (main, clean, pushed)
- Local path: `/Users/alfonsomorales/ZenflowProjects/policy-data-infra`

**Key Files:**
- `analysis/evidence_cards.py` — Multi-source evidence card generator (ACS + CDC PLACES + USDA)
- `ingest/fetch_bls_laus.py` — BLS LAUS fetcher (FIXED: batch size 25, no year restriction)
- `ingest/fetch_cdc_places.py` — CDC PLACES fetcher (FIXED: datavaluetypeid filter)
- `ingest/fetch_usda_food.py` — USDA Food Access fetcher (FIXED: ZIP URL, column aliases)
- `ingest/fetch_wi_dpi.py` — WI DPI school enrollment (FIXED: school-year URL, long→wide pivot)
- `ingest/fetch_epa_ejscreen.py` — EPA EJScreen (EPA FTP offline; Zenodo 5.9 GB mirror)
- `ingest/lib/db.py` — PostGIS loader (FIXED: schema mismatch for indicators and indicator_meta)
- `pkg/store/migrations/003_indicators.up.sql` — Indicators schema (indicators FK → geographies)
- `data/policies/francesca_hong_2026.csv` — 70 policy positions
- `data/crosswalks/wi_equity_crosswalk.json` — 38 equity dimensions → ACS variables
- `analysis/output/` — 5 CSV outputs + evidence_cards.json + evidence_summary.md

**VPS Context:**
- VPS: `gateway.trespies.dev` (Hetzner CPX21, 5.161.84.125)
- PostGIS: PostgreSQL 16 + PostGIS 3.4.2 on VPS
- Currently loaded: 72 WI county geographies + 1,368 ACS indicator records
- SSH: `ssh root@5.161.84.125` (key: dojo_deploy_ed25519)

**Memory Files:**
- `/Users/alfonsomorales/.claude/projects/-Users-alfonsomorales-ZenflowProjects/memory/MEMORY.md`

---

## 3. Task Definition — 10 Items

### Track A: Data Fetching (Sonnet agents, parallel)

**A1. BLS LAUS Re-fetch (blocked by daily API limit Apr 13)**
- Register for BLS API key at https://data.bls.gov/registrationEngine/ OR retry without key (limit resets daily)
- Run: `cd /Users/alfonsomorales/ZenflowProjects/policy-data-infra && python3 ingest/fetch_bls_laus.py --year 2023`
- Expected: 72 counties with unemployment_rate, employed, unemployed, labor_force
- The script now computes annual averages from M01-M12 monthly data (no annualaverage API param needed)
- Verify null audit shows <5% null for unemployment_rate

**A2. WI DPI Chronic Absence Data**
- The enrollment file has no chronic_absence_rate. It requires a SEPARATE WI DPI file.
- Download URL pattern: `https://dpi.wi.gov/sites/default/files/wise/downloads/attendance_dropouts_certified_2022-23.zip`
- Write a new `ingest/fetch_wi_dpi_attendance.py` or add to existing script
- Merge chronic absence data into `analysis/output/wi_schools_dpi.csv`

**A3. EPA EJScreen (optional, large download)**
- EPA FTP offline since Feb 2025. Zenodo mirror: `https://zenodo.org/records/14767363/files/2023.zip?download=1` (5.9 GB)
- Only attempt if bandwidth allows. Extract WI block groups, aggregate to county.
- NOT blocking — skip if impractical.

### Track B: PostGIS Loading (sequential, depends on Track A outputs)

**B1. Seed WI Tract Geographies (PREREQUISITE for B2 and B3)**
- The indicators table has FK `geoid → geographies(geoid)`. CDC PLACES and USDA use 11-digit tract GEOIDs.
- Need to insert ~1,929 WI census tracts into the `geographies` table on the VPS.
- Option 1: Use TIGER/Line shapefiles via `ingest/fetch_tiger.py` + `shp2pgsql`
- Option 2: Generate minimal geography entries from the tract GEOIDs in the existing CSV files:
  ```sql
  INSERT INTO geographies (geoid, level, name, state_fips)
  SELECT DISTINCT geoid, 'tract', 'Tract ' || geoid, '55'
  FROM (VALUES ('55001950100'), ...) AS t(geoid)
  ON CONFLICT DO NOTHING;
  ```
- The easier path: extract unique 11-digit GEOIDs from `wi_health_cdc_places.csv` and `wi_food_access.csv`, generate INSERT SQL

**B2. Load CDC PLACES to PostGIS**
- After B1 completes, seed indicator_meta for CDC measures, then:
  `PDI_DATABASE_URL="postgres://pdi:pdi@localhost:5432/pdi" python3 ingest/fetch_cdc_places.py --load`
- First register CDC source in indicator_sources: `INSERT INTO indicator_sources (source_id, name, category, url) VALUES ('cdc_places', 'CDC PLACES', 'health', 'https://data.cdc.gov')`
- Then register 8 indicator_meta entries for each CDC measure

**B3. Load USDA Food Access to PostGIS**
- Same prerequisite as B2 (tract geographies).
- Register USDA source + 7 indicator_meta entries, then run with `--load`

### Track C: Evidence Cards Enhancement (Sonnet agent)

**C1. Expand equity dimensions to cover 29 skipped policies**
- Currently 29/70 policies are skipped because their `equity_dimension` is not in `DIMENSION_CONFIG`
- Read `data/policies/francesca_hong_2026.csv`, identify the missing equity_dimension values
- Add entries to DIMENSION_CONFIG in `analysis/evidence_cards.py` with appropriate metrics
- Target: 50+ evidence cards (from current 41)

**C2. Add school-level evidence cards**
- WI DPI data is at district level, not county. Create a separate analysis for education policies.
- Cross-reference school districts to counties (DPI has a COUNTY column in enrollment data)
- Add district-level metrics to evidence cards for education_equity and education_funding dimensions

### Track D: Gateway Integration (Opus-level decision)

**D1. Fix gateway.trespies.dev 404**
- The Gateway on the VPS shows 404 on the root URL
- SSH to VPS, check: `systemctl status dojo-gateway`, `journalctl -u dojo-gateway --since today`
- The chat SPA at `/chat` may work even if root returns 404 (SPA handler strips /chat prefix)
- Fix: ensure the Gateway binary on VPS is the latest from the `96aa95d` build

**D2. Register Atlas Pipeline as Gateway DAG Commission (Item 6)**
- This is the last unfinished handoff item from the Apr 13 session
- `atlas/pipeline.json` + `atlas/Makefile` in the CWD repo define the pipeline
- Register as a Gateway commission template so that `make all` can be triggered from chat
- This requires the Gateway's DAG execution feature (Wave 2C document handling)

---

## 4. Definition of Done

- [ ] BLS LAUS CSV has <5% null for unemployment_rate across 72 WI counties
- [ ] Chronic absence rate populated in wi_schools_dpi.csv for ≥300 districts
- [ ] WI tract geographies seeded in PostGIS (≥1,300 tracts)
- [ ] CDC PLACES loaded to PostGIS indicators table (≥8,000 records)
- [ ] USDA Food Access loaded to PostGIS indicators table (≥5,000 records)
- [ ] Evidence cards expanded to ≥50 cards (up from 41)
- [ ] gateway.trespies.dev returns 200 on at least `/chat` or `/v1/models`
- [ ] All changes committed to DojoGenesis/policy-data-infrastructure with conventional commit messages
- [ ] `go build ./...` and `go test ./...` pass in the PDI repo

---

## 5. Constraints & Boundaries

- **DO NOT** modify the Go module path or change the repository structure
- **DO NOT** force push or rebase main
- **DO NOT** modify files in the CWD (atlas/) repo — only reference for Gateway commission
- **MUST** use `gh auth switch --user DojoGenesis` before pushing to DojoGenesis remotes
- **MUST** run `go build ./...` before committing Go changes
- **MUST** use `--dry-run` before any new fetcher's first live run
- BLS API: max 25 series per request, 25 requests/day without key
- CDC PLACES Socrata: filter with `datavaluetypeid='CrdPrv'` (not `data_value_type='CrudePrev'`)
- EPA EJScreen: 5.9 GB download — only if bandwidth allows, skip otherwise

---

## 6. Agent Dispatch Strategy

**Wave 1 (parallel Sonnet agents):**
- Agent 1: A1 (BLS LAUS re-fetch) + A2 (WI DPI chronic absence)
- Agent 2: C1 (expand equity dimensions for 29 skipped policies)

**Wave 2 (sequential, after Wave 1):**
- Agent 3: B1 (seed tract geographies) → B2 (CDC PLACES load) → B3 (USDA load)

**Wave 3 (Opus-level):**
- Main thread: D1 (Gateway 404 fix) + D2 (Atlas DAG commission) — requires architectural decisions

**Verification gate:** After all agents complete, run:
```bash
cd /Users/alfonsomorales/ZenflowProjects/policy-data-infra
go build ./...
go test ./...
python3 analysis/evidence_cards.py
git status
```

---

## 7. Next Steps (After Completion)

- Run `/compress` to distill session insights
- Update MEMORY.md with new data source state
- MCF LOI draft review with Justice (due Jun 3 2026, 50 days)
- HTMLCraft integration for interactive policy map output
