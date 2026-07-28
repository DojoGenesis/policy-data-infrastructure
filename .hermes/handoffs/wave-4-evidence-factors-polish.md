# Handoff Chip — Wave 4: Evidence Cards DB, Factor Analysis, Polish

**Created:** 2026-07-27 20:10 UTC
**Session:** conductor profile, policy-data-infra
**Status:** Ready for pickup

## Context (what happened in Waves 1-3)

Three waves completed in this session:

- **Wave 1:** Reconciled dual frontend directories (`frontend/` vs `cmd/pdi/frontend/`), synced richer versions, verified static assets (1,542 tracts GeoJSON, 70 evidence cards, 72 counties), rebuilt + deployed binary to VPS (root@5.161.84.125, key `~/.ssh/hetzner_deploy_ed25519`). All 10 pages serving live at `https://api.policydatainfrastructure.com`.

- **Wave 2:** Added 3 new data sources via parallel dispatch (deleg_3fe9e1c5): CDC SVI, FBI NIBRS, FCC Broadband. 15 new files, 9 new indicator variables, 3 migrations (009-011). Post-dispatch fixes applied: FCC registration in pipeline.go, FBI in seed_sources.sql, migrations renumbered. All gates green.

- **Wave 3:** Upgraded `map.html` from static SVG choropleth to interactive Leaflet map using vendored Leaflet (already in `cmd/pdi/frontend/leaflet/`). Zero external tile dependencies. LISA cluster coloring (HH/LL/HL/LH/NS) from live API. Tract popups with ACS indicators. Deployed to VPS.

**Current state:** 36 changed files uncommitted (17 modified, 19 untracked). All gates pass (`go build`, `go vet`, `go test -short`). VPS binary up-to-date.

## Wave 4 Tasks (in priority order)

### 4A: Evidence Cards DB Migration + API
- ADR-002 defined a `policies` table (done — migration 008) and evidence cards should also live in DB
- Currently served as static JSON at `/static/evidence_cards.json` (70 cards, 166KB)
- **Do:** Migration 012: `evidence_cards` table (policy_id, category, equity_dimension, title, findings, indicators JSONB, created_at)
- **Do:** Store methods: `PutEvidenceCards`, `QueryEvidenceCards` (filter by category, equity_dimension, policy_id)
- **Do:** API endpoint: `GET /v1/policy/evidence-cards` with query params
- **Do:** Update `evidence.html` to fetch from API instead of static JSON
- **Files:** `pkg/store/migrations/012_evidence_cards.up.sql`, `.down.sql`, `pkg/store/store.go` (interface), `pkg/store/postgres.go` (impl), `pkg/gateway/plugin.go` (route), `pkg/gateway/handlers.go` (handler), `cmd/pdi/frontend/evidence.html`

### 4B: Factor Analysis in Go Pipeline
- Python factor analysis exists (`analysis/factor_analysis.py`) but results aren't in PostGIS
- VPS returns `{"factors":[],"geoid":"55025","total":0}` — factor scores empty
- **Do:** Implement factor score computation in Go (`pkg/stats/factor.go`) or load Python results into DB
- **Do:** Migration 013: ensure `factor_scores` table populated
- **Do:** Wire `GET /v1/policy/geographies/:geoid/factors` to return real data
- **Do:** Update `county.html` factor profile card to show real data

### 4C: HTMLCraft v3.5 Polish
- Per TODO.md: animations, shortcuts, ARIA
- **Do:** Add keyboard shortcuts to county explorer (arrow keys to navigate cards)
- **Do:** Ensure all pages pass WCAG AA (focus indicators, screen reader labels)
- **Do:** Add transition animations on page navigation
- **Files:** `cmd/pdi/frontend/styles.css`, individual page `.html` files

### 4D: ADR-009 (Map Upgrade) + ADR-010 (Evidence Cards DB)
- Write architecture decision records documenting Wave 3 and 4A decisions

### 4E: Cleanup & Commit
- Add `pdi`, `pdi-linux` to `.gitignore`
- Decide on `frontend/crosswalks/` and `frontend/research/` (commit or gitignore)
- Commit all changes with conventional commits

## Key Paths
- Frontend (embedded): `cmd/pdi/frontend/` — always sync to `frontend/` after edits
- API handlers: `pkg/gateway/handlers.go`
- Route registration: `pkg/gateway/plugin.go`
- Store interface: `pkg/store/store.go`
- Store implementation: `pkg/store/postgres.go`
- Migrations: `pkg/store/migrations/`
- ADRs: `adr/`

## Deploy
- Cross-compile: `GOOS=linux GOARCH=amd64 go build -ldflags "..." -o pdi-linux ./cmd/pdi`
- SCP: `scp -i ~/.ssh/hetzner_deploy_ed25519 pdi-linux root@5.161.84.125:/usr/local/bin/pdi.new`
- SSH: `ssh -i ~/.ssh/hetzner_deploy_ed25519 root@5.161.84.125 "mv /usr/local/bin/pdi.new /usr/local/bin/pdi && chown dojo:dojo /usr/local/bin/pdi && systemctl restart pdi"`

## Gates (before marking done)
- `make build` ✅
- `make lint` ✅
- `make test-short` ✅
- VPS health check: `curl https://api.policydatainfrastructure.com/health` → 200
- All pages serving: index, county, compare, evidence, candidates, map, narrative, about, composite, chat
