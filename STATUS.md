# STATUS — policy-data-infrastructure

> Auto-updated by agents. Human-verified dates in parentheses.
> Last agent update: 2026-08-08 night (county explorer truth pass: ranks/reliability/segregation/factor columns/launch warm; see CHANGELOG)

## Run & Aggregation Layer (2026-08-08)

ADR-014 executed: D1 (both load directions), D4/D5 (canonical vocabulary + MOE), D6
(naive aggregations removed), D7/D8 (weighted, coverage-thresholded, CI-carrying
rollup), D9 (cache key with vintage), D10 (budget + optional token BEFORE the
endpoint), D11 (five zero-caller statistics routed). New surface:

- `POST /v1/policy/analyses` — queued runs (202 + run handle), cache-first (identical
  re-run → 200 `cached:true`, no budget spent). Types: `tract_rollup`, `spearman`,
  `isolation_index`, `blinder_oaxaca`, `interaction_ols`, `bootstrap_mean`.
- `GET /v1/policy/analyses/runs/:id` — status → `analysis_id` when done.
- Env: `PDI_RUN_GLOBAL_DAILY` / `PDI_RUN_CLIENT_DAILY` / `PDI_RUN_QUEUE_DEPTH` /
  `PDI_RUN_TOKEN` (empty token = open-with-a-ceiling).
- Migrations 015 (analyses cache-key unique index + dedupe), 016 (analysis_runs
  queue), 017 (WI state geography row — state-scope FK).

Verified live on local: Dane obesity rollup 33.12 CI [32.66, 33.60] (123/125 tracts);
statewide 71/72 published, Iron County withheld `coverage_below_threshold`; SVI rollup
withheld everywhere (`percentile_ranks_never_average`); **obesity×poverty ρ=0.503
CI [0.459, 0.546] n=1,524 — the cross-domain analysis that was structurally impossible
before the tract loads.**

## Data Layer (2026-08-08 — tract loads landed on the 2026-07-31 reseed)

Pinned to **ACS 2020-2024 5-Year** (vintage string `ACS-2024-5yr`).

| Table | Benchmark | Live |
|---|---|---|
| `geographies` county / tract / state | 72 / 1,542 / 1 | **72 / 1,542 / 1** ✅ (state row: migration 017) |
| `indicators` acs-5yr county | ~1,368 | **1,368** ✅ (19 vars × 72, all with MOE) |
| `indicators` acs-5yr tract | 19 × 1,542 | **29,298** ✅ NEW — canonical vocabulary, MOE on 29,242/29,242 non-null |
| `indicators` cdc-places tract | ~12,200 | **12,200** ✅ |
| `indicators` cdc-svi | — | **9,528** (360 county + 9,168 tract; now incl. `svi_total_population` w/ MOE — the rollup denominator) |
| `indicators` usda-food | — | **6,438** ✅ (5 vars × county + 2020-tract, via the population-weighted 2010→2020 crosswalk; OQ6 resolved) |
| `indicator_sources` / `indicator_meta` | ≥4 / ≥20 | **14 / 42** |
| `indicators_latest` | — | **58,832** local / **67,000+** prod |

**Blocked on operator — all queued for Monday 2026-08-11 2pm** (handoff
`2026-08-08_crime-sensitive-data-gathering` + calendar event): FBI NIBRS
`FBI_CDE_API_KEY`, FCC BDC account token (broadband lane chosen 2026-08-08),
IPUMS account (upgrades crosswalk weights to NHGIS TDW). USDA OQ6 is RESOLVED —
loaded via the crosswalk above.

Gates: `go build` ✅ `go vet` ✅ `go test -short` 11/11 ✅ (400+ tests; +17 this session)

## Frontend Usability Pass (2026-07-29, PIP-116)

Measured in headless Chromium across 10 pages × 2 themes × 3 viewports. **Not committed or deployed** — working tree only.

| Item | Before | After |
|---|---|---|
| `/evidence`, `/candidates` | **Blank pages** — 70/71 cards at opacity 0 permanently | All 70 cards render; 0 hidden, 0 pageerrors, normal + reduced-motion |
| `/county` mobile 375px | 118px horizontal overflow | 0px — at 375/768/1280, both themes |
| `.layer-indicator` contrast | dark li-3 4.1:1, li-5 3.12:1; **light li-1 1.2 / li-2 2.71 / li-3 3.85 / li-4 2.2** | 5.05–13.11:1 across 5 indicators × 2 themes × 3 viewports |
| Landing `<h1>` | Zero — only page of ten without one | 1, rendering pixel-identical (36px/600, 783×92) |
| API narrative title | "Five Mornings in " on every generated narrative | Resolves the real scope name; 5 regression tests |
| Reveal stagger | Last card's fade began 5.52s after reveal | Capped at 880ms (evidence) / 560ms (composites) |
| JS errors across all 10 pages | 2 | 0 |
| Horizontal overflow across all 10 pages | 1 page | 0 |

Gates: `go build` ✅ `go vet` ✅ `go test -short` 11/11 packages ✅ · vendored kit files (`motion.js`, `motion.css`, `tokens.css`) and `styles.css` all unmodified.

**Largest remaining experience gap:** cold empty states on the tool pages (`/compare`, `/chat`, `/composite`) — scouted, recommendation recorded in TODO.md P1. Blocked on the Alpine `x-text` i18n gap.

## Quick Reference

| Item | Value |
|------|-------|
| HEAD | see `git log` — ADR-014 run/aggregation layer, 2026-08-08 (6 commits on top of `c3e0f90`) |
| Branch | `main` |
| Build | Clean (`go build` ✅ `go vet` ✅ `go test -short` 11/11 ✅) — verified 2026-08-08 |
| Tests | 400+ pass, 0 fail · smoke test 24/24 against live |
| VPS | `5.161.84.125` — `pdi.service` active. Binary `/usr/local/bin/pdi` 47,816,866 bytes = `v0.1.0-152-gb0ea467`, **deployed 2026-08-08** (checksum-verified; prior binary backed up as `pdi.bak-20260808-*`) |
| Deploy state | **Current with `main` (`b0ea467`).** Migrations 001-017 applied on deploy — the production duplicate analyses pair is deduped (18→17 rows, 0 dup groups), cache-key index live, run queue live. Data legs run the same day via SSH tunnel: SVI tract 9,168 rows (incl. `svi_total_population` w/ MOE), canonical ACS tract 29,298 rows, `indicators_latest` refreshed → **60,403 rows** (was 29,577). `CENSUS_API_KEY` provisioned to `/etc/pdi/env`. Prod smoke: Dane obesity rollup 33.12 CI [32.63, 33.57] (123/125 tracts); obesity×poverty ρ=0.503 CI [0.461, 0.545] n=1,524; cached re-POST returns the same analysis id. **Known env divergence:** prod PLACES rows carry vintage `2022` where the local reseed labeled them `CDC-PLACES-2023` — same underlying release; align labels at the next VPS data reconciliation. **`PDI_RUN_TOKEN` is SET (2026-08-08)** — POST /analyses is bearer-gated (verified: 401 bare + wrong-token, 200 with token, GETs stay open); the budget still applies behind the gate. The value lives ONLY in `/etc/pdi/env` on the VPS (generated on-box, never in git/transcripts); read it with `ssh dojo-gateway 'grep ^PDI_RUN_TOKEN= /etc/pdi/env'` |
| Rollback | `/usr/local/bin/pdi.bak-20260728-194401` (47,403,170 bytes, the pre-rebuild binary) |
| Deploy method | `scp` to `/tmp/pdi-new` → `systemctl stop` → `install -o dojo -g dojo -m 755` → `systemctl start`. SSH via the `dojo-gateway` alias (key `~/.ssh/hetzner_deploy_ed25519`); `root@5.161.84.125` direct does **not** authenticate |
| Live API | `https://api.policydatainfrastructure.com` — all 11 routes 200 |
| Live verification | All 10 pages byte-identical live vs local; every page has lang-toggle, theme-toggle, footer, and the 9-link nav |
| Route form | **Extensionless**: `/county?geoid=`, `/map`, `/composite` (singular). `.html` paths 404 — do not test with them |
| Static Site | `https://policydatainfrastructure.com` (GitHub Pages marketing page only, not the app) |
| PostGIS | 72 counties, **1,669 tracts** (stale vintage), 51 variables, 17 adapters |
| WI tract count | **Mismatch — see TODO P0.** geojson 1,542 (correct, ACS 2024) · PostGIS 1,669 (stale) · LISA 1,524 scores |
| Static tract bundle | `analysis/output/atlas/` — 1,542 tracts x 11 indicators, ACS 2020-2024 |
| LISA analyses | 6 analyses live on VPS (1,524 scores each) — computed on the stale 1,669-tract set |
| Frontend source | `cmd/pdi/frontend/` **only**. The repo-root `/frontend/` orphan fork was deleted 2026-07-28 |
| `styles.css` | 59,068 bytes (was 49,160 — ADR-013 visual layer ported from casa-datos) |
| `tokens.css` | Byte-identical to `trespies-stacks/casa-datos/tokens.css` — do not edit to "fix" a colour |

## Frontend State (2026-07-28)

| Item | Status |
|---|---|
| ADR-011 structure (chat drawer, deep links, PWA, videos, Leaflet) | ✅ shipped Waves 1–4 |
| ADR-011 §2B three-act County Profile | ✅ was already built; Layer 5 un-collapsed 2026-07-28 |
| ADR-013 §1D thread ribbons | ✅ `.thread` was 19 uses / 0 rules — now styled |
| ADR-013 site footer | ✅ `.site-footer` was 6 uses / 0 rules — now styled; AA contrast verified |
| ADR-013 §3B skeleton states | ✅ spectral edge + pulse; fixed shorthand clobbering `.card`'s edge |
| Unified header/footer across pages | ✅ 10/10 pages, headers byte-identical bar `aria-current` |
| ADR-013 §3C light/dark toggle | ✅ **verified 2026-07-29** — `theme-toggle.js` on 10/10 pages and the button *renders* on all 10, measured in-browser. The prior ❌ ("script loads but renders no button") was stale |
| ADR-011 §1C language toggle | ✅ **verified 2026-07-29** — `lang-toggle.js` on 10/10 pages, toggle renders on all 10, and the `es/` twin directory is gone. The prior ❌ ("index only, 10 stale twins remain") was stale. Runtime i18n gaps remain (Alpine `x-text` bypasses the swap layer) — see TODO P1 |
| Compare beyond raw deltas | ✅ statewide rank, standardized gap, polarity-aware, materiality |

## Service Health

| Endpoint | Status | Notes |
|----------|--------|-------|
| `/health` | 200 | Liveness only |
| `/readyz` | 200 | DB connectivity verified |
| `/v1/policy/geographies?level=county` | 200 | 72 WI counties |
| `/v1/policy/sources` | 200 | 9 sources (hardcoded) |

## Coverage

| Package | Coverage |
|---------|----------|
| pkg/geo | 97.3% |
| pkg/htmlcraft | 91.2% |
| pkg/stats | 88.4% |
| pkg/store | 77.5% |
| pkg/datasource | 67.4% |
| pkg/policy | 66.3% |
| pkg/narrative | 61.2% |
| pkg/gateway | 41.2% |
| pkg/pipeline | 30.4% |

## What's Live

- 16 datasource adapters (ACS, TIGER, CDC PLACES, EPA EJScreen, HRSA, GTFS, WI DPI, HUD CHAS, HMDA, EPA TRI, HUD PIT, USDA Food, BLS LAUS, **CDC SVI**, **FBI NIBRS**, **FCC Broadband**)
- REST API with 20+ endpoints (geographies, indicators, analyses, variables, policies, compare, composite, aggregate, query, narrative, sources, factors, chat proxy)
- CORS middleware (configurable origins)
- Pipeline validation gates (ValidateStage, Config.Validate)
- Narrative template engine (3 templates, no LLM dependency)
- VPS deployment with systemd + Caddy + PostGIS
- Frontend: 10 pages (index, county, compare, evidence, candidates, map, narrative, about, composites, chat) served from embedded filesystem
- 6 LISA analyses live (1,524 tract scores each) powering the map page
- 70 evidence cards generated, 42 indicator variables with metadata
- Francesca Hong policy positions (85) seeded in policies table
- Grounded chat endpoint proxying to Dojo Gateway
- 3 new Python ingest scripts (CDC SVI, FBI NIBRS, FCC Broadband) with --dry-run support

## What's NOT Live (v1 gaps)

### Frontend (JavaScript calls exist but data-dependent)
- [ ] Factor scores (Go pipeline needs factor analysis implementation)
- [ ] Interactive Leaflet map (dispatching now — Wave 3)
- [ ] Evidence card gallery wired to live API instead of static JSON

### API Enrichment
- [ ] `GET /v1/policy/variables` — indicator metadata catalog (ADR-003)
- [ ] `GET /v1/policy/policies` — policy positions from DB (ADR-002)
- [ ] `GET /v1/policy/evidence-cards` — evidence cards from DB
- [ ] `GET /v1/policy/analyses` — discover available analysis runs
- [ ] Indicator responses enriched with name/unit/direction
- [ ] handleGenerateDeliverable wired to full narrative engine

### Data
- [ ] `indicator_meta` seeded on VPS (may be empty)
- [ ] Analysis run executed on VPS data
- [ ] `indicators_latest` auto-refresh (cron or pipeline hook)
- [ ] Progressive candidate policy positions added
- [ ] National-scale data (currently WI-only)

## Grant Deadlines

| Grant | Amount | Deadline |
|-------|--------|----------|
| Arnold Ventures | $591K | ~May 2026 (decision) |
| MCF LOI | $40-50K | Jun 3 2026 (49 days) |
