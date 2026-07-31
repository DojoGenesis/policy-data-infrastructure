# STATUS — policy-data-infrastructure

> Auto-updated by agents. Human-verified dates in parentheses.
> Last agent update: 2026-07-31 (data-layer reseed — handoff 2026-07-31_pdi-data-layer-buildout)

## Data Layer (2026-07-31, reseed)

Local Postgres was recreated empty 2026-07-30. Reseeded and pinned to **ACS 2020-2024
5-Year** (resolves ADR-014 open question 2).

| Table | Benchmark | Live |
|---|---|---|
| `geographies` county | 72 | **72** ✅ |
| `geographies` tract | 1,542 | **1,542** ✅ |
| `indicators` acs-5yr county | ~1,368 | **1,368** ✅ (19 vars × 72, 100% non-null, all with MOE) |
| `indicators` cdc-places tract | ~12,200 | **12,200** ✅ |
| `indicators` cdc-svi | — | **8,000** (360 county + 7,640 tract) |
| `indicators` usda-food | ~8,009 | **0** ⛔ blocked — 2010 vs 2020 tract vintage |
| `indicator_sources` | ≥4 | **14** |
| `indicator_meta` | ≥20 | **41** |
| `indicators_latest` | — | **21,568**, now carrying `cv`/`reliability` |

Four defects fixed to make the reseed possible at all: no `indicator_meta` registration
path existed for the four loaded sources; `PutIndicators` could not write the
`reliability` enum; `indicators_latest` never exposed `cv`/`reliability` (ADR-014 D8's
stated blocker); a zero-row fetch reported `ok`. See CHANGELOG 2026-07-31.

**Blocked on operator:** USDA tract vintage (ADR-014 open question 6). FBI NIBRS needs
`FBI_CDE_API_KEY`; FCC Broadband needs a source decision (WAF-blocked).

Gates: `go build` ✅ `go vet` ✅ `go test -short` 11/11 ✅ · layout-check 88/88 ✅

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
| HEAD | `c14cc0e` — frontend rebuild, committed + deployed 2026-07-28 |
| Branch | `cruz/pip-115-pdi-frontend-rebuild` (pushed; **not yet merged to `main`**) |
| Build | Clean (`go build` ✅ `go vet` ✅ `go test -short` 10/10 ✅) |
| Tests | 380+ pass, 0 fail · smoke test 24/24 against live |
| VPS | `5.161.84.125` — `pdi.service` active. Binary `/usr/local/bin/pdi` 59,234,392 bytes, deployed 2026-07-28 19:44 UTC |
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
