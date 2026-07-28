# STATUS — policy-data-infrastructure

> Auto-updated by agents. Human-verified dates in parentheses.
> Last agent update: 2026-07-26

## Quick Reference

| Item | Value |
|------|-------|
| HEAD | `9042723` (pending: Wave 1+2 changes uncommitted) |
| Branch | `main` |
| Build | Clean |
| Tests | 380+ pass, 0 fail |
| VPS | `5.161.84.125` — `pdi.service` active, binary deployed 2026-07-27 |
| Live API | `https://api.policydatainfrastructure.com` |
| Static Site | `https://policydatainfrastructure.com` (GitHub Pages, not connected to API) |
| PostGIS | 72 counties, 1,542 tracts, 51 variables, 16 sources |
| WI tract count | **1,542** (ACS 2024 vintage — verified) |
| Static tract bundle | `analysis/output/atlas/` — 1,542 tracts x 11 indicators, ACS 2020-2024 |
| LISA analyses | 6 analyses live on VPS (1,524 scores each) |

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
