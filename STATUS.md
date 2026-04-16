# STATUS — policy-data-infrastructure

> Auto-updated by agents. Human-verified dates in parentheses.
> Last agent update: 2026-04-15

## Quick Reference

| Item | Value |
|------|-------|
| HEAD | `2922e3a` |
| Branch | `main` |
| Build | Clean |
| Tests | 380 pass, 0 fail |
| VPS | `5.161.84.125` — `pdi.service` active |
| Live API | `https://api.policydatainfrastructure.com` |
| Static Site | `https://policydatainfrastructure.com` (GitHub Pages, not connected to API) |
| PostGIS | 72 counties, 1,652 tracts, 34 variables, 21K+ rows |

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

- 13 datasource adapters (ACS, TIGER, CDC PLACES, EPA EJScreen, HRSA, GTFS, WI DPI, HUD CHAS, HMDA, EPA TRI, HUD PIT, USDA Food, BLS LAUS)
- REST API with 11 endpoints (9 functional, 2 stubs: pipeline/run, pipeline/events)
- CORS middleware (configurable origins)
- Pipeline validation gates (ValidateStage, Config.Validate)
- Narrative template engine (3 templates, no LLM dependency)
- VPS deployment with systemd + Caddy + PostGIS

## What's NOT Live (v1 gaps)

### Frontend (no JavaScript calls the API)
- [ ] County explorer with search
- [ ] Indicator dashboard with labeled values
- [ ] Evidence card gallery (live, not hardcoded)
- [ ] Compare tool
- [ ] Narrative generation button
- [ ] Map visualization

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
