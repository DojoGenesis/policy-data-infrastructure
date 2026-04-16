# ADR-004: v1 Launch Plan — From API to Usable Platform

**Status:** Accepted (with audit amendments below)
**Date:** 2026-04-15
**Deciders:** Alfonso Morales, Claude

## Context

The API is live at `api.policydatainfrastructure.com` serving 72 WI counties, 1,652
tracts, and 34 indicator variables from PostGIS. The marketing site at
`policydatainfrastructure.com` has zero API calls — every number is hardcoded.

Grant deadlines: Arnold Ventures ~May 2026, MCF LOI Jun 3 2026 (49 days).
A grant reviewer visiting the site today sees a polished static page but cannot
interact with live data.

## Decision: 4-Wave v1 Launch

### Wave 1: API Enrichment (enables frontend)

**Goal:** A frontend developer (or Alpine.js `fetch()`) can build against the API
without reading Go source code.

| Track | Deliverable | Effort |
|-------|-------------|--------|
| 1A | `GET /v1/policy/variables` — indicator metadata catalog | 1h |
| 1B | Enrich indicator responses with name/unit/direction | 1h |
| 1C | `GET /v1/policy/analyses` — list available analysis runs | 30m |
| 1D | Seed `indicator_meta` on VPS + run analysis + refresh views | 30m |

**Dependency:** 1D must complete before 1B returns real labels. All of Wave 1 must
complete before Wave 2 can fetch labeled data.

### Wave 2: Data Explorer Frontend (the product)

**Goal:** `policydatainfrastructure.com` serves a live data explorer from the Go binary.

| Track | Deliverable | Effort |
|-------|-------------|--------|
| 2A | `//go:embed frontend/*` in serve.go + serve static files | 30m |
| 2B | County explorer page — search, browse 72 counties | 2h |
| 2C | County profile page — indicator dashboard with labels + bar charts | 2h |
| 2D | Compare tool — side-by-side two counties | 1h |
| 2E | Evidence card gallery — all 70 cards, filterable | 1h |
| 2F | DNS migration: `policydatainfrastructure.com` → VPS | 15m |

**Stack:** Alpine.js + Tailwind CSS (CDN). No build step. Pages are HTML files
in `frontend/` embedded in the binary.

**Architecture:** Single-page app with hash-based routing:
- `#/` — county explorer (default)
- `#/county/:geoid` — county profile with indicators
- `#/compare` — comparison tool
- `#/evidence` — evidence card gallery
- `#/about` — methodology (existing content)

### Wave 3: Policy + Candidate Layer (the story)

**Goal:** Progressive candidates' platforms linked to indicator data.

| Track | Deliverable | Effort |
|-------|-------------|--------|
| 3A | Migration 008: `policies` table (ADR-002) | 30m |
| 3B | Store methods + API endpoints for policies | 1h |
| 3C | Research + add 2-3 progressive candidates with CSVs | 2h |
| 3D | Generalize `evidence_cards.py` for multi-candidate | 1h |
| 3E | Frontend: candidate tracker page | 2h |
| 3F | Wire handleGenerateDeliverable to full narrative engine | 1h |

### Wave 4: National Scale + Polish (the reach)

**Goal:** 50-state data, OpenAPI spec, production hardening.

| Track | Deliverable | Effort |
|-------|-------------|--------|
| 4A | National pipeline run (rate limit budget plan) | 4h |
| 4B | OpenAPI 3.0 spec (generated or hand-written) | 2h |
| 4C | Auto-refresh `indicators_latest` after pipeline runs | 30m |
| 4D | Narrative generation button in frontend | 1h |
| 4E | Map visualization (Leaflet, choropleth by indicator) | 3h |

## Wave Dispatch Strategy

**Waves 1-2 are grant-critical.** They must ship before MCF LOI (Jun 3).
Wave 3 strengthens the narrative. Wave 4 is post-grant polish.

**Parallelism within waves:**
- Wave 1: 1A+1B+1C can run as parallel agents; 1D is VPS ops (main thread)
- Wave 2: 2A is prerequisite; 2B+2C+2D+2E can parallelize after 2A
- Wave 3: 3A→3B is sequential; 3C is research (main thread); 3D+3E parallel after 3B
- Wave 4: all tracks independent

**Total estimated effort:** ~25 hours across 4 waves.
Waves 1+2: ~10 hours (grant-critical).

## Consequences

- **Positive:** Grant reviewers see live data, real candidates, and working analysis
- **Positive:** Incremental delivery — each wave is independently valuable
- **Positive:** Solo-operator friendly — no frontend build chain, no new infrastructure
- **Negative:** Alpine.js + hash routing will feel dated for complex UIs
  (acceptable for v1; revisit if user feedback demands a richer experience)
- **Negative:** National data without a map is underwhelming
  (Wave 4E addresses this but is post-grant)

## Opus-Level Audit (Apr 15)

### Finding 1: indicators_latest was empty (FIXED)

The materialized view had 0 rows on VPS — the API appeared to have no data for any
geography profile. **Fixed during audit** by running `REFRESH MATERIALIZED VIEW`.
Now 21,577 rows across 34 variables.

**Prevention:** Wave 1D must include adding `REFRESH MATERIALIZED VIEW` to the pipeline's
FetchStage post-completion hook. The existing `RefreshViews()` call in the pipeline
already does this, but VPS data was loaded via Python scripts that bypassed the Go pipeline.

### Finding 2: analyses + analysis_scores are 0 rows

The narrative engine requires `analysis_id` and scores. No analysis has been run on VPS.
**Wave 1D underestimates this.** Running the analysis requires either:
- A new CLI command `pdi analyze --state 55` (analyze-only, no re-fetch)
- Running the full pipeline on existing data (but FetchStage would re-fetch)
- A one-off SQL script to insert analysis results

**Recommendation:** Add `pdi analyze` subcommand that runs AnalyzeStage + SynthesizeStage
without FetchStage. This is a reusable tool for reprocessing data after schema changes.
Revise 1D to 1-2h including this new command.

### Finding 3: indicator_meta has 42 rows (better than expected)

The plan assumed indicator_meta might be empty. It has 42 rows for all 34 active
variables. No seeding needed.

### Finding 4: Wave 2 effort is undersized

Alpine.js + Chart.js + responsive design + error handling + loading states for 4 pages
is more than the plan's ~7h. Revised estimate: ~12h. Still achievable in 2-3 sessions
before MCF LOI deadline.

### Finding 5: DNS migration creates a visibility gap

Moving `policydatainfrastructure.com` from GitHub Pages to VPS means the marketing page
goes dark until the Go binary serves static files. **Mitigation:** port `docs/index.html`
into `frontend/` and verify the Go binary serves it BEFORE switching DNS. Track 2A must
produce a working marketing page fallback, not just an app shell.

### Finding 6: Evidence cards are not in the DB

The frontend evidence card gallery (Wave 2E) needs to fetch card data from somewhere.
Options:
- Serve `evidence_cards.json` as a static embedded file (quickest)
- Add an evidence_cards table + API endpoint (cleanest, Wave 3)

**Recommendation:** Wave 2E serves the JSON file embedded in the binary. Wave 3 adds
the DB table. This prevents Wave 2 from depending on Wave 3.

### Finding 7: geometry boundary data is empty

All 1,724 geographies have `NULL` boundary. Maps (Wave 4E) will need TIGER shapefiles
loaded. Not blocking for Waves 1-3.

### Revised Effort Estimates

| Wave | Original | Revised | Delta |
|------|----------|---------|-------|
| 1 | 3h | 4h | +1h (analyze command) |
| 2 | 7h | 12h | +5h (frontend realism) |
| 3 | 7h | 8h | +1h (research variable) |
| 4 | 8h | 10h | +2h (geometry loading) |
| **Total** | **25h** | **34h** | **+9h** |

Still well within the 49-day MCF LOI window. Waves 1+2 at 16h = 2-3 focused sessions.
