# ADR-004: v1 Launch Plan — From API to Usable Platform

**Status:** Proposed
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
