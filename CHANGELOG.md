# CHANGELOG — policy-data-infrastructure

> Format: `## YYYY-MM-DD` sections, newest first. Update after every work session.
> Include row counts for data loads and root causes for fixes.

## 2026-04-14

### Health Audit & Infrastructure
- First comprehensive health audit completed — overall grade B-
- Rescued 6 research files from stale `policy-data-infrastructure/` clone
- Deleted stale clone — canonical directory is `policy-data-infra/`
- Created CLAUDE.md (rewritten from scratch, optimized for Sonnet agents)
- Created TODO.md (38 items across P0-P3 from health audit findings)
- Created CHANGELOG.md (this file)
- Identified 7 P0 critical issues, 9 P1 high issues, 9 P2 medium issues

### Data Pipeline
- Expanded evidence cards to 70 (all 70 policies, 0 skipped) — `32f5032`
- Added WI DPI attendance fetcher (449 districts, chronic_absence_rate 0% null)
- Fixed BLS LAUS script (startyear/endyear params) — data still null due to daily rate limit
- VPS PostGIS state: 72 counties + 1,652 tracts + 1,368 ACS + 12,200 CDC PLACES + 8,009 USDA = 22,949 indicator rows

## 2026-04-13

### Gateway & Narrative
- Wired narrative engine to gateway routes — `4edeebd`
- Fixed ACS FetchCounty: split detail/subject tables + SafeFloat string sentinel — `e205c08`

### Code Quality
- Deep code audit found 10 bugs across stats, pipeline, store — `3961a2d`
- Audit-driven sweep: 9 findings across 8 files — `b421cff`
- Fixed PostGIS-optional store + working analyze + narrative pipeline — `fc7dee6`

## 2026-04-12

### Analysis
- Multi-source evidence cards: CDC PLACES + USDA food access — `b110cb5`
- Fixed 5 data source fetchers, added WI output CSVs — `f3a1d61`
- Multi-source data ingest, idempotent migrations, county-level ACS fetch — `9b8ec01`

### Documentation
- Added README with architecture, data sources, API, and usage guide — `92311a9`

## 2026-04-11

### Infrastructure
- Phases 5+6: VPS deployment, national-scale fetch, CDC PLACES, EPA EJScreen — `8db0054`

## 2026-04-10

### Core Development
- v0.1 proof of concept: policy-to-evidence pipeline for Wisconsin — `f4b6ee7`
- Phases 3+4: pipeline engine, narrative generator, HTMLCraft bridge, gateway API, CLI wiring — `82d716a`

## 2026-04-09

### Foundation
- Policy record schema + Francesca Hong 2026 gubernatorial positions — `a724099`
- Phase 2 data ingest: Census API client, Python scripts, Store CRUD — `89a437d`
- Phase 1 foundation: pkg/geo, pkg/stats, pkg/store, CLI, PostGIS schema — `7680cfa`

---

## Update Conventions
- Update this file after every work session
- Use conventional commit categories: feat, fix, chore, docs, data
- Include commit hashes for traceability
- Include row counts and data state for pipeline changes
- For fixes: include the root cause, not just the symptom
- Agent sessions: append to the current date section or create a new one
