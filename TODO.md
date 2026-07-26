# TODO — policy-data-infrastructure

> Updated: 2026-07-26 | Last audit: 2026-04-15 (full audit + quality infrastructure shipped)
> Update this file after every work session. Move completed items to CHANGELOG.md.

## P1 — High (grant-critical)

- [ ] Verify `ingest/fetch_tiger.py` against a live PostGIS. The TIGERweb repoint is verified only via `--dry-run` (no psycopg on the build machine); `normalize_props()` -> `bulk_load_geographies()` has not been exercised against a real DB [source: PIP-91]
- [ ] Reload PostGIS tract geographies at the ACS 2024 vintage — the DB predates this and the tract count on record (1,652) does not match the verified 1,542 [source: PIP-91]
- [ ] Confirm whether `block_group` supports a statewide Census query. `lib/census._geo_clause()` still requires a county there; only `tract` was relaxed and verified [source: PIP-91]

- [ ] Build policydatainfrastructure.com — domain returns 200, needs real app content beyond static marketing page [source: roadmap]
- [ ] MCF LOI draft review with Justice — 9 open questions, due Jun 3 2026 (49 days) [source: grant]
- [ ] Arnold Ventures $591K — decision ~May 2026, no action required [source: grant]

## P2 — Medium (quality, testing)

- [x] ~~Add tests for `pkg/store/`~~ — DONE: 15 integration tests, 0% → 77.5% (`19c1242`)
- [x] ~~Add post-stage validation gate to pipeline~~ — DONE: ValidateStage + Config.Validate() (`19c1242`)
- [ ] Audit transitive deps: `go mod why github.com/quic-go/quic-go` and `go.mongodb.org/mongo-driver` [source: health-audit]
- [ ] `pkg/pipeline/` coverage at 30.4% — add stage-level unit tests [source: audit]
- [ ] `pkg/gateway/` coverage at 41.2% — add handler tests [source: audit]

## P3 — Low (polish, national-scale)

- [ ] Wire WI DPI attendance data into Go pipeline (currently Python-only) [source: architecture-gap]
- [ ] National-scale data pipeline (all 50 states) — 13 adapters ready, need orchestration + rate limit budget [source: roadmap]
- [ ] GTFS + EPA-TRI end-to-end tests with mock HTTP servers [source: adapter-audit]
- [ ] HTTP 500 error handling tests across all adapters [source: adapter-audit]
- [ ] HTMLCraft v3.5 Polish: animations, shortcuts, ARIA [source: session]

## Backlog — Future Phases

- [ ] Gateway protocol module merge (2→1) [source: deferred]
- [ ] Factor analysis pipeline integration (currently Python-only) [source: analysis]
- [ ] Tract-level EPA TRI attribution via PostGIS ST_Within [source: adapter-limitation]
