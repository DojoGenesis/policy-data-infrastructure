# TODO — policy-data-infrastructure

> Updated: 2026-04-15 | Last audit: 2026-04-15 (full audit + quality infrastructure shipped)
> Update this file after every work session. Move completed items to CHANGELOG.md.

## P1 — High (grant-critical)

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
