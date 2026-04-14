# TODO — policy-data-infrastructure

> Updated: 2026-04-14 | Last audit: 2026-04-14 (first health audit)
> Update this file after every work session. Move completed items to CHANGELOG.md.

## P1 — High (data integrity)

- [ ] Re-run BLS LAUS fetch after daily rate limit reset: `python3 ingest/fetch_bls_laus.py --year 2023` [source: session]

## P2 — Medium (quality, testing)

- [ ] Add tests for `pkg/store/` (integration tests with testcontainers) [source: health-audit, coverage: 0%]
- [ ] Add post-stage validation gate to pipeline (indicator count, null rate, GEOID coverage) [source: health-audit]
- [ ] Audit transitive deps: `go mod why github.com/quic-go/quic-go` and `go.mongodb.org/mongo-driver` [source: health-audit]

## P3 — Low (polish)

- [ ] HTMLCraft v3.5 Polish: animations, shortcuts, ARIA [source: session]

## Backlog — Future Phases

- [ ] Wire WI DPI attendance data into Go pipeline (currently Python-only) [source: architecture-gap]
- [ ] Gateway protocol module merge (2→1) [source: deferred]
- [ ] National-scale data pipeline (all 50 states) [source: roadmap]
- [ ] MCF LOI draft review with Justice — 9 open questions, due Jun 3 2026 [source: grant]
- [ ] Arnold Ventures $591K — decision ~May 2026 [source: grant]
