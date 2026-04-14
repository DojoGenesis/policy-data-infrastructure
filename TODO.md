# TODO — policy-data-infrastructure

> Updated: 2026-04-14 | Last audit: 2026-04-14 (first health audit)
> Update this file after every work session. Move completed items to CHANGELOG.md.

## P0 — Critical (blocks correctness)

- [ ] Fix `.gitignore`: add root `.venv/` and `analysis/output/` [source: health-audit]
- [ ] Untrack committed analysis outputs: `git rm --cached analysis/output/*.csv analysis/output/*.json analysis/output/*.html analysis/output/*.md` [source: health-audit]
- [ ] Fix Makefile ldflags: change PKG from `cmd/pdi` to `internal/version` [source: health-audit]
- [ ] Fix `go.mod` version: `go 1.25.6` → `go 1.24` (1.25 doesn't exist) [source: health-audit]
- [ ] Fix `pkg/pipeline/synthesize.go`: `chronic_absence` variable has no Go DataSource — analyses silently no-op [source: health-audit]
- [ ] Fix `cmd/pdi/analyze.go:244`: Percentile field uses raw score, not `stats.PercentileRank()` [source: health-audit]
- [ ] Fix `pkg/pipeline/analyze.go:165`: replace `-1.0` sentinel with `*float64` nil (violates indicator convention) [source: health-audit]

## P1 — High (security, data integrity)

- [ ] Fix `pkg/gateway/handlers.go:708`: escape `g.Name` before embedding in HTML (XSS risk) [source: health-audit]
- [ ] Fix `pkg/gateway/handlers.go:443,488`: log `LoadEmbeddedTemplates` errors instead of discarding [source: health-audit]
- [ ] Fix `pkg/gateway/handlers.go:770`: use `errors.Is(err, pgx.ErrNoRows)` not string match [source: health-audit]
- [ ] Fix `ingest/lib/census.py`: `_clean_sentinel` crashes on float strings via `int()` cast [source: health-audit]
- [ ] Fix `data/sources.toml`: `cdc-places` `api_key_env` should be `CDC_PLACES_APP_TOKEN` (matches code) [source: health-audit]
- [ ] Fix `schemas/geography.schema.json`: `county_fips` pattern should be 3-digit; rename `geo_level` → `level` [source: health-audit]
- [ ] Re-run BLS LAUS fetch after daily rate limit reset: `python3 ingest/fetch_bls_laus.py --year 2023` [source: session]
- [ ] Promote `pkg/store/export.go` and `import.go` functions to Store interface, or delete [source: health-audit]

## P2 — Medium (quality, testing, documentation)

- [ ] Add tests for `pkg/gateway/` (httptest-based handler tests) [source: health-audit, coverage: 0%]
- [ ] Add tests for `pkg/store/` (integration tests with testcontainers) [source: health-audit, coverage: 0%]
- [ ] Add tests for `pkg/htmlcraft/` [source: health-audit, coverage: 0%]
- [ ] Add CI workflow: `.github/workflows/ci.yml` (go build, go test -short, go vet) [source: health-audit]
- [ ] Remove dead code: `geoLevelDisplay()`, `type geoLevel`, `buildURL()`/`buildStateURL()` in acs.go [source: health-audit]
- [ ] Add rate limiting to `pkg/datasource/cdc_places.go` [source: health-audit]
- [ ] Fix README.md: mark unimplemented data sources (HOLC, Eviction Lab, GTFS, NCES, HRSA) as "Planned" [source: health-audit]
- [ ] Fix README.md: Go version "1.22+" → "1.24+" [source: health-audit]
- [ ] Add `PutIndicatorsBatch` to Store interface (currently only on `*PostgresStore`) [source: health-audit]

## P3 — Low (polish, optimization)

- [ ] Audit transitive deps: `go mod why github.com/quic-go/quic-go` and `go.mongodb.org/mongo-driver` [source: health-audit]
- [ ] Add post-stage validation gate to pipeline (indicator count, null rate, GEOID coverage) [source: health-audit]
- [ ] Improve `pkg/narrative/engine_test.go` — currently smoke tests only [source: health-audit]
- [ ] Document magic numbers in `pkg/narrative/engine.go` (eviction threshold 3.0, transit 30, etc.) [source: health-audit]
- [ ] HTMLCraft v3.5 Polish: animations, shortcuts, ARIA [source: session]

## Backlog — Future Phases

- [ ] Wire WI DPI attendance data into Go pipeline (currently Python-only) [source: architecture-gap]
- [ ] Gateway protocol module merge (2→1) [source: deferred]
- [ ] National-scale data pipeline (all 50 states) [source: roadmap]
- [ ] MCF LOI draft review with Justice — 9 open questions, due Jun 3 2026 [source: grant]
- [ ] Arnold Ventures $591K — decision ~May 2026 [source: grant]
