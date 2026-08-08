-- Two persisted-wrong-value cleanups found during the 2026-08-08 county
-- explorer audit.

-- 1. Census annotation sentinels stored as margins of error. The ACS API
--    encodes "controlled estimate" and kin as large negative sentinels
--    (-555555555, -222222222, ...); the Go adapter stored them raw, and the
--    dashboard rendered "±-555,555,555". A sentinel is missing data, not a
--    margin. (The Python path cleans these at ingest since 2026-08-08; this
--    repairs rows already written by either path.)
UPDATE indicators
SET margin_of_error = NULL
WHERE margin_of_error <= -111111111;

-- 1b. Reliability computed FROM those sentinels. The adapter derived
--     cv = |MOE/1.645/estimate| before anything screened annotation values,
--     so a sentinel MOE minted cv in the hundreds and a 'low' badge that
--     outlived the MOE repair above (the dashboard showed LOW with no
--     margin — reliability laundered from garbage). No legitimate ACS cv
--     approaches 10 (Census calls >0.30 low); everything above it is
--     sentinel-derived.
UPDATE indicators
SET cv = NULL, reliability = NULL
WHERE cv > 10;

-- 2. County factor_scores rows written by the deleted naive aggregation
--    (scripts/aggregate_factor_scores_county.sql, removed under ADR-014 D6):
--    unweighted tract means with no minimum-N, stamped '2023-efa-v1' at
--    county grain. The query-time fallback died in ed4e7c4; these are its
--    persisted output, still serving. Absent beats wrong (D2). Tract-level
--    EFA rows are untouched.
DELETE FROM factor_scores
WHERE LENGTH(geoid) = 5
  AND analysis_vintage = '2023-efa-v1';

-- The view carries margin_of_error, so repair must be visible to readers.
REFRESH MATERIALIZED VIEW indicators_latest;
