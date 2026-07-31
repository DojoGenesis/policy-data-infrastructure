-- indicators_latest was defined in 003 and never rebuilt after 007 added cv and
-- reliability to indicators. Every read path that goes through the view — which
-- is every analysis that does not pin a vintage — therefore cannot see the
-- uncertainty columns at all, so an aggregate has no way to check reliability
-- before publishing a value. ADR-014 D8 depends on this.
--
-- A materialized view's column list cannot be altered, so it is dropped and
-- recreated. Recreating drops the unique index with it; it is restored below,
-- and it is what REFRESH ... CONCURRENTLY requires.

DROP MATERIALIZED VIEW IF EXISTS indicators_latest;

CREATE MATERIALIZED VIEW indicators_latest AS
SELECT DISTINCT ON (geoid, variable_id)
    id,
    geoid,
    variable_id,
    vintage,
    value,
    margin_of_error,
    cv,
    reliability,
    raw_value,
    fetched_at
FROM indicators
ORDER BY geoid, variable_id, vintage DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_indicators_latest_geo_var
    ON indicators_latest(geoid, variable_id);
