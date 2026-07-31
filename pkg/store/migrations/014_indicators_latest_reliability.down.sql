-- Restore the 003 shape: same view without cv/reliability.

DROP MATERIALIZED VIEW IF EXISTS indicators_latest;

CREATE MATERIALIZED VIEW indicators_latest AS
SELECT DISTINCT ON (geoid, variable_id)
    id,
    geoid,
    variable_id,
    vintage,
    value,
    margin_of_error,
    raw_value,
    fetched_at
FROM indicators
ORDER BY geoid, variable_id, vintage DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_indicators_latest_geo_var
    ON indicators_latest(geoid, variable_id);
