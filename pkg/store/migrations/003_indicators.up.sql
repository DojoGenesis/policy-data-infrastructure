CREATE TABLE IF NOT EXISTS indicator_sources (
    source_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    category    TEXT NOT NULL,
    url         TEXT,
    description TEXT
);

CREATE TABLE IF NOT EXISTS indicator_meta (
    variable_id TEXT PRIMARY KEY,
    source_id   TEXT NOT NULL REFERENCES indicator_sources(source_id),
    name        TEXT NOT NULL,
    description TEXT,
    unit        TEXT,
    direction   TEXT
);

CREATE TABLE IF NOT EXISTS indicators (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    geoid          TEXT NOT NULL REFERENCES geographies(geoid),
    variable_id    TEXT NOT NULL REFERENCES indicator_meta(variable_id),
    vintage        TEXT NOT NULL,
    value          DOUBLE PRECISION,
    margin_of_error DOUBLE PRECISION,
    raw_value      TEXT,
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (geoid, variable_id, vintage)
);

CREATE INDEX IF NOT EXISTS idx_indicators_geoid       ON indicators(geoid);
CREATE INDEX IF NOT EXISTS idx_indicators_variable    ON indicators(variable_id);
CREATE INDEX IF NOT EXISTS idx_indicators_vintage     ON indicators(vintage);
CREATE INDEX IF NOT EXISTS idx_indicators_geo_var     ON indicators(geoid, variable_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS indicators_latest AS
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
