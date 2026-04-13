CREATE TABLE IF NOT EXISTS analyses (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type        TEXT NOT NULL,
    scope_geoid TEXT REFERENCES geographies(geoid),
    scope_level geo_level,
    parameters  JSONB,
    results     JSONB,
    vintage     TEXT,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_analyses_type        ON analyses(type);
CREATE INDEX IF NOT EXISTS idx_analyses_scope_geoid ON analyses(scope_geoid);
CREATE INDEX IF NOT EXISTS idx_analyses_scope_level ON analyses(scope_level);
CREATE INDEX IF NOT EXISTS idx_analyses_parameters  ON analyses USING GIN(parameters);

CREATE TABLE IF NOT EXISTS analysis_scores (
    analysis_id UUID        NOT NULL REFERENCES analyses(id) ON DELETE CASCADE,
    geoid       TEXT        NOT NULL REFERENCES geographies(geoid),
    score       DOUBLE PRECISION,
    rank        INTEGER,
    percentile  DOUBLE PRECISION,
    tier        TEXT,
    details     JSONB,
    PRIMARY KEY (analysis_id, geoid)
);

CREATE INDEX IF NOT EXISTS idx_analysis_scores_geoid ON analysis_scores(geoid);
CREATE INDEX IF NOT EXISTS idx_analysis_scores_tier  ON analysis_scores(tier);
CREATE INDEX IF NOT EXISTS idx_analysis_scores_score ON analysis_scores(score);
