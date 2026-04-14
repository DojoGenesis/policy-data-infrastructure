-- 007_indicator_reliability: Add coefficient of variation and reliability flag
-- to the indicators table for ACS margin-of-error tracking.

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'reliability_level') THEN
    CREATE TYPE reliability_level AS ENUM ('high', 'moderate', 'low');
  END IF;
END $$;

ALTER TABLE indicators
  ADD COLUMN IF NOT EXISTS cv DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS reliability reliability_level;

UPDATE indicators SET reliability = CASE
  WHEN cv IS NOT NULL AND cv < 0.15 THEN 'high'::reliability_level
  WHEN cv IS NOT NULL AND cv < 0.30 THEN 'moderate'::reliability_level
  WHEN cv IS NOT NULL THEN 'low'::reliability_level
  ELSE NULL
END
WHERE reliability IS NULL AND cv IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_indicators_reliability ON indicators (reliability);

CREATE TABLE IF NOT EXISTS factor_scores (
  id BIGSERIAL PRIMARY KEY,
  geoid TEXT NOT NULL,
  factor_name TEXT NOT NULL,
  factor_score DOUBLE PRECISION,
  factor_percentile DOUBLE PRECISION,
  loadings_json JSONB,
  analysis_vintage TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(geoid, factor_name, analysis_vintage)
);

CREATE INDEX IF NOT EXISTS idx_factor_scores_geoid ON factor_scores (geoid);
CREATE INDEX IF NOT EXISTS idx_factor_scores_factor ON factor_scores (factor_name);

CREATE TABLE IF NOT EXISTS validated_features (
  id BIGSERIAL PRIMARY KEY,
  geoid TEXT NOT NULL,
  feature_name TEXT NOT NULL,
  feature_value DOUBLE PRECISION,
  source_citation TEXT,
  analysis_vintage TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(geoid, feature_name, analysis_vintage)
);

CREATE INDEX IF NOT EXISTS idx_validated_features_geoid ON validated_features (geoid);
