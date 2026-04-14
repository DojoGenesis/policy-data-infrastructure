DROP TABLE IF EXISTS validated_features;
DROP TABLE IF EXISTS factor_scores;
DROP INDEX IF EXISTS idx_indicators_reliability;
ALTER TABLE indicators DROP COLUMN IF EXISTS reliability;
ALTER TABLE indicators DROP COLUMN IF EXISTS cv;
DROP TYPE IF EXISTS reliability_level;
