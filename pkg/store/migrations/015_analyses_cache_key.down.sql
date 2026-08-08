-- Deduplicated rows are not restorable; only the constraint is reversed.
DROP INDEX IF EXISTS uq_analyses_cache_key;
