-- ADR-014 D9: the analysis cache key is (type, scope_geoid, scope_level,
-- vintage, parameters). PutAnalysis previously inserted unconditionally, so
-- an identical re-run minted a duplicate row — observed in production as two
-- identical composite_index rows, same scope, same vintage, same score
-- count, 11.6 seconds apart (ADR-014 F5).

-- 1. Dedupe: keep the newest row per key. Older duplicates' analysis_scores
--    rows are removed by the ON DELETE CASCADE on analysis_scores.
DELETE FROM analyses a
USING analyses b
WHERE a.type = b.type
  AND a.scope_geoid  IS NOT DISTINCT FROM b.scope_geoid
  AND a.scope_level  IS NOT DISTINCT FROM b.scope_level
  AND a.vintage      IS NOT DISTINCT FROM b.vintage
  AND a.parameters   IS NOT DISTINCT FROM b.parameters
  AND (a.computed_at < b.computed_at
       OR (a.computed_at = b.computed_at AND a.id < b.id));

-- 2. Enforce the key. NULLS NOT DISTINCT so a NULL scope (e.g. statewide)
--    still deduplicates; requires PostgreSQL 15+ (this stack runs 16).
--    Vintage is part of the key, not metadata: a cache keyed without it is
--    the stale-value failure class this repo hit four times in two days.
CREATE UNIQUE INDEX IF NOT EXISTS uq_analyses_cache_key
    ON analyses (type, scope_geoid, scope_level, vintage, parameters)
    NULLS NOT DISTINCT;
