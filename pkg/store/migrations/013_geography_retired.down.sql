-- 013_geography_retired.down.sql
-- Revert the explicit geography lifecycle marker.
--
-- WARNING: dropping retired_at / retired_reason DISCARDS every retirement mark.
-- The geography rows and their historical indicator data survive (nothing is
-- deleted here), but after a down/up cycle every geography reads as "current"
-- again and the tract counts will over-report. Re-apply the backfill after
-- re-running the up migration: deploy/backfill_013_retired_tracts.sql

DROP INDEX IF EXISTS idx_geo_level_current;
DROP INDEX IF EXISTS idx_geo_retired_at;

ALTER TABLE geographies DROP COLUMN IF EXISTS retired_reason;
ALTER TABLE geographies DROP COLUMN IF EXISTS retired_at;
