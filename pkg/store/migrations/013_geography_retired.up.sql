-- 013_geography_retired.up.sql
-- Explicit lifecycle marker for geographies.
--
-- WHY: the geographies table has no vintage concept — only `indicators` carries
-- a `vintage` column. Census geography boundaries are re-drawn every decennial
-- census, so a tract GEOID that was valid under the 2010 vintage may not exist
-- under the 2020 vintage. Those retired rows must be KEPT: they anchor the
-- historical indicator data that ADR-012 §Integration 5 (Temporal Analysis)
-- depends on ("what did this area look like in 2019?"). Deleting them would
-- destroy real data.
--
-- What they must NOT do is leak into "current" reads — map layers, geography
-- listings, child lookups, and pagination totals should describe the present.
--
-- NOTE ON THE MARKER: retirement is recorded EXPLICITLY, never inferred.
-- `boundary IS NULL` is deliberately NOT used as the signal: a *current* tract
-- whose geometry load failed silently (see CLAUDE.md, "Government API silent
-- data drops") would be misclassified as retired, and the data-quality problem
-- would be hidden instead of surfaced.
--
-- This migration is SCHEMA-ONLY. It intentionally contains no backfill: a
-- migration that embeds a specific data snapshot (e.g. a hardcoded GEOID list)
-- is wrong the next time it runs. The backfill ships separately as a reviewable,
-- re-runnable statement: deploy/backfill_013_retired_tracts.sql

ALTER TABLE geographies ADD COLUMN IF NOT EXISTS retired_at     TIMESTAMPTZ;
ALTER TABLE geographies ADD COLUMN IF NOT EXISTS retired_reason TEXT;

COMMENT ON COLUMN geographies.retired_at IS
    'NULL = geography is current. Non-NULL = retired as of this timestamp '
    '(e.g. a 2010-vintage census tract superseded by 2020 redistricting). '
    'Retired rows are excluded from geography reads by default and are '
    'included only via an explicit opt-in, so historical indicator data '
    'remains queryable for temporal analysis. Set only by an explicit '
    'curation statement — never inferred from a NULL boundary, and never '
    'set implicitly by a data load.';

COMMENT ON COLUMN geographies.retired_reason IS
    'Free-text provenance for retired_at, e.g. "2020 census redistricting: '
    '2010-vintage tract with no post-2019 indicator data". NULL when current.';

-- Hot path: every default geography read filters `retired_at IS NULL`, almost
-- always alongside a level filter. A partial index keeps the current-rows scan
-- proportional to the current set as the table grows beyond Wisconsin.
CREATE INDEX IF NOT EXISTS idx_geo_level_current
    ON geographies(level)
    WHERE retired_at IS NULL;

-- Temporal path: locating the retired set directly (ADR-012 §I5).
CREATE INDEX IF NOT EXISTS idx_geo_retired_at
    ON geographies(retired_at)
    WHERE retired_at IS NOT NULL;
