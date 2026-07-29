-- backfill_013_retired_tracts.sql
-- Companion data statement for migration 013 (geographies.retired_at).
-- REVIEW BEFORE RUNNING. Not executed by `pdi migrate up`; not embedded in the
-- binary. Apply with:  psql "$PDI_DATABASE_URL" -f deploy/backfill_013_retired_tracts.sql
--
-- ============================================================================
-- WHAT THIS MARKS, AND WHY IT IS NOT A DELETE
-- ============================================================================
-- The geographies table holds more tract rows than the map can draw. On the
-- production database: 1,669 tract rows vs 1,542 tracts in tracts.geojson.
-- Every tract the map draws has a row; the excess is 127 DB-only rows that
-- carry vintage-2019 indicator data and no geometry. They are 2010-census-
-- vintage tracts retired by the 2020 redistricting — legitimate historical
-- data, not corruption.
--
-- Deleting them would destroy the multi-vintage history that ADR-012
-- §Integration 5 (Temporal Analysis) is specified to need, and would
-- permanently remove the ability to answer "what did this area look like in
-- 2019?". So they stay, and are marked instead: reads describe the present by
-- default, and temporal callers opt in.
--
-- ============================================================================
-- THE PREDICATE, AND WHY EACH CONJUNCT IS THERE
-- ============================================================================
-- A row is marked retired only when ALL of the following hold:
--
--   1. level = 'tract'
--      Scope. Only tract geometry is re-drawn wholesale by decennial
--      redistricting. Counties and states are not, and must not be swept in.
--
--   2. boundary IS NULL
--      Necessary, NEVER sufficient. A row with no geometry cannot render and
--      is silently skipped by spatial queries — but a *current* tract whose
--      geometry load failed would look identical. This codebase has a
--      documented history of silent government-API data drops (CLAUDE.md,
--      "Government API silent data drops"), so NULL boundary alone is treated
--      as a symptom, not a diagnosis. It never decides anything by itself.
--
--   3. No indicator data at any vintage newer than the cutoff year
--      This is the load-bearing conjunct. A geography that is still current
--      keeps receiving data — the live tracts carry 2022 rows. A geography
--      whose data stops at 2019 stopped being collected against, which is
--      what retirement actually means.
--
--   4. At least one indicator row at or before the cutoff year
--      Positive evidence. This is what separates "retired, holds history"
--      from "empty row, probably a failed load". A tract with no geometry AND
--      no data at all is NOT marked — it is left visible so the data-quality
--      problem stays a data-quality problem instead of being quietly filed as
--      history. Conjuncts 2 and 4 together are the guard against mistaking a
--      broken load for a retired geography.
--
--   5. retired_at IS NULL
--      Idempotence. Re-running never re-stamps an already-marked row and never
--      overwrites a timestamp an operator set by hand.
--
-- Derived, not enumerated: there is no hardcoded GEOID list here. The
-- statement re-evaluates against whatever the data says at run time, so it
-- stays correct after the next vintage load — which is exactly why it lives
-- here and not inside migration 013.
--
-- VINTAGE PARSING: indicators.vintage is TEXT and not uniformly a bare year
-- ('2019', but also 'CHAS-2020', 'FBI-NIBRS-2023', 'GTFS-2026'). The year is
-- extracted as the last 4-digit group in the string and compared numerically —
-- a lexicographic comparison would rank 'ACS-2015' above '2019'. A vintage
-- with no parseable year is treated as CURRENT (blocks retirement), so an
-- unrecognised label can never cause a row to be filed away as history.
--
-- RETIRED_AT VALUE: the 2020-census vintage boundary, not the time this script
-- runs. Recording when the geography stopped being current (rather than when
-- we noticed) is what makes point-in-time queries work:
--     WHERE retired_at IS NULL OR retired_at > DATE '2019-12-31'
--       -- geographies that were current during the 2019 vintage
--
-- ASSUMES PostGIS is installed (the `boundary` column exists). It does on the
-- production database; migration 002 makes the column conditional.
--
-- EXPECTED RESULT on production as measured 2026-07-28:
--     127 rows updated; tract count then reads 1,542 current / 127 retired.
-- If the preview in Section 1 reports a materially different number, STOP and
-- investigate before running Section 2 — a large jump means either a genuine
-- vintage change or a load failure, and the two need opposite responses.
-- ============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 1 — PREVIEW (read-only; changes nothing)
-- ─────────────────────────────────────────────────────────────────────────────

-- 1a. How the tract rows break down under the predicate.
WITH cfg AS (SELECT 2019::int AS cutoff_year),
classified AS (
    SELECT
        g.geoid,
        g.boundary IS NULL AS no_geometry,
        EXISTS (
            SELECT 1 FROM indicators i
            WHERE i.geoid = g.geoid
              AND (
                    substring(i.vintage from '(\d{4})\D*$') IS NULL
                 OR substring(i.vintage from '(\d{4})\D*$')::int > cfg.cutoff_year
              )
        ) AS has_current_data,
        EXISTS (
            SELECT 1 FROM indicators i
            WHERE i.geoid = g.geoid
              AND substring(i.vintage from '(\d{4})\D*$')::int <= cfg.cutoff_year
        ) AS has_historical_data
    FROM geographies g, cfg
    WHERE g.level = 'tract'
)
SELECT
    count(*)                                                                    AS tract_rows_total,
    count(*) FILTER (WHERE NOT no_geometry)                                     AS has_geometry,
    count(*) FILTER (WHERE no_geometry AND NOT has_current_data AND has_historical_data)
                                                                                AS would_be_retired,
    count(*) FILTER (WHERE no_geometry AND NOT has_current_data AND NOT has_historical_data)
                                                                                AS suspect_empty_rows,
    count(*) FILTER (WHERE no_geometry AND has_current_data)                    AS suspect_missing_geometry
FROM classified;
-- Reading this row:
--   would_be_retired         → what Section 2 will mark (expect 127).
--   suspect_empty_rows       → no geometry, no data at all. NOT marked. If this
--                              is non-zero, a load failed; chase it separately.
--   suspect_missing_geometry → current data but no geometry. NOT marked. This
--                              is the silent-geometry-drop case; it is a bug to
--                              fix, and hiding it would have been the wrong fix.

-- 1b. The actual rows that Section 2 will mark.
WITH cfg AS (SELECT 2019::int AS cutoff_year)
SELECT
    g.geoid,
    g.name,
    g.state_fips,
    g.county_fips,
    (SELECT count(*) FROM indicators i WHERE i.geoid = g.geoid)     AS indicator_rows,
    (SELECT max(i.vintage) FROM indicators i WHERE i.geoid = g.geoid) AS newest_vintage
FROM geographies g, cfg
WHERE g.level = 'tract'
  AND g.retired_at IS NULL
  AND g.boundary IS NULL
  AND NOT EXISTS (
        SELECT 1 FROM indicators i
        WHERE i.geoid = g.geoid
          AND (
                substring(i.vintage from '(\d{4})\D*$') IS NULL
             OR substring(i.vintage from '(\d{4})\D*$')::int > cfg.cutoff_year
          )
      )
  AND EXISTS (
        SELECT 1 FROM indicators i
        WHERE i.geoid = g.geoid
          AND substring(i.vintage from '(\d{4})\D*$')::int <= cfg.cutoff_year
      )
ORDER BY g.geoid;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 2 — APPLY
-- Wrapped in a transaction: change COMMIT to ROLLBACK at the bottom to test the
-- statement against live data without keeping the result.
-- ─────────────────────────────────────────────────────────────────────────────

BEGIN;

WITH cfg AS (
    SELECT
        2019::int                                AS cutoff_year,
        TIMESTAMPTZ '2020-01-01 00:00:00+00'     AS retired_effective,
        '2020 census redistricting: 2010-vintage tract, no geometry and no indicator data after 2019'::text
                                                 AS reason
)
UPDATE geographies g
SET retired_at     = cfg.retired_effective,
    retired_reason = cfg.reason,
    updated_at     = now()
FROM cfg
WHERE g.level = 'tract'
  AND g.retired_at IS NULL                            -- (5) idempotence
  AND g.boundary IS NULL                              -- (2) cannot render
  AND NOT EXISTS (                                    -- (3) no post-cutoff data
        SELECT 1 FROM indicators i
        WHERE i.geoid = g.geoid
          AND (
                substring(i.vintage from '(\d{4})\D*$') IS NULL
             OR substring(i.vintage from '(\d{4})\D*$')::int > cfg.cutoff_year
          )
      )
  AND EXISTS (                                        -- (4) holds real history
        SELECT 1 FROM indicators i
        WHERE i.geoid = g.geoid
          AND substring(i.vintage from '(\d{4})\D*$')::int <= cfg.cutoff_year
      );


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 3 — VERIFY (still inside the transaction)
-- ─────────────────────────────────────────────────────────────────────────────

-- Expect: tract → 1542 current / 127 retired. Every other level → 0 retired.
SELECT
    level,
    count(*)                                        AS total_rows,
    count(*) FILTER (WHERE retired_at IS NULL)      AS current_rows,
    count(*) FILTER (WHERE retired_at IS NOT NULL)  AS retired_rows
FROM geographies
GROUP BY level
ORDER BY level;

-- Nothing that still carries post-2019 data may end up retired. Expect 0 rows.
WITH cfg AS (SELECT 2019::int AS cutoff_year)
SELECT g.geoid, g.name
FROM geographies g, cfg
WHERE g.retired_at IS NOT NULL
  AND EXISTS (
        SELECT 1 FROM indicators i
        WHERE i.geoid = g.geoid
          AND substring(i.vintage from '(\d{4})\D*$')::int > cfg.cutoff_year
      );

COMMIT;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 4 — REVERSING A MARK
-- ─────────────────────────────────────────────────────────────────────────────
-- Retirement is never applied implicitly: PutGeographies leaves retired_at
-- untouched on upsert, so re-running a geography load will not silently
-- un-retire rows — and equally will not silently un-hide a tract that has
-- genuinely come back. If a marked geography turns out to be current, clear it
-- explicitly:
--
--     UPDATE geographies
--        SET retired_at = NULL, retired_reason = NULL, updated_at = now()
--      WHERE geoid = '<geoid>';
--
-- To unwind the whole backfill (leaves all rows and all indicator data intact):
--
--     UPDATE geographies
--        SET retired_at = NULL, retired_reason = NULL, updated_at = now()
--      WHERE level = 'tract'
--        AND retired_reason LIKE '2020 census redistricting:%';
