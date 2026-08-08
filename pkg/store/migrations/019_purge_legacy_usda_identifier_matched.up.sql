-- Legacy USDA rows from a partial pre-crosswalk load (found on prod
-- 2026-08-08): seven usda_* variables (usda_lila, usda_population,
-- usda_poverty_rate, usda_snap_authorized, usda_urban_flag,
-- usda_low_access_1mi, usda_low_access_10mi) carrying FARA values keyed to
-- 2010 tract identifiers that happened to match rows in geographies —
-- the identifier-matched-across-a-boundary-revision case ADR-014 OQ6
-- existed to forbid, live in indicators_latest and the agent vocabulary.
-- The crosswalked usda_food_* set (2026-08-08) supersedes them entirely.
-- Zero-row variables drop out of the planner vocabulary, dashboards and
-- /variables coverage automatically; the indicator_meta rows may stay.
DELETE FROM indicators
WHERE variable_id IN (
    'usda_lila', 'usda_population', 'usda_poverty_rate',
    'usda_snap_authorized', 'usda_urban_flag',
    'usda_low_access_1mi', 'usda_low_access_10mi'
);

REFRESH MATERIALIZED VIEW indicators_latest;
