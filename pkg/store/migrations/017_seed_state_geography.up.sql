-- State-scoped analysis runs (migration 016) FK scope_geoid to geographies,
-- but the data seeds only ever loaded county and tract rows — so a
-- state-scope run could not be enqueued at all (caught by the first live
-- POST /analyses with scope_level=state). Seed the Wisconsin state row;
-- population is derived from its counties rather than hardcoded, and stays
-- 0 until county populations are loaded.
INSERT INTO geographies (geoid, level, name, state_fips, population)
SELECT '55', 'state', 'Wisconsin', '55',
       (SELECT COALESCE(SUM(population), 0)
        FROM geographies WHERE level = 'county' AND state_fips = '55')::int
ON CONFLICT (geoid) DO NOTHING;
