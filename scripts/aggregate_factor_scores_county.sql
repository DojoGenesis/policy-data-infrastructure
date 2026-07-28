-- Aggregate tract-level factor scores to county level.
-- Averages factor_score per (county_geoid, factor_name) and upserts
-- into factor_scores with the 5-character county GEOID.
--
-- Uses LEFT(geoid, 5) to extract the county FIPS from the 11-char
-- tract GEOID.  Already-populated rows are refreshed via ON CONFLICT.

INSERT INTO factor_scores (
    geoid,
    factor_name,
    factor_score,
    factor_percentile,
    loadings_json,
    analysis_vintage
)
SELECT
    LEFT(geoid, 5) AS county_geoid,
    factor_name,
    AVG(factor_score) AS factor_score,
    NULL::double precision AS factor_percentile,
    NULL::jsonb AS loadings_json,
    '2023-efa-v1'  AS analysis_vintage
FROM factor_scores
WHERE LENGTH(geoid) = 11            -- tract level only
GROUP BY LEFT(geoid, 5), factor_name
ON CONFLICT (geoid, factor_name, analysis_vintage) DO UPDATE
    SET factor_score      = EXCLUDED.factor_score,
        factor_percentile = EXCLUDED.factor_percentile,
        loadings_json     = EXCLUDED.loadings_json;
