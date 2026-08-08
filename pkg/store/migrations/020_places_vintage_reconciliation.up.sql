-- PLACES vintage-label reconciliation (2026-08-08). The Python loader used
-- to write the bare label '2022' while the Go adapter wrote
-- 'CDC-PLACES-2023' for the SAME release (data.cdc.gov cwsq-ngmh) — prod
-- carried one label, local the other, and every cross-environment
-- comparison by vintage string tripped over it. The aligned loader now
-- writes CDC-PLACES-2023 everywhere; this retires rows under the old
-- label. Scoped to the nine cdc_* PLACES measure ids — CDC SVI's
-- legitimate '2022' vintage is untouched.
DELETE FROM indicators
WHERE vintage = '2022'
  AND variable_id IN (
    'cdc_access2', 'cdc_binge', 'cdc_bphigh', 'cdc_casthma',
    'cdc_csmoking', 'cdc_diabetes', 'cdc_mhlth', 'cdc_obesity', 'cdc_phlth'
  );

REFRESH MATERIALIZED VIEW indicators_latest;
