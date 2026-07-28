-- 009_cdc_svi_meta down: Remove CDC SVI variable definitions.
-- Note: does NOT remove the source or any indicator data — only the meta entries.

DELETE FROM indicator_meta WHERE variable_id IN (
    'cdc_svi_overall',
    'cdc_svi_socioeconomic',
    'cdc_svi_household',
    'cdc_svi_racial_ethnic',
    'cdc_svi_housing_transport'
);