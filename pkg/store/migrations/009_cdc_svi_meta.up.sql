-- 009_cdc_svi_meta: Register CDC/ATSDR Social Vulnerability Index (SVI) variables
-- in indicator_meta and seed the indicator_sources entry.

-- 1. Register the source (idempotent).
INSERT INTO indicator_sources (source_id, name, category, url, description) VALUES
    ('cdc-svi', 'CDC/ATSDR Social Vulnerability Index (SVI)', 'health',
     'https://www.atsdr.cdc.gov/placeandhealth/svi/',
     'Biennial SVI ranks census tracts/counties on 16 social factors across four themes.
      Overall and theme-level percentile ranks (0-1, higher = more vulnerable).')
ON CONFLICT (source_id) DO NOTHING;

-- 2. Upsert the five SVI variable definitions.
INSERT INTO indicator_meta (variable_id, source_id, name, description, unit, direction) VALUES
    ('cdc_svi_overall', 'cdc-svi',
     'CDC SVI — Overall Vulnerability',
     'CDC/ATSDR Social Vulnerability Index overall percentile rank (0–1, higher = more vulnerable)',
     'percentile', 'lower_better'),
    ('cdc_svi_socioeconomic', 'cdc-svi',
     'CDC SVI — Theme 1: Socioeconomic Status',
     'SVI socioeconomic theme: poverty, unemployment, income, education (percentile rank 0–1)',
     'percentile', 'lower_better'),
    ('cdc_svi_household', 'cdc-svi',
     'CDC SVI — Theme 2: Household Characteristics',
     'SVI household theme: age 65+, age 17-, disability, single-parent, English proficiency (percentile rank 0–1)',
     'percentile', 'lower_better'),
    ('cdc_svi_racial_ethnic', 'cdc-svi',
     'CDC SVI — Theme 3: Racial & Ethnic Minority Status',
     'SVI racial/ethnic minority theme: non-White, Hispanic/Latino, AI/AN, NHPI groups (percentile rank 0–1)',
     'percentile', 'lower_better'),
    ('cdc_svi_housing_transport', 'cdc-svi',
     'CDC SVI — Theme 4: Housing Type & Transportation',
     'SVI housing/transport theme: multi-unit, mobile homes, crowding, no vehicle, group quarters (percentile rank 0–1)',
     'percentile', 'lower_better')
ON CONFLICT (variable_id) DO UPDATE SET
    source_id   = EXCLUDED.source_id,
    name        = EXCLUDED.name,
    description = EXCLUDED.description,
    unit        = EXCLUDED.unit,
    direction   = EXCLUDED.direction;