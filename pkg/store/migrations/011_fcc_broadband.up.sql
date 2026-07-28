-- 009_fcc_broadband.up.sql
-- FCC Broadband (Form 477) indicator metadata and source registration.
-- Data: residential fixed broadband connections per 1,000 households and
-- provider competition metrics at the county level.
-- Source: https://www.fcc.gov/form-477-county-data-internet-access-services

INSERT INTO indicator_sources (source_id, name, category, url, description) VALUES (
    'fcc-broadband',
    'FCC Broadband Access Data (Form 477)',
    'infrastructure',
    'https://www.fcc.gov/form-477-county-data-internet-access-services',
    'FCC semi-annual county-level residential fixed broadband deployment data: connections per 1,000 households and provider competition metrics.'
) ON CONFLICT (source_id) DO NOTHING;

INSERT INTO indicator_meta (variable_id, source_id, name, description, unit, direction) VALUES
    ('fcc_broadband_access_pct', 'fcc-broadband',
     'Broadband Access Rate',
     'Fixed residential broadband Internet access connections per 1,000 households at the county level. Published semi-annually by FCC Form 477.',
     'per_1000_households', 'higher_better'),
    ('fcc_multiple_providers_pct', 'fcc-broadband',
     'Multiple Broadband Providers',
     'Percentage of census tracts within the county that have more than one fixed broadband provider. Higher values indicate greater provider competition.',
     'percent', 'higher_better')
ON CONFLICT (variable_id) DO UPDATE SET
    source_id   = EXCLUDED.source_id,
    name        = EXCLUDED.name,
    description = EXCLUDED.description,
    unit        = EXCLUDED.unit,
    direction   = EXCLUDED.direction;
