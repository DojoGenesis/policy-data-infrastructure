-- Migration: 009_fbi_nibrs_meta
-- Adds FBI NIBRS source and indicator metadata entries.

-- Register the source in indicator_sources (idempotent).
INSERT INTO indicator_sources (source_id, name, category, url, description) VALUES
    ('fbi-nibrs', 'FBI NIBRS Crime Data', 'crime',
     'https://cde.ucr.cjis.gov/LATEST/webapp/',
     'FBI National Incident-Based Reporting System (NIBRS) estimation data providing violent and property crime rates at state and county levels.')
ON CONFLICT (source_id) DO NOTHING;

-- Register indicator metadata entries (idempotent).
INSERT INTO indicator_meta (variable_id, source_id, name, description, unit, direction) VALUES
    ('fbi_violent_crime_rate', 'fbi-nibrs',
     'Violent Crime Rate',
     'Estimated violent crime rate per 100,000 population derived from NIBRS submission data and estimation models.',
     'rate_per_100k',
     'lower_better'),
    ('fbi_property_crime_rate', 'fbi-nibrs',
     'Property Crime Rate',
     'Estimated property crime rate per 100,000 population derived from NIBRS submission data and estimation models.',
     'rate_per_100k',
     'lower_better')
ON CONFLICT (variable_id) DO UPDATE SET
    source_id   = EXCLUDED.source_id,
    name        = EXCLUDED.name,
    description = EXCLUDED.description,
    unit        = EXCLUDED.unit,
    direction   = EXCLUDED.direction;
