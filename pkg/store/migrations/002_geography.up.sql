DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'geo_level') THEN
        CREATE TYPE geo_level AS ENUM ('nation', 'state', 'county', 'tract', 'block_group', 'ward');
    END IF;
END $$;

-- Core geography table — no PostGIS columns here so it works without PostGIS.
CREATE TABLE IF NOT EXISTS geographies (
    geoid        TEXT PRIMARY KEY,
    level        geo_level NOT NULL,
    parent_geoid TEXT REFERENCES geographies(geoid),
    name         TEXT NOT NULL,
    state_fips   CHAR(2),
    county_fips  CHAR(3),
    population   INTEGER,
    land_area_m2 DOUBLE PRECISION,
    created_at   TIMESTAMPTZ DEFAULT now(),
    updated_at   TIMESTAMPTZ DEFAULT now()
);

-- Add PostGIS geometry columns if PostGIS is available.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'geographies' AND column_name = 'boundary'
    ) THEN
        EXECUTE 'ALTER TABLE geographies ADD COLUMN boundary GEOMETRY(MultiPolygon, 4326)';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'geographies' AND column_name = 'centroid'
    ) THEN
        EXECUTE 'ALTER TABLE geographies ADD COLUMN centroid GEOMETRY(Point, 4326)';
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'PostGIS geometry columns not added (PostGIS unavailable): %', SQLERRM;
END $$;

CREATE INDEX IF NOT EXISTS idx_geo_level    ON geographies(level);
CREATE INDEX IF NOT EXISTS idx_geo_parent   ON geographies(parent_geoid);
CREATE INDEX IF NOT EXISTS idx_geo_state    ON geographies(state_fips);
CREATE INDEX IF NOT EXISTS idx_geo_county   ON geographies(state_fips, county_fips);
CREATE INDEX IF NOT EXISTS idx_geo_name_trgm ON geographies USING GIN(name gin_trgm_ops);

-- Spatial indexes — only created if PostGIS geometry columns exist.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'geographies' AND column_name = 'boundary'
    ) THEN
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_geo_boundary ON geographies USING GIST(boundary)';
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_geo_centroid ON geographies USING GIST(centroid)';
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Spatial indexes not created: %', SQLERRM;
END $$;
