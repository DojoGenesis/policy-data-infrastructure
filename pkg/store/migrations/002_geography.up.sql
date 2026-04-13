CREATE TYPE geo_level AS ENUM ('nation', 'state', 'county', 'tract', 'block_group', 'ward');

CREATE TABLE geographies (
    geoid        TEXT PRIMARY KEY,
    level        geo_level NOT NULL,
    parent_geoid TEXT REFERENCES geographies(geoid),
    name         TEXT NOT NULL,
    state_fips   CHAR(2),
    county_fips  CHAR(3),
    population   INTEGER,
    land_area_m2 DOUBLE PRECISION,
    boundary     GEOMETRY(MultiPolygon, 4326),
    centroid     GEOMETRY(Point, 4326),
    created_at   TIMESTAMPTZ DEFAULT now(),
    updated_at   TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_geo_level    ON geographies(level);
CREATE INDEX idx_geo_parent   ON geographies(parent_geoid);
CREATE INDEX idx_geo_state    ON geographies(state_fips);
CREATE INDEX idx_geo_county   ON geographies(state_fips, county_fips);
CREATE INDEX idx_geo_boundary ON geographies USING GIST(boundary);
CREATE INDEX idx_geo_centroid ON geographies USING GIST(centroid);
CREATE INDEX idx_geo_name_trgm ON geographies USING GIN(name gin_trgm_ops);
