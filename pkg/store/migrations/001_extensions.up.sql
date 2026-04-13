-- PostGIS is optional — geometry features are disabled gracefully when not available.
DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS postgis;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'PostGIS not available — spatial features disabled: %', SQLERRM;
END $$;

CREATE EXTENSION IF NOT EXISTS pg_trgm;
