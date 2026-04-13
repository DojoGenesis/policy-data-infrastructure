"""PostgreSQL connection and bulk loaders for policy-data-infrastructure."""
import json
import os

import psycopg

# Default matches the convention in policy-data-infrastructure/CLAUDE.md
DATABASE_URL = os.environ.get(
    "PDI_DATABASE_URL",
    "postgres://pdi:pdi@localhost:5432/pdi?sslmode=disable",
)


def get_conn() -> psycopg.Connection:
    """Open and return a psycopg connection using PDI_DATABASE_URL."""
    return psycopg.connect(DATABASE_URL)


# ---------------------------------------------------------------------------
# Indicators
# ---------------------------------------------------------------------------

def bulk_load_indicators(conn: psycopg.Connection, indicators: list[dict]) -> int:
    """
    Bulk-insert indicator rows via the COPY protocol.

    Each dict in ``indicators`` must contain:
      geoid        (str)  — 11-digit tract GEOID (or other level)
      variable_id  (str)  — snake_case indicator identifier, e.g. "median_hh_income"
      vintage      (int)  — ACS end year, e.g. 2023
      value        (float | None) — computed indicator value; None = missing/suppressed
      margin_of_error (float | None) — MOE at 90% CI; None if not applicable
      raw_value    (str)  — original Census API string (preserved for audit)

    Returns the number of rows written.

    Note: uses ON CONFLICT DO NOTHING — re-runs are safe (idempotent by unique constraint
    on geoid + variable_id + vintage).  To update existing rows instead, use
    upsert_indicators().
    """
    if not indicators:
        return 0

    # Use a temp table + COPY + INSERT ... ON CONFLICT for proper upsert.
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE _ind_staging (
                geoid           text,
                variable_id     text,
                vintage         integer,
                value           double precision,
                margin_of_error double precision,
                raw_value       text
            ) ON COMMIT DROP
        """)

        with cur.copy(
            "COPY _ind_staging (geoid, variable_id, vintage, value, margin_of_error, raw_value) FROM STDIN"
        ) as copy:
            for ind in indicators:
                copy.write_row((
                    ind["geoid"],
                    ind["variable_id"],
                    int(ind["vintage"]),
                    ind.get("value"),           # None → NULL
                    ind.get("margin_of_error"),  # None → NULL
                    str(ind.get("raw_value", "") or ""),
                ))

        cur.execute("""
            INSERT INTO indicators (geoid, variable_id, vintage, value, margin_of_error, raw_value)
            SELECT geoid, variable_id, vintage, value, margin_of_error, raw_value
            FROM _ind_staging
            ON CONFLICT (geoid, variable_id, vintage) DO UPDATE SET
                value           = EXCLUDED.value,
                margin_of_error = EXCLUDED.margin_of_error,
                raw_value       = EXCLUDED.raw_value,
                updated_at      = now()
        """)
        count = cur.rowcount

    conn.commit()
    return count


def upsert_indicators(conn: psycopg.Connection, indicators: list[dict]) -> int:
    """Alias for bulk_load_indicators (always upserts)."""
    return bulk_load_indicators(conn, indicators)


# ---------------------------------------------------------------------------
# Geographies
# ---------------------------------------------------------------------------

def bulk_load_geographies(conn: psycopg.Connection, features: list[dict]) -> int:
    """
    Insert or update geographic boundary rows from GeoJSON features.

    Each dict in ``features`` should be a GeoJSON Feature with at minimum:
      properties.GEOID    (str)
      properties.NAMELSAD (str) — human-readable name
      properties.STATEFP  (str) — 2-digit state FIPS
      properties.COUNTYFP (str, optional) — 3-digit county FIPS
      properties.ALAND    (int, optional) — land area in sq meters
      properties.AWATER   (int, optional) — water area in sq meters
      geometry            (dict) — GeoJSON geometry object

    The geometry is stored in PostGIS via ST_GeomFromGeoJSON.
    SRID is assumed to be 4326 (WGS84), which is what Census TIGER/cartographic files use.

    Returns the number of rows written.
    """
    if not features:
        return 0

    count = 0
    with conn.cursor() as cur:
        for feat in features:
            props = feat.get("properties") or {}
            geoid = props.get("GEOID") or props.get("geoid")
            name = props.get("NAMELSAD") or props.get("NAME") or props.get("name") or geoid
            state_fips = (props.get("STATEFP") or "").zfill(2) or None
            county_fips_raw = props.get("COUNTYFP") or props.get("county_fips")
            county_fips = (state_fips + county_fips_raw.zfill(3)) if county_fips_raw and state_fips else None
            level = _infer_geo_level(geoid)
            aland = props.get("ALAND")
            awater = props.get("AWATER")
            geometry = feat.get("geometry")
            geom_json = json.dumps(geometry) if geometry else None

            cur.execute("""
                INSERT INTO geographies (geoid, level, name, state_fips, county_fips, land_area_sqm, water_area_sqm, geom)
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    CASE WHEN %s IS NOT NULL THEN ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326) ELSE NULL END
                )
                ON CONFLICT (geoid) DO UPDATE SET
                    name          = EXCLUDED.name,
                    land_area_sqm = EXCLUDED.land_area_sqm,
                    water_area_sqm = EXCLUDED.water_area_sqm,
                    geom          = EXCLUDED.geom,
                    updated_at    = now()
            """, (
                geoid, level, name, state_fips, county_fips,
                aland, awater,
                geom_json, geom_json,
            ))
            count += cur.rowcount

    conn.commit()
    return count


# ---------------------------------------------------------------------------
# Indicator metadata
# ---------------------------------------------------------------------------

def upsert_indicator_meta(conn: psycopg.Connection, variables: dict[str, dict]) -> int:
    """
    Insert or update rows in the indicator_meta table.

    ``variables`` is a dict keyed by variable_id:
    {
        "median_hh_income": {
            "label":     "Median Household Income",
            "source":    "acs-5yr",
            "table":     "B19013_001E",
            "unit":      "dollars",
            "direction": "higher_better",   # higher_better | lower_better | neutral
        },
        ...
    }

    Returns the number of rows written.
    """
    if not variables:
        return 0

    count = 0
    with conn.cursor() as cur:
        for variable_id, meta in variables.items():
            cur.execute("""
                INSERT INTO indicator_meta (variable_id, label, source_id, source_table, unit, direction)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (variable_id) DO UPDATE SET
                    label        = EXCLUDED.label,
                    source_id    = EXCLUDED.source_id,
                    source_table = EXCLUDED.source_table,
                    unit         = EXCLUDED.unit,
                    direction    = EXCLUDED.direction,
                    updated_at   = now()
            """, (
                variable_id,
                meta.get("label", variable_id.replace("_", " ").title()),
                meta.get("source", ""),
                meta.get("table"),
                meta.get("unit", ""),
                meta.get("direction", "neutral"),
            ))
            count += cur.rowcount

    conn.commit()
    return count


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _infer_geo_level(geoid: str | None) -> str:
    """Infer geography level from GEOID length."""
    if not geoid:
        return "unknown"
    length = len(str(geoid))
    return {
        2:  "state",
        5:  "county",
        11: "tract",
        12: "block_group",
        7:  "place",
    }.get(length, "unknown")
