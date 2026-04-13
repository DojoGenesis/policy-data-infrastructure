"""GEOID validation — supports GeoJSON file or PostgreSQL backend."""
import json
import re
from pathlib import Path

# Expected FIPS lengths by geo level (matches policy-data-infrastructure schema)
_GEOID_LENGTHS: dict[str, int] = {
    "state": 2,
    "county": 5,
    "tract": 11,
    "block_group": 12,
    "place": 7,
    "zcta": 5,
}

# Regex: digits only, no leading/trailing whitespace
_FIPS_RE = re.compile(r"^\d+$")

# Module-level cache: geoid_set keyed by (source_description)
_cache: dict[str, set[str]] = {}


def get_valid_geoids(
    geojson_path: str | Path | None = None,
    db_conn=None,
    geo_level: str = "tract",
) -> set[str]:
    """
    Return the set of valid GEOIDs for the given geography level.

    Resolution order:
    1. If ``db_conn`` is provided, query the ``geographies`` table in PostgreSQL.
    2. Otherwise fall back to loading ``geojson_path`` (a local GeoJSON file).
    Both results are cached for the lifetime of the process.

    Parameters
    ----------
    geojson_path : path to a GeoJSON file whose features have a ``GEOID`` property.
                   Ignored when db_conn is provided.
    db_conn      : an open psycopg connection. If provided, geojson_path is ignored.
    geo_level    : geography level to query from the DB (e.g. "tract", "county").
                   Not used when loading from GeoJSON (all GEOIDs in the file are returned).
    """
    if db_conn is not None:
        cache_key = f"db:{geo_level}"
        if cache_key not in _cache:
            _cache[cache_key] = _load_from_db(db_conn, geo_level)
        return _cache[cache_key]

    if geojson_path is None:
        raise ValueError("Either geojson_path or db_conn must be provided.")

    cache_key = str(geojson_path)
    if cache_key not in _cache:
        _cache[cache_key] = _load_from_geojson(Path(geojson_path))
    return _cache[cache_key]


def _load_from_db(conn, geo_level: str) -> set[str]:
    """Query geographies table for valid GEOIDs at the given level."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT geoid FROM geographies WHERE level = %s",
            (geo_level,),
        )
        return {row[0] for row in cur.fetchall()}


def _load_from_geojson(path: Path) -> set[str]:
    """Load GEOIDs from a GeoJSON FeatureCollection."""
    with open(path) as f:
        fc = json.load(f)
    geoids: set[str] = set()
    for feat in fc.get("features", []):
        props = feat.get("properties") or {}
        # Census TIGER files use 'GEOID'; atlas files may use 'GEOID' or 'geoid'
        geoid = props.get("GEOID") or props.get("geoid")
        if geoid:
            geoids.add(str(geoid))
    return geoids


def validate_geoid(geoid: str, geo_level: str | None = None) -> bool:
    """
    Check that a GEOID is a valid FIPS format string.

    If ``geo_level`` is provided, also validates expected digit length.
    Does NOT check against a known set of valid GEOIDs — use get_valid_geoids() for that.
    """
    if not isinstance(geoid, str):
        return False
    if not _FIPS_RE.match(geoid):
        return False
    if geo_level is not None:
        expected_len = _GEOID_LENGTHS.get(geo_level)
        if expected_len is not None and len(geoid) != expected_len:
            return False
    return True


def filter_valid(
    records: list[dict],
    valid_geoids: set[str],
    geoid_field: str = "geoid",
) -> tuple[list[dict], list[str]]:
    """
    Partition records into (valid, rejected_geoids).

    Returns
    -------
    valid    : records whose geoid_field value is in valid_geoids
    rejected : list of GEOID strings that were not in valid_geoids
    """
    valid: list[dict] = []
    rejected: list[str] = []
    for rec in records:
        g = rec.get(geoid_field)
        if g in valid_geoids:
            valid.append(rec)
        else:
            rejected.append(str(g))
    return valid, rejected


def clear_cache() -> None:
    """Evict all cached GEOID sets (useful between test runs)."""
    _cache.clear()
