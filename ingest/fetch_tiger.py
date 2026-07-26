#!/usr/bin/env python3
"""
Fetch Census TIGER boundaries and load them to PostGIS.

Pulls vintage-matched boundary geometry as GeoJSON from the TIGERweb REST
service, then inserts/updates rows in the ``geographies`` table.

Source change (2026-07-26): this script used to download bulk cartographic
boundary files from ``https://www2.census.gov/geo/tiger/GENZ{year}/json/``.
That directory no longer exists — ``GENZ2024/json/`` returns HTTP 404 and the
Census now publishes those layers as shapefiles only. Every run of this script
had been failing at the download step. It now reads TIGERweb instead, which
serves GeoJSON directly and is vintage-parameterized (``tigerWMS_ACS{year}``).
See ``lib/tigerweb.py`` for the full rationale.

Usage examples:
  # Wisconsin tracts (ACS 2024 vintage)
  python fetch_tiger.py --state 55 --year 2024

  # Cook County, IL — tracts only
  python fetch_tiger.py --state 17 --county 031 --year 2024 --levels tract

  # All counties in Wisconsin
  python fetch_tiger.py --state 55 --year 2024 --levels county

  # Multiple levels
  python fetch_tiger.py --state 55 --year 2024 --levels tract county

  # Dry run (download + parse, no DB write)
  python fetch_tiger.py --state 55 --year 2024 --dry-run
"""
import argparse
import sys

from lib import tigerweb

# lib.db imports psycopg at module scope. Importing it here would make --dry-run
# require a Postgres driver to be installed, which defeats the point of a dry
# run (and of this repo's "first run of any script is --dry-run" rule). The DB
# import is deferred into main(), past the dry-run return.

# TIGER vintage default, matched to the ACS 5-Year vintage the indicators use.
DEFAULT_YEAR = 2024

# Geography levels this script can load. Superset lives in lib/tigerweb.LAYERS;
# this is the subset that maps onto rows in the ``geographies`` table.
_LEVELS = ["tract", "block_group", "county", "state", "place"]


# TIGERweb names its attributes differently from the old bulk TIGER files.
# lib/db.bulk_load_geographies() reads the bulk-file names, so translate here —
# without this, state_fips/county_fips/land area all silently write as NULL
# (the load still "succeeds", which is the dangerous part).
_PROP_ALIASES = {
    "STATE":     "STATEFP",
    "COUNTY":    "COUNTYFP",
    "AREALAND":  "ALAND",
    "AREAWATER": "AWATER",
}


def normalize_props(props: dict) -> dict:
    """Add bulk-TIGER-style aliases for the fields lib/db.py looks up.

    Original keys are kept — callers writing GeoJSON out (rather than to the
    DB) may prefer TIGERweb's own names.
    """
    out = dict(props)
    for src, dst in _PROP_ALIASES.items():
        if src in out and dst not in out:
            out[dst] = out[src]
    # A tract's TIGERweb NAME ("Census Tract 7") is already the NAMELSAD form.
    if "NAMELSAD" not in out and "NAME" in out:
        out["NAMELSAD"] = out["NAME"]
    return out


def fetch_level(year: int, level: str, state_fips: str, county_fips: str | None) -> list[dict]:
    """
    Fetch TIGER features for one geography level.
    Returns a list of GeoJSON Feature dicts (with GEOID in properties).
    """
    if level not in _LEVELS:
        raise ValueError(f"Unsupported level: {level!r}. Choose from: {_LEVELS}")

    # Full resolution here — this path feeds PostGIS, which does its own
    # simplification at query time. Generalization is for static-file exports.
    features = tigerweb.fetch_features(
        year=year,
        level=level,
        state_fips=state_fips,
        county_fips=county_fips,
        simplify=None,
    )
    for feat in features:
        feat["properties"] = normalize_props(feat.get("properties") or {})
    return features


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Census TIGER/Line boundaries and load to PostGIS.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--state",   required=True, help="2-digit state FIPS (e.g., 55 for Wisconsin)")
    parser.add_argument("--county",  default=None,  help="3-digit county FIPS to filter features (optional)")
    parser.add_argument("--year",    type=int, default=DEFAULT_YEAR,
                        help=f"TIGER vintage year (default: {DEFAULT_YEAR})")
    parser.add_argument(
        "--levels",
        nargs="+",
        default=["tract"],
        choices=_LEVELS,
        help="Geography levels to fetch (default: tract). Multiple values accepted.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Download and parse but do not write to database")
    args = parser.parse_args()

    state  = args.state.zfill(2)
    county = args.county.zfill(3) if args.county else None
    year   = args.year
    levels = args.levels

    print(f"Fetching TIGER {year} boundaries — state={state} county={county or 'all'} levels={levels}")
    print()

    all_features: list[dict] = []
    for level in levels:
        print(f"Level: {level}")
        try:
            features = fetch_level(year, level, state, county)
            all_features.extend(features)
        except RuntimeError as exc:
            print(f"  Error fetching {level}: {exc}", file=sys.stderr)
            sys.exit(1)
        print()

    total = len(all_features)
    print(f"Total features collected: {total}")

    if total == 0:
        print("No features to load.")
        return

    # Print a small sample for verification
    sample = all_features[:3]
    print("\nSample features (first 3):")
    for feat in sample:
        props = feat.get("properties") or {}
        geoid = props.get("GEOID", "?")
        name  = props.get("NAMELSAD") or props.get("NAME", "?")
        geom_type = (feat.get("geometry") or {}).get("type", "?")
        print(f"  GEOID={geoid}  name={name!r}  geom={geom_type}")

    if args.dry_run:
        print("\n[dry-run] Skipping database write.")
        return

    from lib.db import get_conn, bulk_load_geographies  # noqa: PLC0415 — see module docstring

    print("\nConnecting to database...")
    conn = get_conn()

    print("Loading geographies via bulk_load_geographies...")
    n = bulk_load_geographies(conn, all_features)
    print(f"  {n} geography rows written")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
