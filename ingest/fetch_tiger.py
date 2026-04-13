#!/usr/bin/env python3
"""
Fetch Census TIGER/Line cartographic boundary files and load boundaries to PostGIS.

Downloads GeoJSON-format cartographic boundary files from the Census Bureau
(500k resolution, WGS84) for the requested state and geography levels,
then inserts/updates rows in the ``geographies`` table.

Usage examples:
  # Wisconsin tracts (2023 vintage)
  python fetch_tiger.py --state 55 --year 2023

  # Cook County, IL — tracts only
  python fetch_tiger.py --state 17 --county 031 --year 2023 --levels tract

  # All counties in Wisconsin
  python fetch_tiger.py --state 55 --year 2023 --levels county

  # Multiple levels
  python fetch_tiger.py --state 55 --year 2023 --levels tract county

  # Dry run (download + parse, no DB write)
  python fetch_tiger.py --state 55 --year 2023 --dry-run
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from lib.db import get_conn, bulk_load_geographies

# ---------------------------------------------------------------------------
# Census cartographic boundary file URL templates
# 500k resolution GeoJSON — suitable for map rendering and area calculations.
# Census releases these at https://www2.census.gov/geo/tiger/GENZ{year}/
# ---------------------------------------------------------------------------

# Resolution to use (500k balances file size with polygon detail)
_CB_BASE = "https://www2.census.gov/geo/tiger/GENZ{year}/json"

_LAYER_URLS: dict[str, str] = {
    # State-level files (national, filtered by STATEFP after download)
    "state":   "{base}/cb_{year}_us_state_500k.json",
    # County-level file (national; filter by STATEFP)
    "county":  "{base}/cb_{year}_us_county_500k.json",
    # Tract-level files are per-state: cb_{year}_{state}_tract_500k.json
    "tract":   "{base}/cb_{year}_{state}_tract_500k.json",
    # Block-group files are also per-state
    "block_group": "{base}/cb_{year}_{state}_bg_500k.json",
    # Place (incorporated places) — national file
    "place":   "{base}/cb_{year}_us_place_500k.json",
    # ZCTA — national file (large; ~50MB)
    "zcta":    "{base}/cb_{year}_us_zcta520_500k.json",
}

# Levels that have a per-state file (use {state} in URL template)
_PER_STATE_LEVELS = {"tract", "block_group"}

# Download timeout in seconds — TIGER files can be large
_TIMEOUT = 120

# Polite delay between requests
_RATE_DELAY = 2.0


def build_url(year: int, level: str, state_fips: str) -> str:
    base = _CB_BASE.format(year=year)
    template = _LAYER_URLS[level]
    return template.format(base=base, year=year, state=state_fips.zfill(2))


def download_geojson(url: str) -> dict:
    """Download a GeoJSON file from the Census Bureau. Retries once on transient errors."""
    print(f"  GET {url}")
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "policy-data-infrastructure/1.0"},
    )
    for attempt in (1, 2):
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                data = resp.read()
            print(f"  Downloaded {len(data) / 1024:.1f} KB")
            return json.loads(data.decode("utf-8"))
        except (urllib.error.URLError, OSError) as exc:
            if attempt == 2:
                raise RuntimeError(f"Failed to download {url}: {exc}") from exc
            print(f"  Attempt {attempt} failed ({exc}), retrying…", file=sys.stderr)
            time.sleep(3)
    raise RuntimeError("unreachable")  # pragma: no cover


def filter_features(features: list[dict], state_fips: str, county_fips: str | None) -> list[dict]:
    """
    Filter GeoJSON features to only those matching state (and optionally county).
    Census national files include all states; per-state files may include adjacent counties.
    """
    state = state_fips.zfill(2)
    county = county_fips.zfill(3) if county_fips else None

    result = []
    for feat in features:
        props = feat.get("properties") or {}
        feat_state = props.get("STATEFP", "")
        if feat_state != state:
            continue
        if county is not None:
            feat_county = props.get("COUNTYFP", "")
            if feat_county != county:
                continue
        result.append(feat)
    return result


def fetch_level(year: int, level: str, state_fips: str, county_fips: str | None) -> list[dict]:
    """
    Download and filter TIGER features for one geography level.
    Returns a list of GeoJSON Feature dicts (with GEOID in properties).
    """
    if level not in _LAYER_URLS:
        raise ValueError(f"Unsupported level: {level!r}. Choose from: {list(_LAYER_URLS)}")

    url = build_url(year, level, state_fips)
    fc = download_geojson(url)
    time.sleep(_RATE_DELAY)

    all_features = fc.get("features", [])
    print(f"  Total features in file: {len(all_features)}")

    # Per-state files don't need state filtering; national files do
    if level in _PER_STATE_LEVELS:
        # Still filter by county if requested
        if county_fips:
            filtered = filter_features(all_features, state_fips, county_fips)
        else:
            filtered = all_features
    else:
        filtered = filter_features(all_features, state_fips, county_fips)

    print(f"  Features after filter (state={state_fips} county={county_fips or 'all'}): {len(filtered)}")
    return filtered


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
    parser.add_argument("--year",    type=int, default=2023, help="TIGER vintage year (default: 2023)")
    parser.add_argument(
        "--levels",
        nargs="+",
        default=["tract"],
        choices=list(_LAYER_URLS),
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

    print("\nConnecting to database...")
    conn = get_conn()

    print("Loading geographies via bulk_load_geographies...")
    n = bulk_load_geographies(conn, all_features)
    print(f"  {n} geography rows written")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
