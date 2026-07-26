#!/usr/bin/env python3
"""Fetch vintage-matched tract (and county) boundaries as GeoJSON.

Companion to ``fetch_wi_tracts.py``: that script produces the indicator values,
this one produces the geometry they are drawn on. Both key on GEOID.

Geometry comes from TIGERweb's ``tigerWMS_ACS{year}`` service, so the boundaries
are the same vintage the ACS estimates are tabulated on — the join is 1:1 by
construction rather than approximately-right. See ``ingest/lib/tigerweb.py`` for
why this replaced the bulk cartographic-boundary file downloads.

Geometry is generalized server-side (``--simplify``, degrees). Wisconsin tracts
are ~21 MB at full resolution and ~0.75 MB at the 0.0005 default, with no
visible difference in a choropleth. That is the difference between a static
Atlas that loads and one that does not.

Usage:
    python3 analysis/fetch_tract_boundaries.py --dry-run
    python3 analysis/fetch_tract_boundaries.py
    python3 analysis/fetch_tract_boundaries.py --levels tract county
    python3 analysis/fetch_tract_boundaries.py --simplify 0 --levels tract

No API key required — TIGERweb is open.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingest"))

from lib import tigerweb  # noqa: E402

DEFAULT_STATE = "55"
DEFAULT_YEAR = 2024

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")

# Properties worth carrying into a published map. Everything else TIGERweb
# returns (MTFCC, FUNCSTAT, LSADC, OID, area blobs) is dead weight in a payload
# a browser has to download.
KEEP_PROPS = {
    "tract":  ["GEOID", "NAME", "BASENAME", "COUNTY", "INTPTLAT", "INTPTLON"],
    "county": ["GEOID", "NAME", "BASENAME", "INTPTLAT", "INTPTLON"],
    "state":  ["GEOID", "NAME", "BASENAME"],
}

FILE_STEMS = {
    "tract":  "wi_tracts",
    "county": "wi_counties",
    "state":  "wi_state",
}


def trim_properties(features: list[dict], level: str) -> list[dict]:
    keep = KEEP_PROPS.get(level)
    if not keep:
        return features
    for feat in features:
        props = feat.get("properties") or {}
        feat["properties"] = {k: props[k] for k in keep if k in props}
    return features


def geojson_bytes(fc: dict) -> int:
    return len(json.dumps(fc, separators=(",", ":")).encode())


def write_geojson(fc: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(fc, f, separators=(",", ":"))
    size_kb = os.path.getsize(path) / 1024
    print(f"  Wrote {len(fc['features'])} features to {path} ({size_kb:.0f} KB)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch vintage-matched boundary GeoJSON from TIGERweb.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--state", default=DEFAULT_STATE,
                        help=f"2-digit state FIPS (default: {DEFAULT_STATE} = Wisconsin)")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR,
                        help=f"ACS vintage for boundary geometry (default: {DEFAULT_YEAR})")
    parser.add_argument("--levels", nargs="+", default=["tract", "county"],
                        choices=["tract", "county", "state"],
                        help="Geography levels to fetch (default: tract county)")
    parser.add_argument("--simplify", type=float, default=tigerweb.DEFAULT_SIMPLIFY,
                        help=("Generalization tolerance in degrees "
                              f"(default: {tigerweb.DEFAULT_SIMPLIFY}; 0 = full resolution)"))
    parser.add_argument("--dry-run", action="store_true",
                        help="Report feature counts without downloading geometry.")
    args = parser.parse_args()

    state = args.state.zfill(2)
    simplify = args.simplify or None

    print(f"TIGERweb ACS{args.year} — state {state}, levels {args.levels}, "
          f"simplify={simplify or 'none (full resolution)'}")

    if args.dry_run:
        print("\n[dry-run] Counting features only:")
        for level in args.levels:
            n = tigerweb.count_features(args.year, level, state)
            print(f"  {level:<8} {n} features "
                  f"-> {os.path.join(OUTPUT_DIR, FILE_STEMS[level] + '.geojson')}")
        print("\n[dry-run] No geometry downloaded.")
        return

    for level in args.levels:
        print(f"\nLevel: {level}")
        features = tigerweb.fetch_features(
            year=args.year, level=level, state_fips=state, simplify=simplify,
        )
        if not features:
            print(f"  No features returned for {level}; skipping write.")
            continue

        features = trim_properties(features, level)
        fc = tigerweb.feature_collection(
            features,
            source="U.S. Census Bureau, TIGERweb",
            service=f"tigerWMS_ACS{args.year}",
            vintage_year=args.year,
            state_fips=state,
            level=level,
            simplify_degrees=simplify,
            feature_count=len(features),
        )
        write_geojson(fc, os.path.join(OUTPUT_DIR, f"{FILE_STEMS[level]}.geojson"))

    print("\nDone.")


if __name__ == "__main__":
    main()
