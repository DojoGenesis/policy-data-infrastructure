#!/usr/bin/env python3
"""
Fetch FCC Broadband Access Data (Form 477) — county-level fixed broadband metrics.

Source: https://www.fcc.gov/form-477-county-data-internet-access-services
Tract-level: https://www.fcc.gov/form-477-census-tract-data-internet-access-services

Format: CSV download — no API key required.

Usage:
  python fetch_fcc_broadband.py --dry-run            # preview only
  python fetch_fcc_broadband.py --year 2023           # fetch and save CSV
  python fetch_fcc_broadband.py --year 2023 --load    # fetch and load to PostGIS

Key variables:
  fcc_broadband_access_pct    — Fixed residential broadband connections per
                                 1,000 households (county-level)
  fcc_multiple_providers_pct  — Percentage of census tracts with more than
                                 one fixed broadband provider (county-level)

The FCC publishes Form 477 data semi-annually (June and December releases).
The CSV includes data at county, state, and tract levels. This script parses
the county-level file; tract-level data is available at the separate URL.
"""

import argparse
import csv
import io
import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Sequence

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "analysis", "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "fcc_broadband_county.csv")

# FCC Form 477 data URLs (county-level and tract-level)
FCC_COUNTY_URL = "https://www.fcc.gov/form-477-county-data-internet-access-services"
FCC_TRACT_URL  = "https://www.fcc.gov/form-477-census-tract-data-internet-access-services"

# The FCC website serves the raw CSV; the direct download URL is typically
# linked from the page. For county-level, the latest data file is at:
FCC_COUNTY_CSV_URL = (
    "https://www.fcc.gov/sites/default/files/fixed_broadband_deployment_data_jun2024.csv"
    # TODO: Update the year in the filename for each new semi-annual release.
    # Available releases: jun2023, dec2023, jun2024, dec2024, jun2025, etc.
    # Pattern: https://www.fcc.gov/sites/default/files/fixed_broadband_deployment_data_{period}.csv
    # where period is monthYYYY, e.g. jun2024, dec2024.
)

RATE_LIMIT_DELAY = 1.5  # seconds between requests

OUTPUT_COLUMNS = [
    "geoid",
    "county_name",
    "state_fips",
    "year",
    "broadband_access_pct",
    "multiple_providers_pct",
]


def print_plan(args: argparse.Namespace) -> None:
    print("[dry-run] FCC Broadband Form 477 fetch plan")
    print(f"  County URL: {FCC_COUNTY_URL}")
    print(f"  CSV URL   : {FCC_COUNTY_CSV_URL}")
    print(f"  Year      : {args.year}")
    print(f"  Geo level : county (state + county FIPS → 5-digit GEOID)")
    print(f"  Variables : broadband_access_pct (connections per 1,000 HH)")
    print(f"              multiple_providers_pct (tracts with >1 provider)")
    print(f"  Format    : CSV (direct download, no API key)")
    print(f"  Rate limit: {RATE_LIMIT_DELAY}s delay, {int(60/RATE_LIMIT_DELAY)} req/min")
    print(f"  Output    : {OUTPUT_FILE}")
    print(f"  Columns   : {', '.join(OUTPUT_COLUMNS)}")
    print()
    print("  Note: The FCC publishes county-level Form 477 data semi-annually.")
    print("  CSV column names vary by vintage — this script maps known columns.")
    print("  First run MUST use --dry-run to verify column names.")


def _download_csv(url: str) -> str:
    """Download CSV text from URL."""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "policy-data-infrastructure/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            if resp.status != 200:
                raise RuntimeError(
                    f"FCC server returned HTTP {resp.status} for {url}"
                )
            return resp.read().decode("utf-8-sig", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(
            f"FCC HTTP {exc.code}: {exc.reason}\nBody: {body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"FCC network error: {exc.reason}") from exc


def _safe_float(val: str | None) -> float | None:
    """Convert a string to float, returning None for non-parseable values."""
    if val is None:
        return None
    try:
        v = float(str(val).strip())
        return v
    except (ValueError, TypeError):
        return None


def _map_columns(header: Sequence[str]) -> dict[str, str | None]:
    """
    Map FCC CSV column names to canonical keys.

    FCC column names vary across vintages. Known patterns:
      - "FIPS Code" or "fips" → state+county FIPS
      - "State FIPS" / "state_fips"
      - "County FIPS" / "county_fips"
      - "Fixed Residential Broadband Connections per 1000 Households"
      - "Number of Providers" or "Providers"
      - Various demographic columns

    Returns a dict of canonical_key → csv_column_name (or None if not found).
    """
    header_lower = {h.lower().strip(): h for h in header}

    mapping: dict[str, str | None] = {}

    # GEOID: prefer combined FIPS column, fall back to state+county
    for candidate in [
        "fips code", "fips", "geoid", "fips_code",
        "county fips code", "county_fips",
    ]:
        if candidate in header_lower:
            mapping["geoid"] = header_lower[candidate]
            break

    # If no combined FIPS column, look for separate State/County FIPS
    if "geoid" not in mapping:
        state_col = header_lower.get("state fips") or header_lower.get("state_fips")
        county_col = header_lower.get("county fips") or header_lower.get("county_fips")
        if state_col and county_col:
            mapping["state_fips_col"] = state_col
            mapping["county_fips_col"] = county_col
            mapping["geoid"] = "_composite"  # sentinel

    # County name
    for candidate in ["county name", "county", "county_name", "name"]:
        if candidate in header_lower:
            mapping["county_name"] = header_lower[candidate]
            break

    # Broadband access per 1000 households
    for candidate in [
        "fixed residential broadband connections per 1000 households",
        "broadband connections per 1000 hh",
        "broadband_per_1000_hh",
        "connections per 1000",
        "broadband_access_per_1000",
    ]:
        if candidate in header_lower:
            mapping["broadband_access_pct"] = header_lower[candidate]
            break

    # Multiple providers / competition
    for candidate in [
        "percent with more than one provider",
        "multiple providers pct",
        "providers_gt_1_pct",
        "multiple_providers_pct",
        "competition_pct",
        "number of providers",  # raw count, not pct — note in audit
    ]:
        if candidate in header_lower:
            mapping["multiple_providers_pct"] = header_lower[candidate]
            break

    return mapping


def fetch_data(args: argparse.Namespace) -> list[dict]:
    year = args.year

    print(f"Fetching FCC Broadband Form 477 data for {year}...")
    print(f"  Downloading CSV from {FCC_COUNTY_CSV_URL}")

    csv_text = _download_csv(FCC_COUNTY_CSV_URL)
    time.sleep(RATE_LIMIT_DELAY)

    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        raise RuntimeError("FCC CSV has no header row")

    print(f"  CSV columns ({len(reader.fieldnames)}): {', '.join(reader.fieldnames[:10])}...")

    col_map = _map_columns(reader.fieldnames)
    print(f"  Column mapping: {col_map}")

    if col_map.get("geoid") is None:
        # Print actual column names to help debug
        print("ERROR: Could not identify a GEOID/FIPS column in CSV header.", file=sys.stderr)
        print(f"  Available columns: {reader.fieldnames}", file=sys.stderr)
        raise RuntimeError(
            "FCC CSV header does not contain a recognisable FIPS/GEOID column. "
            "Check the file format at the FCC website and update _map_columns()."
        )

    records: list[dict] = []
    for row in reader:
        # Build GEOID
        if col_map["geoid"] == "_composite":
            sf = row.get(col_map.get("state_fips_col", ""), "")
            cf = row.get(col_map.get("county_fips_col", ""), "")
            geoid = sf.zfill(2) + cf.zfill(3)
        else:
            geoid_col = col_map.get("geoid", "")
            geoid = (row.get(geoid_col, "") or "").strip().zfill(5)

        if not geoid or len(geoid) < 5:
            continue

        county_name = row.get(col_map.get("county_name", ""), "") or ""

        bb_col = col_map.get("broadband_access_pct")
        broadband_raw = row.get(bb_col, None) if bb_col else None

        mp_col = col_map.get("multiple_providers_pct")
        multi_raw = row.get(mp_col, None) if mp_col else None

        records.append({
            "geoid": geoid[:5],  # ensure 5-digit county GEOID
            "county_name": (county_name or "").strip(),
            "state_fips": geoid[:2],
            "year": year,
            "broadband_access_pct": _fmt(_safe_float(broadband_raw)),
            "multiple_providers_pct": _fmt(_safe_float(multi_raw)),
        })

    pop = sum(1 for r in records if r["broadband_access_pct"] is not None)
    print(f"\n  {len(records)} county records ({pop} with broadband_access_pct data)")
    return records


def _fmt(val: float | None) -> str | None:
    if val is None:
        return None
    return str(val)


def write_csv(records: list[dict]) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(records)
    print(f"  Wrote {len(records):,} rows to {OUTPUT_FILE}")


def null_audit(records: list[dict]) -> None:
    if not records:
        return
    print("\nNull audit:")
    fail = False
    for col in ["broadband_access_pct", "multiple_providers_pct"]:
        null_count = sum(1 for r in records if r.get(col) is None)
        pct = null_count / len(records) * 100
        mark = ""
        if col == "broadband_access_pct" and pct > 30:
            mark = "  ← FAIL (>30% null for primary indicator)"
            fail = True
        elif col == "broadband_access_pct" and pct == 0:
            mark = "  ← SUSPICIOUS (0% null — check suppression handling)"
        print(f"  {col:<35} {null_count:>5} null  ({pct:.1f}%){mark}")

    # Also check geoid
    null_geoid = sum(1 for r in records if not r.get("geoid"))
    if null_geoid:
        print(f"  {'geoid':<35} {null_geoid:>5} null  ({null_geoid / len(records) * 100:.1f}%)")

    if fail:
        print("\n  ABORT: primary indicator exceeds 30% null threshold.")
        sys.exit(1)


def load_to_db(records: list[dict]) -> None:
    """Load records to PostGIS via db.py."""
    try:
        sys.path.insert(0, SCRIPT_DIR)
        from lib.db import get_conn, bulk_load_indicators
    except ImportError as exc:
        print(f"  ERROR: Cannot import lib.db — {exc}", file=sys.stderr)
        sys.exit(1)

    print("Connecting to database...")
    conn = get_conn()

    year_val = records[0]["year"] if records else 0
    numeric_cols = ["broadband_access_pct", "multiple_providers_pct"]
    indicators = []

    for rec in records:
        geoid = rec.get("geoid")
        if not geoid:
            continue
        for col in numeric_cols:
            raw = rec.get(col)
            try:
                val = float(raw) if raw is not None else None
            except (ValueError, TypeError):
                val = None
            indicators.append({
                "geoid":           geoid,
                "variable_id":     f"fcc_{col}",
                "vintage":         int(year_val),
                "value":           val,
                "margin_of_error": None,
                "raw_value":       str(raw or ""),
            })

    n = bulk_load_indicators(conn, indicators)
    conn.close()
    print(f"  {n} indicator rows written to database")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch FCC Broadband (Form 477) county-level data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Year to fetch (default: 2024)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print fetch plan without downloading",
    )
    parser.add_argument(
        "--load",
        action="store_true",
        help="Load records to PostGIS after CSV write (requires PDI_DATABASE_URL)",
    )
    args = parser.parse_args()

    if args.dry_run:
        print_plan(args)
        return

    records = fetch_data(args)
    write_csv(records)
    null_audit(records)

    if args.load:
        load_to_db(records)

    print("\nDone.")


if __name__ == "__main__":
    main()
