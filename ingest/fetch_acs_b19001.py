#!/usr/bin/env python3
"""
Fetch ACS B19001 (household income by race) for true ICE computation.

Computes the Index of Concentration at the Extremes (Krieger et al., 2016)
using cross-tabulated income-by-race data instead of the poverty×race
approximation used by the base pipeline.

ICE = (A_privileged - A_deprived) / T

Where:
  A_privileged = White non-Hispanic households with income >= $100K
  A_deprived   = Non-White-non-Hispanic households with income < $25K
  T            = Total households

Usage:
  python fetch_acs_b19001.py --dry-run
  python fetch_acs_b19001.py --state 55 --year 2023
  python fetch_acs_b19001.py --state 55 --year 2023 --load
"""
import argparse
import csv
import os
import sys
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from lib.census import fetch_acs_table, safe_int

OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "analysis", "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "wi_ice_b19001.csv")

# B19001: Household Income — all households
# B19001H: Household Income — White alone, Not Hispanic or Latino
# We fetch both in one API call per county (14 vars < 50 limit).

# Income < $25K brackets (deprived denominator)
LOW_INCOME_SUFFIXES = ["_002E", "_003E", "_004E", "_005E"]

# Income >= $100K brackets (privileged numerator)
HIGH_INCOME_SUFFIXES = ["_014E", "_015E", "_016E", "_017E"]

B19001_TOTAL = "B19001_001E"
B19001H_TOTAL = "B19001H_001E"

B19001_LOW = [f"B19001{s}" for s in LOW_INCOME_SUFFIXES]
B19001H_LOW = [f"B19001H{s}" for s in LOW_INCOME_SUFFIXES]
B19001H_HIGH = [f"B19001H{s}" for s in HIGH_INCOME_SUFFIXES]

ALL_VARS = [B19001_TOTAL] + B19001_LOW + [B19001H_TOTAL] + B19001H_LOW + B19001H_HIGH

OUTPUT_COLUMNS = [
    "geoid", "name", "year",
    "total_households", "total_low_income", "wnh_low_income",
    "wnh_high_income", "ice_privileged", "ice_deprived",
    "ice_score",
]

# All 72 Wisconsin county FIPS codes
WI_COUNTY_FIPS = [
    "001", "003", "005", "007", "009", "011", "013", "015", "017", "019",
    "021", "023", "025", "027", "029", "031", "033", "035", "037", "039",
    "041", "043", "045", "047", "049", "051", "053", "055", "057", "059",
    "061", "063", "065", "067", "069", "071", "073", "075", "077", "078",
    "079", "081", "083", "085", "087", "089", "091", "093", "095", "097",
    "099", "101", "103", "105", "107", "109", "111", "113", "115", "117",
    "119", "121", "123", "125", "127", "129", "131", "133", "135", "137",
    "139", "141",
]


def print_plan(args: argparse.Namespace) -> None:
    print("[dry-run] ACS B19001 ICE fetch plan")
    print(f"  Table     : B19001 (all) + B19001H (White non-Hispanic)")
    print(f"  Variables : {len(ALL_VARS)} ({', '.join(ALL_VARS[:4])}...)")
    print(f"  Year      : {args.year}")
    print(f"  State     : {args.state}")
    print(f"  Counties  : {len(WI_COUNTY_FIPS)} (all WI)")
    print(f"  Geo level : tract")
    print(f"  API calls : {len(WI_COUNTY_FIPS)} (one per county)")
    print(f"  Output    : {OUTPUT_FILE}")
    print(f"  API key   : {'SET' if os.environ.get('CENSUS_API_KEY') else 'NOT SET'}")
    print()
    print("  ICE formula: (WNH_high_income - nonWNH_low_income) / total_households")
    print("  Privileged : White non-Hispanic HH with income >= $100K")
    print("  Deprived   : Non-White-non-Hispanic HH with income < $25K")
    print(f"  Citation   : Krieger et al., 2016. Am J Public Health 106(2):256-263")


def _sum_vars(row: dict, var_list: list[str]) -> int | None:
    """Sum a list of Census variables, returning None if any is None."""
    total = 0
    for v in var_list:
        val = safe_int(row.get(v))
        if val is None:
            return None
        total += val
    return total


def fetch_data(args: argparse.Namespace) -> list[dict]:
    year = args.year
    state = args.state
    counties = WI_COUNTY_FIPS

    print(f"Fetching ACS B19001 + B19001H for state {state}, year {year}...")
    print(f"  {len(counties)} counties, tract level, {len(ALL_VARS)} variables per call")

    all_records: list[dict] = []

    for i, county_fips in enumerate(counties):
        print(f"  County {i+1}/{len(counties)} (FIPS {state}{county_fips})...",
              end="", flush=True)
        try:
            rows = fetch_acs_table(
                year=year,
                variables=ALL_VARS,
                state_fips=state,
                county_fips=county_fips,
                geo_level="tract",
            )
        except RuntimeError as exc:
            print(f" ERROR: {exc}")
            continue

        county_records = 0
        for row in rows:
            geoid = row["geoid"]
            name = row.get("NAME", geoid)

            total_hh = safe_int(row.get(B19001_TOTAL))
            total_low = _sum_vars(row, B19001_LOW)
            wnh_low = _sum_vars(row, B19001H_LOW)
            wnh_high = _sum_vars(row, B19001H_HIGH)

            # ICE components
            if total_low is not None and wnh_low is not None:
                ice_deprived = total_low - wnh_low  # non-WNH low income
            else:
                ice_deprived = None

            ice_privileged = wnh_high  # WNH high income

            # ICE score
            if (ice_privileged is not None and ice_deprived is not None
                    and total_hh is not None and total_hh > 0):
                ice_score = round((ice_privileged - ice_deprived) / total_hh, 4)
            else:
                ice_score = None

            all_records.append({
                "geoid": geoid,
                "name": name,
                "year": year,
                "total_households": total_hh,
                "total_low_income": total_low,
                "wnh_low_income": wnh_low,
                "wnh_high_income": wnh_high,
                "ice_privileged": ice_privileged,
                "ice_deprived": ice_deprived,
                "ice_score": ice_score,
            })
            county_records += 1

        print(f" {county_records} tracts")

    with_score = sum(1 for r in all_records if r["ice_score"] is not None)
    print(f"\n  {len(all_records)} total tracts ({with_score} with ICE scores)")
    return all_records


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
    for col in OUTPUT_COLUMNS:
        null_count = sum(1 for r in records if r.get(col) is None)
        pct = null_count / len(records) * 100
        flag = " <-- WARNING" if col == "ice_score" and pct > 30 else ""
        print(f"  {col:<25} {null_count:>5} null  ({pct:.1f}%){flag}")


def load_to_db(records: list[dict]) -> None:
    from lib.db import get_conn, bulk_load_indicators
    print("Connecting to database...")
    conn = get_conn()
    fetched_at = datetime.utcnow().isoformat() + "Z"

    indicators: list[dict] = []
    for rec in records:
        geoid = rec["geoid"]
        vintage = rec["year"]
        for var_id in ["ice_privileged", "ice_deprived", "ice_score", "total_households"]:
            raw = rec.get(var_id)
            val = float(raw) if raw is not None else None
            indicators.append({
                "geoid": geoid,
                "variable_id": f"b19001_{var_id}",
                "vintage": int(vintage),
                "value": val,
                "margin_of_error": None,
                "raw_value": str(raw or ""),
            })

    n = bulk_load_indicators(conn, indicators)
    conn.close()
    print(f"  {n} indicator rows written to database")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch ACS B19001 income-by-race data for true ICE computation.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--year", type=int, default=2023,
                        help="ACS 5-year release year (default: 2023)")
    parser.add_argument("--state", type=str, default="55",
                        help="State FIPS code (default: 55 = Wisconsin)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without fetching")
    parser.add_argument("--load", action="store_true",
                        help="Load to PostGIS after CSV write")
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
