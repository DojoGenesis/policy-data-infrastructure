#!/usr/bin/env python3
"""Fetch ACS 5-Year county-level data for all Wisconsin counties.

Outputs CSV at analysis/output/wi_counties_acs.csv.
Does not require PostGIS. Uses stdlib only + ../ingest/lib/census.py.

Usage:
    python analysis/fetch_wi_counties.py              # fetch and write CSV
    python analysis/fetch_wi_counties.py --dry-run    # print plan, no API calls
    python analysis/fetch_wi_counties.py --year 2022  # use a different ACS vintage
"""
import sys
import os
import csv
import argparse

# Allow running as a script from the repo root or directly from analysis/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ingest'))

from lib.census import fetch_acs_table, safe_int, safe_float, safe_pct, build_geoid

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WI_STATE_FIPS = "55"
DEFAULT_YEAR = 2023

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "wi_counties_acs.csv")

# ACS variable groups (mirrored from ingest/fetch_acs.py)
INCOME_VARS = ["B19013_001E"]

RACE_VARS = [
    "B03002_001E",  # Total population
    "B03002_003E",  # Non-Hispanic white alone
    "B03002_004E",  # Non-Hispanic Black/AA alone
    "B03002_012E",  # Hispanic or Latino (any race)
]

COST_BURDEN_VARS = [
    "B25106_001E",  # Total occupied housing units
    "B25106_006E",  # Owner: cost burdened (30%+)
    "B25106_010E",  # Owner: severely cost burdened (50%+)
    "B25106_024E",  # Renter: cost burdened
    "B25106_028E",  # Renter: severely cost burdened
]

POP_VARS = ["B01001_001E"]

EDUC_VARS = [
    "B15003_001E",  # Total population 25+
    "B15003_022E",  # Bachelor's degree
    "B15003_023E",  # Master's degree
    "B15003_024E",  # Professional degree
    "B15003_025E",  # Doctorate
]

# Combined detail variables — fetched in a single API call
DETAIL_VARS = INCOME_VARS + RACE_VARS + COST_BURDEN_VARS + POP_VARS + EDUC_VARS

# Subject table variables — each requires a separate API call (different dataset path)
POVERTY_VARS = ["S1701_C03_001E"]    # Percent below poverty level
INSURANCE_VARS = ["S2701_C05_001E"]  # Percent uninsured

OUTPUT_COLUMNS = [
    "geoid",
    "county_name",
    "total_population",
    "median_hh_income",
    "poverty_rate",
    "uninsured_rate",
    "pct_poc",
    "pct_non_hispanic_black",
    "pct_hispanic",
    "pct_cost_burdened",
    "pct_severely_cost_burdened",
    "pct_bachelors_or_higher",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_add(a, b):
    """Add two nullable ints; returns None only if both are None."""
    if a is None and b is None:
        return None
    return (a or 0) + (b or 0)


def _extract_county_name(raw_name: str) -> str:
    """
    Census NAME for a county looks like 'Dane County, Wisconsin'.
    Strip the state suffix and return just 'Dane County'.
    """
    if "," in raw_name:
        return raw_name.split(",")[0].strip()
    return raw_name.strip()


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

def merge_rows(
    detail_rows: list[dict],
    poverty_rows: list[dict],
    insurance_rows: list[dict],
) -> list[dict]:
    """
    Merge detail + subject table rows by GEOID into one flat dict per county.
    Returns a list of output dicts ready for CSV writing.
    """
    # Index subject rows by geoid for O(1) lookup
    poverty_by_geoid = {r["geoid"]: r for r in poverty_rows}
    insurance_by_geoid = {r["geoid"]: r for r in insurance_rows}

    output_rows: list[dict] = []

    for row in detail_rows:
        geoid = row["geoid"]
        county_name = _extract_county_name(row.get("NAME", ""))

        # --- Population ---
        total_pop = safe_int(row.get("B01001_001E"))

        # --- Income ---
        median_income = safe_float(row.get("B19013_001E"))

        # --- Race / ethnicity ---
        race_total = safe_int(row.get("B03002_001E"))
        nhw = safe_int(row.get("B03002_003E"))
        nhb = safe_int(row.get("B03002_004E"))
        hispanic = safe_int(row.get("B03002_012E"))

        pct_poc = None
        if race_total is not None and nhw is not None:
            poc_count = race_total - nhw
            pct_poc = safe_pct(poc_count, race_total)

        pct_nhb = safe_pct(nhb, race_total)
        pct_hisp = safe_pct(hispanic, race_total)

        # --- Housing cost burden ---
        total_units = safe_int(row.get("B25106_001E"))
        owner_burdened = safe_int(row.get("B25106_006E"))
        owner_severe = safe_int(row.get("B25106_010E"))
        renter_burdened = safe_int(row.get("B25106_024E"))
        renter_severe = safe_int(row.get("B25106_028E"))

        total_burdened = _safe_add(owner_burdened, renter_burdened)
        total_severe = _safe_add(owner_severe, renter_severe)

        pct_burdened = safe_pct(total_burdened, total_units)
        pct_severe = safe_pct(total_severe, total_units)

        # --- Educational attainment ---
        educ_total = safe_int(row.get("B15003_001E"))
        bachelors = safe_int(row.get("B15003_022E"))
        masters = safe_int(row.get("B15003_023E"))
        prof = safe_int(row.get("B15003_024E"))
        doctorate = safe_int(row.get("B15003_025E"))
        ba_plus = _safe_add(_safe_add(_safe_add(bachelors, masters), prof), doctorate)
        pct_ba = safe_pct(ba_plus, educ_total)

        # --- Subject table values (poverty, insurance) ---
        pov_row = poverty_by_geoid.get(geoid, {})
        poverty_rate = safe_float(pov_row.get("S1701_C03_001E"))

        ins_row = insurance_by_geoid.get(geoid, {})
        uninsured_rate = safe_float(ins_row.get("S2701_C05_001E"))

        output_rows.append({
            "geoid": geoid,
            "county_name": county_name,
            "total_population": total_pop,
            "median_hh_income": median_income,
            "poverty_rate": poverty_rate,
            "uninsured_rate": uninsured_rate,
            "pct_poc": pct_poc,
            "pct_non_hispanic_black": pct_nhb,
            "pct_hispanic": pct_hisp,
            "pct_cost_burdened": pct_burdened,
            "pct_severely_cost_burdened": pct_severe,
            "pct_bachelors_or_higher": pct_ba,
        })

    return output_rows


def write_csv(rows: list[dict], path: str) -> None:
    """Write output rows to CSV. Creates parent directory if needed."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {path}")


# ---------------------------------------------------------------------------
# Dry-run plan printer
# ---------------------------------------------------------------------------

def print_dry_run_plan(year: int) -> None:
    """Print what would be fetched without calling the Census API."""
    print(f"=== DRY RUN: fetch_wi_counties.py ===")
    print(f"ACS vintage:  {year} (5-Year estimates, {year-4}–{year})")
    print(f"State FIPS:   {WI_STATE_FIPS} (Wisconsin)")
    print(f"Geography:    county — all 72 WI counties")
    print(f"Output file:  {OUTPUT_CSV}")
    print()
    print("API calls planned (3 total, 1.5 s delay each):")
    print()
    print(f"  [1] Detail table (acs5)")
    print(f"      Variables ({len(DETAIL_VARS)}): {', '.join(DETAIL_VARS)}")
    print(f"      → income, race/ethnicity, housing cost burden, population, education")
    print()
    print(f"  [2] Subject table S1701 (acs5/subject)")
    print(f"      Variables: {', '.join(POVERTY_VARS)}")
    print(f"      → percent below poverty level")
    print()
    print(f"  [3] Subject table S2701 (acs5/subject)")
    print(f"      Variables: {', '.join(INSURANCE_VARS)}")
    print(f"      → percent uninsured")
    print()
    print("Output columns:")
    for col in OUTPUT_COLUMNS:
        print(f"  {col}")
    print()
    print("Minimum estimated time: ~4.5 seconds (3 × 1.5 s rate-limit delay)")
    print("[dry-run] No API calls made.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch ACS 2023 5-Year county data for all 72 Wisconsin counties.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--year",
        type=int,
        default=DEFAULT_YEAR,
        help=f"ACS end year (default: {DEFAULT_YEAR} → {DEFAULT_YEAR-4}–{DEFAULT_YEAR} estimates)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print fetch plan without calling the Census API or writing files.",
    )
    args = parser.parse_args()

    if args.dry_run:
        print_dry_run_plan(args.year)
        return

    year = args.year
    print(f"Fetching ACS {year} 5-Year — all Wisconsin counties (state FIPS {WI_STATE_FIPS})")

    # --- Detail variables ---
    print(f"\n[1/3] Fetching detail variables ({len(DETAIL_VARS)} vars, acs5)...")
    detail_rows = fetch_acs_table(
        year=year,
        variables=DETAIL_VARS,
        state_fips=WI_STATE_FIPS,
        county_fips=None,
        geo_level="county",
        subject=False,
    )
    print(f"      Received {len(detail_rows)} county rows")

    # --- Poverty rate (S1701) ---
    print(f"\n[2/3] Fetching S1701 poverty rate (acs5/subject)...")
    poverty_rows: list[dict] = []
    try:
        poverty_rows = fetch_acs_table(
            year=year,
            variables=POVERTY_VARS,
            state_fips=WI_STATE_FIPS,
            county_fips=None,
            geo_level="county",
            subject=True,
        )
        print(f"      Received {len(poverty_rows)} rows")
    except RuntimeError as exc:
        print(f"      Warning: S1701 fetch failed: {exc}", file=sys.stderr)

    # --- Uninsured rate (S2701) ---
    print(f"\n[3/3] Fetching S2701 uninsured rate (acs5/subject)...")
    insurance_rows: list[dict] = []
    try:
        insurance_rows = fetch_acs_table(
            year=year,
            variables=INSURANCE_VARS,
            state_fips=WI_STATE_FIPS,
            county_fips=None,
            geo_level="county",
            subject=True,
        )
        print(f"      Received {len(insurance_rows)} rows")
    except RuntimeError as exc:
        print(f"      Warning: S2701 fetch failed: {exc}", file=sys.stderr)

    # --- Merge and compute indicators ---
    print(f"\nMerging and computing derived indicators...")
    output_rows = merge_rows(detail_rows, poverty_rows, insurance_rows)
    print(f"Produced {len(output_rows)} county records")

    # Quick null audit
    null_counts: dict[str, int] = {col: 0 for col in OUTPUT_COLUMNS}
    for row in output_rows:
        for col in OUTPUT_COLUMNS:
            if row.get(col) is None:
                null_counts[col] += 1

    print("\nNull value audit:")
    for col, n in null_counts.items():
        if n > 0:
            print(f"  {col:<40} {n:>3} null")
        else:
            print(f"  {col:<40}   0 null")

    # --- Write CSV ---
    print()
    write_csv(output_rows, OUTPUT_CSV)
    print("Done.")


if __name__ == "__main__":
    main()
