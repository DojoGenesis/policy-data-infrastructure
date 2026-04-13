#!/usr/bin/env python3
"""
Fetch ACS 5-Year data for any state/county and load to PostgreSQL.

Usage examples:
  # Dane County, WI — tract level, dry run
  python fetch_acs.py --state 55 --county 025 --year 2023 --dry-run

  # All Wisconsin counties
  python fetch_acs.py --state 55 --year 2023 --geo-level county

  # Block groups in Cook County, IL
  python fetch_acs.py --state 17 --county 031 --year 2023 --geo-level block_group
"""
import argparse
import sys
from datetime import datetime

from lib.census import (
    fetch_acs_table,
    safe_float,
    safe_int,
    safe_pct,
    build_geoid,
)
from lib.db import get_conn, bulk_load_indicators, upsert_indicator_meta

# ---------------------------------------------------------------------------
# ACS variable definitions
# Sourced from data/sources.toml and atlas/data-context.md
# ---------------------------------------------------------------------------

# B19013 — Median household income
INCOME_VARS = ["B19013_001E", "B19013_001M"]

# B03002 — Race and Hispanic/Latino origin
RACE_VARS = [
    "B03002_001E",  # Total population
    "B03002_003E",  # Non-Hispanic white alone
    "B03002_004E",  # Non-Hispanic Black/AA alone
    "B03002_012E",  # Hispanic or Latino (any race)
]

# S1701 — Poverty status (subject table)
POVERTY_VARS = ["S1701_C03_001E"]  # Percent below poverty level

# S2701 — Health insurance coverage (subject table)
INSURANCE_VARS = ["S2701_C05_001E"]  # Uninsured rate (percent)

# B25106 — Housing cost burden by tenure
COST_BURDEN_VARS = [
    "B25106_001E",  # Total occupied housing units
    "B25106_006E",  # Owner: cost burdened (30%+ of income)
    "B25106_010E",  # Owner: severely cost burdened (50%+)
    "B25106_024E",  # Renter: cost burdened
    "B25106_028E",  # Renter: severely cost burdened
]

# B01001 — Sex by age (total population)
POP_VARS = ["B01001_001E"]

# B15003 — Educational attainment (25+)
EDUC_VARS = [
    "B15003_001E",  # Total population 25+
    "B15003_022E",  # Bachelor's degree
    "B15003_023E",  # Master's degree
    "B15003_024E",  # Professional degree
    "B15003_025E",  # Doctorate
]

# Non-subject variables (acs5 dataset)
DETAIL_VARS = INCOME_VARS + RACE_VARS + COST_BURDEN_VARS + POP_VARS + EDUC_VARS

# Subject table variables (acs5/subject dataset — separate API call)
SUBJECT_VARS_POVERTY   = POVERTY_VARS
SUBJECT_VARS_INSURANCE = INSURANCE_VARS

# ---------------------------------------------------------------------------
# Indicator metadata registry
# ---------------------------------------------------------------------------

VARIABLE_META: dict[str, dict] = {
    "median_hh_income": {
        "label":     "Median Household Income",
        "source":    "acs-5yr",
        "table":     "B19013_001E",
        "unit":      "dollars",
        "direction": "higher_better",
    },
    "pct_poc": {
        "label":     "Percent People of Color",
        "source":    "acs-5yr",
        "table":     "derived:B03002",
        "unit":      "percent",
        "direction": "neutral",
    },
    "pct_non_hispanic_white": {
        "label":     "Percent Non-Hispanic White",
        "source":    "acs-5yr",
        "table":     "derived:B03002",
        "unit":      "percent",
        "direction": "neutral",
    },
    "pct_non_hispanic_black": {
        "label":     "Percent Non-Hispanic Black or African American",
        "source":    "acs-5yr",
        "table":     "derived:B03002",
        "unit":      "percent",
        "direction": "neutral",
    },
    "pct_hispanic": {
        "label":     "Percent Hispanic or Latino",
        "source":    "acs-5yr",
        "table":     "derived:B03002",
        "unit":      "percent",
        "direction": "neutral",
    },
    "poverty_rate": {
        "label":     "Poverty Rate",
        "source":    "acs-5yr",
        "table":     "S1701_C03_001E",
        "unit":      "percent",
        "direction": "lower_better",
    },
    "uninsured_rate": {
        "label":     "Uninsured Rate",
        "source":    "acs-5yr",
        "table":     "S2701_C05_001E",
        "unit":      "percent",
        "direction": "lower_better",
    },
    "pct_cost_burdened": {
        "label":     "Percent Cost-Burdened Households",
        "source":    "acs-5yr",
        "table":     "derived:B25106",
        "unit":      "percent",
        "direction": "lower_better",
    },
    "pct_severely_cost_burdened": {
        "label":     "Percent Severely Cost-Burdened Households (50%+ income on housing)",
        "source":    "acs-5yr",
        "table":     "derived:B25106",
        "unit":      "percent",
        "direction": "lower_better",
    },
    "total_population": {
        "label":     "Total Population",
        "source":    "acs-5yr",
        "table":     "B01001_001E",
        "unit":      "count",
        "direction": "neutral",
    },
    "pct_bachelors_or_higher": {
        "label":     "Percent with Bachelor's Degree or Higher (age 25+)",
        "source":    "acs-5yr",
        "table":     "derived:B15003",
        "unit":      "percent",
        "direction": "higher_better",
    },
}


# ---------------------------------------------------------------------------
# Record processing
# ---------------------------------------------------------------------------

def process_detail_records(rows: list[dict], vintage: int) -> list[dict]:
    """
    Transform raw Census API rows (detail tables) into normalized indicator records.

    One input row → multiple indicator output rows (one per computed indicator).
    """
    indicators: list[dict] = []
    fetched_at = datetime.utcnow().isoformat() + "Z"

    for row in rows:
        geoid = row["geoid"]

        # --- Median household income ---
        income_raw = row.get("B19013_001E")
        moe_raw    = row.get("B19013_001M")
        income     = safe_float(income_raw)
        income_moe = safe_float(moe_raw)
        indicators.append({
            "geoid":          geoid,
            "variable_id":    "median_hh_income",
            "vintage":        vintage,
            "value":          income,
            "margin_of_error": income_moe,
            "raw_value":      str(income_raw or ""),
            "fetched_at":     fetched_at,
        })

        # --- Race / ethnicity ---
        total_pop = safe_int(row.get("B03002_001E"))
        nhw       = safe_int(row.get("B03002_003E"))   # Non-Hispanic white
        nhb       = safe_int(row.get("B03002_004E"))   # Non-Hispanic Black
        hispanic  = safe_int(row.get("B03002_012E"))   # Hispanic or Latino

        poc = None
        if total_pop is not None and nhw is not None:
            poc_count = total_pop - nhw
            poc = safe_pct(poc_count, total_pop)

        indicators.append({
            "geoid":          geoid,
            "variable_id":    "pct_poc",
            "vintage":        vintage,
            "value":          poc,
            "margin_of_error": None,
            "raw_value":      f"{total_pop or ''}|{nhw or ''}",
            "fetched_at":     fetched_at,
        })
        indicators.append({
            "geoid":          geoid,
            "variable_id":    "pct_non_hispanic_white",
            "vintage":        vintage,
            "value":          safe_pct(nhw, total_pop),
            "margin_of_error": None,
            "raw_value":      str(nhw or ""),
            "fetched_at":     fetched_at,
        })
        indicators.append({
            "geoid":          geoid,
            "variable_id":    "pct_non_hispanic_black",
            "vintage":        vintage,
            "value":          safe_pct(nhb, total_pop),
            "margin_of_error": None,
            "raw_value":      str(nhb or ""),
            "fetched_at":     fetched_at,
        })
        indicators.append({
            "geoid":          geoid,
            "variable_id":    "pct_hispanic",
            "vintage":        vintage,
            "value":          safe_pct(hispanic, total_pop),
            "margin_of_error": None,
            "raw_value":      str(hispanic or ""),
            "fetched_at":     fetched_at,
        })

        # --- Total population ---
        indicators.append({
            "geoid":          geoid,
            "variable_id":    "total_population",
            "vintage":        vintage,
            "value":          float(total_pop) if total_pop is not None else None,
            "margin_of_error": None,
            "raw_value":      str(row.get("B01001_001E", "")),
            "fetched_at":     fetched_at,
        })

        # --- Housing cost burden ---
        owner_burdened   = safe_int(row.get("B25106_006E"))
        owner_severe     = safe_int(row.get("B25106_010E"))
        renter_burdened  = safe_int(row.get("B25106_024E"))
        renter_severe    = safe_int(row.get("B25106_028E"))
        total_units      = safe_int(row.get("B25106_001E"))

        total_burdened = _safe_add(owner_burdened, renter_burdened)
        total_severe   = _safe_add(owner_severe, renter_severe)

        indicators.append({
            "geoid":          geoid,
            "variable_id":    "pct_cost_burdened",
            "vintage":        vintage,
            "value":          safe_pct(total_burdened, total_units),
            "margin_of_error": None,
            "raw_value":      f"{owner_burdened or ''}|{renter_burdened or ''}|{total_units or ''}",
            "fetched_at":     fetched_at,
        })
        indicators.append({
            "geoid":          geoid,
            "variable_id":    "pct_severely_cost_burdened",
            "vintage":        vintage,
            "value":          safe_pct(total_severe, total_units),
            "margin_of_error": None,
            "raw_value":      f"{owner_severe or ''}|{renter_severe or ''}|{total_units or ''}",
            "fetched_at":     fetched_at,
        })

        # --- Educational attainment ---
        educ_total = safe_int(row.get("B15003_001E"))
        bachelors  = safe_int(row.get("B15003_022E"))
        masters    = safe_int(row.get("B15003_023E"))
        prof       = safe_int(row.get("B15003_024E"))
        doctorate  = safe_int(row.get("B15003_025E"))
        ba_plus = _safe_add(_safe_add(_safe_add(bachelors, masters), prof), doctorate)
        indicators.append({
            "geoid":          geoid,
            "variable_id":    "pct_bachelors_or_higher",
            "vintage":        vintage,
            "value":          safe_pct(ba_plus, educ_total),
            "margin_of_error": None,
            "raw_value":      f"{ba_plus or ''}|{educ_total or ''}",
            "fetched_at":     fetched_at,
        })

    return indicators


def process_subject_records(rows: list[dict], variable_id: str, vintage: int) -> list[dict]:
    """
    Transform a single subject-table variable into indicator records.
    Subject tables (S-prefix) return percent values directly.
    """
    indicators: list[dict] = []
    fetched_at = datetime.utcnow().isoformat() + "Z"

    # Map variable_id to the ACS variable name
    var_map = {
        "poverty_rate":   "S1701_C03_001E",
        "uninsured_rate": "S2701_C05_001E",
    }
    acs_var = var_map.get(variable_id)
    if not acs_var:
        raise ValueError(f"Unknown subject variable_id: {variable_id!r}. "
                         f"Expected one of: {list(var_map)}")

    for row in rows:
        raw = row.get(acs_var)
        indicators.append({
            "geoid":          row["geoid"],
            "variable_id":    variable_id,
            "vintage":        vintage,
            "value":          safe_float(raw),
            "margin_of_error": None,
            "raw_value":      str(raw or ""),
            "fetched_at":     fetched_at,
        })

    return indicators


def _safe_add(a, b):
    """Add two values, returning None if both are None."""
    if a is None and b is None:
        return None
    return (a or 0) + (b or 0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch ACS 5-Year data for any state/county and load to PostgreSQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--state",     required=True, help="2-digit state FIPS (e.g., 55 for Wisconsin)")
    parser.add_argument("--county",    default=None,  help="3-digit county FIPS (e.g., 025 for Dane). Required for tract/block_group.")
    parser.add_argument("--year",      type=int, default=2023, help="ACS end year (default: 2023 → 2019–2023 estimates)")
    parser.add_argument("--geo-level", default="tract",
                        choices=["tract", "block_group", "county", "state"],
                        help="Geographic level to fetch (default: tract)")
    parser.add_argument("--dry-run",   action="store_true", help="Fetch and process but do not write to the database")
    args = parser.parse_args()

    state  = args.state.zfill(2)
    county = args.county.zfill(3) if args.county else None
    year   = args.year
    level  = args.geo_level

    print(f"Fetching ACS {year} 5-Year — state={state} county={county or 'all'} level={level}")

    # --- Fetch detail tables ---
    print(f"  Fetching detail variables ({len(DETAIL_VARS)} vars)...")
    detail_rows = fetch_acs_table(year, DETAIL_VARS, state, county, level, subject=False)
    print(f"  Received {len(detail_rows)} {level} rows")

    # --- Fetch subject tables (poverty, insurance) ---
    # Subject tables use a different dataset path; fetch each separately.
    print("  Fetching S1701 poverty rate (subject table)...")
    poverty_rows = []
    try:
        poverty_rows = fetch_acs_table(year, SUBJECT_VARS_POVERTY, state, county, level, subject=True)
        print(f"  Received {len(poverty_rows)} rows")
    except RuntimeError as exc:
        print(f"  Warning: S1701 fetch failed: {exc}", file=sys.stderr)

    print("  Fetching S2701 uninsured rate (subject table)...")
    insurance_rows = []
    try:
        insurance_rows = fetch_acs_table(year, SUBJECT_VARS_INSURANCE, state, county, level, subject=True)
        print(f"  Received {len(insurance_rows)} rows")
    except RuntimeError as exc:
        print(f"  Warning: S2701 fetch failed: {exc}", file=sys.stderr)

    # --- Process into indicator records ---
    all_indicators: list[dict] = []
    all_indicators.extend(process_detail_records(detail_rows, year))
    if poverty_rows:
        all_indicators.extend(process_subject_records(poverty_rows, "poverty_rate", year))
    if insurance_rows:
        all_indicators.extend(process_subject_records(insurance_rows, "uninsured_rate", year))

    print(f"\nProcessed {len(all_indicators)} indicator records from {len(detail_rows)} geographies")

    # --- Print summary ---
    from collections import Counter
    var_counts = Counter(ind["variable_id"] for ind in all_indicators)
    none_counts = Counter(
        ind["variable_id"] for ind in all_indicators if ind.get("value") is None
    )
    print("\nIndicator summary:")
    for var_id, count in sorted(var_counts.items()):
        null_n = none_counts.get(var_id, 0)
        print(f"  {var_id:<40} {count:>5} records  ({null_n} null)")

    if args.dry_run:
        print("\n[dry-run] Skipping database write.")
        return

    # --- Load to PostgreSQL ---
    print("\nConnecting to database...")
    conn = get_conn()

    print("Upserting indicator metadata...")
    n_meta = upsert_indicator_meta(conn, VARIABLE_META)  # type: ignore[arg-type]
    print(f"  {n_meta} indicator_meta rows written")

    print("Bulk loading indicators via COPY...")
    n_ind = bulk_load_indicators(conn, all_indicators)
    print(f"  {n_ind} indicator rows written")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
