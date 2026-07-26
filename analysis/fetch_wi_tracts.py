#!/usr/bin/env python3
"""Fetch ACS 5-Year tract-level indicators for a whole state.

Phase 2 of the statewide Atlas: Phase 1 resolved to counties (72 rows for WI),
this resolves to census tracts (1,542 for WI on the 2020 tract vintage).

Two resolution modes
--------------------
``--mode statewide`` (default)
    One API call per table for the entire state (``for=tract:*&in=state:55``).
    Three calls total. Verified working for WI on ACS 2024.

``--mode county-drill``
    Walks the state's counties and fetches each county's tracts separately —
    the pattern PIP-91 specifies. Three calls per county (216 for WI, ~6 min at
    the polite rate limit). Slower, but it is the mode that still works when a
    statewide call is refused or truncated, and it is the only mode that scales
    to block groups, where the statewide response is far too large. It also
    fails per-county instead of all-or-nothing.

    Prefer statewide; reach for the drill when statewide breaks. ``--mode
    county-drill --compare`` runs both and diffs them, which is how the two
    modes were confirmed to agree.

Outputs ``analysis/output/wi_tracts_acs.{csv,json}``. No database required —
stdlib only, per the analysis/ convention.

Usage:
    python3 analysis/fetch_wi_tracts.py --dry-run
    python3 analysis/fetch_wi_tracts.py
    python3 analysis/fetch_wi_tracts.py --mode county-drill
    python3 analysis/fetch_wi_tracts.py --state 17 --year 2024

Requires CENSUS_API_KEY — the Census API rejects keyless requests.
"""
import argparse
import csv
import json
import os
import sys

# Allow running as a script from the repo root or directly from analysis/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingest"))

from lib.census import (  # noqa: E402
    fetch_acs_table,
    safe_int,
    safe_float,
    safe_pct,
    require_api_key,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_STATE = "55"          # Wisconsin
# ACS 2020-2024 5-Year released 2025-12-11 — the newest published vintage.
DEFAULT_YEAR = 2024

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")

# ACS variable groups — mirrored from ingest/fetch_acs.py so the tract and
# county tables carry identical indicator definitions.
INCOME_VARS = ["B19013_001E", "B19013_001M"]

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

TENURE_VARS = [
    "B25003_001E",  # Total occupied units
    "B25003_003E",  # Renter-occupied
]

DETAIL_VARS = (INCOME_VARS + RACE_VARS + COST_BURDEN_VARS + POP_VARS
               + EDUC_VARS + TENURE_VARS)

POVERTY_VARS = ["S1701_C03_001E"]    # Percent below poverty level
INSURANCE_VARS = ["S2701_C05_001E"]  # Percent uninsured

OUTPUT_COLUMNS = [
    "geoid",
    "tract_name",
    "county_fips",
    "county_name",
    "total_population",
    "median_hh_income",
    "median_hh_income_moe",
    "poverty_rate",
    "uninsured_rate",
    "pct_poc",
    "pct_non_hispanic_black",
    "pct_hispanic",
    "pct_cost_burdened",
    "pct_severely_cost_burdened",
    "pct_renter_occupied",
    "pct_bachelors_or_higher",
]

# Indicators a reader would actually map. Kept separate from OUTPUT_COLUMNS
# because the identity columns are not indicators.
INDICATOR_COLUMNS = [
    "total_population",
    "median_hh_income",
    "poverty_rate",
    "uninsured_rate",
    "pct_poc",
    "pct_non_hispanic_black",
    "pct_hispanic",
    "pct_cost_burdened",
    "pct_severely_cost_burdened",
    "pct_renter_occupied",
    "pct_bachelors_or_higher",
]

# A fresh run with more than this share null for an indicator is a failure —
# the repo's standing null-audit rule.
NULL_FAILURE_THRESHOLD = 0.30


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_add(a, b):
    """Add two nullable ints; returns None only if both are None."""
    if a is None and b is None:
        return None
    return (a or 0) + (b or 0)


def _split_name(raw_name: str) -> tuple[str, str]:
    """Split a Census tract NAME into (tract_name, county_name).

    ACS returns tract names as 'Census Tract 9501; Adams County; Wisconsin'.
    Older vintages used commas. Handle both rather than assuming one.
    """
    sep = ";" if ";" in raw_name else ","
    parts = [p.strip() for p in raw_name.split(sep)]
    tract_name = parts[0] if parts else raw_name.strip()
    county_name = parts[1] if len(parts) > 1 else ""
    return tract_name, county_name


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def fetch_state_counties(year: int, state: str) -> list[tuple[str, str]]:
    """Return [(county_fips, county_name), ...] for a state, sorted by FIPS."""
    rows = fetch_acs_table(
        year=year, variables=["B01001_001E"], state_fips=state,
        county_fips=None, geo_level="county", subject=False,
    )
    out = []
    for r in rows:
        geoid = r["geoid"]
        name = (r.get("NAME") or "").split(",")[0].strip()
        out.append((geoid[2:5], name))
    return sorted(out)


def _fetch_tract_tables(year: int, state: str, county: str | None) -> tuple[list, list, list]:
    """Fetch the three tract tables for one scope (whole state, or one county)."""
    detail = fetch_acs_table(
        year=year, variables=DETAIL_VARS, state_fips=state,
        county_fips=county, geo_level="tract", subject=False,
    )
    poverty: list[dict] = []
    try:
        poverty = fetch_acs_table(
            year=year, variables=POVERTY_VARS, state_fips=state,
            county_fips=county, geo_level="tract", subject=True,
        )
    except RuntimeError as exc:
        print(f"      Warning: S1701 fetch failed: {exc}", file=sys.stderr)

    insurance: list[dict] = []
    try:
        insurance = fetch_acs_table(
            year=year, variables=INSURANCE_VARS, state_fips=state,
            county_fips=county, geo_level="tract", subject=True,
        )
    except RuntimeError as exc:
        print(f"      Warning: S2701 fetch failed: {exc}", file=sys.stderr)

    return detail, poverty, insurance


def fetch_statewide(year: int, state: str) -> tuple[list, list, list]:
    print(f"\n[statewide] Fetching all tracts in state {state} (3 API calls)")
    print(f"  [1/3] detail variables ({len(DETAIL_VARS)} vars, acs5)...")
    detail, poverty, insurance = _fetch_tract_tables(year, state, None)
    print(f"        {len(detail)} tract rows")
    print(f"  [2/3] S1701 poverty: {len(poverty)} rows")
    print(f"  [3/3] S2701 uninsured: {len(insurance)} rows")
    return detail, poverty, insurance


def fetch_county_drill(year: int, state: str) -> tuple[list, list, list]:
    """Walk every county in the state, fetching its tracts one county at a time."""
    counties = fetch_state_counties(year, state)
    print(f"\n[county-drill] {len(counties)} counties in state {state}; "
          f"{len(counties) * 3} API calls")

    detail_all: list[dict] = []
    poverty_all: list[dict] = []
    insurance_all: list[dict] = []
    failed: list[tuple[str, str, str]] = []

    for i, (cfips, cname) in enumerate(counties, 1):
        print(f"  [{i:>2}/{len(counties)}] {cfips} {cname}", end="", flush=True)
        try:
            d, p, ins = _fetch_tract_tables(year, state, cfips)
        except RuntimeError as exc:
            # One county failing must not lose the other 71.
            print(f"  FAILED: {exc}")
            failed.append((cfips, cname, str(exc)))
            continue
        detail_all.extend(d)
        poverty_all.extend(p)
        insurance_all.extend(ins)
        print(f"  {len(d)} tracts")

    if failed:
        print(f"\n  {len(failed)} counties failed and are MISSING from the output:")
        for cfips, cname, exc in failed:
            print(f"    {cfips} {cname}: {exc}")

    return detail_all, poverty_all, insurance_all


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

def merge_rows(detail_rows, poverty_rows, insurance_rows) -> list[dict]:
    """Merge detail + subject rows by GEOID into one flat dict per tract."""
    poverty_by_geoid = {r["geoid"]: r for r in poverty_rows}
    insurance_by_geoid = {r["geoid"]: r for r in insurance_rows}

    output_rows: list[dict] = []

    for row in detail_rows:
        geoid = row["geoid"]
        tract_name, county_name = _split_name(row.get("NAME", ""))

        total_pop = safe_int(row.get("B01001_001E"))
        median_income = safe_float(row.get("B19013_001E"))
        median_income_moe = safe_float(row.get("B19013_001M"))

        race_total = safe_int(row.get("B03002_001E"))
        nhw = safe_int(row.get("B03002_003E"))
        nhb = safe_int(row.get("B03002_004E"))
        hispanic = safe_int(row.get("B03002_012E"))

        pct_poc = None
        if race_total is not None and nhw is not None:
            pct_poc = safe_pct(race_total - nhw, race_total)

        total_units = safe_int(row.get("B25106_001E"))
        total_burdened = _safe_add(safe_int(row.get("B25106_006E")),
                                   safe_int(row.get("B25106_024E")))
        total_severe = _safe_add(safe_int(row.get("B25106_010E")),
                                 safe_int(row.get("B25106_028E")))

        tenure_total = safe_int(row.get("B25003_001E"))
        renter_units = safe_int(row.get("B25003_003E"))

        educ_total = safe_int(row.get("B15003_001E"))
        ba_plus = _safe_add(
            _safe_add(_safe_add(safe_int(row.get("B15003_022E")),
                                safe_int(row.get("B15003_023E"))),
                      safe_int(row.get("B15003_024E"))),
            safe_int(row.get("B15003_025E")),
        )

        pov_row = poverty_by_geoid.get(geoid, {})
        ins_row = insurance_by_geoid.get(geoid, {})

        output_rows.append({
            "geoid": geoid,
            "tract_name": tract_name,
            "county_fips": geoid[2:5],
            "county_name": county_name,
            "total_population": total_pop,
            "median_hh_income": median_income,
            "median_hh_income_moe": median_income_moe,
            "poverty_rate": safe_float(pov_row.get("S1701_C03_001E")),
            "uninsured_rate": safe_float(ins_row.get("S2701_C05_001E")),
            "pct_poc": pct_poc,
            "pct_non_hispanic_black": safe_pct(nhb, race_total),
            "pct_hispanic": safe_pct(hispanic, race_total),
            "pct_cost_burdened": safe_pct(total_burdened, total_units),
            "pct_severely_cost_burdened": safe_pct(total_severe, total_units),
            "pct_renter_occupied": safe_pct(renter_units, tenure_total),
            "pct_bachelors_or_higher": safe_pct(ba_plus, educ_total),
        })

    output_rows.sort(key=lambda r: r["geoid"])
    return output_rows


def null_audit(rows: list[dict]) -> bool:
    """Print a null audit. Returns False if any indicator breaches the threshold.

    Zero nulls across the board is itself suspicious for tract-level government
    data — small tracts get suppressed — so that is called out too rather than
    silently treated as a clean bill of health.
    """
    if not rows:
        print("\nNull audit: no rows to audit.")
        return False

    n = len(rows)
    print(f"\nNull audit ({n} tracts):")
    ok = True
    any_null = False
    for col in INDICATOR_COLUMNS:
        nulls = sum(1 for r in rows if r.get(col) is None)
        share = nulls / n
        flag = ""
        if share > NULL_FAILURE_THRESHOLD:
            flag = f"  <-- FAIL (>{NULL_FAILURE_THRESHOLD:.0%})"
            ok = False
        if nulls:
            any_null = True
        print(f"  {col:<32} {nulls:>5} null  ({share:>5.1%}){flag}")

    if not any_null:
        print("  NOTE: zero nulls anywhere. For tract-level ACS data that is "
              "unusual — verify suppression handling rather than assuming a clean run.")
    return ok


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_outputs(rows: list[dict], state: str, year: int, mode: str) -> tuple[str, str]:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stem = f"wi_tracts_acs" if state == DEFAULT_STATE else f"state{state}_tracts_acs"

    csv_path = os.path.join(OUTPUT_DIR, f"{stem}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {csv_path}")

    json_path = os.path.join(OUTPUT_DIR, f"{stem}.json")
    payload = {
        "metadata": {
            "source": "U.S. Census Bureau, American Community Survey 5-Year Estimates",
            "vintage": f"{year - 4}-{year}",
            "vintage_year": year,
            "state_fips": state,
            "geography": "census tract",
            "resolution_mode": mode,
            "tract_count": len(rows),
            "indicators": INDICATOR_COLUMNS,
        },
        "tracts": rows,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
    print(f"Wrote {json_path}")
    return csv_path, json_path


def print_dry_run_plan(year: int, state: str, mode: str) -> None:
    print("=== DRY RUN: fetch_wi_tracts.py ===")
    print(f"ACS vintage:  {year} (5-Year estimates, {year-4}-{year})")
    print(f"State FIPS:   {state}")
    print(f"Geography:    census tract")
    print(f"Mode:         {mode}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print()
    if mode == "statewide":
        print("API calls planned: 3 (1 detail + 2 subject), ~4.5 s")
    else:
        print("API calls planned: 3 per county (1 county-list call first).")
        print("  For Wisconsin that is 1 + 216 calls, ~5.5 min at the 1.5 s rate limit.")
    print()
    print(f"Detail variables ({len(DETAIL_VARS)}): {', '.join(DETAIL_VARS)}")
    print(f"Subject variables: {', '.join(POVERTY_VARS + INSURANCE_VARS)}")
    print()
    print("Output columns:")
    for col in OUTPUT_COLUMNS:
        print(f"  {col}")
    print()
    print("[dry-run] No API calls made.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"Fetch ACS {DEFAULT_YEAR} 5-Year tract-level data for a whole state.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--state", default=DEFAULT_STATE,
                        help=f"2-digit state FIPS (default: {DEFAULT_STATE} = Wisconsin)")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR,
                        help=f"ACS end year (default: {DEFAULT_YEAR} -> {DEFAULT_YEAR-4}-{DEFAULT_YEAR})")
    parser.add_argument("--mode", default="statewide",
                        choices=["statewide", "county-drill"],
                        help="Tract resolution strategy (default: statewide)")
    parser.add_argument("--compare", action="store_true",
                        help="Run BOTH modes and diff them. Slow; use to validate.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the fetch plan without calling the Census API.")
    args = parser.parse_args()

    state = args.state.zfill(2)

    if args.dry_run:
        print_dry_run_plan(args.year, state, args.mode)
        return

    require_api_key()  # fail here, not mid-fetch as an unparseable-JSON error

    if args.compare:
        sw = merge_rows(*fetch_statewide(args.year, state))
        cd = merge_rows(*fetch_county_drill(args.year, state))
        sw_ids = {r["geoid"] for r in sw}
        cd_ids = {r["geoid"] for r in cd}
        print(f"\n=== mode comparison ===")
        print(f"  statewide:    {len(sw)} tracts")
        print(f"  county-drill: {len(cd)} tracts")
        print(f"  only in statewide:    {sorted(sw_ids - cd_ids) or 'none'}")
        print(f"  only in county-drill: {sorted(cd_ids - sw_ids) or 'none'}")
        sw_by = {r["geoid"]: r for r in sw}
        diffs = [g for g in (sw_ids & cd_ids)
                 for r in [next(x for x in cd if x["geoid"] == g)]
                 if any(sw_by[g][c] != r[c] for c in INDICATOR_COLUMNS)]
        print(f"  value mismatches on shared tracts: {len(diffs)}")
        rows, mode = sw, "statewide"
    elif args.mode == "statewide":
        rows = merge_rows(*fetch_statewide(args.year, state))
        mode = "statewide"
    else:
        rows = merge_rows(*fetch_county_drill(args.year, state))
        mode = "county-drill"

    print(f"\nProduced {len(rows)} tract records")

    audit_ok = null_audit(rows)

    print()
    write_outputs(rows, state, args.year, mode)

    if not audit_ok:
        print("\nNull audit FAILED — an indicator exceeded the threshold. "
              "Files were written for inspection; do not load them.", file=sys.stderr)
        sys.exit(1)

    print("Done.")


if __name__ == "__main__":
    main()
