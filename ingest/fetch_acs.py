#!/usr/bin/env python3
"""
Fetch ACS 5-Year data for any state/county and load to PostgreSQL — in the
CANONICAL variable vocabulary.

This script emits exactly the 19 variable IDs that pkg/datasource/acs.go
defines and the live county data carries (ADR-014 D4). It exists so tract
loads share identifiers with county loads: the platform's cross-level
analysis silently cannot work if the two levels speak different vocabularies
(ADR-014 F4 found three vocabularies; this script previously minted one of
them — derived percents like `pct_poc` with `margin_of_error: None`).

What that means concretely:

  - Raw estimates only. No derived percentages are computed here; derivation
    happens at read time or in an explicit step that registers its own IDs at
    BOTH levels (D4).
  - Every variable fetches its margin-of-error companion and stores it in
    indicators.margin_of_error (D5: tract MOEs are proportionally much larger
    than county — loading tract data without them removes the reader's only
    signal of when not to trust a number).
  - Vintage is the same string the Go adapter writes ("ACS-2024-5yr"), never
    a bare year — a tract row whose vintage string differs from its county
    counterpart never joins it in indicators_latest comparisons.
  - Metadata is NOT upserted here. RegisterSource (the Go adapter) is the
    authority on the canonical 19; this script refuses to load if they are
    missing rather than registering a drifting copy.

Usage examples:
  # Statewide Wisconsin tracts (one Census call per dataset), dry run
  python fetch_acs.py --state 55 --year 2024 --geo-level tract --dry-run

  # Load them
  python fetch_acs.py --state 55 --year 2024 --geo-level tract --load

  # All Wisconsin counties
  python fetch_acs.py --state 55 --year 2024 --geo-level county --load

Requires CENSUS_API_KEY (the Census API rejects keyless requests).
"""
import argparse
import sys
from datetime import datetime, timezone

from lib.census import (
    fetch_acs_table,
    safe_float,
    require_api_key,
)
from lib.db import get_conn, bulk_load_indicators

# ACS 2020-2024 5-Year released 2025-12-11 — the newest published vintage.
DEFAULT_YEAR = 2024

# ---------------------------------------------------------------------------
# The canonical vocabulary (ADR-014 D4) — pkg/datasource/acs.go's 19 entries.
# (variable_id, estimate_code, moe_code, subject_table)
# ---------------------------------------------------------------------------

CANONICAL_VARS: list[tuple[str, str, str, bool]] = [
    ("median_household_income",    "B19013_001E",    "B19013_001M",    False),
    ("total_population_race",      "B03002_001E",    "B03002_001M",    False),
    ("pop_white_non_hispanic",     "B03002_003E",    "B03002_003M",    False),
    ("pop_black",                  "B03002_004E",    "B03002_004M",    False),
    ("pop_hispanic_latino",        "B03002_012E",    "B03002_012M",    False),
    ("poverty_rate",               "S1701_C03_001E", "S1701_C03_001M", True),
    ("total_population",           "B01001_001E",    "B01001_001M",    False),
    ("uninsured_rate",             "S2701_C05_001E", "S2701_C05_001M", True),
    ("housing_units_cost_burden",  "B25106_001E",    "B25106_001M",    False),
    ("owner_cost_burden_30pct_1",  "B25106_006E",    "B25106_006M",    False),
    ("owner_cost_burden_30pct_2",  "B25106_010E",    "B25106_010M",    False),
    ("owner_cost_burden_30pct_3",  "B25106_014E",    "B25106_014M",    False),
    ("owner_cost_burden_30pct_4",  "B25106_018E",    "B25106_018M",    False),
    ("owner_cost_burden_30pct_5",  "B25106_022E",    "B25106_022M",    False),
    ("renter_cost_burden_30pct_1", "B25106_024E",    "B25106_024M",    False),
    ("renter_cost_burden_30pct_2", "B25106_028E",    "B25106_028M",    False),
    ("renter_cost_burden_30pct_3", "B25106_032E",    "B25106_032M",    False),
    ("renter_cost_burden_30pct_4", "B25106_036E",    "B25106_036M",    False),
    ("renter_cost_burden_30pct_5", "B25106_040E",    "B25106_040M",    False),
]

DETAIL_CODES = [c for _, e, m, subj in CANONICAL_VARS if not subj for c in (e, m)]
SUBJECT_CODES = [c for _, e, m, subj in CANONICAL_VARS if subj for c in (e, m)]

# Census annotation sentinels beyond the -666666666 the lib already strips:
# -555555555 (controlled), -888888888 (not applicable), -333333333 /
# -222222222 (annotation codes). All are impossible as real estimates or
# MOEs; anything at or below this floor is missing data, not a value.
ANNOTATION_FLOOR = -111111111


def clean_value(raw) -> float | None:
    """Convert a Census value to float, treating all annotation sentinels as None."""
    v = safe_float(raw)
    if v is None or v <= ANNOTATION_FLOOR:
        return None
    return v


def clean_moe(raw) -> float | None:
    """MOEs are additionally never negative."""
    v = clean_value(raw)
    if v is None or v < 0:
        return None
    return v


def vintage_string(year: int) -> str:
    """The exact vintage string the Go adapter writes for this release."""
    return f"ACS-{year}-5yr"


# ---------------------------------------------------------------------------
# Record processing
# ---------------------------------------------------------------------------

def process_records(detail_rows: list[dict], subject_rows: list[dict], year: int) -> list[dict]:
    """
    Merge detail + subject responses by GEOID and emit one indicator record
    per canonical variable per geography: raw estimate + its MOE.
    """
    vintage = vintage_string(year)
    fetched_at = datetime.now(timezone.utc).isoformat()
    subj_by_geoid = {r["geoid"]: r for r in subject_rows}

    indicators: list[dict] = []
    for row in detail_rows:
        geoid = row["geoid"]
        subj = subj_by_geoid.get(geoid, {})
        for var_id, e_code, m_code, is_subject in CANONICAL_VARS:
            src = subj if is_subject else row
            raw = src.get(e_code)
            indicators.append({
                "geoid":           geoid,
                "variable_id":     var_id,
                "vintage":         vintage,
                "value":           clean_value(raw),
                "margin_of_error": clean_moe(src.get(m_code)),
                "raw_value":       "" if raw is None else str(raw),
                "fetched_at":      fetched_at,
            })
    return indicators


def null_audit(indicators: list[dict]) -> bool:
    """Per-variable null audit. >30% null on any variable fails the load."""
    if not indicators:
        print("\nNull audit: no records (empty fetch)")
        return False

    by_var: dict[str, list[dict]] = {}
    for ind in indicators:
        by_var.setdefault(ind["variable_id"], []).append(ind)

    print(f"\nNull audit ({len(indicators):,} records, {len(by_var)} variables):")
    passed = True
    for var_id in sorted(by_var):
        rows = by_var[var_id]
        nulls = sum(1 for r in rows if r["value"] is None)
        moes = sum(1 for r in rows if r["value"] is not None and r["margin_of_error"] is not None)
        pct = nulls / len(rows) * 100
        flag = "FAIL" if pct > 30 else "ok"
        if pct > 30:
            passed = False
        print(f"  {var_id:<28} {len(rows):>6} rows  {nulls:>5} null ({pct:4.1f}%)  "
              f"{moes:>6} with MOE  [{flag}]")

    if not passed:
        print("\n  AUDIT FAILED: >30% null for one or more variables. "
              "Aborting load — check vintage availability for this geography level.",
              file=sys.stderr)
    return passed


def verify_canonical_meta(conn) -> None:
    """
    Refuse to load if the canonical variables are not registered.
    Registration belongs to the Go adapter (RegisterSource); loading rows for
    unregistered variables would fail the indicator_meta FK anyway — this
    just makes the failure explain itself.
    """
    ids = [v[0] for v in CANONICAL_VARS]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT variable_id FROM indicator_meta WHERE variable_id = ANY(%s)",
            (ids,),
        )
        present = {r[0] for r in cur.fetchall()}
    missing = [i for i in ids if i not in present]
    if missing:
        print(
            "ERROR: canonical ACS variables are not registered in indicator_meta:\n"
            f"       {', '.join(missing)}\n"
            "       Run the Go adapter's county fetch once (it registers the canonical\n"
            "       vocabulary): go run ./cmd/pdi fetch --source acs-5yr\n"
            "       This script deliberately does not register metadata (ADR-014 D4:\n"
            "       one authority, no drifting copies).",
            file=sys.stderr,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch ACS 5-Year data in the canonical variable vocabulary and load to PostgreSQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--state",     required=True, help="2-digit state FIPS (e.g., 55 for Wisconsin)")
    parser.add_argument("--county",    default=None,  help="3-digit county FIPS (e.g., 025). Optional for tract (statewide without it); required for block_group.")
    parser.add_argument("--year",      type=int, default=DEFAULT_YEAR,
                        help=f"ACS end year (default: {DEFAULT_YEAR} → {DEFAULT_YEAR-4}–{DEFAULT_YEAR} estimates)")
    parser.add_argument("--geo-level", default="tract",
                        choices=["tract", "block_group", "county", "state"],
                        help="Geographic level to fetch (default: tract)")
    parser.add_argument("--dry-run",   action="store_true", help="Fetch and audit but do not write to the database")
    parser.add_argument("--load",      action="store_true", help="Write to the database (with --dry-run, --dry-run wins)")
    args = parser.parse_args()

    require_api_key()  # fail here, not mid-fetch as an unparseable-JSON error

    state = args.state.zfill(2)
    county = args.county.zfill(3) if args.county else None
    year = args.year
    level = args.geo_level

    print(f"Fetching ACS {year} 5-Year — state={state} county={county or 'all'} level={level}")
    print(f"  Vocabulary: canonical ({len(CANONICAL_VARS)} variables, all with MOE)")
    print(f"  Vintage   : {vintage_string(year)}")

    # Both fetches must succeed: a partial vocabulary load at one level is the
    # exact asymmetry this script exists to prevent, so any failure aborts.
    print(f"  Fetching {len(DETAIL_CODES)} detail codes (acs5)...")
    detail_rows = fetch_acs_table(year, DETAIL_CODES, state, county, level, subject=False)
    print(f"    {len(detail_rows)} {level} rows")

    print(f"  Fetching {len(SUBJECT_CODES)} subject codes (acs5/subject)...")
    subject_rows = fetch_acs_table(year, SUBJECT_CODES, state, county, level, subject=True)
    print(f"    {len(subject_rows)} {level} rows")

    if not detail_rows:
        print("ERROR: detail fetch returned no rows.", file=sys.stderr)
        sys.exit(1)
    if len(subject_rows) < len(detail_rows) * 0.95:
        print(f"ERROR: subject fetch returned {len(subject_rows)} rows against "
              f"{len(detail_rows)} detail rows — a partial merge would load "
              "asymmetric coverage. Aborting.", file=sys.stderr)
        sys.exit(1)

    indicators = process_records(detail_rows, subject_rows, year)
    print(f"\nProcessed {len(indicators):,} indicator records from {len(detail_rows):,} geographies")

    if not null_audit(indicators):
        sys.exit(1)

    if args.dry_run or not args.load:
        print("\n[dry-run] Skipping database write." if args.dry_run
              else "\nNo --load flag: skipping database write.")
        return

    print("\nConnecting to database...")
    conn = get_conn()
    verify_canonical_meta(conn)

    print("Bulk loading indicators via COPY...")
    n_ind = bulk_load_indicators(conn, indicators)
    print(f"  {n_ind} indicator rows written")

    conn.close()
    print("\nDone. Remember: REFRESH MATERIALIZED VIEW CONCURRENTLY indicators_latest;")


if __name__ == "__main__":
    main()
