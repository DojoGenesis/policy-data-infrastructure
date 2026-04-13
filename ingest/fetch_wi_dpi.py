#!/usr/bin/env python3
"""
Fetch WI DPI WISEdash certified school data for Wisconsin.

Source: https://dpi.wi.gov/wisedash/download-files (certified data downloads)

Usage:
  python fetch_wi_dpi.py --dry-run               # preview only
  python fetch_wi_dpi.py --year 2023             # fetch and save CSV
  python fetch_wi_dpi.py --year 2023 --load      # fetch and load to PostGIS

URL pattern (may change year-to-year — check DPI WISEdash if download fails):
  https://dpi.wi.gov/sites/default/files/imce/wisedash/download/enrollment_certified_{year}.csv

If the URL returns a 404, visit https://dpi.wi.gov/wisedash/download-files to
locate the current certified enrollment download for the desired year.
"""
import argparse
import csv
import io
import os
import sys
import time
import urllib.error
import urllib.request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "analysis", "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "wi_schools_dpi.csv")

# URL template — year is substituted at runtime.
# NOTE: DPI changes file naming conventions between releases.
# Fallback: visit https://dpi.wi.gov/wisedash/download-files
URL_TEMPLATE = (
    "https://dpi.wi.gov/sites/default/files/imce/wisedash/download/"
    "enrollment_certified_{year}.csv"
)

RATE_LIMIT_DELAY = 2.0  # seconds between requests

# Output column definitions
OUTPUT_COLUMNS = [
    "district_code",
    "school_code",
    "school_name",
    "enrollment",
    "chronic_absence_rate",
    "pct_econ_disadvantaged",
    "pct_students_with_disabilities",
    "pct_ell",
    "pct_white",
    "pct_black",
    "pct_hispanic",
]

# Mapping from DPI source column names to our output column names.
# DPI column names vary by release; these are the most common aliases.
# Add additional aliases here if the download format changes.
COLUMN_ALIASES: dict[str, str] = {
    # district
    "district_code":              "district_code",
    "DISTRICT_CODE":              "district_code",
    "District Code":              "district_code",
    "DistrictCode":               "district_code",
    # school
    "school_code":                "school_code",
    "SCHOOL_CODE":                "school_code",
    "School Code":                "school_code",
    "SchoolCode":                 "school_code",
    # school name
    "school_name":                "school_name",
    "SCHOOL_NAME":                "school_name",
    "School Name":                "school_name",
    "SchoolName":                 "school_name",
    # enrollment
    "enrollment":                 "enrollment",
    "ENROLLMENT":                 "enrollment",
    "Enrollment":                 "enrollment",
    "Total Enrollment":           "enrollment",
    # chronic absence
    "chronic_absence_rate":       "chronic_absence_rate",
    "CHRONIC_ABSENCE_RATE":       "chronic_absence_rate",
    "Chronic Absence Rate":       "chronic_absence_rate",
    "ChronicAbsenceRate":         "chronic_absence_rate",
    "Chronic Absence %":          "chronic_absence_rate",
    # economic disadvantage
    "pct_econ_disadvantaged":     "pct_econ_disadvantaged",
    "PCT_ECON_DISADVANTAGED":     "pct_econ_disadvantaged",
    "% Economically Disadvantaged": "pct_econ_disadvantaged",
    "EconDisadvantagedPct":       "pct_econ_disadvantaged",
    # disabilities
    "pct_students_with_disabilities": "pct_students_with_disabilities",
    "PCT_STUDENTS_WITH_DISABILITIES": "pct_students_with_disabilities",
    "% Students with Disabilities": "pct_students_with_disabilities",
    "DisabilitiesPct":            "pct_students_with_disabilities",
    # ELL
    "pct_ell":                    "pct_ell",
    "PCT_ELL":                    "pct_ell",
    "% ELL":                      "pct_ell",
    "ELLPct":                     "pct_ell",
    "% English Learners":         "pct_ell",
    # race/ethnicity
    "pct_white":                  "pct_white",
    "PCT_WHITE":                  "pct_white",
    "% White":                    "pct_white",
    "WhitePct":                   "pct_white",
    "pct_black":                  "pct_black",
    "PCT_BLACK":                  "pct_black",
    "% Black or African American": "pct_black",
    "BlackPct":                   "pct_black",
    "pct_hispanic":               "pct_hispanic",
    "PCT_HISPANIC":               "pct_hispanic",
    "% Hispanic":                 "pct_hispanic",
    "HispanicPct":                "pct_hispanic",
}


def print_plan(args: argparse.Namespace) -> None:
    url = URL_TEMPLATE.format(year=args.year)
    print(f"[dry-run] WI DPI WISEdash fetch plan")
    print(f"  Year    : {args.year}")
    print(f"  URL     : {url}")
    print(f"  Output  : {OUTPUT_FILE}")
    print(f"  Columns : {', '.join(OUTPUT_COLUMNS)}")
    print()
    print("  NOTE: If the URL returns 404, visit:")
    print("        https://dpi.wi.gov/wisedash/download-files")
    print("        and update URL_TEMPLATE in this script.")


def fetch_data(args: argparse.Namespace) -> list[dict]:
    url = URL_TEMPLATE.format(year=args.year)
    print(f"Fetching WI DPI WISEdash {args.year}...")
    print(f"  URL: {url}")

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "policy-data-infrastructure/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            if resp.status != 200:
                print(
                    f"  ERROR: HTTP {resp.status} — check URL pattern.\n"
                    f"  Visit https://dpi.wi.gov/wisedash/download-files for current URLs.",
                    file=sys.stderr,
                )
                sys.exit(1)
            raw_bytes = resp.read()
    except urllib.error.HTTPError as exc:
        print(
            f"  ERROR: HTTP {exc.code} {exc.reason}\n"
            f"  URL: {url}\n"
            f"  The WI DPI URL pattern changes between releases.\n"
            f"  Visit https://dpi.wi.gov/wisedash/download-files to find current URL.",
            file=sys.stderr,
        )
        sys.exit(1)
    except urllib.error.URLError as exc:
        print(f"  ERROR: Network error — {exc.reason}", file=sys.stderr)
        sys.exit(1)

    print(f"  Received {len(raw_bytes):,} bytes")
    time.sleep(RATE_LIMIT_DELAY)

    # Parse CSV — try UTF-8 first, fall back to latin-1
    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw_bytes.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))
    source_cols = reader.fieldnames or []

    # Build mapping from source column name → output column name
    col_map: dict[str, str] = {}
    for src_col in source_cols:
        if src_col in COLUMN_ALIASES:
            col_map[src_col] = COLUMN_ALIASES[src_col]

    mapped_targets = set(col_map.values())
    missing = [c for c in OUTPUT_COLUMNS if c not in mapped_targets]
    if missing:
        print(
            f"  WARNING: Could not map source columns for: {missing}\n"
            f"  Source columns found: {source_cols}\n"
            f"  Add entries to COLUMN_ALIASES if the DPI format changed.",
            file=sys.stderr,
        )

    records: list[dict] = []
    for row in reader:
        out: dict = {col: None for col in OUTPUT_COLUMNS}
        for src_col, target_col in col_map.items():
            raw = row.get(src_col, "").strip()
            out[target_col] = raw if raw not in ("", "N/A", "*", "-") else None
        records.append(out)

    print(f"  Parsed {len(records):,} school records")
    return records


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
        print(f"  {col:<40} {null_count:>5} null  ({pct:.1f}%)")


def load_to_db(records: list[dict]) -> None:
    """Load records to PostGIS via db.py.  Requires PDI_DATABASE_URL to be set."""
    try:
        import sys as _sys
        _sys.path.insert(0, SCRIPT_DIR)
        from lib.db import get_conn, bulk_load_indicators
    except ImportError as exc:
        print(f"  ERROR: Cannot import lib.db — {exc}", file=sys.stderr)
        sys.exit(1)

    print("Connecting to database...")
    conn = get_conn()

    # Flatten to indicator rows keyed by (school_code, variable)
    indicators = []
    numeric_cols = [
        "enrollment",
        "chronic_absence_rate",
        "pct_econ_disadvantaged",
        "pct_students_with_disabilities",
        "pct_ell",
        "pct_white",
        "pct_black",
        "pct_hispanic",
    ]
    for rec in records:
        school_id = rec.get("school_code") or rec.get("district_code") or "unknown"
        for col in numeric_cols:
            raw = rec.get(col)
            try:
                val = float(raw) if raw is not None else None
            except (ValueError, TypeError):
                val = None
            indicators.append({
                "geoid":          school_id,
                "variable_id":    f"wi_dpi_{col}",
                "vintage":        0,
                "value":          val,
                "margin_of_error": None,
                "raw_value":      str(raw or ""),
            })

    n = bulk_load_indicators(conn, indicators)
    conn.close()
    print(f"  {n} indicator rows written to database")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch WI DPI WISEdash certified school data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2023,
        help="School year to fetch (default: 2023)",
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
