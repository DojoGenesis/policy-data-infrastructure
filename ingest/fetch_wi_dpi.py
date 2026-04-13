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
import zipfile

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "analysis", "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "wi_schools_dpi.csv")

# URL template — DPI uses school year format (YYYY-YY) and ZIP archives.
# The --year flag (e.g. 2023) is the school year END year, so 2023 → "2022-23".
# NOTE: DPI changes file path and naming conventions between releases.
# Fallback: visit https://dpi.wi.gov/wisedash/download-files
URL_TEMPLATE = (
    "https://dpi.wi.gov/sites/default/files/wise/downloads/"
    "enrollment_certified_{school_year}.zip"
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


def _school_year(year: int) -> str:
    """Convert end-year integer to DPI school-year string. 2023 → '2022-23'."""
    short = str(year)[-2:]
    return f"{year - 1}-{short}"


def print_plan(args: argparse.Namespace) -> None:
    sy = _school_year(args.year)
    url = URL_TEMPLATE.format(school_year=sy)
    print(f"[dry-run] WI DPI WISEdash fetch plan")
    print(f"  Year        : {args.year} (school year {sy})")
    print(f"  URL         : {url}")
    print(f"  Format      : ZIP archive containing CSV")
    print(f"  Output      : {OUTPUT_FILE}")
    print(f"  Columns     : {', '.join(OUTPUT_COLUMNS)}")
    print()
    print("  NOTE: If the URL returns 404, visit:")
    print("        https://dpi.wi.gov/wisedash/download-files")
    print("        and update URL_TEMPLATE in this script.")


def fetch_data(args: argparse.Namespace) -> list[dict]:
    sy = _school_year(args.year)
    url = URL_TEMPLATE.format(school_year=sy)
    print(f"Fetching WI DPI WISEdash {args.year} (school year {sy})...")
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

    # DPI distributes as ZIP archives — extract the largest CSV inside
    if url.endswith(".zip"):
        try:
            zf = zipfile.ZipFile(io.BytesIO(raw_bytes))
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                print(f"  ERROR: ZIP contains no CSV. Members: {zf.namelist()}", file=sys.stderr)
                sys.exit(1)
            csv_name = sorted(csv_names, key=lambda n: zf.getinfo(n).file_size, reverse=True)[0]
            print(f"  Extracting {csv_name} from ZIP...")
            raw_bytes = zf.read(csv_name)
        except zipfile.BadZipFile as exc:
            print(f"  ERROR: Not a valid ZIP: {exc}", file=sys.stderr)
            sys.exit(1)

    # Parse CSV — try UTF-8 first, fall back to latin-1
    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw_bytes.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))

    # DPI enrollment files use a LONG (group-by) format.
    # Each row is: school × GROUP_BY × GROUP_BY_VALUE → STUDENT_COUNT / PERCENT_OF_GROUP
    # We pivot to WIDE format: one row per (district_code, school_code) with all indicators.

    # Keyed by (district_code, school_code): wide dict
    schools: dict[tuple, dict] = {}

    def _safe_pct(val_str: str) -> str | None:
        """Clean and return percentage string, or None for suppressed/missing values."""
        v = val_str.strip()
        return v if v not in ("", "N/A", "*", "-", "null") else None

    def _school_key(row: dict) -> tuple:
        return (
            row.get("DISTRICT_CODE", "").strip(),
            row.get("SCHOOL_CODE", "").strip(),
        )

    for row in reader:
        # Skip statewide/district rollup rows (empty SCHOOL_CODE for school-level only)
        # Keep both district (empty school_code) and school level records
        key = _school_key(row)
        district_code = key[0]
        school_code = key[1]

        # Skip the synthetic [Statewide] row
        if district_code == "0000":
            continue

        grade_group = row.get("GRADE_GROUP", "").strip()
        if grade_group not in ("[All]", "All Grades"):
            continue  # Only aggregate [All] grade rows

        group_by = row.get("GROUP_BY", "").strip()
        group_val = row.get("GROUP_BY_VALUE", "").strip()
        pct = _safe_pct(row.get("PERCENT_OF_GROUP", ""))
        count = _safe_pct(row.get("STUDENT_COUNT", ""))

        if key not in schools:
            schools[key] = {
                "district_code": district_code,
                "school_code":   school_code or None,
                "school_name":   row.get("SCHOOL_NAME", "").strip() or row.get("DISTRICT_NAME", "").strip(),
                "enrollment":    None,
                "chronic_absence_rate":         None,
                "pct_econ_disadvantaged":        None,
                "pct_students_with_disabilities": None,
                "pct_ell":       None,
                "pct_white":     None,
                "pct_black":     None,
                "pct_hispanic":  None,
            }

        rec = schools[key]

        if group_by == "All Students" and group_val == "All Students":
            rec["enrollment"] = count
        elif group_by == "Race/Ethnicity":
            if group_val == "White":
                rec["pct_white"] = pct
            elif group_val == "Black":
                rec["pct_black"] = pct
            elif group_val == "Hispanic":
                rec["pct_hispanic"] = pct
        elif group_by == "Economic Status":
            if group_val == "Econ Disadv":
                rec["pct_econ_disadvantaged"] = pct
        elif group_by == "Disability Status":
            if group_val == "SwD":  # Students with Disability
                rec["pct_students_with_disabilities"] = pct
        elif group_by == "EL Status":
            if group_val == "EL":  # English Learner
                rec["pct_ell"] = pct

    records = list(schools.values())
    print(f"  Pivoted to {len(records):,} school/district records")
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
