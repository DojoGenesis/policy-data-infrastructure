#!/usr/bin/env python3
"""
Fetch WI DPI WISEdash certified attendance data and merge chronic absence rate
into the existing wi_schools_dpi.csv output.

Source: https://dpi.wi.gov/wisedash/download-files

The attendance ZIP contains per-school, per-group ATTENDANCE_RATE data.
Chronic absenteeism (missing 10%+ of school days) corresponds to an
attendance rate below 90%. Since the raw file reports aggregate group-level
attendance rates (not individual student-level counts), we derive the
chronic_absence_rate column as:

    chronic_absence_rate = 100.0 - attendance_rate   (i.e., the absence rate)

This is the best available proxy from the DPI certified attendance download.
The figure represents the district- or school-level mean absence rate for
All Students across all grade levels.

Usage:
  python fetch_wi_dpi_attendance.py --dry-run         # preview only
  python fetch_wi_dpi_attendance.py                   # fetch, parse, merge

The output file (wi_schools_dpi.csv) is updated in-place with the new
chronic_absence_rate column populated wherever a matching district_code exists.
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

LOCAL_ZIP = "/tmp/wi_dpi_attendance.zip"

PRIMARY_URL = (
    "https://dpi.wi.gov/sites/default/files/wise/downloads/"
    "attendance_dropouts_certified_2022-23.zip"
)
FALLBACK_URL = (
    "https://dpi.wi.gov/sites/default/files/imce/wisedash/downloads/"
    "attendance_dropouts_certified_2022-23.zip"
)

RATE_LIMIT_DELAY = 2.0  # seconds after download

# Column in the output CSV that we are populating
ABSENCE_COL = "chronic_absence_rate"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _school_year_label() -> str:
    return "2022-23"


def _download_zip(dry_run: bool) -> bytes:
    """Download the attendance ZIP.  Returns raw bytes."""
    if os.path.exists(LOCAL_ZIP):
        print(f"  Using cached ZIP at {LOCAL_ZIP}")
        with open(LOCAL_ZIP, "rb") as f:
            return f.read()

    for url in (PRIMARY_URL, FALLBACK_URL):
        print(f"  Trying URL: {url}")
        if dry_run:
            print(f"  [dry-run] Would download from {url}")
            return b""
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "policy-data-infrastructure/1.0"},
        )
        try:
            with urllib.request.urlopen(req, timeout=90) as resp:
                if resp.status != 200:
                    print(f"  HTTP {resp.status} — trying fallback...", file=sys.stderr)
                    continue
                raw = resp.read()
                print(f"  Received {len(raw):,} bytes")
                with open(LOCAL_ZIP, "wb") as out:
                    out.write(raw)
                print(f"  Saved to {LOCAL_ZIP}")
                time.sleep(RATE_LIMIT_DELAY)
                return raw
        except urllib.error.HTTPError as exc:
            print(f"  HTTP {exc.code} {exc.reason} — trying fallback...", file=sys.stderr)
        except urllib.error.URLError as exc:
            print(f"  Network error: {exc.reason} — trying fallback...", file=sys.stderr)

    print("ERROR: Both URLs failed. Cannot download attendance data.", file=sys.stderr)
    sys.exit(1)


def _parse_attendance(raw_bytes: bytes) -> dict[str, float | None]:
    """
    Parse the attendance ZIP and return a dict:
        district_code (zero-padded 4-digit str) -> chronic_absence_rate (float)

    chronic_absence_rate = 100.0 - attendance_rate for 'All Students' / '[All]'
    at the district level (SCHOOL_CODE empty).

    Falls back to school-level '[All]' rows for districts not found at district level.
    """
    zf = zipfile.ZipFile(io.BytesIO(raw_bytes))

    # Find the attendance CSV (largest CSV)
    csv_names = [
        n for n in zf.namelist()
        if n.lower().startswith("attendance") and n.lower().endswith(".csv")
        and "layout" not in n.lower()
    ]
    if not csv_names:
        # Fallback: any CSV that isn't a layout
        csv_names = [
            n for n in zf.namelist()
            if n.lower().endswith(".csv") and "layout" not in n.lower()
        ]
    csv_name = sorted(csv_names, key=lambda n: zf.getinfo(n).file_size, reverse=True)[0]
    print(f"  Parsing {csv_name} from ZIP...")

    raw_csv = zf.read(csv_name)
    try:
        text = raw_csv.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw_csv.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))

    # district_code -> attendance_rate (from district-level row)
    district_rates: dict[str, float] = {}
    # district_code -> list of school-level attendance rates (fallback)
    school_rates: dict[str, list[float]] = {}

    for row in reader:
        dc = row.get("DISTRICT_CODE", "").strip()
        sc = row.get("SCHOOL_CODE", "").strip()

        # Skip statewide rollup
        if dc in ("", "0000"):
            continue

        grade_group = row.get("GRADE_GROUP", "").strip()
        if grade_group not in ("[All]", "All Grades"):
            continue

        group_by = row.get("GROUP_BY", "").strip()
        group_val = row.get("GROUP_BY_VALUE", "").strip()

        if group_by != "All Students" or group_val != "All Students":
            continue

        att_str = row.get("ATTENDANCE_RATE", "").strip()
        try:
            att_rate = float(att_str)
        except (ValueError, TypeError):
            continue

        if sc == "":
            # District-level row — preferred
            district_rates[dc] = att_rate
        else:
            # School-level row — keep for fallback
            if dc not in school_rates:
                school_rates[dc] = []
            school_rates[dc].append(att_rate)

    # Build final map: district_code -> chronic_absence_rate
    result: dict[str, float | None] = {}

    all_districts = set(district_rates) | set(school_rates)
    for dc in all_districts:
        if dc in district_rates:
            att = district_rates[dc]
        else:
            # Average school-level rates as fallback
            rates = school_rates[dc]
            att = sum(rates) / len(rates) if rates else None
        if att is not None:
            result[dc] = round(100.0 - att, 2)
        else:
            result[dc] = None

    print(f"  Computed chronic_absence_rate for {len(result)} districts")
    district_only = len(district_rates)
    school_fallback = len(result) - district_only
    print(f"    {district_only} from district-level rows")
    print(f"    {school_fallback} from school-level fallback")
    return result


def _merge_and_write(absence_map: dict[str, float | None], dry_run: bool) -> tuple[int, int]:
    """
    Read wi_schools_dpi.csv, populate the chronic_absence_rate column, write back.
    Returns (total_rows, rows_with_value).
    """
    if not os.path.exists(OUTPUT_FILE):
        print(f"ERROR: Output file not found: {OUTPUT_FILE}", file=sys.stderr)
        sys.exit(1)

    with open(OUTPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    if ABSENCE_COL not in fieldnames:
        print(f"ERROR: Column '{ABSENCE_COL}' not found in {OUTPUT_FILE}", file=sys.stderr)
        print(f"  Available columns: {fieldnames}", file=sys.stderr)
        sys.exit(1)

    filled = 0
    for row in rows:
        dc = str(row.get("district_code", "")).strip()
        if dc and dc in absence_map and absence_map[dc] is not None:
            row[ABSENCE_COL] = str(absence_map[dc])
            filled += 1
        # else: leave existing value or blank

    if dry_run:
        print(f"  [dry-run] Would write {len(rows)} rows to {OUTPUT_FILE}")
        print(f"  [dry-run] {filled} rows would receive chronic_absence_rate values")
        return len(rows), filled

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Wrote {len(rows)} rows to {OUTPUT_FILE}")
    print(f"  {filled} rows populated with chronic_absence_rate")
    return len(rows), filled


def print_plan(args: argparse.Namespace) -> None:
    print("[dry-run] WI DPI Attendance fetch plan")
    print(f"  School year : {_school_year_label()}")
    print(f"  Primary URL : {PRIMARY_URL}")
    print(f"  Fallback URL: {FALLBACK_URL}")
    print(f"  Local cache : {LOCAL_ZIP}")
    print(f"  Target col  : {ABSENCE_COL} in {OUTPUT_FILE}")
    print()
    print("  Derivation: chronic_absence_rate = 100.0 - ATTENDANCE_RATE")
    print("  (DPI reports group-level attendance rate, not individual-level)")
    print("  (Absence rate < 90% = chronic absentee threshold per federal definition)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch WI DPI attendance data and populate chronic_absence_rate.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print plan and counts without downloading or writing",
    )
    args = parser.parse_args()

    if args.dry_run:
        print_plan(args)
        # Still parse if cached, to show expected counts
        if os.path.exists(LOCAL_ZIP):
            with open(LOCAL_ZIP, "rb") as f:
                raw = f.read()
            absence_map = _parse_attendance(raw)
            _merge_and_write(absence_map, dry_run=True)
        return

    print(f"Fetching WI DPI attendance data ({_school_year_label()})...")
    raw = _download_zip(dry_run=False)

    print("Parsing attendance data...")
    absence_map = _parse_attendance(raw)

    print("Merging into wi_schools_dpi.csv...")
    total, filled = _merge_and_write(absence_map, dry_run=False)

    # Null audit
    print("\nNull audit:")
    null_count = total - filled
    null_pct = null_count / total * 100 if total else 0
    print(f"  {ABSENCE_COL:<40} {null_count:>5} null  ({null_pct:.1f}%)")
    print(f"  Total rows  : {total}")
    print(f"  With value  : {filled}")
    print(f"  Null        : {null_count}")

    print("\nDone.")


if __name__ == "__main__":
    main()
