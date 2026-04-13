#!/usr/bin/env python3
"""
Fetch USDA Food Access Research Atlas data for Wisconsin.

Source: https://www.ers.usda.gov/data-products/food-access-research-atlas/

Usage:
  python fetch_usda_food.py --dry-run       # preview only
  python fetch_usda_food.py                 # fetch and save CSV
  python fetch_usda_food.py --load          # fetch and load to PostGIS

The USDA ERS publishes a national CSV updated with each ACS/decennial revision.
If the URL below returns a 404, visit:
  https://www.ers.usda.gov/data-products/food-access-research-atlas/
to find the current download link and update DATA_URL in this script.

Wisconsin state FIPS: 55
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
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "wi_food_access.csv")

# USDA ERS Food Access Research Atlas — 2019 vintage (current as of 2025).
# The ERS now distributes this as a ZIP archive containing a CSV.
# If this URL returns 404, visit:
#   https://www.ers.usda.gov/data-products/food-access-research-atlas/
# to find the updated download link.
DATA_URL = (
    "https://www.ers.usda.gov/media/5627/"
    "food-access-research-atlas-data-download-2019.zip"
)
# Whether the URL points to a ZIP archive (auto-detected by extension)
_IS_ZIP = DATA_URL.endswith(".zip")

WI_STATE_FIPS = "55"
RATE_LIMIT_DELAY = 2.0  # seconds; ERS rate limit: ~10 req/min

# Output columns
OUTPUT_COLUMNS = [
    "geoid",
    "county_name",
    "urban_flag",
    "low_access_1mi",
    "low_access_10mi",
    "low_income_low_access",
    "snap_flag",
    "population",
    "poverty_rate",
]

# Mapping of USDA ERS source column names → our output column names.
# The ERS renames columns between vintages; extend this dict as needed.
COLUMN_ALIASES: dict[str, str] = {
    # GEOID (tract)
    "CensusTract":        "geoid",
    "census_tract":       "geoid",
    "GEOID":              "geoid",
    "geoid":              "geoid",
    # county name
    "County":             "county_name",
    "county":             "county_name",
    "County Name":        "county_name",
    # urban flag
    "Urban":              "urban_flag",
    "urban":              "urban_flag",
    "UrbanFlag":          "urban_flag",
    # low access 1 mile
    "LAPOP1_10":          "low_access_1mi",
    "lapop1_10":          "low_access_1mi",
    "LowAccess1Mi":       "low_access_1mi",
    "LAPOP1":             "low_access_1mi",
    # low access 10 mile — population share with low access at 10-mile threshold
    "LAPOP10_10":         "low_access_10mi",
    "lapop10_10":         "low_access_10mi",
    "LowAccess10Mi":      "low_access_10mi",
    "LAPOP10":            "low_access_10mi",
    "lapop10":            "low_access_10mi",    # 2019 column name
    "lapop10share":       "low_access_10mi",    # share version (2019)
    # low income low access
    "LILATracts_1And10":  "low_income_low_access",
    "lilatracts_1and10":  "low_income_low_access",
    "LILATracts_halfAnd10": "low_income_low_access",
    "LowIncomeLowAccess": "low_income_low_access",
    # SNAP households with low access — share of SNAP-participant households
    # with low access at 1-mile threshold; useful proxy for food-desert severity
    "SNAP_1":             "snap_flag",
    "snap_1":             "snap_flag",
    "SNAPFlag":           "snap_flag",
    "SNAP":               "snap_flag",
    "lasnap1share":       "snap_flag",          # 2019 column name
    "TractSNAP":          "snap_flag",          # SNAP store count (fallback)
    # population
    "Pop2010":            "population",
    "pop2010":            "population",
    "POP2010":            "population",
    "Population":         "population",
    "pop":                "population",
    # poverty rate
    "PovertyRate":        "poverty_rate",
    "poverty_rate":       "poverty_rate",
    "POVERTYRATE":        "poverty_rate",
    "PovRate":            "poverty_rate",
}

# ERS CSV includes a State column for filtering
STATE_COLUMN_ALIASES = ["State", "state", "STATE", "StateAbbr", "stateabbr"]


def print_plan(args: argparse.Namespace) -> None:
    print("[dry-run] USDA Food Access Research Atlas fetch plan")
    print(f"  URL       : {DATA_URL}")
    print(f"  State FIPS: {WI_STATE_FIPS} (Wisconsin)")
    print(f"  Output    : {OUTPUT_FILE}")
    print(f"  Columns   : {', '.join(OUTPUT_COLUMNS)}")
    print()
    print("  NOTE: If the URL returns 404, visit:")
    print("        https://www.ers.usda.gov/data-products/food-access-research-atlas/")
    print("        and update DATA_URL in this script.")


def _find_state_col(header: list[str]) -> str | None:
    for alias in STATE_COLUMN_ALIASES:
        if alias in header:
            return alias
    return None


def _find_state_fips_col(header: list[str]) -> str | None:
    for candidate in ["State", "state", "StateFIPS", "state_fips", "STATEFP"]:
        if candidate in header:
            return candidate
    return None


def fetch_data(args: argparse.Namespace) -> list[dict]:
    print("Fetching USDA Food Access Research Atlas...")
    print(f"  URL: {DATA_URL}")

    req = urllib.request.Request(
        DATA_URL,
        headers={"User-Agent": "policy-data-infrastructure/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            if resp.status != 200:
                print(
                    f"  ERROR: HTTP {resp.status}.\n"
                    f"  Visit https://www.ers.usda.gov/data-products/food-access-research-atlas/ "
                    f"to find the current URL.",
                    file=sys.stderr,
                )
                sys.exit(1)
            raw_bytes = resp.read()
    except urllib.error.HTTPError as exc:
        print(
            f"  ERROR: HTTP {exc.code} {exc.reason}\n"
            f"  URL: {DATA_URL}\n"
            f"  The USDA ERS URL changes with each vintage release.\n"
            f"  Visit https://www.ers.usda.gov/data-products/food-access-research-atlas/",
            file=sys.stderr,
        )
        sys.exit(1)
    except urllib.error.URLError as exc:
        print(f"  ERROR: Network error — {exc.reason}", file=sys.stderr)
        sys.exit(1)

    print(f"  Received {len(raw_bytes):,} bytes")
    time.sleep(RATE_LIMIT_DELAY)

    # If the download is a ZIP, extract the first CSV inside it
    if _IS_ZIP:
        try:
            zf = zipfile.ZipFile(io.BytesIO(raw_bytes))
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                print(f"  ERROR: ZIP archive contains no CSV files. Members: {zf.namelist()}", file=sys.stderr)
                sys.exit(1)
            # Use the largest CSV (main data table)
            csv_name = sorted(csv_names, key=lambda n: zf.getinfo(n).file_size, reverse=True)[0]
            print(f"  Extracting {csv_name} from ZIP...")
            raw_bytes = zf.read(csv_name)
        except zipfile.BadZipFile as exc:
            print(f"  ERROR: Downloaded file is not a valid ZIP: {exc}", file=sys.stderr)
            sys.exit(1)

    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw_bytes.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))
    source_cols = reader.fieldnames or []

    # Determine the column used to identify Wisconsin tracts
    # ERS uses a "CensusTract" column starting with state FIPS for filtering,
    # or a separate State/StateAbbr column.
    state_col = _find_state_col(source_cols)
    geoid_col_raw = next((c for c in source_cols if c in COLUMN_ALIASES and COLUMN_ALIASES[c] == "geoid"), None)

    # Build column mapping
    col_map: dict[str, str] = {}
    for src_col in source_cols:
        if src_col in COLUMN_ALIASES:
            col_map[src_col] = COLUMN_ALIASES[src_col]

    mapped_targets = set(col_map.values())
    missing = [c for c in OUTPUT_COLUMNS if c not in mapped_targets]
    if missing:
        print(
            f"  WARNING: Could not map source columns for: {missing}\n"
            f"  Source columns found: {source_cols[:20]}{'...' if len(source_cols) > 20 else ''}\n"
            f"  Add entries to COLUMN_ALIASES if the ERS format changed.",
            file=sys.stderr,
        )

    records: list[dict] = []
    total_rows = 0
    for row in reader:
        total_rows += 1

        # Filter to Wisconsin
        is_wi = False
        if state_col and row.get(state_col, "").strip() == "WI":
            is_wi = True
        elif geoid_col_raw:
            tract_raw = row.get(geoid_col_raw, "").strip().zfill(11)
            if tract_raw.startswith(WI_STATE_FIPS):
                is_wi = True
        else:
            # Fallback: check any column that looks like a GEOID
            for col in source_cols:
                val = row.get(col, "").strip()
                if len(val) == 11 and val.startswith(WI_STATE_FIPS):
                    is_wi = True
                    break

        if not is_wi:
            continue

        out: dict = {col: None for col in OUTPUT_COLUMNS}
        for src_col, target_col in col_map.items():
            raw = row.get(src_col, "").strip()
            out[target_col] = raw if raw not in ("", "N/A", "-", ".") else None

        # Normalize geoid to 11 digits
        if out.get("geoid"):
            out["geoid"] = str(out["geoid"]).zfill(11)

        records.append(out)

    print(f"  Total rows in file: {total_rows:,}")
    print(f"  Wisconsin tracts  : {len(records):,}")
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
        pct = null_count / len(records) * 100 if records else 0
        print(f"  {col:<35} {null_count:>5} null  ({pct:.1f}%)")


def load_to_db(records: list[dict]) -> None:
    """Load records to PostGIS via db.py."""
    try:
        import sys as _sys
        _sys.path.insert(0, SCRIPT_DIR)
        from lib.db import get_conn, bulk_load_indicators
    except ImportError as exc:
        print(f"  ERROR: Cannot import lib.db — {exc}", file=sys.stderr)
        sys.exit(1)

    print("Connecting to database...")
    conn = get_conn()

    numeric_cols = [
        "urban_flag",
        "low_access_1mi",
        "low_access_10mi",
        "low_income_low_access",
        "snap_flag",
        "population",
        "poverty_rate",
    ]
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
                "geoid":          geoid,
                "variable_id":    f"usda_food_{col}",
                "vintage":        2019,
                "value":          val,
                "margin_of_error": None,
                "raw_value":      str(raw or ""),
            })

    n = bulk_load_indicators(conn, indicators)
    conn.close()
    print(f"  {n} indicator rows written to database")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch USDA Food Access Research Atlas data for Wisconsin.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
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
