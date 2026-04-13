#!/usr/bin/env python3
"""
Fetch EPA EJScreen environmental justice data for Wisconsin.

Source: https://gaftp.epa.gov/EJSCREEN/

Usage:
  python fetch_epa_ejscreen.py --dry-run              # preview only
  python fetch_epa_ejscreen.py --year 2023            # fetch and save CSV
  python fetch_epa_ejscreen.py --year 2023 --load     # fetch and load to PostGIS
  python fetch_epa_ejscreen.py --year 2023 --tract    # aggregate block groups to tract level

EJScreen data is released annually at the block group level.

URL pattern (exact filename varies by release year):
  https://gaftp.epa.gov/EJSCREEN/{year}/EJSCREEN_{year}_StatePctile.csv.zip

If the above URL returns a 404, check:
  https://gaftp.epa.gov/EJSCREEN/{year}/
  for the actual filename. EPA sometimes uses:
    - EJSCREEN_{year}_StatePctile.csv.zip
    - EJSCREEN_{year}_USPR_StatePctile.csv.zip
    - EJSCREEN_{year}_BG_StatePctile.csv.zip
  Update URL_CANDIDATES in this script if needed.

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
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "wi_ejscreen.csv")

WI_STATE_FIPS = "55"

# NOTE: EPA removed EJScreen from their servers in February 2025 (gaftp.epa.gov is offline).
# A community preservation mirror is available on Zenodo (record 14767363) for years 2015-2024.
# Warning: the Zenodo full-year ZIP is ~5.9 GB. The script will warn before downloading.
#
# EPA gaftp URLs (kept for documentation; all return 404 as of 2025):
#   https://gaftp.epa.gov/EJSCREEN/{year}/EJSCREEN_{year}_StatePctile.csv.zip
#
# Zenodo mirror (block-group level, US + PR, state percentiles):
#   https://zenodo.org/records/14767363/files/{year}.zip?download=1
#
# We try the Zenodo mirror first for 2023 and earlier; for 2024+ update manually.
URL_CANDIDATES_TEMPLATE = [
    # Zenodo preservation mirror — confirmed live (2023 only; ~5.9 GB)
    "https://zenodo.org/records/14767363/files/{year}.zip?download=1",
    # EPA gaftp (offline as of Feb 2025 — kept for future recovery)
    "https://gaftp.epa.gov/EJSCREEN/{year}/EJSCREEN_{year}_StatePctile.csv.zip",
    "https://gaftp.epa.gov/EJSCREEN/{year}/EJSCREEN_{year}_USPR_StatePctile.csv.zip",
    "https://gaftp.epa.gov/EJSCREEN/{year}/EJSCREEN_{year}_BG_StatePctile.csv.zip",
    "https://gaftp.epa.gov/EJSCREEN/{year}/EJSCREEN_{year}_with_AS_CNMI_GU_VI.zip",
]
# Warn before downloading large Zenodo files
LARGE_DOWNLOAD_THRESHOLD_GB = 1.0

RATE_LIMIT_DELAY = 3.0  # EPA FTP: ~20 req/min courtesy limit

# Key EJScreen environmental indicator columns
# EPA changes column names between versions; we map common aliases
OUTPUT_COLUMNS = [
    "geoid",
    "pm25",
    "ozone",
    "dslpm",
    "cancer",
    "resp",
    "traffic",
    "lead",
    "pnpl",
    "pwdis",
    "ust",
]

# Mapping from EJScreen source column names → our output column names.
# EPA has used different capitalization and naming conventions.
COLUMN_ALIASES: dict[str, str] = {
    # GEOID / block group ID
    "ID":             "geoid",
    "id":             "geoid",
    "GEOID":          "geoid",
    "geoid":          "geoid",
    "GEOID10":        "geoid",
    "BGID":           "geoid",
    # PM2.5
    "PM25":           "pm25",
    "pm25":           "pm25",
    "P_PM25":         "pm25",
    # Ozone
    "OZONE":          "ozone",
    "ozone":          "ozone",
    "P_OZONE":        "ozone",
    # Diesel PM (DSLPM is the canonical EJScreen column name)
    "DSLPM":          "dslpm",
    "dslpm":          "dslpm",
    "P_DSLPM":        "dslpm",
    # Cancer risk
    "CANCER":         "cancer",
    "cancer":         "cancer",
    "P_CANCER":       "cancer",
    # Respiratory hazard
    "RESP":           "resp",
    "resp":           "resp",
    "P_RESP":         "resp",
    # Traffic proximity
    "TRAFFIC":        "traffic",
    "traffic":        "traffic",
    "PTRAF":          "traffic",
    "P_PTRAF":        "traffic",
    # Lead paint indicator
    "LEAD":           "lead",
    "lead":           "lead",
    "PRE1960PCT":     "lead",
    "P_LDPNT":        "lead",
    "LDPNT":          "lead",
    # Superfund/NPL proximity
    "PNPL":           "pnpl",
    "pnpl":           "pnpl",
    "P_PNPL":         "pnpl",
    # Wastewater discharge
    "PWDIS":          "pwdis",
    "pwdis":          "pwdis",
    "P_PWDIS":        "pwdis",
    # Underground storage tanks
    "UST":            "ust",
    "ust":            "ust",
    "PUST":           "ust",
    "P_PUST":         "ust",
}

# Alternative names for the state FIPS column in EJScreen
STATE_COL_CANDIDATES = ["ST", "STATE_ID", "STATEFIPS", "statefips", "state", "ST_ABBREV"]


def _url_candidates(year: int) -> list[str]:
    return [t.format(year=year) for t in URL_CANDIDATES_TEMPLATE]


def print_plan(args: argparse.Namespace) -> None:
    candidates = _url_candidates(args.year)
    print("[dry-run] EPA EJScreen fetch plan")
    print(f"  Year           : {args.year}")
    print(f"  State FIPS     : {WI_STATE_FIPS} (Wisconsin)")
    print(f"  Aggregate tracts: {args.tract}")
    print(f"  Output         : {OUTPUT_FILE}")
    print(f"  Columns        : {', '.join(OUTPUT_COLUMNS)}")
    print()
    print("  URL candidates (tried in order):")
    for i, url in enumerate(candidates, 1):
        print(f"    {i}. {url}")
    print()
    print("  NOTE: If all URLs return 404, browse:")
    print(f"        https://gaftp.epa.gov/EJSCREEN/{args.year}/")
    print("        and update URL_CANDIDATES_TEMPLATE in this script.")


def _try_download(year: int) -> bytes:
    """Try each URL candidate; return the raw ZIP bytes for the first success."""
    candidates = _url_candidates(year)
    last_error: Exception | None = None

    for url in candidates:
        print(f"  Trying: {url}...", end="", flush=True)
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "policy-data-infrastructure/1.0"},
        )
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                if resp.status == 200:
                    data = resp.read()
                    print(f" OK ({len(data):,} bytes)")
                    return data
                else:
                    print(f" HTTP {resp.status} — skipping")
        except urllib.error.HTTPError as exc:
            print(f" HTTP {exc.code} — skipping")
            last_error = exc
        except urllib.error.URLError as exc:
            print(f" network error ({exc.reason}) — skipping")
            last_error = exc

        time.sleep(1.0)  # brief pause between fallback attempts

    print(
        f"\n  ERROR: All URL candidates failed for year {year}.\n"
        f"  Last error: {last_error}\n"
        f"  Browse https://gaftp.epa.gov/EJSCREEN/{year}/ for the current filename.\n"
        f"  Update URL_CANDIDATES_TEMPLATE in this script to add it.",
        file=sys.stderr,
    )
    sys.exit(1)


def _extract_csv_from_zip(zip_bytes: bytes) -> io.StringIO:
    """Extract the first CSV from a ZIP archive and return as StringIO."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            print(
                "  ERROR: No CSV file found inside the ZIP archive.",
                file=sys.stderr,
            )
            sys.exit(1)

        # Prefer the largest CSV (national file) if multiple are present
        csv_names.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
        chosen = csv_names[0]
        print(f"  Extracting: {chosen} ({zf.getinfo(chosen).file_size:,} bytes uncompressed)")

        raw = zf.read(chosen)
        try:
            return io.StringIO(raw.decode("utf-8-sig"))
        except UnicodeDecodeError:
            return io.StringIO(raw.decode("latin-1"))


def fetch_data(args: argparse.Namespace) -> list[dict]:
    print(f"Fetching EPA EJScreen {args.year}...")
    zip_bytes = _try_download(args.year)
    time.sleep(RATE_LIMIT_DELAY)

    text_io = _extract_csv_from_zip(zip_bytes)
    reader = csv.DictReader(text_io)
    source_cols = reader.fieldnames or []

    # Determine state column
    state_col = next((c for c in source_cols if c in STATE_COL_CANDIDATES), None)
    if not state_col:
        # Try partial match
        state_col = next((c for c in source_cols if "state" in c.lower() or c.upper() == "ST"), None)

    # Build column mapping (handle DSLPM/PNPL dual-mapping: prefer first match)
    col_map: dict[str, str] = {}
    assigned_targets: set[str] = set()
    for src_col in source_cols:
        if src_col in COLUMN_ALIASES:
            target = COLUMN_ALIASES[src_col]
            if target not in assigned_targets:
                col_map[src_col] = target
                assigned_targets.add(target)

    mapped_targets = set(col_map.values())
    missing = [c for c in OUTPUT_COLUMNS if c not in mapped_targets]
    if missing:
        print(
            f"  WARNING: Could not map source columns for: {missing}\n"
            f"  Source columns (first 30): {source_cols[:30]}\n"
            f"  Add entries to COLUMN_ALIASES if EPA changed column names.",
            file=sys.stderr,
        )

    records: list[dict] = []
    total_rows = 0

    for row in reader:
        total_rows += 1

        # Filter to Wisconsin
        is_wi = False
        if state_col:
            state_val = row.get(state_col, "").strip()
            # EJScreen uses numeric state FIPS or 2-letter abbreviation
            if state_val == WI_STATE_FIPS or state_val == "WI":
                is_wi = True
        else:
            # Fallback: check geoid prefix
            for col in source_cols:
                val = row.get(col, "").strip()
                if len(val) == 12 and val.startswith(WI_STATE_FIPS):
                    is_wi = True
                    break

        if not is_wi:
            continue

        out: dict = {col: None for col in OUTPUT_COLUMNS}
        for src_col, target_col in col_map.items():
            raw = row.get(src_col, "").strip()
            out[target_col] = raw if raw not in ("", "N/A", "-", "None") else None

        # Normalize geoid to 12 digits (block group)
        if out.get("geoid"):
            out["geoid"] = str(out["geoid"]).zfill(12)

        records.append(out)

    print(f"  Total rows in file : {total_rows:,}")
    print(f"  Wisconsin records  : {len(records):,}")
    return records


def aggregate_to_tract(records: list[dict]) -> list[dict]:
    """
    Population-weighted average aggregation of block group records to tract level.
    Since EJScreen doesn't include population in the main file, we use simple
    averaging as an approximation.  For production use, join ACS population
    before aggregating.
    """
    from collections import defaultdict

    tract_groups: dict[str, list[dict]] = defaultdict(list)
    for rec in records:
        geoid = rec.get("geoid", "")
        if len(geoid) >= 11:
            tract_geoid = geoid[:11]
            tract_groups[tract_geoid].append(rec)

    numeric_cols = [c for c in OUTPUT_COLUMNS if c != "geoid"]
    tract_records: list[dict] = []

    for tract_geoid, bg_recs in sorted(tract_groups.items()):
        out: dict = {"geoid": tract_geoid}
        for col in numeric_cols:
            vals = []
            for r in bg_recs:
                raw = r.get(col)
                if raw is not None:
                    try:
                        vals.append(float(raw))
                    except (ValueError, TypeError):
                        pass
            if vals:
                out[col] = str(round(sum(vals) / len(vals), 4))
            else:
                out[col] = None
        tract_records.append(out)

    return tract_records


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
        print(f"  {col:<15} {null_count:>6} null  ({pct:.1f}%)")


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

    numeric_cols = [c for c in OUTPUT_COLUMNS if c != "geoid"]
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
                "variable_id":    f"epa_ejscreen_{col}",
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
        description="Fetch EPA EJScreen environmental justice data for Wisconsin.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2023,
        help="EJScreen release year to fetch (default: 2023)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print fetch plan without downloading",
    )
    parser.add_argument(
        "--tract",
        action="store_true",
        help="Aggregate block group records to tract level (simple average)",
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

    if args.tract:
        print(f"\nAggregating {len(records):,} block groups to tract level...")
        records = aggregate_to_tract(records)
        print(f"  Aggregated to {len(records):,} tracts")

    write_csv(records)
    null_audit(records)

    if args.load:
        load_to_db(records)

    print("\nDone.")


if __name__ == "__main__":
    main()
