#!/usr/bin/env python3
"""
Fetch FBI NIBRS crime data from the FBI Crime Data Explorer (CDE).

Source: https://cde.ucr.cjis.gov/LATEST/webapp/
API:    https://api.usa.gov/crime/fbi/cde/ (requires api.data.gov key)

Usage:
  python fetch_fbi_nibrs.py --dry-run            # preview only
  python fetch_fbi_nibrs.py --year 2023          # fetch and save CSV
  python fetch_fbi_nibrs.py --year 2023 --load   # fetch and load to PostGIS

Data:
  The FBI CDE provides NIBRS estimation data at state and county levels.
  Key indicators: violent_crime_rate (per 100K), property_crime_rate (per 100K).

  Primary path: NIBRS Estimation summary tables via the CDE API.
  Fallback: CSV bulk downloads from the CDE webapp data downloads page.

API key:
  The FBI CDE API is behind the federal api.data.gov gateway.
  Sign up at https://api.data.gov/signup/ and set the FBI_CDE_API_KEY env var.
  Without a key, the API returns HTTP 403.

  TODO: When the API key is configured, the script will:
    1. Query /nibrs-estimations/counties/{year} for county-level estimates
    2. Parse violent and property crime rates from the response
    3. Alternatively, download pre-aggregated CSV tables from:
       https://cde.ucr.cjis.gov/LATEST/webapp/ (data downloads section)

Null handling:
  The FBI may suppress data for small populations (e.g., < 10 incidents).
  Suppressed values appear as -1, null, or empty strings.
  The script converts these to Python None for the null audit.
"""
import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "analysis", "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "fbi_nibrs_crime.csv")

FBI_CDE_BASE = "https://api.usa.gov/crime/fbi/cde"
FBI_CDE_API_KEY = os.environ.get("FBI_CDE_API_KEY", "")

# Rate limiting
RATE_LIMIT_DELAY = 2.0  # seconds between requests (30 req/min safe floor)

OUTPUT_COLUMNS = [
    "geoid",
    "county_name",
    "state_abbr",
    "year",
    "violent_crime_rate",
    "property_crime_rate",
    "population",
]

# Suppression sentinels (FBI marks small-population data as unavailable)
SUPPRESSION_SENTINELS = frozenset({-1, -9999, None, ""})


def print_plan(args: argparse.Namespace) -> None:
    print("[dry-run] FBI NIBRS crime data fetch plan")
    print(f"  API Base  : {FBI_CDE_BASE}")
    print(f"  Year      : {args.year}")
    print(f"  Scope     : national (all counties with NIBRS estimates)")
    print(f"  Variables : violent_crime_rate, property_crime_rate")
    print(f"  API key   : {'SET' if FBI_CDE_API_KEY else 'NOT SET (set FBI_CDE_API_KEY for access)'}")
    print(f"  Output    : {OUTPUT_FILE}")
    print(f"  Columns   : {', '.join(OUTPUT_COLUMNS)}")
    print()
    if not FBI_CDE_API_KEY:
        print("  WARNING: FBI_CDE_API_KEY is not set. The api.usa.gov gateway requires")
        print("  an API key. Sign up at https://api.data.gov/signup/ to obtain one.")
        print("  Alternative: download CSV tables manually from")
        print("  https://cde.ucr.cjis.gov/LATEST/webapp/ and use --csv-input <path>")
    print()
    print("  Endpoint: /nibrs-estimations/counties/{year}")
    print("  Returns county-level estimates of violent and property crime.")
    print("  GEOIDs are 5-digit county FIPS codes.")


def _api_headers() -> dict[str, str]:
    """Return HTTP headers for FBI CDE API requests."""
    headers = {
        "Accept": "application/json",
        "User-Agent": "policy-data-infrastructure/1.0",
    }
    if FBI_CDE_API_KEY:
        headers["X-Api-Key"] = FBI_CDE_API_KEY
    return headers


def _get_json(endpoint: str) -> dict:
    """
    Fetch a JSON response from the FBI CDE API.

    TODO: Wire the actual endpoint URL construction once the API key is
    available and the exact endpoint path is confirmed. The FBI CDE API
    uses the federal api.data.gov gateway; valid endpoints include:
      - /nibrs-estimations/counties/{year}  (county estimates)
      - /summarized/agencies/offenses       (agency-level, needs ORI)
    """
    url = f"{FBI_CDE_BASE}{endpoint}"
    req = urllib.request.Request(url, headers=_api_headers())

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status != 200:
                raise RuntimeError(f"FBI CDE API returned HTTP {resp.status}")
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(
            f"FBI CDE API HTTP {exc.code}: {exc.reason}\nBody: {body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"FBI CDE API network error: {exc.reason}") from exc


def _is_suppressed(val) -> bool:
    """Return True if the value is an FBI suppression sentinel."""
    if val is None or val == "":
        return True
    try:
        num = float(val)
        return int(num) in SUPPRESSION_SENTINELS or num < 0
    except (ValueError, TypeError):
        return True


def _safe_float(val) -> float | None:
    """Parse a float, returning None for suppressed/invalid values."""
    if _is_suppressed(val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    """Parse an int, returning None for suppressed/invalid values."""
    if _is_suppressed(val):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def fetch_data(args: argparse.Namespace) -> list[dict]:
    """
    Fetch FBI NIBRS crime data for the given year.

    TODO: Wire the actual HTTP fetch once the FBI CDE API key is available.
    The current implementation returns an empty list with a diagnostic message.

    Planned API flow when key is configured:
      1. GET /nibrs-estimations/counties/{year} → list of county estimates
      2. Each record contains: county FIPS (GEOID), violent crime rate,
         property crime rate, and population.
      3. Parse, apply suppression checks, and assemble output records.
    """
    year = args.year

    if not FBI_CDE_API_KEY:
        print(
            "ERROR: FBI_CDE_API_KEY environment variable is not set.",
            file=sys.stderr,
        )
        print(
            "The FBI CDE API (api.usa.gov/crime/fbi/cde/) requires an api.data.gov key.",
            file=sys.stderr,
        )
        print(
            "Sign up at https://api.data.gov/signup/, then set FBI_CDE_API_KEY.",
            file=sys.stderr,
        )
        print(
            "Alternative: download CSV from https://cde.ucr.cjis.gov/LATEST/webapp/",
            file=sys.stderr,
        )
        print(
            "and place it at analysis/output/fbi_nibrs_county_{year}.csv, "
            "then re-run with --csv-input to load from local file.",
            file=sys.stderr,
        )
        # Return empty — caller should check and exit.
        return []

    print(f"Fetching FBI NIBRS crime estimates for {year}...")

    # --- TODO: Wire the actual API call here ---
    # Example structure:
    #
    #   try:
    #       resp = _get_json(f"/nibrs-estimations/counties/{year}")
    #   except RuntimeError as exc:
    #       print(f"  ERROR: {exc}", file=sys.stderr)
    #       return []
    #
    #   data = resp.get("data", resp.get("results", []))
    #   for row in data:
    #       geoid = str(row.get("county_fips", "")).zfill(5)
    #       ...
    #
    # For now, return an empty list with a diagnostic message.
    # ----------------------------------------------------------

    print("  TODO: API fetch not yet wired. Set FBI_CDE_API_KEY and implement")
    print("  the _get_json() call to /nibrs-estimations/counties/{year}.")
    print("  See fetch_fbi_nibrs.py for TODO markers and planned structure.")
    return []


def _fmt(val: float | int | None) -> str | None:
    """Format a value for CSV output; None → empty string."""
    if val is None:
        return None
    return str(val)


def write_csv(records: list[dict]) -> None:
    if not records:
        print("  No records to write.")
        return
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(records)
    print(f"  Wrote {len(records):,} rows to {OUTPUT_FILE}")


def null_audit(records: list[dict]) -> None:
    if not records:
        print("\nNull audit: no records to audit.")
        return
    print("\nNull audit:")
    fail = False
    for col in OUTPUT_COLUMNS:
        null_count = sum(1 for r in records if r.get(col) is None)
        pct = null_count / len(records) * 100 if records else 0
        flag = ""
        if col in ("violent_crime_rate", "property_crime_rate") and pct > 30:
            flag = "  ← FAIL (>30% null)"
            fail = True
        print(f"  {col:<35} {null_count:>5} null  ({pct:.1f}%){flag}")
    if fail:
        print(
            "\n  NULL AUDIT FAILURE: >30% null for primary indicator(s). "
            "Do NOT load to DB.",
            file=sys.stderr,
        )
    else:
        print("  Null audit: PASS")


def load_to_db(records: list[dict]) -> None:
    """Load records to PostGIS via db.py."""
    if not records:
        print("  No records to load.")
        return

    try:
        sys.path.insert(0, SCRIPT_DIR)
        from lib.db import get_conn, bulk_load_indicators
    except ImportError as exc:
        print(f"  ERROR: Cannot import lib.db — {exc}", file=sys.stderr)
        sys.exit(1)

    print("Connecting to database...")
    conn = get_conn()

    year_val = records[0]["year"] if records else 0
    numeric_cols = ["violent_crime_rate", "property_crime_rate"]
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
                "geoid": geoid,
                "variable_id": f"fbi_nibrs_{col}",
                "vintage": int(year_val),
                "value": val,
                "margin_of_error": None,
                "raw_value": str(raw or ""),
            })

    n = bulk_load_indicators(conn, indicators)
    conn.close()
    print(f"  {n} indicator rows written to database")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch FBI NIBRS crime data for US counties.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2023,
        help="Year to fetch (default: 2023)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print fetch plan without making API calls",
    )
    parser.add_argument(
        "--load",
        action="store_true",
        help="Load records to PostGIS after CSV write (requires PDI_DATABASE_URL)",
    )
    parser.add_argument(
        "--csv-input",
        type=str,
        default="",
        help="Path to a pre-downloaded CSV from the CDE webapp (bypasses API fetch)",
    )
    args = parser.parse_args()

    if args.dry_run:
        print_plan(args)
        return

    if args.csv_input:
        # Load from a pre-downloaded CSV (manual CDE webapp download).
        print(f"Loading from local CSV: {args.csv_input}")
        records = []
        try:
            with open(args.csv_input, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    geoid = str(row.get("county_fips", row.get("geoid", ""))).zfill(5)
                    if not geoid or geoid == "00000":
                        continue
                    records.append({
                        "geoid": geoid,
                        "county_name": row.get("county_name", ""),
                        "state_abbr": row.get("state_abbr", ""),
                        "year": args.year,
                        "violent_crime_rate": _safe_float(
                            row.get("violent_crime_rate", row.get("violent_rate"))
                        ),
                        "property_crime_rate": _safe_float(
                            row.get("property_crime_rate", row.get("property_rate"))
                        ),
                        "population": _safe_int(row.get("population")),
                    })
        except FileNotFoundError:
            print(f"ERROR: File not found: {args.csv_input}", file=sys.stderr)
            sys.exit(1)
        except Exception as exc:
            print(f"ERROR: Failed to parse CSV: {exc}", file=sys.stderr)
            sys.exit(1)

        print(f"  Parsed {len(records)} county records from CSV")
    else:
        records = fetch_data(args)

    if not records:
        print("No records fetched. Exiting.")
        sys.exit(1)

    write_csv(records)
    null_audit(records)

    if args.load:
        load_to_db(records)

    print("\nDone.")


if __name__ == "__main__":
    main()
