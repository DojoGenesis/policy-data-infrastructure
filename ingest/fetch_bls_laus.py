#!/usr/bin/env python3
"""
Fetch BLS Local Area Unemployment Statistics (LAUS) for all 72 Wisconsin counties.

Source: https://api.bls.gov/publicAPI/v2/timeseries/data/

Usage:
  python fetch_bls_laus.py --dry-run            # preview only
  python fetch_bls_laus.py --year 2023          # fetch and save CSV
  python fetch_bls_laus.py --year 2023 --load   # fetch and load to PostGIS

Series ID format:
  LAUCN{ss}{ccc}0000000{mm}
    ss  = 2-digit state FIPS  (55 for Wisconsin)
    ccc = 3-digit county FIPS
    mm  = measure code:
          03 = unemployment rate
          04 = unemployed count
          05 = employed count
          06 = labor force

BLS API limits:
  - Unregistered: 50 series per request, 25 queries/day
  - Registered (BLS_API_KEY env var): 500 series per request, 500 queries/day

Wisconsin has 72 counties; 72 * 4 measures = 288 series.
With unregistered key (50/request): 6 batches per measure type, or 24 total batches.
With registered key (500/request): 1 batch covers all 288 series.
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
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "wi_unemployment_bls.csv")

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_API_KEY = os.environ.get("BLS_API_KEY", "")

WI_STATE_FIPS = "55"

# BLS API limits
BATCH_SIZE_UNREGISTERED = 50
BATCH_SIZE_REGISTERED = 500
RATE_LIMIT_DELAY = 1.2  # seconds between requests

# Measure codes
MEASURE_UNEMPLOYMENT_RATE = "03"
MEASURE_UNEMPLOYED = "04"
MEASURE_EMPLOYED = "05"
MEASURE_LABOR_FORCE = "06"

OUTPUT_COLUMNS = [
    "geoid",
    "county_name",
    "year",
    "labor_force",
    "employed",
    "unemployed",
    "unemployment_rate",
]

# All 72 Wisconsin counties: (3-digit county FIPS, county name)
WI_COUNTIES: list[tuple[str, str]] = [
    ("001", "Adams County"),
    ("003", "Ashland County"),
    ("005", "Barron County"),
    ("007", "Bayfield County"),
    ("009", "Brown County"),
    ("011", "Buffalo County"),
    ("013", "Burnett County"),
    ("015", "Calumet County"),
    ("017", "Chippewa County"),
    ("019", "Clark County"),
    ("021", "Columbia County"),
    ("023", "Crawford County"),
    ("025", "Dane County"),
    ("027", "Dodge County"),
    ("029", "Door County"),
    ("031", "Douglas County"),
    ("033", "Dunn County"),
    ("035", "Eau Claire County"),
    ("037", "Florence County"),
    ("039", "Fond du Lac County"),
    ("041", "Forest County"),
    ("043", "Grant County"),
    ("045", "Green County"),
    ("047", "Green Lake County"),
    ("049", "Iowa County"),
    ("051", "Iron County"),
    ("053", "Jackson County"),
    ("055", "Jefferson County"),
    ("057", "Juneau County"),
    ("059", "Kenosha County"),
    ("061", "Kewaunee County"),
    ("063", "La Crosse County"),
    ("065", "Lafayette County"),
    ("067", "Langlade County"),
    ("069", "Lincoln County"),
    ("071", "Manitowoc County"),
    ("073", "Marathon County"),
    ("075", "Marinette County"),
    ("077", "Marquette County"),
    ("078", "Menominee County"),
    ("079", "Milwaukee County"),
    ("081", "Monroe County"),
    ("083", "Oconto County"),
    ("085", "Oneida County"),
    ("087", "Outagamie County"),
    ("089", "Ozaukee County"),
    ("091", "Pepin County"),
    ("093", "Pierce County"),
    ("095", "Polk County"),
    ("097", "Portage County"),
    ("099", "Price County"),
    ("101", "Racine County"),
    ("103", "Richland County"),
    ("105", "Rock County"),
    ("107", "Rusk County"),
    ("109", "St. Croix County"),
    ("111", "Sauk County"),
    ("113", "Sawyer County"),
    ("115", "Shawano County"),
    ("117", "Sheboygan County"),
    ("119", "Taylor County"),
    ("121", "Trempealeau County"),
    ("123", "Vernon County"),
    ("125", "Vilas County"),
    ("127", "Walworth County"),
    ("129", "Washburn County"),
    ("131", "Washington County"),
    ("133", "Waukesha County"),
    ("135", "Waupaca County"),
    ("137", "Waushara County"),
    ("139", "Winnebago County"),
    ("141", "Wood County"),
]


def _series_id(county_fips: str, measure: str) -> str:
    """Build a BLS LAUS series ID for a Wisconsin county."""
    return f"LAUCN{WI_STATE_FIPS}{county_fips}0000000{measure}"


def _batch_size() -> int:
    return BATCH_SIZE_REGISTERED if BLS_API_KEY else BATCH_SIZE_UNREGISTERED


def print_plan(args: argparse.Namespace) -> None:
    batch = _batch_size()
    total_series = len(WI_COUNTIES) * 4  # 4 measures per county
    batches = (total_series + batch - 1) // batch
    print("[dry-run] BLS LAUS Wisconsin fetch plan")
    print(f"  API URL   : {BLS_API_URL}")
    print(f"  Year      : {args.year}")
    print(f"  Counties  : {len(WI_COUNTIES)} WI counties")
    print(f"  Measures  : unemployment_rate, unemployed, employed, labor_force")
    print(f"  Series    : {total_series} total")
    print(f"  Batch size: {batch} ({'registered' if BLS_API_KEY else 'unregistered'})")
    print(f"  Batches   : {batches}")
    print(f"  API key   : {'SET' if BLS_API_KEY else 'NOT SET (set BLS_API_KEY for higher limits)'}")
    print(f"  Output    : {OUTPUT_FILE}")
    print(f"  Columns   : {', '.join(OUTPUT_COLUMNS)}")
    print()
    print("  Series ID format: LAUCN{state}{county}0000000{measure}")
    print(f"  Example: {_series_id('025', MEASURE_UNEMPLOYMENT_RATE)} = Dane County unemployment rate")


def _post_bls(series_ids: list[str], year: int) -> dict:
    payload = json.dumps({
        "seriesid": series_ids,
        "startyear": str(year),
        "endyear":   str(year),
        "annualaverage": True,
        **({"registrationkey": BLS_API_KEY} if BLS_API_KEY else {}),
    }).encode("utf-8")

    req = urllib.request.Request(
        BLS_API_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent":   "policy-data-infrastructure/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            if resp.status != 200:
                raise RuntimeError(f"BLS API returned HTTP {resp.status}")
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(
            f"BLS API HTTP {exc.code}: {exc.reason}\nBody: {body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"BLS API network error: {exc.reason}") from exc


def _extract_annual(series_data: dict, year: int) -> float | None:
    """Extract the annual average value from a BLS series result."""
    for entry in series_data.get("data", []):
        if entry.get("year") == str(year) and entry.get("period") == "M13":
            val = entry.get("value", "")
            try:
                return float(val)
            except (ValueError, TypeError):
                return None
    return None


def fetch_data(args: argparse.Namespace) -> list[dict]:
    year = args.year
    batch = _batch_size()

    # Map series_id → (county_fips, measure)
    series_index: dict[str, tuple[str, str]] = {}
    for county_fips, _ in WI_COUNTIES:
        for measure in [MEASURE_LABOR_FORCE, MEASURE_EMPLOYED, MEASURE_UNEMPLOYED, MEASURE_UNEMPLOYMENT_RATE]:
            sid = _series_id(county_fips, measure)
            series_index[sid] = (county_fips, measure)

    all_series = list(series_index.keys())
    total_batches = (len(all_series) + batch - 1) // batch

    print(f"Fetching BLS LAUS for Wisconsin {year}...")
    print(f"  {len(WI_COUNTIES)} counties × 4 measures = {len(all_series)} series")
    print(f"  Batch size: {batch}  →  {total_batches} request(s)")

    # Collect raw data keyed by series_id
    raw: dict[str, float | None] = {}

    for i in range(0, len(all_series), batch):
        chunk = all_series[i : i + batch]
        batch_num = i // batch + 1
        print(f"  Batch {batch_num}/{total_batches} ({len(chunk)} series)...", end="", flush=True)

        try:
            resp = _post_bls(chunk, year)
        except RuntimeError as exc:
            print(f"\n  WARNING: batch {batch_num} failed — {exc}", file=sys.stderr)
            for sid in chunk:
                raw[sid] = None
            time.sleep(RATE_LIMIT_DELAY)
            continue

        status = resp.get("status", "UNKNOWN")
        series_list = resp.get("Results", {}).get("series", [])
        print(f" status={status}, got {len(series_list)} series")

        if status not in ("REQUEST_SUCCEEDED", "REQUEST_FAILED_OVER_LIMIT"):
            messages = resp.get("message", [])
            if messages:
                print(f"  API messages: {messages}", file=sys.stderr)

        for s in series_list:
            sid = s.get("seriesID", "")
            raw[sid] = _extract_annual(s, year)

        time.sleep(RATE_LIMIT_DELAY)

    # Assemble output records keyed by county
    county_lookup: dict[str, str] = {fips: name for fips, name in WI_COUNTIES}
    records: list[dict] = []

    for county_fips, county_name in WI_COUNTIES:
        geoid = WI_STATE_FIPS + county_fips  # 5-digit county GEOID

        labor_force = raw.get(_series_id(county_fips, MEASURE_LABOR_FORCE))
        employed    = raw.get(_series_id(county_fips, MEASURE_EMPLOYED))
        unemployed  = raw.get(_series_id(county_fips, MEASURE_UNEMPLOYED))
        unemp_rate  = raw.get(_series_id(county_fips, MEASURE_UNEMPLOYMENT_RATE))

        records.append({
            "geoid":             geoid,
            "county_name":       county_name,
            "year":              year,
            "labor_force":       _fmt(labor_force),
            "employed":          _fmt(employed),
            "unemployed":        _fmt(unemployed),
            "unemployment_rate": _fmt(unemp_rate),
        })

    populated = sum(1 for r in records if r["unemployment_rate"] is not None)
    print(f"\n  {len(records)} county records ({populated} with unemployment rate data)")
    return records


def _fmt(val: float | None) -> str | None:
    if val is None:
        return None
    return str(val)


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

    year_val = records[0]["year"] if records else 0
    numeric_cols = ["labor_force", "employed", "unemployed", "unemployment_rate"]
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
                "variable_id":    f"bls_laus_{col}",
                "vintage":        int(year_val),
                "value":          val,
                "margin_of_error": None,
                "raw_value":      str(raw or ""),
            })

    n = bulk_load_indicators(conn, indicators)
    conn.close()
    print(f"  {n} indicator rows written to database")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch BLS LAUS unemployment data for all 72 Wisconsin counties.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2023,
        help="Year to fetch annual averages for (default: 2023)",
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
