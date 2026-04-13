#!/usr/bin/env python3
"""
Fetch CDC PLACES health indicators for Wisconsin via the Socrata SODA API.

Source: https://data.cdc.gov/resource/cwsq-ngmh.json

Usage:
  python fetch_cdc_places.py --dry-run           # preview only
  python fetch_cdc_places.py                     # fetch and save CSV
  python fetch_cdc_places.py --load              # fetch and load to PostGIS

Auth: No auth required for basic access (1 req/sec without token).
      Set SOCRATA_APP_TOKEN env var for higher rate limits.

Key measures fetched:
  OBESITY   — Adult obesity prevalence
  DIABETES  — Adult diabetes prevalence
  BPHIGH    — High blood pressure prevalence
  MHLTH     — Mental health not good (14+ days/month)
  PHLTH     — Physical health not good (14+ days/month)
  ACCESS2   — No healthcare access (no doctor visit in past year)
  CASTHMA   — Current asthma prevalence
  SMOKING   — Current cigarette smoking prevalence (also CSMOKING)
"""
import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "analysis", "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "wi_health_cdc_places.csv")

SOCRATA_BASE = "https://data.cdc.gov/resource/cwsq-ngmh.json"
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")

WI_STATE_ABBR = "WI"

# Measures to fetch (CrudePrev = crude prevalence estimate)
TARGET_MEASURES = [
    "OBESITY",
    "DIABETES",
    "BPHIGH",
    "MHLTH",
    "PHLTH",
    "ACCESS2",
    "CASTHMA",
    "SMOKING",
    "CSMOKING",
]

# Socrata page size (max 50,000 per request)
PAGE_SIZE = 5000

# Rate limiting: 1 req/sec without token; relax with token
RATE_LIMIT_DELAY_NO_TOKEN = 1.1   # seconds
RATE_LIMIT_DELAY_WITH_TOKEN = 0.2  # seconds

OUTPUT_COLUMNS = [
    "geoid",
    "measure",
    "data_value",
    "low_confidence_limit",
    "high_confidence_limit",
]


def _rate_delay() -> float:
    return RATE_LIMIT_DELAY_WITH_TOKEN if SOCRATA_APP_TOKEN else RATE_LIMIT_DELAY_NO_TOKEN


def print_plan(args: argparse.Namespace) -> None:
    print("[dry-run] CDC PLACES health indicators fetch plan")
    print(f"  Endpoint  : {SOCRATA_BASE}")
    print(f"  State     : {WI_STATE_ABBR} (Wisconsin)")
    print(f"  Measures  : {', '.join(TARGET_MEASURES)}")
    print(f"  Filter    : stateabbr=WI, data_value_type=CrudePrev")
    print(f"  Page size : {PAGE_SIZE}")
    print(f"  App token : {'SET (higher limits)' if SOCRATA_APP_TOKEN else 'NOT SET (1 req/sec)'}")
    print(f"  Output    : {OUTPUT_FILE}")
    print(f"  Columns   : {', '.join(OUTPUT_COLUMNS)}")
    print()
    print("  To set a Socrata app token (free, raises rate limits):")
    print("    export SOCRATA_APP_TOKEN=your_token_here")


def _build_url(measure: str, offset: int) -> str:
    params = {
        "$where": f"stateabbr='{WI_STATE_ABBR}' AND measureid='{measure}' AND data_value_type='CrudePrev'",
        "$limit": str(PAGE_SIZE),
        "$offset": str(offset),
        "$select": "locationid,measureid,data_value,low_confidence_limit,high_confidence_limit",
    }
    if SOCRATA_APP_TOKEN:
        params["$$app_token"] = SOCRATA_APP_TOKEN

    query = urllib.parse.urlencode(params)
    return f"{SOCRATA_BASE}?{query}"


def _fetch_page(url: str) -> list[dict]:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "policy-data-infrastructure/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status} from CDC PLACES API: {url}")
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(
            f"CDC PLACES API HTTP {exc.code}: {exc.reason}\nURL: {url}\nBody: {body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"CDC PLACES API network error: {exc.reason}\nURL: {url}") from exc


def fetch_data(args: argparse.Namespace) -> list[dict]:
    all_records: list[dict] = []
    delay = _rate_delay()

    for measure in TARGET_MEASURES:
        print(f"  Fetching measure: {measure}...", end="", flush=True)
        offset = 0
        measure_records: list[dict] = []

        while True:
            url = _build_url(measure, offset)
            try:
                page = _fetch_page(url)
            except RuntimeError as exc:
                print(f"\n  WARNING: {exc}", file=sys.stderr)
                break

            if not page:
                break

            for row in page:
                geoid = str(row.get("locationid", "")).strip().zfill(11)
                measure_id = str(row.get("measureid", "")).strip()
                data_val = row.get("data_value", "")
                low_ci = row.get("low_confidence_limit", "")
                high_ci = row.get("high_confidence_limit", "")

                measure_records.append({
                    "geoid":                geoid,
                    "measure":              measure_id,
                    "data_value":           _clean_val(data_val),
                    "low_confidence_limit": _clean_val(low_ci),
                    "high_confidence_limit": _clean_val(high_ci),
                })

            offset += len(page)
            time.sleep(delay)

            if len(page) < PAGE_SIZE:
                break

        print(f" {len(measure_records):,} tracts")
        all_records.extend(measure_records)

    print(f"\n  Total records: {len(all_records):,}")
    return all_records


def _clean_val(val) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s if s not in ("", "N/A", "-", "null") else None


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
        print(f"  {col:<35} {null_count:>6} null  ({pct:.1f}%)")

    # Summary by measure
    from collections import Counter
    counts = Counter(r["measure"] for r in records)
    print("\nRecord counts by measure:")
    for measure in sorted(counts):
        print(f"  {measure:<15} {counts[measure]:>5}")


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

    indicators = []
    for rec in records:
        geoid = rec.get("geoid")
        measure = rec.get("measure")
        if not geoid or not measure:
            continue
        raw = rec.get("data_value")
        try:
            val = float(raw) if raw is not None else None
        except (ValueError, TypeError):
            val = None
        low_raw = rec.get("low_confidence_limit")
        high_raw = rec.get("high_confidence_limit")
        try:
            low_val = float(low_raw) if low_raw is not None else None
            high_val = float(high_raw) if high_raw is not None else None
        except (ValueError, TypeError):
            low_val = high_val = None

        # Store as a single indicator row; MOE approximated as half the CI width
        moe = None
        if low_val is not None and high_val is not None:
            moe = round((high_val - low_val) / 2, 4)

        indicators.append({
            "geoid":          geoid,
            "variable_id":    f"cdc_places_{measure.lower()}",
            "vintage":        2022,
            "value":          val,
            "margin_of_error": moe,
            "raw_value":      str(raw or ""),
        })

    n = bulk_load_indicators(conn, indicators)
    conn.close()
    print(f"  {n} indicator rows written to database")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch CDC PLACES health indicators for Wisconsin.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
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

    print("Fetching CDC PLACES health indicators for Wisconsin...")
    records = fetch_data(args)
    write_csv(records)
    null_audit(records)

    if args.load:
        load_to_db(records)

    print("\nDone.")


if __name__ == "__main__":
    main()
