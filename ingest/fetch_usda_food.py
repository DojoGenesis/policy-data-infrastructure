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
    "snap_count",
    "population",
]

# Mapping of USDA ERS source column names → our output column names.
#
# ONE source column per target, pinned to the FARA 2019 release. The old
# table aliased both counts (lapop10) and shares (lapop10share) into the
# same target, so which semantics loaded depended on CSV column order —
# the same silent-vocabulary failure class as ADR-014 F4. If ERS renames
# a column in a future vintage, add the ONE new name deliberately and
# remove the old, never both.
COLUMN_ALIASES: dict[str, str] = {
    # GEOID (2010-vintage tract — the whole reason the crosswalk exists)
    "CensusTract":        "geoid",
    # county name
    "County":             "county_name",
    # urban designation flag (0/1)
    "Urban":              "urban_flag",
    # low-access population COUNT at 1 mile (urban) — people, not a share
    "lapop1":             "low_access_1mi",
    # low-access population COUNT at 10 miles (rural)
    "lapop10":            "low_access_10mi",
    # LILA designation flag (low income + low access at 1 and 10 miles)
    "LILATracts_1And10":  "low_income_low_access",
    # SNAP-recipient household COUNT
    "TractSNAP":          "snap_count",
    # 2010 census tract population (FARA's own denominator)
    "Pop2010":            "population",
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
            # FARA writes the literal string "NULL" for missing values —
            # a raw-string audit that misses it reports 0% null while the
            # measure is genuinely sparse (lapop10 exists only for tracts
            # measured at the rural threshold).
            out[target_col] = raw if raw not in ("", "N/A", "-", ".", "NULL", "null") else None

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


# ── crosswalk-aware load (ADR-014 OQ6, resolved 2026-08-08) ────────────────
#
# FARA 2019 is keyed to 2010 tracts; the platform's geographies are 2020.
# Loading "by identifier" would drop 127 tracts loudly and mis-place an
# unknown number silently. Instead every tract-level value crosses the
# population-weighted crosswalk built by build_tract_crosswalk.py, under
# per-class rules (the D7 discipline, applied to boundary translation):
#
#   count — allocate: value(t20) = Σ value(t10) × weight(t10→t20)
#   flag  — population share: value(t20) = Σ alloc_pop×flag / Σ alloc_pop,
#           i.e. "share of this 2020 tract's (2010-allocated) population
#           living in tracts that carried the designation". A 0/1 becomes
#           a 0..1 share and the metadata SAYS so — a translated flag
#           presented as a crisp designation would be a caveated-wrong.
#
# County rows are computed straight from the 2010 tracts (WI county
# boundaries are stable across the vintages), so they carry no crosswalk
# error at all: counts sum; flags become population shares of the county.

VINTAGE = "USDA-FARA-2019"
CROSSWALK_FILE = os.path.join(
    SCRIPT_DIR, "..", "data", "crosswalks", "wi_tract2010_tract2020.csv"
)

# variable_id → (output column, class). usda_food_desert keeps its
# long-registered id; it is the LILA 1-and-10 designation.
LOAD_PLAN: dict[str, tuple[str, str]] = {
    "usda_food_low_access_1mi":  ("low_access_1mi", "count"),
    "usda_food_low_access_10mi": ("low_access_10mi", "count"),
    "usda_food_snap_count":      ("snap_count", "count"),
    "usda_food_population":      ("population", "count"),
    "usda_food_desert":          ("low_income_low_access", "flag"),
}

INDICATOR_META: dict[str, dict] = {
    "usda_food_low_access_1mi": {
        "source_id": "usda-food", "unit": "count", "direction": "lower_better",
        "name": "Low-Access Population (1-Mile Urban)",
        "description": "People beyond 1 mile (urban) from a supermarket, FARA 2019. Computed only for tracts FARA measures at the 1-mile threshold — absent elsewhere by design, not missing. Tract values crosswalked 2010→2020 boundaries by population-weighted allocation (see data/crosswalks/wi_tract2010_tract2020.meta.json); county values summed directly from source tracts.",
    },
    "usda_food_low_access_10mi": {
        "source_id": "usda-food", "unit": "count", "direction": "lower_better",
        "name": "Low-Access Population (10-Mile Rural)",
        "description": "People beyond 10 miles (rural) from a supermarket, FARA 2019. Computed only for tracts FARA measures at the 10-mile (rural) threshold — sparse by design (279 of 1,542 WI 2020 tracts). Same crosswalk treatment as the 1-mile measure.",
    },
    "usda_food_snap_count": {
        "source_id": "usda-food", "unit": "count", "direction": "neutral",
        "name": "SNAP Recipients",
        "description": "SNAP-recipient households (TractSNAP), FARA 2019, crosswalked 2010→2020 boundaries by population-weighted allocation.",
    },
    "usda_food_population": {
        "source_id": "usda-food", "unit": "count", "direction": "neutral",
        "name": "Population (FARA 2010 base)",
        "description": "FARA's own 2010 census tract population (POP2010) — the denominator its access measures were computed against. Crosswalked to 2020 boundaries; kept so rates derived from FARA counts use FARA's denominator, not a mismatched vintage.",
    },
    "usda_food_desert": {
        "source_id": "usda-food", "unit": "percent_share", "direction": "lower_better",
        "name": "Food Desert Designation (population share)",
        "description": "Share (0–1) of the geography's population living in tracts designated low-income-low-access at 1 and 10 miles (LILATracts_1And10, FARA 2019, 2010 boundaries). NOT a crisp 0/1 at 2020 boundaries: the designation was made on 2010 tracts, so the translated value is the population share under it.",
    },
}


def _load_crosswalk() -> dict[str, list[tuple[str, float, float]]]:
    """tract2010 → [(tract2020, weight, allocated_pop)]."""
    if not os.path.exists(CROSSWALK_FILE):
        print(
            f"  ERROR: crosswalk missing at {CROSSWALK_FILE}\n"
            f"  Run: python ingest/build_tract_crosswalk.py",
            file=sys.stderr,
        )
        sys.exit(1)
    xwalk: dict[str, list[tuple[str, float, float]]] = {}
    with open(CROSSWALK_FILE, newline="") as f:
        for row in csv.DictReader(f):
            xwalk.setdefault(row["tract2010"], []).append(
                (row["tract2020"], float(row["weight"]), float(row["allocated_pop2010"]))
            )
    return xwalk


def _fnum(raw) -> float | None:
    try:
        return float(raw) if raw is not None else None
    except (ValueError, TypeError):
        return None


def load_to_db(records: list[dict]) -> None:
    """Crosswalk tract values to 2020 boundaries, roll up counties, load both."""
    try:
        import sys as _sys
        _sys.path.insert(0, SCRIPT_DIR)
        from lib.db import get_conn, bulk_load_indicators, upsert_indicator_meta
    except ImportError as exc:
        print(f"  ERROR: Cannot import lib.db — {exc}", file=sys.stderr)
        sys.exit(1)

    xwalk = _load_crosswalk()
    unmatched = [r["geoid"] for r in records if r.get("geoid") and r["geoid"] not in xwalk]
    if unmatched:
        print(
            f"  ERROR: {len(unmatched)} source tracts missing from the crosswalk "
            f"(first: {unmatched[:3]}) — regenerate it before loading.",
            file=sys.stderr,
        )
        sys.exit(1)

    indicators: list[dict] = []
    conservation: dict[str, tuple[float, float, float]] = {}
    pop_by_tract: dict[str, float] = {
        r["geoid"]: (_fnum(r.get("population")) or 0.0)
        for r in records if r.get("geoid")
    }

    for var_id, (col, cls) in LOAD_PLAN.items():
        # Source values by 2010 tract.
        src: dict[str, float | None] = {}
        for rec in records:
            g = rec.get("geoid")
            if g:
                src[g] = _fnum(rec.get(col))

        # County rollup (boundary-stable, no crosswalk error).
        county_num: dict[str, float] = {}
        county_pop: dict[str, float] = {}
        # 2020-tract translation.
        t20_num: dict[str, float] = {}
        t20_pop: dict[str, float] = {}

        for t10, val in src.items():
            county = t10[:5]
            pop10 = pop_by_tract.get(t10, 0.0)
            if val is not None:
                if cls == "count":
                    county_num[county] = county_num.get(county, 0.0) + val
                else:  # flag → population share
                    county_num[county] = county_num.get(county, 0.0) + pop10 * val
                    county_pop[county] = county_pop.get(county, 0.0) + pop10
                for t20, w, apop in xwalk[t10]:
                    if cls == "count":
                        t20_num[t20] = t20_num.get(t20, 0.0) + val * w
                    else:
                        t20_num[t20] = t20_num.get(t20, 0.0) + apop * val
                        t20_pop[t20] = t20_pop.get(t20, 0.0) + apop
            elif cls == "flag":
                # A tract with an unknown flag still contributes population to
                # nothing — its people are simply absent from the share, and
                # the denominator must not pretend otherwise.
                continue

        source_total = sum(v for v in src.values() if v is not None)
        rows = 0
        for county, num in sorted(county_num.items()):
            value = num if cls == "count" else (num / county_pop[county] if county_pop.get(county) else None)
            if value is None:
                continue
            indicators.append({
                "geoid": county, "variable_id": var_id, "vintage": VINTAGE,
                "value": value, "margin_of_error": None,
                "raw_value": f"county_{'sum' if cls == 'count' else 'pop_share'}",
            })
            rows += 1
        for t20, num in sorted(t20_num.items()):
            value = num if cls == "count" else (num / t20_pop[t20] if t20_pop.get(t20) else None)
            if value is None:
                continue
            indicators.append({
                "geoid": t20, "variable_id": var_id, "vintage": VINTAGE,
                "value": value, "margin_of_error": None,
                "raw_value": f"xwalk_{'alloc' if cls == 'count' else 'pop_share'}",
            })
            rows += 1

        if cls == "count":
            conservation[var_id] = (
                source_total,
                sum(county_num.values()),
                sum(t20_num.values()),
            )
        print(f"  {var_id:<28} {cls:<5} → {rows} rows")

    print("\nConservation (counts must survive both translations):")
    ok = True
    for var_id, (src_t, cty_t, t20_t) in conservation.items():
        drift_cty = abs(cty_t - src_t)
        drift_t20 = abs(t20_t - src_t)
        flag = "ok" if drift_cty < 1.0 and drift_t20 < 1.0 else "FAIL"
        if flag == "FAIL":
            ok = False
        print(f"  {var_id:<28} source {src_t:>12,.0f}  county {cty_t:>12,.0f}  tract20 {t20_t:>12,.0f}  [{flag}]")
    if not ok:
        print("  ABORT: counts were not conserved across translation.", file=sys.stderr)
        sys.exit(1)

    print("\nConnecting to database...")
    conn = get_conn()
    n_meta = upsert_indicator_meta(conn, INDICATOR_META)
    print(f"  {n_meta} indicator_meta rows upserted")
    n = bulk_load_indicators(conn, indicators)
    conn.close()
    print(f"  {n} indicator rows written to database")
    print("  Remember: REFRESH MATERIALIZED VIEW CONCURRENTLY indicators_latest;")


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
