#!/usr/bin/env python3
"""
Fetch CDC/ATSDR Social Vulnerability Index (SVI) data.

Source: https://www.atsdr.cdc.gov/placeandhealth/svi/data_documentation_download.html

The CDC SVI ranks census tracts and counties on 16 social factors grouped into
four themes, producing an overall vulnerability percentile and four theme-level
percentiles on a 0.0–1.0 scale (higher = more vulnerable).

Usage:
  python fetch_cdc_svi.py --dry-run                         # preview only
  python fetch_cdc_svi.py --year 2022 --level county        # fetch and save CSV
  python fetch_cdc_svi.py --year 2022 --level county --load # fetch and load to DB

Biennial releases: 2020, 2022 (2024 may be available as of early 2025).
Direct CSV download — no API key required.
"""
import argparse
import csv
import os
import sys
import urllib.error
import urllib.request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "analysis", "output")

# CDC SVI download endpoint.
#
# REPOINTED 2026-07-30. The previous base — https://svi.cdc.gov/Documents/Data
# with static filenames like SVI_2022_US_county.csv — is dead. Every path under
# it 404s, which is why this source had metadata registered in indicator_meta
# and ZERO rows in indicators: the script had never successfully fetched.
#
# CDC moved the downloads to a React app on a NEW HOST (svi2.cdc.gov) that
# serves files from a query-parameter webapi rather than static paths. The new
# host is a single-page app, so it answers 200 with its HTML shell for ANY
# path — probing it with a HEAD/GET status check reports success while
# returning no data at all. Same shape as the Census keyless-redirect trap
# CLAUDE.md documents: the failure presents as a parse error on HTML, not as
# a 404. Verify a fetch by Content-Type and by parsing a row, never by status.
#
# Endpoint (extracted from the app bundle at
# https://svi2.cdc.gov/data-downloads/assets/main-*.js):
#
#   https://svi2.cdc.gov/webapi/Documents/download
#       ?year=<YYYY>&type=csv&category=<category>&name=<NAME>
#
#   category=states_counties  name=<STATE>_COUNTY   -> county rows
#   category=states           name=<STATE>          -> tract rows
#   category=zcta             name=<STATE>_ZCTA     -> ZCTA rows (2022 only)
#   nationwide uses name=SVI_<year>_US
#
# NAME is uppercased and space-free ("NEWYORK", "DISTRICTOFCOLUMBIA").
# Verified 2026-07-30: Wisconsin county -> 72 data rows, tract -> 1,528 rows,
# both Content-Type: text/csv.
#
# Ranking scope matters and is why this is fetched per state rather than
# sliced from the national file: within a state download, tracts/counties are
# ranked against others IN THAT STATE. The national file ranks against the
# whole country. Those are different numbers with the same column names.
SVI_BASE_URL = "https://svi2.cdc.gov/webapi/Documents/download"

# Category + name-suffix per geography level.
SVI_CATEGORY = {
    "county": ("states_counties", "_COUNTY"),
    "tract":  ("states", ""),
}

# Years the endpoint serves. ZCTA exists only for 2022.
SVI_YEARS = [2022, 2020, 2018, 2016, 2014, 2010, 2000]


def svi_url(year: int, level: str, state_name: str = "WISCONSIN") -> str:
    """Build the CDC SVI download URL for a state at a geography level."""
    category, suffix = SVI_CATEGORY[level]
    name = (state_name + suffix).upper().replace(" ", "")
    return f"{SVI_BASE_URL}?year={year}&type=csv&category={category}&name={name}"

# CDC SVI column names → PDI variable IDs and display labels.
# E_TOTPOP is total population for denominator checks.
# EPL_ = percentile ranks for each theme + overall.
VARIABLE_MAP = {
    "EPL_POV":     "cdc_svi_socioeconomic",
    "EPL_UNEMP":   None,   # sub-component — not a top-level variable
    "EPL_PCI":     None,   # sub-component
    "EPL_NOHSDP":  None,   # sub-component
    "EPL_AGE65":   None,   # sub-component — theme 2
    "EPL_AGE17":   None,   # sub-component
    "EPL_DISABL":  None,   # sub-component
    "EPL_SNGPNT":  None,   # sub-component
    "EPL_LIMENG":  None,   # sub-component
    "EPL_MINRTY":  "cdc_svi_racial_ethnic",
    "EPL_MUNIT":   None,   # sub-component — theme 4
    "EPL_MOBILE":  None,   # sub-component
    "EPL_CROWD":   None,   # sub-component
    "EPL_NOVEH":   None,   # sub-component
    "EPL_GROUPQ":  None,   # sub-component
    # Theme-level and overall percentiles (derived from sub-components above)
    "RPL_THEME1":  "cdc_svi_socioeconomic",
    "RPL_THEME2":  "cdc_svi_household",
    "RPL_THEME3":  "cdc_svi_racial_ethnic",
    "RPL_THEME4":  "cdc_svi_housing_transport",
    "RPL_THEMES":  "cdc_svi_overall",
    # Estimate columns (not percentiles)
    "E_TOTPOP":    "svi_total_population",
}

# The canonical PDI variable IDs for CDC SVI: five percentile-rank themes
# plus the tract population estimate. The population is why this source can
# serve as the tract→county weighting denominator — E_TOTPOP shares its 2022
# vintage with CDC PLACES rates, which is exactly the same-vintage
# denominator ADR-014 F3 found missing (handoff 2026-08-02 item 3).
PDI_VARIABLES: list[str] = [
    "cdc_svi_overall",
    "cdc_svi_socioeconomic",
    "cdc_svi_household",
    "cdc_svi_racial_ethnic",
    "cdc_svi_housing_transport",
    "svi_total_population",
]

# CDC SVI column name → PDI variable ID mapping (canonical columns for each year).
# 2022 uses RPL_THEME*; 2020 uses RPL_THEME* as well but column ordering differs.
# We map the RPL_THEME* columns to PDI variables.
COLUMN_MAP = {
    "RPL_THEMES": "cdc_svi_overall",
    "RPL_THEME1": "cdc_svi_socioeconomic",
    "RPL_THEME2": "cdc_svi_household",
    "RPL_THEME3": "cdc_svi_racial_ethnic",
    "RPL_THEME4": "cdc_svi_housing_transport",
    "E_TOTPOP":   "svi_total_population",
}

# Margin-of-error companions: source column → the PDI variable whose
# margin_of_error it fills. Parsed alongside COLUMN_MAP, stored in the
# indicators.margin_of_error column (never as separate variables).
MOE_COLUMN_MAP = {
    "M_TOTPOP": "svi_total_population",
}

# GEOID column varies by geography level:
#   county → FIPS (5-digit county FIPS)
#   tract  → FIPS (11-digit tract FIPS — same column name in the CSV)
GEOID_COL = "FIPS"


def print_plan(args: argparse.Namespace) -> None:
    """Print a fetch plan without making any network calls."""
    year = args.year
    level = args.level

    if year not in SVI_YEARS:
        print(f"[dry-run] ERROR: Year {year} not in known SVI release years: "
              f"{sorted(SVI_YEARS)}")
        sys.exit(1)

    if level not in SVI_CATEGORY:
        print(f"[dry-run] ERROR: Level '{level}' not supported. "
              f"Available: {sorted(SVI_CATEGORY.keys())}")
        sys.exit(1)

    url = svi_url(year, level)
    output_file = os.path.join(OUTPUT_DIR, f"svi_{year}_{level}.csv")

    print("[dry-run] CDC SVI fetch plan")
    print(f"  URL       : {url}")
    print(f"  Year      : {year}")
    print(f"  Geo level : {level}")
    print(f"  Variables : {', '.join(PDI_VARIABLES)}")
    print(f"  Output    : {output_file}")
    print(f"  Rate limit: ~10 req/min (direct download, server courtesy limit)")
    print()
    print("  CDC SVI themes:")
    print("    Theme 1 — Socioeconomic Status (poverty, unemployment, income, education)")
    print("    Theme 2 — Household Characteristics (age 65+, age 17-, disability, single-parent, English)")
    print("    Theme 3 — Racial & Ethnic Minority Status (all non-White non-Hispanic groups)")
    print("    Theme 4 — Housing Type & Transportation (multi-unit, mobile, crowding, no vehicle, group quarters)")
    print("  All values are 0.0–1.0 percentile ranks (higher = more vulnerable).")
    print()


def fetch_data(args: argparse.Namespace) -> list[dict]:
    """Download and parse the CDC SVI CSV file."""
    year = args.year
    level = args.level

    if year not in SVI_YEARS:
        raise ValueError(f"Year {year} not in known SVI release years: {sorted(SVI_YEARS)}")

    if level not in SVI_CATEGORY:
        raise ValueError(f"Level '{level}' not supported: {sorted(SVI_CATEGORY.keys())}")

    url = svi_url(year, level)
    stream = None

    print(f"Fetching CDC SVI {year} ({level}-level)...")
    print(f"  URL: {url}")

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "policy-data-infrastructure/1.0"})
        resp = urllib.request.urlopen(req, timeout=120)
        raw = resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            print(f"  WARNING: SVI {year} {level} CSV not found at {url}", file=sys.stderr)
            print(f"  The CDC may have moved the file or the vintage year may not be released yet.", file=sys.stderr)
            print(f"  Check https://www.atsdr.cdc.gov/placeandhealth/svi/data_documentation_download.html", file=sys.stderr)
            return []
        raise RuntimeError(f"HTTP {exc.code} fetching {url}: {exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Network error fetching {url}: {exc.reason}") from exc

    # CDC SVI CSVs are UTF-8 with BOM and sometimes latin-1 characters.
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("latin-1")

    lines = text.splitlines()
    if not lines:
        return []

    reader = csv.DictReader(lines)
    records: list[dict] = []
    skipped_geoid = 0
    skipped_value = 0

    for row in reader:
        geoid = row.get(GEOID_COL, "").strip()
        if not geoid:
            skipped_geoid += 1
            continue

        # Normalize GEOID to PDI convention (zero-padded strings).
        geoid = geoid.zfill({
            "county": 5,
            "tract":  11,
        }.get(level, len(geoid)))

        record = {"geoid": geoid}

        for src_col, pdi_var in COLUMN_MAP.items():
            raw_val = row.get(src_col, "").strip()
            if raw_val == "" or raw_val == "-999" or raw_val.lower() in ("null", "none", "na", "n/a"):
                record[pdi_var] = None
                skipped_value += 1
                continue

            try:
                val = float(raw_val)
                # Percentile columns are 0.0–1.0; E_TOTPOP is a raw count.
                # Either way the only invalid values are the -999 sentinels.
                if val < -0.001:
                    record[pdi_var] = None
                    skipped_value += 1
                else:
                    record[pdi_var] = val
            except (ValueError, TypeError):
                record[pdi_var] = None
                skipped_value += 1

        # Margin-of-error companions (kept beside, not among, the variables).
        for src_col, pdi_var in MOE_COLUMN_MAP.items():
            raw_val = row.get(src_col, "").strip()
            moe_key = pdi_var + "__moe"
            record[moe_key] = None
            if raw_val and raw_val != "-999" and raw_val.lower() not in ("null", "none", "na", "n/a"):
                try:
                    moe = float(raw_val)
                    if moe >= 0:
                        record[moe_key] = moe
                except (ValueError, TypeError):
                    pass

        records.append(record)

    print(f"  Parsed {len(records):,} {level} records")
    if skipped_geoid:
        print(f"  Skipped {skipped_geoid} rows with missing GEOID")
    if skipped_value:
        print(f"  Skipped {skipped_value} null/missing values across all variables")
    return records


def write_csv(records: list[dict], args: argparse.Namespace) -> str:
    """Write records to a CSV file; return the output path."""
    if not records:
        print("  No records to write.")
        return ""

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f"svi_{args.year}_{args.level}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)

    fieldnames = ["geoid"] + PDI_VARIABLES + [v + "__moe" for v in MOE_COLUMN_MAP.values()]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    print(f"  Wrote {len(records):,} rows to {filepath}")
    return filepath


def null_audit(records: list[dict]) -> bool:
    """Audit null rates for all PDI variables. Returns True if audit passes."""
    if not records:
        print("\nNull audit: no records (empty fetch)")
        return True

    n = len(records)
    print(f"\nNull audit ({n:,} records):")
    passed = True

    for var in PDI_VARIABLES:
        null_count = sum(1 for r in records if r.get(var) is None)
        pct = null_count / n * 100
        flag = "FAIL" if pct > 30 else "ok"
        if pct > 30:
            passed = False
        print(f"  {var:<35} {null_count:>7,} null  ({pct:5.1f}%)  [{flag}]")

    if not passed:
        print("\n  AUDIT FAILED: >30% null for one or more primary indicators. "
              "Aborting load — check data source availability and column mappings.",
              file=sys.stderr)
    return passed


def load_to_db(records: list[dict], args: argparse.Namespace) -> None:
    """Load records to PostGIS via db.py."""
    try:
        sys.path.insert(0, SCRIPT_DIR)
        from lib.db import get_conn, bulk_load_indicators, upsert_indicator_meta
    except ImportError as exc:
        print(f"  ERROR: Cannot import lib.db — {exc}", file=sys.stderr)
        sys.exit(1)

    print("Connecting to database...")
    conn = get_conn()

    year_val = args.year

    # Upsert indicator metadata so the variables are queryable by the API.
    meta: dict[str, dict] = {}
    meta["cdc_svi_overall"] = {
        "source_id":   "cdc-svi",
        "name":        "CDC SVI — Overall Vulnerability",
        "description": "CDC/ATSDR Social Vulnerability Index overall percentile rank (0–1, higher = more vulnerable)",
        "unit":        "percentile",
        "direction":   "lower_better",
    }
    meta["cdc_svi_socioeconomic"] = {
        "source_id":   "cdc-svi",
        "name":        "CDC SVI — Theme 1: Socioeconomic Status",
        "description": "SVI socioeconomic theme: poverty, unemployment, income, education (percentile rank 0–1)",
        "unit":        "percentile",
        "direction":   "lower_better",
    }
    meta["cdc_svi_household"] = {
        "source_id":   "cdc-svi",
        "name":        "CDC SVI — Theme 2: Household Characteristics",
        "description": "SVI household theme: age 65+, age 17-, disability, single-parent, English proficiency (percentile rank 0–1)",
        "unit":        "percentile",
        "direction":   "lower_better",
    }
    meta["cdc_svi_racial_ethnic"] = {
        "source_id":   "cdc-svi",
        "name":        "CDC SVI — Theme 3: Racial & Ethnic Minority Status",
        "description": "SVI racial/ethnic minority theme: non-White, Hispanic/Latino, AI/AN, NHPI groups (percentile rank 0–1)",
        "unit":        "percentile",
        "direction":   "lower_better",
    }
    meta["cdc_svi_housing_transport"] = {
        "source_id":   "cdc-svi",
        "name":        "CDC SVI — Theme 4: Housing Type & Transportation",
        "description": "SVI housing/transport theme: multi-unit, mobile homes, crowding, no vehicle, group quarters (percentile rank 0–1)",
        "unit":        "percentile",
        "direction":   "lower_better",
    }
    meta["svi_total_population"] = {
        "source_id":   "cdc-svi",
        "name":        "Total Population (CDC SVI E_TOTPOP)",
        "description": "ACS total population estimate carried in the CDC SVI file (E_TOTPOP, with M_TOTPOP margin of error). Same 2022 vintage as CDC PLACES — the tract→county weighting denominator (ADR-014 D7).",
        "unit":        "count",
        "direction":   "neutral",
    }
    n_meta = upsert_indicator_meta(conn, meta)
    print(f"  {n_meta} indicator_meta rows upserted")

    # Build indicator records for bulk load.
    indicators = []
    for rec in records:
        geoid = rec.get("geoid")
        if not geoid:
            continue
        for var in PDI_VARIABLES:
            val = rec.get(var)
            indicators.append({
                "geoid":          geoid,
                "variable_id":    var,
                "vintage":        int(year_val),
                "value":          float(val) if val is not None else None,
                "margin_of_error": rec.get(var + "__moe"),
                "raw_value":      str(val) if val is not None else "",
            })

    n = bulk_load_indicators(conn, indicators)
    conn.close()
    print(f"  {n} indicator rows written to database")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch CDC/ATSDR Social Vulnerability Index (SVI) data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2022,
        help="SVI release year: 2020, 2022 (default: 2022)",
    )
    parser.add_argument(
        "--level",
        type=str,
        choices=["county", "tract"],
        default="county",
        help="Geography level (default: county)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print fetch plan without making network calls",
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
    if not records:
        print("No data fetched. The URL may have changed — check the CDC SVI page for updated download paths.",
              file=sys.stderr)
        return

    write_csv(records, args)

    if not null_audit(records):
        sys.exit(1)

    if args.load:
        load_to_db(records, args)

    print("\nDone.")


if __name__ == "__main__":
    main()