#!/usr/bin/env python3
"""
Load existing atlas-format JSON data files into PostgreSQL.

Reads JSON files produced by the Madison Equity Atlas scripts and maps their
records into normalized indicator rows for the policy-data-infrastructure
database.  This bridges the atlas pipeline into the generalized PDI schema
without re-fetching data from the Census API.

Supported atlas file formats:
  income_by_tract.json   — dict with 'tracts' list; tract_id field
  cost_burden.json       — list; geoid field
  uninsured_rate.json    — list; geoid field
  race_by_tract.json     — list or dict; flexible

Usage:
  # Load all supported files from a directory
  python load_to_postgres.py --data-dir /path/to/atlas/data

  # Load a specific file
  python load_to_postgres.py --data-dir /path/to/atlas/data --file cost_burden.json

  # Dry run
  python load_to_postgres.py --data-dir /path/to/atlas/data --dry-run
"""
import argparse
import json
import sys
from pathlib import Path
from datetime import datetime

from lib.db import get_conn, bulk_load_indicators, upsert_indicator_meta

# ---------------------------------------------------------------------------
# File handler registry
# Each entry maps a filename pattern to a function (records, vintage) → [indicator_dict]
# ---------------------------------------------------------------------------

def _geoid(record: dict) -> str | None:
    """Extract GEOID from an atlas record (handles both 'geoid' and 'tract_id' keys)."""
    return record.get("geoid") or record.get("tract_id")


def _fetched_at() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _indicator(geoid: str, variable_id: str, vintage: int,
               value, moe=None, raw_value: str = "") -> dict:
    return {
        "geoid":           geoid,
        "variable_id":     variable_id,
        "vintage":         vintage,
        "value":           value,
        "margin_of_error": moe,
        "raw_value":       str(raw_value),
        "fetched_at":      _fetched_at(),
    }


# ---------------------------------------------------------------------------
# Per-file handlers
# ---------------------------------------------------------------------------

def handle_income_by_tract(data, vintage: int) -> list[dict]:
    """
    income_by_tract.json — dict with top-level 'tracts' list.
    Each tract has: tract_id, median_household_income, poverty_rate, total_households.
    """
    records: list[dict] = []
    tracts = data.get("tracts", data) if isinstance(data, dict) else data
    if isinstance(tracts, dict):
        tracts = list(tracts.values())

    for tract in tracts:
        geoid = _geoid(tract)
        if not geoid:
            continue

        income = tract.get("median_household_income")
        records.append(_indicator(
            geoid, "median_hh_income", vintage,
            float(income) if income is not None else None,
            raw_value=str(income or ""),
        ))

        poverty_rate = tract.get("poverty_rate")
        records.append(_indicator(
            geoid, "poverty_rate", vintage,
            float(poverty_rate) if poverty_rate is not None else None,
            raw_value=str(poverty_rate or ""),
        ))

    return records


def handle_cost_burden(data, vintage: int) -> list[dict]:
    """
    cost_burden.json — list of dicts with geoid, pct_cost_burdened, owner/renter counts.
    """
    records: list[dict] = []
    rows = data if isinstance(data, list) else []

    for row in rows:
        geoid = _geoid(row)
        if not geoid:
            continue

        pct = row.get("pct_cost_burdened")
        records.append(_indicator(
            geoid, "pct_cost_burdened", vintage,
            float(pct) if pct is not None else None,
            raw_value=str(pct or ""),
        ))

        # Severely cost burdened (>50% of income) — not in this atlas file, skip.

    return records


def handle_uninsured_rate(data, vintage: int) -> list[dict]:
    """
    uninsured_rate.json — list of dicts with geoid and pct_uninsured.
    """
    records: list[dict] = []
    rows = data if isinstance(data, list) else []

    for row in rows:
        geoid = _geoid(row)
        if not geoid:
            continue

        pct = row.get("pct_uninsured")
        records.append(_indicator(
            geoid, "uninsured_rate", vintage,
            float(pct) if pct is not None else None,
            raw_value=str(pct or ""),
        ))

    return records


def handle_race_by_tract(data, vintage: int) -> list[dict]:
    """
    race_by_tract.json — list of dicts with geoid, pct_poc, total_population.
    Field names vary across atlas versions; try multiple.
    """
    records: list[dict] = []
    rows = data if isinstance(data, list) else []

    for row in rows:
        geoid = _geoid(row)
        if not geoid:
            continue

        pop = row.get("total_population") or row.get("total_pop")
        if pop is not None:
            records.append(_indicator(
                geoid, "total_population", vintage,
                float(pop),
                raw_value=str(pop),
            ))

        poc = row.get("pct_poc") or row.get("pct_people_of_color")
        if poc is not None:
            records.append(_indicator(
                geoid, "pct_poc", vintage,
                float(poc),
                raw_value=str(poc),
            ))

        nhw = row.get("pct_non_hispanic_white") or row.get("pct_white_non_hispanic")
        if nhw is not None:
            records.append(_indicator(
                geoid, "pct_non_hispanic_white", vintage,
                float(nhw),
                raw_value=str(nhw),
            ))

        nhb = row.get("pct_non_hispanic_black") or row.get("pct_black_non_hispanic")
        if nhb is not None:
            records.append(_indicator(
                geoid, "pct_non_hispanic_black", vintage,
                float(nhb),
                raw_value=str(nhb),
            ))

        hisp = row.get("pct_hispanic") or row.get("pct_hispanic_or_latino")
        if hisp is not None:
            records.append(_indicator(
                geoid, "pct_hispanic", vintage,
                float(hisp),
                raw_value=str(hisp),
            ))

    return records


# ---------------------------------------------------------------------------
# File handler dispatch table
# Map filename (without extension) to (handler_fn, default_vintage)
# ---------------------------------------------------------------------------

_HANDLERS: dict[str, tuple] = {
    "income_by_tract":  (handle_income_by_tract, 2023),
    "cost_burden":      (handle_cost_burden,      2023),
    "uninsured_rate":   (handle_uninsured_rate,   2023),
    "race_by_tract":    (handle_race_by_tract,    2023),
}

# Minimal metadata for atlas-sourced indicators
_ATLAS_VARIABLE_META = {
    "median_hh_income":       {"label": "Median Household Income",       "source": "acs-5yr", "unit": "dollars",  "direction": "higher_better"},
    "poverty_rate":            {"label": "Poverty Rate",                  "source": "acs-5yr", "unit": "percent",  "direction": "lower_better"},
    "pct_cost_burdened":      {"label": "Percent Cost-Burdened HH",       "source": "acs-5yr", "unit": "percent",  "direction": "lower_better"},
    "uninsured_rate":          {"label": "Uninsured Rate",                 "source": "acs-5yr", "unit": "percent",  "direction": "lower_better"},
    "total_population":        {"label": "Total Population",               "source": "acs-5yr", "unit": "count",    "direction": "neutral"},
    "pct_poc":                 {"label": "Percent People of Color",        "source": "acs-5yr", "unit": "percent",  "direction": "neutral"},
    "pct_non_hispanic_white":  {"label": "Percent Non-Hispanic White",     "source": "acs-5yr", "unit": "percent",  "direction": "neutral"},
    "pct_non_hispanic_black":  {"label": "Percent Non-Hispanic Black/AA",  "source": "acs-5yr", "unit": "percent",  "direction": "neutral"},
    "pct_hispanic":            {"label": "Percent Hispanic or Latino",     "source": "acs-5yr", "unit": "percent",  "direction": "neutral"},
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load atlas-format JSON data files into PostgreSQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--data-dir", required=True,
        help="Directory containing atlas JSON files (e.g. atlas/data/)",
    )
    parser.add_argument(
        "--file", default=None,
        help="Load only this filename (e.g. cost_burden.json). Default: all supported files.",
    )
    parser.add_argument(
        "--vintage", type=int, default=None,
        help="Override ACS vintage year. Default: per-file default (2023).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse files and report counts but do not write to database.",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        print(f"Error: --data-dir {data_dir!r} is not a directory.", file=sys.stderr)
        sys.exit(1)

    # Determine which files to process
    if args.file:
        stem = Path(args.file).stem
        if stem not in _HANDLERS:
            print(f"Error: no handler for {args.file!r}. Supported: {list(_HANDLERS)}", file=sys.stderr)
            sys.exit(1)
        files_to_process = [stem]
    else:
        files_to_process = list(_HANDLERS)

    all_indicators: list[dict] = []

    for stem in files_to_process:
        handler_fn, default_vintage = _HANDLERS[stem]
        vintage = args.vintage if args.vintage is not None else default_vintage
        json_path = data_dir / f"{stem}.json"

        if not json_path.exists():
            print(f"  Skipping {stem}.json — file not found at {json_path}")
            continue

        print(f"Loading {json_path.name} (vintage={vintage})...")
        with open(json_path) as f:
            data = json.load(f)

        indicators = handler_fn(data, vintage)
        print(f"  Parsed {len(indicators)} indicator records")
        all_indicators.extend(indicators)

    total = len(all_indicators)
    print(f"\nTotal indicator records: {total}")

    if total == 0:
        print("Nothing to load.")
        return

    if args.dry_run:
        # Print a breakdown by variable_id
        from collections import Counter
        counts = Counter(ind["variable_id"] for ind in all_indicators)
        null_counts = Counter(ind["variable_id"] for ind in all_indicators if ind.get("value") is None)
        print("\nBreakdown by indicator (dry-run):")
        for var_id, count in sorted(counts.items()):
            nulls = null_counts.get(var_id, 0)
            print(f"  {var_id:<42} {count:>5} records  ({nulls} null)")
        print("\n[dry-run] No data written.")
        return

    # Write to database
    print("\nConnecting to database...")
    conn = get_conn()

    print("Upserting indicator metadata...")
    n_meta = upsert_indicator_meta(conn, _ATLAS_VARIABLE_META)  # type: ignore[arg-type]
    print(f"  {n_meta} indicator_meta rows written")

    print("Bulk loading indicators via COPY...")
    n_ind = bulk_load_indicators(conn, all_indicators)
    print(f"  {n_ind} indicator rows written")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
