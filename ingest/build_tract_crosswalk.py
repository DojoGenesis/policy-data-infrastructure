#!/usr/bin/env python3
"""
Build the population-weighted 2010→2020 tract crosswalk for Wisconsin.

Why this exists (ADR-014 OQ6, resolved 2026-08-08): USDA FARA 2019 — and any
other source still keyed to 2010 tracts — cannot load against the platform's
2020 geographies. 127 of WI's 1,409 2010 tracts have no 2020 counterpart at
all, and the 91% that match *by identifier* may not match by geography. The
operator ruled: wire a real crosswalk rather than load the lookalikes or
carry the source at county only.

Method — the same foundation NHGIS builds its crosswalks on:

  1. Census 2020 relationship file (block level, t10t20 product): every
     2010 tabulation block × 2020 tabulation block intersection, with the
     land area of each intersection (AREALAND_INT). Public, no account.
  2. 2010 block populations (Census API, dec/sf1 P001001): the size of each
     2010 block. One call per county, cached.
  3. Allocate each 2010 block's population across its intersections by land
     area share, then aggregate allocations to (tract10, tract20) pairs.
     weight(t10→t20) = allocated_pop / pop(t10).

NHGIS's own crosswalks refine step 3 with target-density weighting; their
files sit behind an IPUMS login (verified 2026-08-08: the asset host serves
an HTML shell with HTTP 200 for unauthenticated requests — checked by
content, not status). If an IPUMS account lands (Monday handoff), swap the
weights in place; the output schema stays identical.

Where a 2010 tract has zero population, weights fall back to land-area
share and the row is flagged method=area — consumers can decide whether an
area-weighted allocation of a population measure is acceptable (for
population counts it allocates zeros, so it is moot in practice).

Verification gates (the script refuses to write on failure):
  - per-tract10 weights sum to 1 ± 1e-6 (population method)
  - total allocated population equals total 2010 block population
  - tract counts match the known WI universes (1,409 × 2010; ⊆ 1,542 × 2020)

Output:
  data/crosswalks/wi_tract2010_tract2020.csv   (tract2010, tract2020,
      weight, allocated_pop2010, method)
  data/crosswalks/wi_tract2010_tract2020.meta.json  (provenance + gates)

Usage:
  python build_tract_crosswalk.py --dry-run     # gates + summary, no write
  python build_tract_crosswalk.py               # build + write
Requires CENSUS_API_KEY.
"""
import argparse
import csv
import io
import json
import os
import sys
import time
import urllib.request
import zipfile
from collections import defaultdict
from datetime import datetime, timezone

from lib.census import require_api_key

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "analysis", "output")
CROSSWALK_DIR = os.path.join(SCRIPT_DIR, "..", "data", "crosswalks")

REL_URL = "https://www2.census.gov/geo/docs/maps-data/data/rel2020/t10t20/TAB2010_TAB2020_ST55.zip"
REL_CACHE = os.path.join(OUTPUT_DIR, "tab2010_tab2020_st55_wi.txt")
POP_CACHE = os.path.join(OUTPUT_DIR, "wi_blocks_2010_pop.csv")

OUT_CSV = os.path.join(CROSSWALK_DIR, "wi_tract2010_tract2020.csv")
OUT_META = os.path.join(CROSSWALK_DIR, "wi_tract2010_tract2020.meta.json")

STATE = "55"
EXPECTED_TRACTS_2010 = 1409
EXPECTED_TRACTS_2020 = 1542
RATE_LIMIT_DELAY = 1.2


def fetch_relationship_file() -> str:
    """Download (or reuse) the WI block relationship file; return its path."""
    if os.path.exists(REL_CACHE) and os.path.getsize(REL_CACHE) > 1_000_000:
        print(f"  Relationship file cached: {REL_CACHE}")
        return REL_CACHE
    print(f"  Downloading {REL_URL} ...")
    req = urllib.request.Request(REL_URL, headers={"User-Agent": "policy-data-infrastructure/1.0"})
    with urllib.request.urlopen(req, timeout=180) as resp:
        raw = resp.read()
    zf = zipfile.ZipFile(io.BytesIO(raw))
    names = [n for n in zf.namelist() if n.lower().endswith(".txt")]
    if not names:
        raise RuntimeError(f"relationship ZIP holds no .txt member: {zf.namelist()}")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(REL_CACHE, "wb") as f:
        f.write(zf.read(names[0]))
    print(f"  Cached {REL_CACHE} ({os.path.getsize(REL_CACHE):,} bytes)")
    return REL_CACHE


def fetch_block_populations(counties: list[str]) -> dict[str, int]:
    """2010 block GEOID (15-digit) → P001001, from dec/sf1, cached to CSV."""
    if os.path.exists(POP_CACHE):
        pops: dict[str, int] = {}
        with open(POP_CACHE, newline="") as f:
            for row in csv.DictReader(f):
                pops[row["geoid"]] = int(row["pop"])
        print(f"  Block populations cached: {len(pops):,} blocks")
        return pops

    key = require_api_key()
    pops = {}
    print(f"  Fetching 2010 block populations for {len(counties)} counties (dec/sf1)...")
    for i, county in enumerate(sorted(counties), 1):
        url = (
            "https://api.census.gov/data/2010/dec/sf1?get=P001001"
            f"&for=block:*&in=state:{STATE}%20county:{county}&key={key}"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "policy-data-infrastructure/1.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            if "missing_key" in resp.url or "invalid_key" in resp.url:
                raise RuntimeError("Census API rejected the key (redirect to error page)")
            data = json.loads(resp.read().decode())
        header = data[0]
        idx = {name: k for k, name in enumerate(header)}
        for row in data[1:]:
            geoid = (
                row[idx["state"]].zfill(2)
                + row[idx["county"]].zfill(3)
                + row[idx["tract"]].zfill(6)
                + row[idx["block"]].zfill(4)
            )
            pops[geoid] = int(row[idx["P001001"]])
        if i % 12 == 0 or i == len(counties):
            print(f"    {i}/{len(counties)} counties, {len(pops):,} blocks")
        time.sleep(RATE_LIMIT_DELAY)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(POP_CACHE, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["geoid", "pop"])
        for g in sorted(pops):
            w.writerow([g, pops[g]])
    print(f"  Cached {POP_CACHE} ({len(pops):,} blocks)")
    return pops


def build(dry_run: bool) -> None:
    rel_path = fetch_relationship_file()

    # Parse intersections, filtered to WI on both sides. Cross-state slivers
    # are counted and reported — they are boundary-correction artifacts, not
    # tract reassignments.
    rows_kept = 0
    rows_cross_state = 0
    counties: set[str] = set()
    # block10 geoid -> list of (tract20, arealand_int, areawater_int)
    by_block: dict[str, list[tuple[str, int, int]]] = defaultdict(list)

    with open(rel_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f, delimiter="|"):
            if row["STATE_2010"] != STATE or row["STATE_2020"] != STATE:
                rows_cross_state += 1
                continue
            rows_kept += 1
            counties.add(row["COUNTY_2010"].zfill(3))
            blk10 = (
                row["STATE_2010"].zfill(2)
                + row["COUNTY_2010"].zfill(3)
                + row["TRACT_2010"].zfill(6)
                + row["BLK_2010"].zfill(4)
            )
            tract20 = (
                row["STATE_2020"].zfill(2)
                + row["COUNTY_2020"].zfill(3)
                + row["TRACT_2020"].zfill(6)
            )
            by_block[blk10].append(
                (tract20, int(row["AREALAND_INT"] or 0), int(row["AREAWATER_INT"] or 0))
            )
    print(f"  Intersections kept: {rows_kept:,} (cross-state dropped: {rows_cross_state:,})")
    print(f"  2010 blocks: {len(by_block):,}")

    pops = fetch_block_populations(sorted(counties))

    # Allocate each block's population across its intersections.
    alloc: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))  # t10 -> t20 -> pop
    area: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))  # t10 -> t20 -> land
    tract10_pop: dict[str, float] = defaultdict(float)
    missing_pop_blocks = 0

    for blk10, parts in by_block.items():
        tract10 = blk10[:11]
        pop = pops.get(blk10)
        if pop is None:
            missing_pop_blocks += 1
            pop = 0
        tract10_pop[tract10] += pop

        land_total = sum(p[1] for p in parts)
        water_total = sum(p[2] for p in parts)
        for tract20, land, water in parts:
            if land_total > 0:
                share = land / land_total
            elif water_total > 0:
                share = water / water_total
            else:
                share = 1.0 / len(parts)
            alloc[tract10][tract20] += pop * share
            area[tract10][tract20] += land

    # Weights per tract10.
    out_rows: list[dict] = []
    method_counts = {"population": 0, "area": 0}
    max_weight_dev = 0.0
    for tract10 in sorted(alloc):
        t10pop = tract10_pop[tract10]
        targets = alloc[tract10]
        if t10pop > 0:
            method = "population"
            weights = {t20: p / t10pop for t20, p in targets.items()}
        else:
            method = "area"
            land_sum = sum(area[tract10].values())
            if land_sum > 0:
                weights = {t20: a / land_sum for t20, a in area[tract10].items()}
            else:
                n = len(targets)
                weights = {t20: 1.0 / n for t20 in targets}
        method_counts[method] += 1
        max_weight_dev = max(max_weight_dev, abs(sum(weights.values()) - 1.0))
        for t20, w in sorted(weights.items()):
            if w <= 0:
                continue
            out_rows.append({
                "tract2010": tract10,
                "tract2020": t20,
                "weight": f"{w:.8f}",
                "allocated_pop2010": f"{targets[t20]:.2f}",
                "method": method,
            })

    # ── Gates ──
    total_block_pop = sum(pops.values())
    total_alloc = sum(sum(t.values()) for t in alloc.values())
    n_t10 = len(alloc)
    n_t20 = len({r["tract2020"] for r in out_rows})
    print("\nGates:")
    print(f"  tract2010 universe : {n_t10} (expected {EXPECTED_TRACTS_2010})")
    print(f"  tract2020 coverage : {n_t20} (of {EXPECTED_TRACTS_2020})")
    print(f"  max |Σw − 1|       : {max_weight_dev:.2e}")
    print(f"  pop conservation   : allocated {total_alloc:,.0f} vs source {total_block_pop:,.0f}")
    print(f"  methods            : {method_counts}")
    print(f"  blocks missing pop : {missing_pop_blocks}")

    failures = []
    if n_t10 != EXPECTED_TRACTS_2010:
        failures.append(f"tract2010 count {n_t10} != {EXPECTED_TRACTS_2010}")
    if n_t20 > EXPECTED_TRACTS_2020:
        failures.append(f"tract2020 coverage {n_t20} exceeds {EXPECTED_TRACTS_2020}")
    if max_weight_dev > 1e-6:
        failures.append(f"weight sums deviate by {max_weight_dev:.2e}")
    if abs(total_alloc - total_block_pop) > 1.0:
        failures.append(f"population not conserved: {total_alloc:,.0f} vs {total_block_pop:,.0f}")
    if failures:
        print("\nGATES FAILED:\n  " + "\n  ".join(failures), file=sys.stderr)
        sys.exit(1)

    if dry_run:
        print(f"\n[dry-run] {len(out_rows):,} crosswalk rows pass all gates; not written.")
        return

    os.makedirs(CROSSWALK_DIR, exist_ok=True)
    with open(OUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["tract2010", "tract2020", "weight", "allocated_pop2010", "method"])
        w.writeheader()
        w.writerows(out_rows)
    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "2010 block population allocated over Census t10t20 block intersections by AREALAND_INT share, aggregated to tract pairs",
        "sources": {
            "relationship_file": REL_URL,
            "block_population": "Census API 2010 dec/sf1 P001001, block level, per county",
        },
        "filters": {"state_both_sides": STATE, "cross_state_rows_dropped": rows_cross_state},
        "gates": {
            "tract2010_count": n_t10,
            "tract2020_coverage": n_t20,
            "max_weight_sum_deviation": max_weight_dev,
            "population_allocated": total_alloc,
            "population_source": total_block_pop,
            "methods": method_counts,
            "blocks_missing_population": missing_pop_blocks,
        },
        "upgrade_path": "swap weights for NHGIS TDW crosswalk when an IPUMS account is available; schema unchanged",
        "rows": len(out_rows),
    }
    with open(OUT_META, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"\nWrote {OUT_CSV} ({len(out_rows):,} rows) and {OUT_META}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", help="run gates and summary without writing")
    args = p.parse_args()
    build(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
