#!/usr/bin/env python3
"""Join policy positions to the tracts where their target conditions are acute.

The chain, end to end:

    tract indicator value
      -> is it in the worst statewide quintile for that indicator?   (acuity)
      -> which equity dimensions use that indicator?                 (crosswalk)
      -> which policy positions target those dimensions?             (policies)

"Acute" means the tract sits in the worst fifth of the STATEWIDE tract
distribution, read through each indicator's own `direction`: for a
lower-is-better indicator that is the top class, for higher-is-better the
bottom class. Indicators marked `neutral` (population, race and ethnicity
shares) are never treated as good or bad and never drive acuity — they
describe who lives somewhere, not how they are doing.

What this is NOT: a claim that a policy would help a tract, or an endorsement.
It is a lookup — "these positions are on record about conditions this tract
measures in the worst fifth of the state for." The output carries that framing
as data (`interpretation`) so a consumer cannot lose it.

Sources: data/policies/*.csv (positions on record, with attribution) and
data/crosswalks/wi_equity_crosswalk.json (dimension -> indicator mapping).

Outputs (analysis/output/atlas/):
    policy_impact.json     dimensions, policies, and per-tract acute lists

Usage:
    python3 analysis/build_policy_impact.py --dry-run
    python3 analysis/build_policy_impact.py
    python3 analysis/build_policy_impact.py --state-scope WI
"""
import argparse
import csv
import glob
import json
import os
import sys
from collections import Counter, defaultdict

HERE = os.path.dirname(__file__)
REPO = os.path.join(HERE, "..")
BUNDLE_DIR = os.path.join(HERE, "output", "atlas")
POLICY_GLOB = os.path.join(REPO, "data", "policies", "*.csv")
CROSSWALK = os.path.join(REPO, "data", "crosswalks", "wi_equity_crosswalk.json")

# Directions that make "worst quintile" a meaningful idea at all.
_DIRECTED = {"lower_better", "higher_better"}


def load_crosswalk() -> dict:
    with open(CROSSWALK, encoding="utf-8") as f:
        raw = json.load(f)["dimensions"]
    # The file ships as a list of [key, body] pairs; tolerate a plain object too.
    return dict(raw) if isinstance(raw, list) else raw


def load_policies(state_scope: str | None) -> list[dict]:
    out = []
    for path in sorted(glob.glob(POLICY_GLOB)):
        with open(path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if state_scope and (r.get("state") or "").strip().upper() != state_scope.upper():
                    continue
                out.append({
                    "id": r.get("id"),
                    "title": r.get("policy_title"),
                    "description": r.get("description"),
                    "category": r.get("category"),
                    "dimension": (r.get("equity_dimension") or "").strip(),
                    "attributedTo": r.get("candidate"),
                    "office": r.get("office"),
                    "state": r.get("state"),
                    "billReferences": r.get("bill_references") or None,
                    "sourceFile": os.path.basename(path),
                })
    return out


def acute_indicators(tract_props: dict, indicators: list[dict]) -> list[str]:
    """Which indicators put this tract in the worst statewide quintile."""
    hits = []
    for ind in indicators:
        if ind.get("direction") not in _DIRECTED:
            continue
        dist = ind.get("tract")
        if not dist or not dist.get("breaks"):
            continue
        v = tract_props.get(ind["id"])
        if v is None:
            continue
        breaks = dist["breaks"]
        if ind["direction"] == "lower_better":
            if v >= breaks[-1]:          # top class = worst
                hits.append(ind["id"])
        else:
            if v < breaks[0]:            # bottom class = worst
                hits.append(ind["id"])
    return hits


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state-scope", default="WI",
                    help="Only include policies whose `state` matches (default: WI). "
                         "Pass an empty string to include every policy on file.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    scope = args.state_scope or None

    crosswalk = load_crosswalk()
    policies = load_policies(scope)

    if args.dry_run:
        allp = load_policies(None)
        print("=== DRY RUN: build_policy_impact.py ===")
        print(f"Crosswalk dimensions : {len(crosswalk)}")
        print(f"Policies on file     : {len(allp)}")
        print(f"Policies in scope    : {len(policies)} (state={scope or 'any'})")
        by_state = Counter((p.get('state') or '?') for p in allp)
        print(f"By state             : {dict(by_state)}")
        by_person = Counter(p.get("attributedTo") for p in policies)
        print(f"Attributed to        : {dict(by_person)}")
        dims = Counter(p["dimension"] for p in policies)
        unknown = [d for d in dims if d not in crosswalk]
        print(f"Dimensions used      : {len(dims)}")
        print(f"Not in crosswalk     : {unknown or 'none'}")
        print("[dry-run] Nothing written.")
        return

    tracts_path = os.path.join(BUNDLE_DIR, "tracts.geojson")
    ind_path = os.path.join(BUNDLE_DIR, "indicators.json")
    for p in (tracts_path, ind_path):
        if not os.path.exists(p):
            sys.exit(f"Missing {p} — run build_atlas_bundle.py first.")

    tracts = json.load(open(tracts_path, encoding="utf-8"))["features"]
    indicators = json.load(open(ind_path, encoding="utf-8"))["indicators"]
    ind_by_id = {i["id"]: i for i in indicators}

    print(f"Tracts: {len(tracts)}   indicators: {len(indicators)}   "
          f"policies in scope: {len(policies)} (state={scope or 'any'})")

    # indicator -> dimensions that use it (only dimensions some policy targets)
    used_dims = {p["dimension"] for p in policies}
    ind_to_dims = defaultdict(set)
    unmapped_dims = []
    for dim in used_dims:
        body = crosswalk.get(dim)
        if not body:
            unmapped_dims.append(dim)
            continue
        for ind_id in body.get("indicators", []):
            if ind_id in ind_by_id:
                ind_to_dims[ind_id].add(dim)
    if unmapped_dims:
        print(f"  NOTE: {len(unmapped_dims)} policy dimension(s) are not in the crosswalk "
              f"and can never match a tract: {sorted(unmapped_dims)}")

    policies_by_dim = defaultdict(list)
    for p in policies:
        policies_by_dim[p["dimension"]].append(p["id"])

    per_tract = {}
    acuity_counts = Counter()
    dim_hits = Counter()
    no_acute = 0

    for f in tracts:
        props = f["properties"]
        gid = props["GEOID"]
        acute = acute_indicators(props, indicators)
        acuity_counts[len(acute)] += 1
        if not acute:
            no_acute += 1
        dims = sorted({d for i in acute for d in ind_to_dims.get(i, ())})
        for d in dims:
            dim_hits[d] += 1
        pol_ids = sorted({pid for d in dims for pid in policies_by_dim.get(d, [])})
        per_tract[gid] = {
            "acuteIndicators": acute,
            "dimensions": dims,
            "policies": pol_ids,
        }

    print(f"\nTracts with no acute indicator: {no_acute}")
    print("Acute-indicator count distribution:")
    for k in sorted(acuity_counts):
        print(f"  {k} acute: {acuity_counts[k]:>5} tracts")
    print("\nTop dimensions by tracts matched:")
    for d, n in dim_hits.most_common(8):
        print(f"  {d:<26} {n:>5} tracts   ({len(policies_by_dim[d])} policy position(s))")

    os.makedirs(BUNDLE_DIR, exist_ok=True)
    out_path = os.path.join(BUNDLE_DIR, "policy_impact.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump({
            "interpretation": (
                "A policy is listed for a tract when the tract measures in the worst "
                "statewide fifth for an indicator that policy's equity dimension covers. "
                "This is a lookup of positions on record about a measured condition. It "
                "is not a prediction that the policy would change the number, not an "
                "assessment of the policy, and not an endorsement."
            ),
            "acuityRule": (
                "Worst statewide tract quintile, read through each indicator's own "
                "direction. Indicators marked neutral never drive acuity."
            ),
            "stateScope": scope,
            "sources": [
                {"name": "Policy positions", "path": "data/policies/*.csv",
                 "note": "Positions on record, attributed to the person who holds them."},
                {"name": "Equity crosswalk", "path": "data/crosswalks/wi_equity_crosswalk.json",
                 "note": "Maps an equity dimension to the indicators that measure it."},
            ],
            "dimensions": {d: {
                "label": crosswalk.get(d, {}).get("label", d),
                "indicators": [i for i in crosswalk.get(d, {}).get("indicators", []) if i in ind_by_id],
                "policies": policies_by_dim[d],
            } for d in sorted(used_dims) if d in crosswalk},
            "policies": {p["id"]: p for p in policies},
            "tracts": per_tract,
        }, fh, separators=(",", ":"))
    print(f"\nWrote {out_path} ({os.path.getsize(out_path)/1024:.0f} KB)")
    print("Done.")


if __name__ == "__main__":
    main()
