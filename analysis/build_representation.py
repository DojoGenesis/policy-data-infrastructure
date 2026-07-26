#!/usr/bin/env python3
"""Answer "who represents this tract?" — districts joined to real officeholders.

Two halves, both from citable public sources; nothing here is inferred or
hand-entered.

  GEOMETRY   TIGERweb tigerWMS_ACS2024, the same vintage as the tract
             boundaries, so a tract and its districts are drawn on the same
             geography: congressional (119th), state legislative upper
             (WI Senate), state legislative lower (WI Assembly).

  OFFICEHOLDERS
             Federal — unitedstates/congress-legislators, public domain,
             the long-standing community roster of sitting members.
             State — Open States bulk export (data.openstates.org), the
             standard open dataset for state legislatures.

             Neither is scraped from a page and neither is typed in by hand.
             If a source is unreachable the run FAILS rather than shipping a
             partial roster — a representation tool that quietly omits a
             legislator is worse than one that refuses to build.

Tract-to-district assignment uses the Census's own interior point
(INTPTLAT/INTPTLON), which is guaranteed to fall inside the tract polygon —
unlike a computed centroid, which can land outside a crescent-shaped tract.
Point-in-polygon is ray casting with hole support, stdlib only.

A tract can straddle a district boundary. The interior point gives ONE
district, which is right for "who represents the middle of this tract" and
wrong for "every district this tract touches." The output records the
assignment method explicitly so a consumer can't mistake one for the other.

Outputs (analysis/output/atlas/):
    districts.geojson      all three district layers, tagged by `layer`
    representation.json    tract GEOID -> its districts and their officeholders

Usage:
    python3 analysis/build_representation.py --dry-run
    python3 analysis/build_representation.py
"""
import argparse
import csv
import io
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingest"))

from lib import tigerweb  # noqa: E402

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
BUNDLE_DIR = os.path.join(OUTPUT_DIR, "atlas")

DEFAULT_STATE = "55"
STATE_USPS = "WI"
DEFAULT_YEAR = 2024

CONGRESS_CSV = "https://unitedstates.github.io/congress-legislators/legislators-current.csv"
OPENSTATES_CSV = "https://data.openstates.org/people/current/{usps}.csv"

# TIGERweb level -> (district-number field, human label, chamber key)
LAYERS = {
    "congressional": ("CD119", "U.S. House district", "us_house"),
    "state_upper":   ("SLDU",  "State Senate district", "state_upper"),
    "state_lower":   ("SLDL",  "State Assembly district", "state_lower"),
}

# TIGER uses ZZ / 'not defined' placeholders for unassigned water and similar.
_PLACEHOLDER_DISTRICTS = {"ZZ", "ZZZ", "98", "999"}


def norm_district(value) -> str:
    """Canonical district key.

    TIGER zero-pads district numbers to the field width ("01" for congressional,
    "001" for state legislative); both rosters publish them unpadded ("1"). Left
    unnormalized, every single join misses — and it misses SILENTLY, producing a
    complete map with no officeholder on it. Both sides go through this function.
    Non-numeric districts (at-large letters, territory codes) pass through
    uppercased rather than being forced into an int.
    """
    s = str(value or "").strip()
    if not s:
        return ""
    return str(int(s)) if s.isdigit() else s.upper()


def fetch_csv(url: str) -> list[dict]:
    req = urllib.request.Request(url, headers={"User-Agent": "policy-data-infrastructure/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        text = resp.read().decode("utf-8")
    return list(csv.DictReader(io.StringIO(text)))


# ---------------------------------------------------------------------------
# Rosters
# ---------------------------------------------------------------------------

def load_federal(usps: str) -> dict[str, dict]:
    """Return {district_number: officeholder} for U.S. House seats in a state."""
    rows = fetch_csv(CONGRESS_CSV)
    out: dict[str, dict] = {}
    for r in rows:
        if r.get("state") != usps or r.get("type") != "rep":
            continue
        district = norm_district(r.get("district"))
        if not district:
            continue
        out[district] = {
            "name": r.get("full_name") or f"{r.get('first_name')} {r.get('last_name')}".strip(),
            "party": r.get("party") or None,
            "url": r.get("url") or None,
            "chamber": "us_house",
            "source": "unitedstates/congress-legislators",
        }
    return out


def load_state(usps: str) -> dict[str, dict[str, dict]]:
    """Return {chamber: {district_number: officeholder}} for a state legislature."""
    rows = fetch_csv(OPENSTATES_CSV.format(usps=usps.lower()))
    chambers: dict[str, dict[str, dict]] = {"state_upper": {}, "state_lower": {}}
    for r in rows:
        chamber = (r.get("current_chamber") or "").strip()
        district = norm_district(r.get("current_district"))
        if not district:
            continue
        key = {"upper": "state_upper", "lower": "state_lower"}.get(chamber)
        if not key:
            continue
        chambers[key][district] = {
            "name": r.get("name"),
            "party": r.get("current_party") or None,
            "url": (r.get("links") or "").split(";")[0].strip() or None,
            "chamber": key,
            "source": "openstates.org bulk export",
        }
    return chambers


# ---------------------------------------------------------------------------
# Geometry — point in polygon
# ---------------------------------------------------------------------------

def _ring_contains(ring, x, y) -> bool:
    """Ray casting against one linear ring."""
    inside = False
    n = len(ring)
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if (yi > y) != (yj > y):
            denom = (yj - yi)
            if denom != 0 and x < (xj - xi) * (y - yi) / denom + xi:
                inside = not inside
        j = i
    return inside


def _polygon_contains(rings, x, y) -> bool:
    """Outer ring contains the point and no hole excludes it."""
    if not rings or not _ring_contains(rings[0], x, y):
        return False
    for hole in rings[1:]:
        if _ring_contains(hole, x, y):
            return False
    return True


def feature_contains(geom, x, y) -> bool:
    if not geom:
        return False
    if geom["type"] == "Polygon":
        return _polygon_contains(geom["coordinates"], x, y)
    if geom["type"] == "MultiPolygon":
        return any(_polygon_contains(rings, x, y) for rings in geom["coordinates"])
    return False


def _bbox(geom):
    xs, ys = [], []
    polys = [geom["coordinates"]] if geom["type"] == "Polygon" else geom["coordinates"]
    for rings in polys:
        for pt in rings[0]:
            xs.append(pt[0]); ys.append(pt[1])
    return min(xs), min(ys), max(xs), max(ys)


def assign(tracts, districts, number_field):
    """Assign each tract's interior point to a district. Returns (map, unassigned)."""
    # Precompute bounding boxes — a full ray cast against 99 assembly districts
    # x 1,542 tracts is otherwise a lot of pointless arithmetic.
    indexed = []
    for f in districts:
        raw = (f["properties"].get(number_field) or "").strip()
        if raw in _PLACEHOLDER_DISTRICTS:
            continue
        indexed.append((_bbox(f["geometry"]), norm_district(raw), f["geometry"]))

    out, unassigned = {}, []
    for t in tracts:
        p = t["properties"]
        try:
            y = float(p["INTPTLAT"]); x = float(p["INTPTLON"])
        except (KeyError, TypeError, ValueError):
            unassigned.append(p.get("GEOID"))
            continue
        hit = None
        for (minx, miny, maxx, maxy), num, geom in indexed:
            if x < minx or x > maxx or y < miny or y > maxy:
                continue
            if feature_contains(geom, x, y):
                hit = num
                break
        if hit is None:
            unassigned.append(p.get("GEOID"))
        else:
            out[p["GEOID"]] = hit
    return out, unassigned


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state", default=DEFAULT_STATE)
    ap.add_argument("--usps", default=STATE_USPS)
    ap.add_argument("--year", type=int, default=DEFAULT_YEAR)
    ap.add_argument("--simplify", type=float, default=tigerweb.DEFAULT_SIMPLIFY)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    state = args.state.zfill(2)

    if args.dry_run:
        print("=== DRY RUN: build_representation.py ===")
        print(f"State: {state} ({args.usps})   TIGER vintage: ACS{args.year}")
        for level, (field, label, _) in LAYERS.items():
            n = tigerweb.count_features(args.year, level, state)
            print(f"  {level:<14} {n:>4} features   number field: {field}   ({label})")
        print(f"  federal roster : {CONGRESS_CSV}")
        print(f"  state roster   : {OPENSTATES_CSV.format(usps=args.usps.lower())}")
        print("[dry-run] Nothing fetched or written.")
        return

    tracts_path = os.path.join(BUNDLE_DIR, "tracts.geojson")
    if not os.path.exists(tracts_path):
        sys.exit(f"Missing {tracts_path} — run build_atlas_bundle.py first.")
    tracts = json.load(open(tracts_path, encoding="utf-8"))["features"]
    print(f"Loaded {len(tracts)} tracts")

    print("\nRosters:")
    federal = load_federal(args.usps)
    print(f"  federal  : {len(federal)} U.S. House seats")
    state_rosters = load_state(args.usps)
    print(f"  state    : {len(state_rosters['state_upper'])} upper, "
          f"{len(state_rosters['state_lower'])} lower")
    if not federal or not state_rosters["state_upper"] or not state_rosters["state_lower"]:
        sys.exit("A roster came back empty — refusing to ship partial representation data.")

    all_district_features = []
    representation: dict[str, dict] = {gid["properties"]["GEOID"]: {} for gid in tracts}
    coverage = {}

    for level, (field, label, chamber) in LAYERS.items():
        print(f"\nLevel: {level}")
        feats = tigerweb.fetch_features(
            year=args.year, level=level, state_fips=state, simplify=args.simplify,
        )
        roster = federal if chamber == "us_house" else state_rosters[chamber]

        assigned, unassigned = assign(tracts, feats, field)
        matched_officials = 0
        for gid, num in assigned.items():
            person = roster.get(num)
            if person:
                matched_officials += 1
            representation[gid][chamber] = {
                "district": num,
                "districtLabel": f"{label} {num}",
                "official": person,   # None if the roster has no entry — never invented
            }

        print(f"  tracts assigned: {len(assigned)}/{len(tracts)}"
              + (f"  ({len(unassigned)} unassigned)" if unassigned else ""))
        print(f"  tracts whose district has a named officeholder: {matched_officials}")
        if unassigned[:5]:
            print(f"  unassigned sample: {unassigned[:5]}")

        districts_in_roster = set(roster)
        districts_on_map = {norm_district(f["properties"].get(field)) for f in feats
                            if (f["properties"].get(field) or "").strip() not in _PLACEHOLDER_DISTRICTS}
        missing = sorted(districts_on_map - districts_in_roster)
        if missing:
            print(f"  WARNING: {len(missing)} district(s) on the map have no roster entry: {missing}")

        coverage[chamber] = {
            "districtsOnMap": len(districts_on_map),
            "districtsWithOfficial": len(districts_on_map & districts_in_roster),
            "tractsAssigned": len(assigned),
            "tractsUnassigned": len(unassigned),
        }

        for f in feats:
            raw = (f["properties"].get(field) or "").strip()
            if raw in _PLACEHOLDER_DISTRICTS:
                continue
            num = norm_district(raw)
            person = roster.get(num)
            all_district_features.append({
                "type": "Feature",
                "properties": {
                    "layer": chamber,
                    "district": num,
                    "districtLabel": f"{label} {num}",
                    "GEOID": f["properties"].get("GEOID"),
                    "officialName": person["name"] if person else None,
                    "officialParty": person["party"] if person else None,
                    "officialUrl": person["url"] if person else None,
                },
                "geometry": f["geometry"],
            })

    os.makedirs(BUNDLE_DIR, exist_ok=True)

    dpath = os.path.join(BUNDLE_DIR, "districts.geojson")
    with open(dpath, "w", encoding="utf-8") as fh:
        json.dump({"type": "FeatureCollection", "features": all_district_features}, fh,
                  separators=(",", ":"))
    print(f"\nWrote {len(all_district_features)} district features to {dpath} "
          f"({os.path.getsize(dpath)/1024:.0f} KB)")

    rpath = os.path.join(BUNDLE_DIR, "representation.json")
    with open(rpath, "w", encoding="utf-8") as fh:
        json.dump({
            "method": "census-interior-point-in-polygon",
            "methodNote": ("Each tract is assigned by its Census interior point "
                           "(INTPTLAT/INTPTLON), which is guaranteed to fall inside the "
                           "tract. A tract that straddles a district boundary is reported "
                           "under ONE district — the one containing that point. This is "
                           "not a list of every district a tract touches."),
            "vintage": f"ACS{args.year}",
            "sources": [
                {"name": "TIGERweb", "publisher": "U.S. Census Bureau",
                 "used_for": "district boundaries", "url": tigerweb.service_url(args.year)},
                {"name": "unitedstates/congress-legislators", "publisher": "public domain community dataset",
                 "used_for": "U.S. House officeholders", "url": CONGRESS_CSV},
                {"name": "Open States", "publisher": "Open States / Plural",
                 "used_for": "state legislature officeholders",
                 "url": OPENSTATES_CSV.format(usps=args.usps.lower())},
            ],
            "coverage": coverage,
            "tracts": representation,
        }, fh, separators=(",", ":"))
    print(f"Wrote {rpath} ({os.path.getsize(rpath)/1024:.0f} KB)")
    print("\nDone.")


if __name__ == "__main__":
    main()
