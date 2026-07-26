#!/usr/bin/env python3
"""Join tract indicators to tract geometry and emit the Atlas data bundle.

Inputs (all produced by sibling scripts in this directory):
    output/wi_tracts_acs.json     <- fetch_wi_tracts.py
    output/wi_counties_acs.csv    <- fetch_wi_counties.py
    output/wi_tracts.geojson      <- fetch_tract_boundaries.py
    output/wi_counties.geojson    <- fetch_tract_boundaries.py

Outputs (output/atlas/):
    tracts.geojson      geometry + indicator values, one file a map can consume
    counties.geojson    same, at county resolution
    indicators.json     indicator metadata, class breaks, distribution stats
    manifest.json       provenance: sources, vintages, counts, generated-at

The join is validated, not assumed. Any GEOID present on one side and missing
on the other is a hard failure — a choropleth silently missing 40 tracts looks
exactly like a choropleth that is fine, which is why this exits non-zero
instead of warning.

County values come from the ACS county estimates, NOT from aggregating tract
values. Averaging tract medians does not produce a county median, and
population-weighting a rate whose denominators differ per indicator quietly
produces a different number than the Census publishes. The county file is the
Census's own county answer.

Usage:
    python3 analysis/build_atlas_bundle.py
    python3 analysis/build_atlas_bundle.py --out-dir /some/other/place
"""
import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
DEFAULT_BUNDLE_DIR = os.path.join(OUTPUT_DIR, "atlas")

# ---------------------------------------------------------------------------
# Indicator registry
#
# `label`/`labelEs` are the public-facing names. Spanish here is written, not
# machine-translated, but it still carries the standing review flag — a
# plausible Spanish sentence is not the same as a reviewed one, and this text
# ships on a public page.
# ---------------------------------------------------------------------------

INDICATORS: list[dict] = [
    {
        "id": "median_hh_income",
        "label": "Median household income",
        "labelEs": "Ingreso familiar medio",
        "unit": "dollars",
        "unitEs": "dólares",
        "format": "currency",
        "direction": "higher_better",
        "table": "B19013",
        "descriptionEn": "Median income of all households in the tract, in the past 12 months.",
        "descriptionEs": "Ingreso medio de todos los hogares del sector censal, en los últimos 12 meses.",
    },
    {
        "id": "poverty_rate",
        "label": "Poverty rate",
        "labelEs": "Tasa de pobreza",
        "unit": "percent",
        "unitEs": "por ciento",
        "format": "percent",
        "direction": "lower_better",
        "table": "S1701",
        "descriptionEn": "Share of people with income below the federal poverty level.",
        "descriptionEs": "Proporción de personas con ingresos por debajo del nivel federal de pobreza.",
    },
    {
        "id": "pct_cost_burdened",
        "label": "Cost-burdened households",
        "labelEs": "Hogares con carga de vivienda",
        "unit": "percent",
        "unitEs": "por ciento",
        "format": "percent",
        "direction": "lower_better",
        "table": "B25106",
        "descriptionEn": "Share of households spending 30% or more of income on housing.",
        "descriptionEs": "Proporción de hogares que gastan 30% o más de sus ingresos en vivienda.",
    },
    {
        "id": "pct_severely_cost_burdened",
        "label": "Severely cost-burdened households",
        "labelEs": "Hogares con carga de vivienda severa",
        "unit": "percent",
        "unitEs": "por ciento",
        "format": "percent",
        "direction": "lower_better",
        "table": "B25106",
        "descriptionEn": "Share of households spending 50% or more of income on housing.",
        "descriptionEs": "Proporción de hogares que gastan 50% o más de sus ingresos en vivienda.",
    },
    {
        "id": "uninsured_rate",
        "label": "Uninsured rate",
        "labelEs": "Tasa sin seguro médico",
        "unit": "percent",
        "unitEs": "por ciento",
        "format": "percent",
        "direction": "lower_better",
        "table": "S2701",
        "descriptionEn": "Share of people with no health insurance coverage.",
        "descriptionEs": "Proporción de personas sin ningún seguro médico.",
    },
    {
        "id": "pct_renter_occupied",
        "label": "Renter-occupied homes",
        "labelEs": "Viviendas de alquiler",
        "unit": "percent",
        "unitEs": "por ciento",
        "format": "percent",
        "direction": "neutral",
        "table": "B25003",
        "descriptionEn": "Share of occupied homes lived in by renters rather than owners.",
        "descriptionEs": "Proporción de viviendas ocupadas por inquilinos en lugar de propietarios.",
    },
    {
        "id": "pct_bachelors_or_higher",
        "label": "Bachelor's degree or higher",
        "labelEs": "Título universitario o superior",
        "unit": "percent",
        "unitEs": "por ciento",
        "format": "percent",
        "direction": "higher_better",
        "table": "B15003",
        "descriptionEn": "Share of adults 25 and older holding at least a bachelor's degree.",
        "descriptionEs": "Proporción de adultos de 25 años o más con al menos un título universitario.",
    },
    {
        "id": "pct_poc",
        "label": "People of color",
        "labelEs": "Personas de color",
        "unit": "percent",
        "unitEs": "por ciento",
        "format": "percent",
        "direction": "neutral",
        "table": "B03002",
        "descriptionEn": "Share of residents who are not non-Hispanic white alone.",
        "descriptionEs": "Proporción de residentes que no son blancos no hispanos solamente.",
    },
    {
        "id": "pct_hispanic",
        "label": "Hispanic or Latino residents",
        "labelEs": "Residentes hispanos o latinos",
        "unit": "percent",
        "unitEs": "por ciento",
        "format": "percent",
        "direction": "neutral",
        "table": "B03002",
        "descriptionEn": "Share of residents of Hispanic or Latino origin, of any race.",
        "descriptionEs": "Proporción de residentes de origen hispano o latino, de cualquier raza.",
    },
    {
        "id": "pct_non_hispanic_black",
        "label": "Black or African American residents",
        "labelEs": "Residentes negros o afroamericanos",
        "unit": "percent",
        "unitEs": "por ciento",
        "format": "percent",
        "direction": "neutral",
        "table": "B03002",
        "descriptionEn": "Share of residents who are non-Hispanic Black or African American alone.",
        "descriptionEs": "Proporción de residentes negros o afroamericanos no hispanos solamente.",
    },
    {
        "id": "total_population",
        "label": "Total population",
        "labelEs": "Población total",
        "unit": "people",
        "unitEs": "personas",
        "format": "count",
        "direction": "neutral",
        "table": "B01001",
        "descriptionEn": "Total number of residents.",
        "descriptionEs": "Número total de residentes.",
    },
]

INDICATOR_IDS = [ind["id"] for ind in INDICATORS]

N_CLASSES = 5  # quintiles — matches a 5-step sequential ramp


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def _quantile(sorted_vals: list[float], q: float) -> float:
    """Linear-interpolation quantile. Avoids a numpy dependency."""
    if not sorted_vals:
        raise ValueError("empty")
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = q * (len(sorted_vals) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def distribution(values: list[float]) -> dict | None:
    """Class breaks and summary stats for one indicator."""
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None

    # Interior quantile breaks: N_CLASSES classes need N_CLASSES-1 cut points.
    breaks = [round(_quantile(vals, i / N_CLASSES), 2)
              for i in range(1, N_CLASSES)]

    # Deduplicate: a heavily-tied distribution (many zeros) can produce
    # identical cut points, which would render as empty classes in a legend.
    deduped: list[float] = []
    for b in breaks:
        if not deduped or b > deduped[-1]:
            deduped.append(b)

    return {
        "min": round(vals[0], 2),
        "max": round(vals[-1], 2),
        "median": round(_quantile(vals, 0.5), 2),
        "p10": round(_quantile(vals, 0.10), 2),
        "p90": round(_quantile(vals, 0.90), 2),
        "breaks": deduped,
        "classes": len(deduped) + 1,
        "n": len(vals),
        "n_missing": len(values) - len(vals),
        "method": "quantile",
        "tied_classes_collapsed": len(deduped) != len(breaks),
    }


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_json(path: str) -> dict:
    if not os.path.exists(path):
        sys.exit(f"Missing input: {path}\nRun the sibling fetch script first.")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_county_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        sys.exit(f"Missing input: {path}\nRun analysis/fetch_wi_counties.py first.")
    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        for k, v in list(r.items()):
            if k in ("geoid", "county_name"):
                continue
            r[k] = float(v) if v not in ("", None) else None
    return rows


# ---------------------------------------------------------------------------
# Join
# ---------------------------------------------------------------------------

def join(records: list[dict], features: list[dict], id_key: str,
         label: str) -> list[dict]:
    """Attach indicator values to geometry, failing loudly on any mismatch."""
    by_geoid = {r[id_key]: r for r in records}
    geo_ids = {(f.get("properties") or {}).get("GEOID") for f in features}
    rec_ids = set(by_geoid)

    missing_geometry = sorted(rec_ids - geo_ids)
    missing_data = sorted(geo_ids - rec_ids)

    print(f"\n{label} join:")
    print(f"  records:  {len(rec_ids)}")
    print(f"  features: {len(geo_ids)}")
    print(f"  matched:  {len(rec_ids & geo_ids)}")

    if missing_geometry or missing_data:
        print(f"  data without geometry ({len(missing_geometry)}): "
              f"{missing_geometry[:10]}{' ...' if len(missing_geometry) > 10 else ''}")
        print(f"  geometry without data ({len(missing_data)}): "
              f"{missing_data[:10]}{' ...' if len(missing_data) > 10 else ''}")
        sys.exit(
            f"\nJOIN FAILED for {label}. Indicator geography and boundary "
            f"geography disagree.\nMost likely cause: the ACS vintage and the "
            f"TIGERweb vintage are different years. Refetch both at the same "
            f"--year before continuing."
        )

    joined: list[dict] = []
    for feat in features:
        props = dict(feat.get("properties") or {})
        rec = by_geoid[props["GEOID"]]
        for ind_id in INDICATOR_IDS:
            if ind_id in rec:
                props[ind_id] = rec[ind_id]
        for extra in ("tract_name", "county_fips", "county_name"):
            if extra in rec and extra not in props:
                props[extra] = rec[extra]
        joined.append({
            "type": "Feature",
            "properties": props,
            "geometry": feat.get("geometry"),
        })

    print(f"  OK — {len(joined)} features carry indicator values")
    return joined


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", default=DEFAULT_BUNDLE_DIR,
                        help=f"Bundle output directory (default: {DEFAULT_BUNDLE_DIR})")
    args = parser.parse_args()

    tract_data = load_json(os.path.join(OUTPUT_DIR, "wi_tracts_acs.json"))
    tract_geo = load_json(os.path.join(OUTPUT_DIR, "wi_tracts.geojson"))
    county_geo = load_json(os.path.join(OUTPUT_DIR, "wi_counties.geojson"))
    county_rows = load_county_csv(os.path.join(OUTPUT_DIR, "wi_counties_acs.csv"))

    meta = tract_data.get("metadata", {})
    acs_year = meta.get("vintage_year")
    tiger_year = (tract_geo.get("metadata") or {}).get("vintage_year")

    print(f"ACS vintage:     {meta.get('vintage')} (year {acs_year})")
    print(f"TIGER vintage:   ACS{tiger_year}")
    if acs_year != tiger_year:
        # Not fatal on its own — the join check below is the real gate — but
        # this is the cause behind almost every join failure, so name it early.
        print(f"  WARNING: indicator vintage ({acs_year}) != boundary vintage "
              f"({tiger_year}). Tract boundaries change between vintages.")

    tract_features = join(tract_data["tracts"], tract_geo["features"],
                          "geoid", "tract")
    county_features = join(county_rows, county_geo["features"],
                           "geoid", "county")

    # --- Distributions, computed per level ---
    print("\nIndicator distributions (tract level):")
    indicators_out = []
    for ind in INDICATORS:
        tract_vals = [(f["properties"].get(ind["id"])) for f in tract_features]
        county_vals = [(f["properties"].get(ind["id"])) for f in county_features]
        entry = dict(ind)
        entry["tract"] = distribution(tract_vals)
        entry["county"] = distribution(county_vals)
        indicators_out.append(entry)

        d = entry["tract"]
        if d:
            print(f"  {ind['id']:<30} min={d['min']:>10,.1f}  med={d['median']:>10,.1f}  "
                  f"max={d['max']:>10,.1f}  breaks={d['breaks']}")
        else:
            print(f"  {ind['id']:<30} NO DATA")

    # --- Write bundle ---
    os.makedirs(args.out_dir, exist_ok=True)

    def write(name: str, payload: dict) -> None:
        path = os.path.join(args.out_dir, name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"))
        print(f"  {name:<20} {os.path.getsize(path) / 1024:>8.0f} KB")

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"\nWriting bundle to {args.out_dir}:")
    write("tracts.geojson", {
        "type": "FeatureCollection",
        "features": tract_features,
    })
    write("counties.geojson", {
        "type": "FeatureCollection",
        "features": county_features,
    })
    write("indicators.json", {
        "generated": generated_at,
        "classCount": N_CLASSES,
        "indicators": indicators_out,
    })
    write("manifest.json", {
        "generated": generated_at,
        "sources": [
            {
                "name": "American Community Survey 5-Year Estimates",
                "nameEs": "Encuesta sobre la Comunidad Estadounidense, estimaciones de 5 años",
                "publisher": "U.S. Census Bureau",
                "vintage": meta.get("vintage"),
                "released": "2025-12-11",
                "url": "https://www.census.gov/programs-surveys/acs/",
                "used_for": "indicator values",
            },
            {
                "name": "TIGERweb",
                "nameEs": "TIGERweb",
                "publisher": "U.S. Census Bureau",
                "vintage": f"ACS{tiger_year}",
                "url": "https://tigerweb.geo.census.gov/",
                "used_for": "tract and county boundary geometry",
            },
        ],
        "geography": {
            "state_fips": meta.get("state_fips"),
            "tracts": len(tract_features),
            "counties": len(county_features),
        },
        "indicatorCount": len(indicators_out),
        "resolutionMode": meta.get("resolution_mode"),
        "spanishReviewStatus": "pending-native-review",
    })

    print("\nDone.")


if __name__ == "__main__":
    main()
