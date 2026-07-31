#!/usr/bin/env python3
"""
Spatial LISA analysis for PDI — computes Local Moran's I on tract-level
indicators, classifies each tract (HH/LL/HL/LH/NS), and stores results
in the analysis_scores table.

Requires: psycopg, geopandas, libpysal, esda
Install: pip install geopandas esda libpysal psycopg[binary]

Usage:
  # Dry run — compute but don't write to DB
  python spatial_lisa.py --state 55 --variables poverty_rate,median_hh_income --dry-run

  # Full run — compute and store
  python spatial_lisa.py --state 55 --variables poverty_rate,median_hh_income,pct_cost_burdened

  # All variables
  python spatial_lisa.py --state 55 --variables all

Output: LISA cluster classifications for each tract stored in analysis_scores
with analysis type 'lisa' and score details containing:
  - variable_id: the indicator analyzed
  - cluster: HH, LL, HL, LH, or NS
  - moran_i: local Moran's I value
  - p_value: significance level
"""

import argparse
import os
import sys
import json

import geopandas as gpd
import pandas as pd
import libpysal
from esda.moran import Moran_Local

# Connection resolution lives in ingest/lib/db.py — see the note there on why
# the port-5432 default is dangerous. This file used to carry its own copy of
# that constant; two other files did too, and all three drifted from .env.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ingest"))

# Default variables to run LISA on.
DEFAULT_VARIABLES = [
    "poverty_rate",
    "median_household_income",
    "pct_cost_burdened",
    "uninsured_rate",
    "pct_poc",
]

CLUSTER_LABELS = {1: "HH", 2: "LL", 3: "HL", 4: "LH"}


def get_conn():
    """Lazy import to avoid a psycopg dependency for dry-run."""
    from lib.db import get_conn as _get_conn
    return _get_conn()


def fetch_tract_data(state_fips: str, variable_ids: list[str], atlas_dir: str = None) -> gpd.GeoDataFrame:
    """Fetch tract geometries and indicator values from Atlas static files."""
    import json

    if atlas_dir is None:
        atlas_dir = os.path.join(os.path.dirname(__file__), "output", "atlas")

    tracts_path = os.path.join(atlas_dir, "tracts.geojson")
    indicators_path = os.path.join(atlas_dir, "indicators.json")

    print(f"Reading tracts from {tracts_path}...")
    gdf = gpd.read_file(tracts_path)
    print(f"  Loaded {len(gdf)} tracts")

    # The GeoJSON properties contain indicator values with keys matching variable_ids
    # Check which variables are available
    available = [v for v in variable_ids if v in gdf.columns]
    missing = [v for v in variable_ids if v not in gdf.columns]

    if missing:
        print(f"  Variables not in GeoJSON: {missing}")
        # Try loading from indicators.json for metadata
        if os.path.exists(indicators_path):
            with open(indicators_path) as f:
                ind_meta = json.load(f)
            known_ids = [i["id"] for i in ind_meta.get("indicators", [])]
            print(f"  Known indicator IDs: {known_ids}")
            available = [v for v in variable_ids if v in known_ids and v in gdf.columns]

    if not available:
        print("No requested variables found in Atlas data")
        sys.exit(1)

    print(f"Fetched {len(gdf)} tracts with variables: {available}")
    return gdf, available


def compute_lisa(gdf: gpd.GeoDataFrame, variable_id: str) -> pd.DataFrame:
    """Compute Local Moran's I for one variable. Returns DataFrame of results."""
    values = gdf[variable_id].values

    if gdf[variable_id].isna().all():
        print(f"  {variable_id}: all null, skipping")
        return pd.DataFrame()

    # Drop nulls for weight building — need full contiguity
    valid_mask = ~gdf[variable_id].isna()
    valid_gdf = gdf[valid_mask].copy()
    valid_values = valid_gdf[variable_id].values

    if len(valid_values) < 10:
        print(f"  {variable_id}: too few valid tracts ({len(valid_values)}), skipping")
        return pd.DataFrame()

    # Build queen contiguity weights
    print(f"  {variable_id}: building queen weights for {len(valid_gdf)} tracts...")
    w = libpysal.weights.Queen.from_dataframe(valid_gdf, use_index=False, silence_warnings=True)

    # Run LISA with 999 permutations
    print(f"  {variable_id}: running LISA (999 permutations)...")
    lisa = Moran_Local(valid_values, w, permutations=999, seed=42)

    # Classify
    sig = lisa.p_sim < 0.05
    quads = lisa.q
    cluster = pd.Series("NS", index=valid_gdf.index)
    for label_code, label_name in CLUSTER_LABELS.items():
        cluster[(quads == label_code) & sig] = label_name

    # Use GEOID from the GeoJSON (uppercase)
    geoid_col = "GEOID" if "GEOID" in valid_gdf.columns else "geoid"
    results = pd.DataFrame({
        geoid_col: valid_gdf[geoid_col],
        "variable_id": variable_id,
        "moran_i": lisa.Is,
        "p_value": lisa.p_sim,
        "cluster": cluster.values,
        "quadrant": quads,
    })

    n_sig = (cluster != "NS").sum()
    n_total = len(results)
    print(f"  {variable_id}: {n_sig}/{n_total} significant tracts "
          f"({cluster.value_counts().to_dict() if n_sig > 0 else 'none'})")

    return results


def store_results(conn, results: pd.DataFrame, state_fips: str, variable_id: str, vintage: str = "ACS-2024-5yr"):
    """Store LISA results for ONE variable in the analysis_scores table.
    Each variable gets its own analysis record to avoid composite-key overwrites."""
    import uuid
    analysis_id = str(uuid.uuid4())

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO analyses (type, scope_geoid, scope_level, parameters, results, vintage)
            VALUES ('lisa', %s, 'state'::geo_level, %s::jsonb, %s::jsonb, %s)
            RETURNING id
        """, (
            state_fips,
            json.dumps({"variable": variable_id}),
            json.dumps({"tracts_analyzed": len(results)}),
            vintage,
        ))
        analysis_id = cur.fetchone()[0]

        geoid_col = "GEOID" if "GEOID" in results.columns else "geoid"
        for idx in range(len(results)):
            row = results.iloc[idx]
            cur.execute("""
                INSERT INTO analysis_scores (analysis_id, geoid, score, percentile, tier, details)
                VALUES (%s, %s, %s, 0, %s, %s::jsonb)
                ON CONFLICT (analysis_id, geoid) DO UPDATE SET
                    score = EXCLUDED.score, tier = EXCLUDED.tier, details = EXCLUDED.details
            """, (
                analysis_id,
                row[geoid_col],
                float(row["moran_i"]) if not pd.isna(row["moran_i"]) else 0.0,
                row["cluster"],
                json.dumps({
                    "variable_id": variable_id,
                    "moran_i": float(row["moran_i"]) if not pd.isna(row["moran_i"]) else None,
                    "p_value": float(row["p_value"]) if not pd.isna(row["p_value"]) else None,
                    "quadrant": int(row["quadrant"]) if not pd.isna(row["quadrant"]) else None,
                }),
            ))
    conn.commit()
    print(f"  Stored {len(results)} scores for {variable_id} under analysis_id={analysis_id}")


def main():
    parser = argparse.ArgumentParser(description="PDI Spatial LISA Analysis")
    parser.add_argument("--state", required=True, help="2-digit state FIPS (e.g. 55)")
    parser.add_argument("--variables", default="all",
                        help="Comma-separated variable IDs or 'all' for defaults")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute but don't write to database")
    parser.add_argument("--vintage", default="ACS-2024-5yr",
                        help="Data vintage label")
    args = parser.parse_args()

    if args.variables == "all":
        variable_ids = DEFAULT_VARIABLES
    else:
        variable_ids = [v.strip() for v in args.variables.split(",")]

    print(f"PDI Spatial LISA — state={args.state} variables={variable_ids}")

    # Fetch data from Atlas static files (run locally where the files are)
    gdf, available_vars = fetch_tract_data(args.state, variable_ids)

    # Compute LISA for each available variable
    all_results = []
    for var_id in available_vars:
        if var_id not in gdf.columns:
            print(f"  {var_id}: not in data, skipping")
            continue
        results = compute_lisa(gdf, var_id)
        if not results.empty:
            all_results.append(results)

    if not all_results:
        print("No results to store.")
        sys.exit(0)

    print(f"\nTotal: {len(all_results)} variable(s) computed")

    if args.dry_run:
        print("\n[dry-run] Skipping database write.")
        combined = pd.concat(all_results, ignore_index=True)
        print("Cluster distribution:")
        print(combined["cluster"].value_counts().to_string())
    else:
        conn = get_conn()
        for i, results in enumerate(all_results):
            var_id = available_vars[i]
            store_results(conn, results, args.state, var_id, args.vintage)
        conn.close()

    print("Done.")


if __name__ == "__main__":
    main()
