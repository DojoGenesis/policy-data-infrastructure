#!/usr/bin/env python3
"""
DB-driven EFA on WI tract-level SDOH indicators from PostGIS.

Pulls CDC PLACES and USDA food access indicators directly from the
indicators table, pivots to a wide feature matrix, runs EFA with
parallel analysis + oblimin rotation, and writes factor scores to
the factor_scores table.

Usage:
  python factor_analysis_db.py --dry-run     # preview feature matrix
  python factor_analysis_db.py               # full EFA + CSV + print
  python factor_analysis_db.py --load        # EFA + write to factor_scores
"""
import argparse
import csv
import os
import sys
from collections import defaultdict

import numpy as np
import psycopg

# Monkey-patch sklearn.utils.validation.check_array for scikit-learn 1.8+.
import sklearn.utils.validation as _skv
_orig_check_array = _skv.check_array
def _patched_check_array(*args, **kwargs):
    if "force_all_finite" in kwargs:
        kwargs["ensure_all_finite"] = kwargs.pop("force_all_finite")
    return _orig_check_array(*args, **kwargs)
_skv.check_array = _patched_check_array
import factor_analyzer.factor_analyzer as _fa_mod
import factor_analyzer.utils as _fa_utils
if hasattr(_fa_mod, "check_array"):
    _fa_mod.check_array = _patched_check_array
if hasattr(_fa_utils, "check_array"):
    _fa_utils.check_array = _patched_check_array

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")

DATABASE_URL = os.environ.get(
    "PDI_DATABASE_URL",
    "postgres://pdi:pdi@localhost:5432/pdi?sslmode=disable",
)

# Feature definitions: (variable_id_in_db, feature_name_for_matrix)
CDC_FEATURES = [
    ("cdc_access2",  "ACCESS2"),
    ("cdc_bphigh",   "BPHIGH"),
    ("cdc_casthma",  "CASTHMA"),
    ("cdc_csmoking", "CSMOKING"),
    ("cdc_diabetes", "DIABETES"),
    ("cdc_mhlth",    "MHLTH"),
    ("cdc_obesity",  "OBESITY"),
    ("cdc_phlth",    "PHLTH"),
]

USDA_FEATURES = [
    ("usda_lila",            "low_income_low_access"),
    ("usda_snap_authorized", "snap_flag"),
    ("usda_poverty_rate",    "poverty_rate"),
]

# Output CSVs
LOADINGS_CSV = os.path.join(OUTPUT_DIR, "factor_loadings.csv")
SCORES_CSV = os.path.join(OUTPUT_DIR, "factor_scores.csv")


def get_conn():
    return psycopg.connect(DATABASE_URL)


def fetch_cdc(conn) -> dict[str, dict[str, float]]:
    """Pull CDC PLACES data from indicators, pivot to wide."""
    placeholders = ",".join(["%s"] * len(CDC_FEATURES))
    var_ids = [f[0] for f in CDC_FEATURES]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT geoid, variable_id, value
            FROM indicators
            WHERE variable_id IN ({placeholders})
              AND geoid LIKE '55%%'
              AND LENGTH(geoid) = 11
              AND value IS NOT NULL
        """, var_ids)
        rows = cur.fetchall()

    data: dict[str, dict[str, float]] = defaultdict(dict)
    for geoid, var_id, val in rows:
        data[geoid][var_id] = float(val)
    return dict(data)


def fetch_usda(conn) -> dict[str, dict[str, float]]:
    """Pull USDA food access data from indicators, pivot to wide."""
    placeholders = ",".join(["%s"] * len(USDA_FEATURES))
    var_ids = [f[0] for f in USDA_FEATURES]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT geoid, variable_id, value
            FROM indicators
            WHERE variable_id IN ({placeholders})
              AND geoid LIKE '55%%'
              AND LENGTH(geoid) = 11
              AND value IS NOT NULL
        """, var_ids)
        rows = cur.fetchall()

    data: dict[str, dict[str, float]] = defaultdict(dict)
    for geoid, var_id, val in rows:
        data[geoid][var_id] = float(val)
    return dict(data)


def build_feature_matrix(conn) -> tuple[list[str], list[str], np.ndarray]:
    """Build wide feature matrix from PostGIS indicators."""
    cdc = fetch_cdc(conn)
    usda = fetch_usda(conn)
    print(f"  CDC tracts: {len(cdc)}, USDA tracts: {len(usda)}")

    # Feature column names (in matrix order)
    feature_names = [f[1] for f in CDC_FEATURES] + [f[1] for f in USDA_FEATURES]

    # Intersect geoids across both sources
    all_geoids = sorted(set(cdc.keys()) & set(usda.keys()))
    print(f"  Intersection: {len(all_geoids)} tracts")

    geoids: list[str] = []
    rows: list[list[float]] = []

    for geoid in all_geoids:
        row: list[float] = []
        complete = True

        for db_id, _feat_name in CDC_FEATURES:
            val = cdc[geoid].get(db_id)
            if val is None:
                complete = False
                break
            row.append(val)
        if not complete:
            continue

        for db_id, _feat_name in USDA_FEATURES:
            val = usda[geoid].get(db_id)
            if val is None:
                complete = False
                break
            row.append(val)
        if not complete:
            continue

        geoids.append(geoid)
        rows.append(row)

    matrix = np.array(rows, dtype=np.float64)
    return geoids, feature_names, matrix


def print_plan(conn) -> None:
    """Dry-run: show feature matrix statistics."""
    print("[dry-run] Factor Analysis plan (PostGIS-driven)")
    print()

    geoids, features, matrix = build_feature_matrix(conn)
    print(f"  Feature matrix: {matrix.shape[0]} tracts x {matrix.shape[1]} features")
    print(f"  Features: {', '.join(features)}")
    print()
    print("  Per-feature statistics:")
    print(f"  {'Feature':<25} {'Mean':>8} {'Std':>8} {'Min':>8} {'Max':>8} {'Nulls':>6}")
    for i, name in enumerate(features):
        col = matrix[:, i]
        print(f"  {name:<25} {np.mean(col):>8.2f} {np.std(col):>8.2f} "
              f"{np.min(col):>8.2f} {np.max(col):>8.2f} {np.sum(np.isnan(col)):>6.0f}")

    print()
    print("  Method: EFA with parallel analysis + oblimin rotation")
    print("  Expected factors: 3-5 (from 11 features)")
    print("  Note: ICE score excluded — B19001 data not available in indicators table")


def run_efa(matrix: np.ndarray, feature_names: list[str],
            max_factors: int = 8) -> tuple[int, np.ndarray, np.ndarray]:
    """Run EFA with parallel analysis for factor count, then oblimin rotation."""
    from factor_analyzer import FactorAnalyzer
    from factor_analyzer.factor_analyzer import calculate_bartlett_sphericity, calculate_kmo

    n_samples, n_features = matrix.shape

    # Standardize
    means = np.mean(matrix, axis=0)
    stds = np.std(matrix, axis=0)
    stds[stds == 0] = 1
    standardized = (matrix - means) / stds

    # Bartlett's test
    chi2, p_value = calculate_bartlett_sphericity(standardized)
    print(f"\n  Bartlett's test: chi2={chi2:.1f}, p={p_value:.2e}")
    if p_value > 0.05:
        print("  WARNING: Bartlett's test not significant")

    # KMO test
    kmo_all, kmo_model = calculate_kmo(standardized)
    print(f"  KMO: {kmo_model:.3f}", end="")
    if kmo_model < 0.6:
        print(" (mediocre)")
    elif kmo_model < 0.7:
        print(" (middling)")
    elif kmo_model < 0.8:
        print(" (meritorious)")
    else:
        print(" (marvelous)")

    # Parallel analysis
    print("\n  Parallel analysis (eigenvalue comparison):")
    corr_matrix = np.corrcoef(standardized.T)
    actual_eigenvalues = np.sort(np.linalg.eigvalsh(corr_matrix))[::-1]

    n_iterations = 100
    random_eigenvalues = np.zeros((n_iterations, n_features))
    for i in range(n_iterations):
        random_data = np.random.normal(size=(n_samples, n_features))
        random_eigenvalues[i] = np.sort(np.linalg.eigvalsh(
            np.corrcoef(random_data.T)
        ))[::-1]

    threshold = np.percentile(random_eigenvalues, 95, axis=0)

    n_factors = 0
    for i in range(min(max_factors, n_features)):
        actual = actual_eigenvalues[i]
        thresh = threshold[i]
        retain = "RETAIN" if actual > thresh else "drop"
        print(f"    Factor {i+1}: eigenvalue={actual:.3f}, 95th pct threshold={thresh:.3f}  [{retain}]")
        if actual > thresh:
            n_factors = i + 1

    if n_factors == 0:
        n_factors = 1
        print("  WARNING: No factors exceeded parallel analysis threshold, using 1 factor")

    print(f"\n  Selected {n_factors} factors via parallel analysis")

    fa = FactorAnalyzer(n_factors=n_factors, rotation="oblimin", method="minres")
    fa.fit(standardized)

    loadings = fa.loadings_
    scores = fa.transform(standardized)
    variance = fa.get_factor_variance()

    print(f"\n  Variance explained:")
    for i in range(n_factors):
        print(f"    Factor {i+1}: {variance[1][i]*100:.1f}% (cumulative: {variance[2][i]*100:.1f}%)")

    return n_factors, loadings, scores


def name_factors(loadings: np.ndarray, feature_names: list[str],
                 threshold: float = 0.35) -> list[str]:
    """Name factors based on highest-loading features."""
    factor_names: list[str] = []
    for j in range(loadings.shape[1]):
        col = loadings[:, j]
        high_features = [
            (feature_names[i], col[i])
            for i in range(len(feature_names))
            if abs(col[i]) > threshold
        ]
        high_features.sort(key=lambda x: abs(x[1]), reverse=True)

        if not high_features:
            factor_names.append(f"factor_{j+1}")
            continue

        top = [f[0] for f in high_features[:2]]
        name_map = {
            "OBESITY": "metabolic", "DIABETES": "metabolic",
            "BPHIGH": "cardiovascular", "CSMOKING": "behavioral",
            "CASTHMA": "respiratory", "MHLTH": "mental_health",
            "PHLTH": "physical_health", "ACCESS2": "health_access",
            "poverty_rate": "economic_deprivation",
            "low_income_low_access": "food_insecurity",
            "snap_flag": "food_assistance",
        }
        mapped = [name_map.get(t, t) for t in top]
        name = "_".join(dict.fromkeys(mapped))
        factor_names.append(name)

    return factor_names


def write_loadings(feature_names: list[str], factor_names: list[str],
                   loadings: np.ndarray) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(LOADINGS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["feature"] + factor_names)
        for i, feat in enumerate(feature_names):
            writer.writerow([feat] + [f"{loadings[i, j]:.4f}" for j in range(loadings.shape[1])])
    print(f"  Wrote loadings to {LOADINGS_CSV}")


def write_scores(geoids: list[str], factor_names: list[str],
                 scores: np.ndarray) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(SCORES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["geoid"] + factor_names)
        for i, geoid in enumerate(geoids):
            writer.writerow([geoid] + [f"{scores[i, j]:.4f}" for j in range(scores.shape[1])])
    print(f"  Wrote {len(geoids)} tract scores to {SCORES_CSV}")


def load_to_db(conn, geoids: list[str], factor_names: list[str],
               scores: np.ndarray) -> None:
    """Write factor scores to the factor_scores table."""
    vintage = "2023-efa-v1"
    count = 0
    with conn.cursor() as cur:
        for i, geoid in enumerate(geoids):
            for j, fname in enumerate(factor_names):
                cur.execute("""
                    INSERT INTO factor_scores (geoid, factor_name, factor_score, analysis_vintage)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (geoid, factor_name, analysis_vintage) DO UPDATE
                    SET factor_score = EXCLUDED.factor_score
                """, (geoid, fname, float(scores[i, j]), vintage))
                count += 1
        conn.commit()
    print(f"  {count} factor score rows written to database (vintage={vintage})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DB-driven EFA on WI tract-level SDOH indicators from PostGIS.",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show feature matrix stats without running EFA")
    parser.add_argument("--load", action="store_true",
                        help="Write factor scores to PostGIS after EFA")
    args = parser.parse_args()

    conn = get_conn()
    try:
        if args.dry_run:
            print_plan(conn)
            return

        print("Building feature matrix from PostGIS...")
        geoids, feature_names, matrix = build_feature_matrix(conn)
        print(f"  {matrix.shape[0]} tracts x {matrix.shape[1]} features")

        print("\nRunning Exploratory Factor Analysis...")
        n_factors, loadings, scores = run_efa(matrix, feature_names)

        factor_names_list = name_factors(loadings, feature_names)
        print(f"\n  Named factors: {', '.join(factor_names_list)}")

        # Print loading table
        print(f"\n  Factor Loading Matrix (|loading| > 0.30 highlighted):")
        header = f"  {'Feature':<25}" + "".join(f"{fn:>25}" for fn in factor_names_list)
        print(header)
        print("  " + "-" * (25 + 25 * n_factors))
        for i, feat in enumerate(feature_names):
            row = f"  {feat:<25}"
            for j in range(n_factors):
                val = loadings[i, j]
                marker = "*" if abs(val) > 0.30 else " "
                row += f"{val:>24.3f}{marker}"
            print(row)

        write_loadings(feature_names, factor_names_list, loadings)
        write_scores(geoids, factor_names_list, scores)

        if args.load:
            load_to_db(conn, geoids, factor_names_list, scores)

        print("\nDone.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
