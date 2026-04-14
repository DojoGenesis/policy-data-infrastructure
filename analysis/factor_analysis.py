#!/usr/bin/env python3
"""
Exploratory Factor Analysis (EFA) on Wisconsin tract-level SDOH indicators.

Combines CDC PLACES health measures, ICE income-race scores, and USDA food
access indicators into a unified feature matrix, then runs EFA with parallel
analysis for factor count determination and oblimin rotation for interpretability.

Reference: Kolak et al. 2020 — 4 SDOH factors explaining 71% variance at 72K tracts.
We expect 3-5 factors from our ~12-feature, ~1,400-tract Wisconsin dataset.

Usage:
  python factor_analysis.py --dry-run          # preview feature matrix stats
  python factor_analysis.py                    # full EFA + CSV output
  python factor_analysis.py --load             # EFA + write to factor_scores table

Outputs:
  analysis/output/factor_loadings.csv    — factor loading matrix
  analysis/output/factor_scores.csv      — per-tract factor scores
"""
import argparse
import csv
import os
import sys
from collections import defaultdict

import numpy as np

# Monkey-patch sklearn.utils.validation.check_array for scikit-learn 1.8+.
# factor_analyzer uses check_array(force_all_finite=...) which was renamed
# to ensure_all_finite in sklearn 1.8. We patch at the source module AND
# in factor_analyzer's imported reference.
import sklearn.utils.validation as _skv
_orig_check_array = _skv.check_array
def _patched_check_array(*args, **kwargs):
    if "force_all_finite" in kwargs:
        kwargs["ensure_all_finite"] = kwargs.pop("force_all_finite")
    return _orig_check_array(*args, **kwargs)
_skv.check_array = _patched_check_array
# Also patch the reference inside factor_analyzer modules
import factor_analyzer.factor_analyzer as _fa_mod
import factor_analyzer.utils as _fa_utils
if hasattr(_fa_mod, "check_array"):
    _fa_mod.check_array = _patched_check_array
if hasattr(_fa_utils, "check_array"):
    _fa_utils.check_array = _patched_check_array

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")

# Input CSVs
CDC_PLACES_CSV = os.path.join(OUTPUT_DIR, "wi_health_cdc_places.csv")
ICE_CSV = os.path.join(OUTPUT_DIR, "wi_ice_b19001.csv")
FOOD_CSV = os.path.join(OUTPUT_DIR, "wi_food_access.csv")

# Output CSVs
LOADINGS_CSV = os.path.join(OUTPUT_DIR, "factor_loadings.csv")
SCORES_CSV = os.path.join(OUTPUT_DIR, "factor_scores.csv")

# CDC PLACES measures to include (all 8 available)
CDC_MEASURES = [
    "ACCESS2",   # Lack of health insurance
    "BPHIGH",    # High blood pressure
    "CASTHMA",   # Current asthma
    "CSMOKING",  # Current smoking
    "DIABETES",  # Diabetes
    "MHLTH",     # Mental health not good
    "OBESITY",   # Obesity
    "PHLTH",     # Physical health not good
]

# Food access features
FOOD_FEATURES = ["low_income_low_access", "snap_flag", "poverty_rate"]


def load_cdc_places() -> dict[str, dict[str, float]]:
    """Load CDC PLACES data, pivot from long to wide format."""
    data: dict[str, dict[str, float]] = defaultdict(dict)
    with open(CDC_PLACES_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            geoid = row["geoid"]
            measure = row["measure"]
            val = row.get("data_value", "")
            if measure in CDC_MEASURES and val:
                try:
                    data[geoid][measure] = float(val)
                except ValueError:
                    pass
    return dict(data)


def load_ice() -> dict[str, float]:
    """Load ICE scores from B19001 CSV."""
    data: dict[str, float] = {}
    with open(ICE_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            val = row.get("ice_score", "")
            if val:
                try:
                    data[row["geoid"]] = float(val)
                except ValueError:
                    pass
    return data


def load_food_access() -> dict[str, dict[str, float]]:
    """Load USDA food access features."""
    data: dict[str, dict[str, float]] = {}
    with open(FOOD_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            geoid = row["geoid"]
            features: dict[str, float] = {}
            for feat in FOOD_FEATURES:
                val = row.get(feat, "")
                if val and val != "NULL":
                    try:
                        features[feat] = float(val)
                    except ValueError:
                        pass
            if features:
                data[geoid] = features
    return data


def build_feature_matrix() -> tuple[list[str], list[str], np.ndarray]:
    """
    Build the feature matrix by joining CDC PLACES + ICE + food access.

    Returns:
        geoids: list of tract GEOIDs
        feature_names: list of feature column names
        matrix: (n_tracts, n_features) numpy array
    """
    cdc = load_cdc_places()
    ice = load_ice()
    food = load_food_access()

    # Feature columns
    feature_names = CDC_MEASURES + ["ice_score"] + FOOD_FEATURES

    # Find tracts with data across all sources
    all_geoids = sorted(set(cdc.keys()) & set(ice.keys()) & set(food.keys()))

    geoids: list[str] = []
    rows: list[list[float]] = []

    for geoid in all_geoids:
        row: list[float] = []
        complete = True

        # CDC PLACES
        for m in CDC_MEASURES:
            val = cdc.get(geoid, {}).get(m)
            if val is None:
                complete = False
                break
            row.append(val)

        if not complete:
            continue

        # ICE
        row.append(ice[geoid])

        # Food access
        for feat in FOOD_FEATURES:
            val = food.get(geoid, {}).get(feat)
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


def print_plan() -> None:
    """Dry-run: show feature matrix statistics."""
    print("[dry-run] Factor Analysis plan")
    print()

    geoids, features, matrix = build_feature_matrix()
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
    print("  Expected factors: 3-5 (from 12 features, ~1,400 tracts)")
    print("  Reference: Kolak et al. 2020 (4 factors from 50+ features)")


def run_efa(matrix: np.ndarray, feature_names: list[str],
            max_factors: int = 8) -> tuple[int, np.ndarray, np.ndarray]:
    """
    Run EFA with parallel analysis for factor count, then oblimin rotation.

    Returns:
        n_factors: number of factors selected
        loadings: (n_features, n_factors) loading matrix
        scores: (n_tracts, n_factors) factor scores
    """
    from factor_analyzer import FactorAnalyzer
    from factor_analyzer.factor_analyzer import calculate_bartlett_sphericity, calculate_kmo

    n_samples, n_features = matrix.shape

    # Standardize
    means = np.mean(matrix, axis=0)
    stds = np.std(matrix, axis=0)
    stds[stds == 0] = 1  # avoid division by zero
    standardized = (matrix - means) / stds

    # Bartlett's test of sphericity
    chi2, p_value = calculate_bartlett_sphericity(standardized)
    print(f"\n  Bartlett's test: chi2={chi2:.1f}, p={p_value:.2e}")
    if p_value > 0.05:
        print("  WARNING: Bartlett's test not significant — data may not be suitable for EFA")

    # KMO test
    kmo_all, kmo_model = calculate_kmo(standardized)
    print(f"  KMO: {kmo_model:.3f}", end="")
    if kmo_model < 0.6:
        print(" (mediocre — proceed with caution)")
    elif kmo_model < 0.7:
        print(" (middling)")
    elif kmo_model < 0.8:
        print(" (meritorious)")
    else:
        print(" (marvelous)")

    # Parallel analysis for factor count — use correlation matrix eigenvalues
    # directly to avoid scikit-learn API compatibility issues with factor_analyzer.
    print("\n  Parallel analysis (eigenvalue comparison):")
    corr_matrix = np.corrcoef(standardized.T)
    actual_eigenvalues = np.sort(np.linalg.eigvalsh(corr_matrix))[::-1]

    # Generate random eigenvalues (parallel analysis)
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

    # Final EFA with oblimin rotation
    # Use minres method which is more robust than ML for small sample/feature ratios
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
    """
    Name factors based on their highest-loading features.
    """
    factor_names: list[str] = []
    for j in range(loadings.shape[1]):
        col = loadings[:, j]
        # Get features with absolute loading > threshold
        high_features = [
            (feature_names[i], col[i])
            for i in range(len(feature_names))
            if abs(col[i]) > threshold
        ]
        high_features.sort(key=lambda x: abs(x[1]), reverse=True)

        if not high_features:
            factor_names.append(f"factor_{j+1}")
            continue

        # Name by top 2 loading features
        top = [f[0] for f in high_features[:2]]
        # Map to interpretable names
        name_map = {
            "OBESITY": "metabolic",
            "DIABETES": "metabolic",
            "BPHIGH": "cardiovascular",
            "CSMOKING": "behavioral",
            "CASTHMA": "respiratory",
            "MHLTH": "mental_health",
            "PHLTH": "physical_health",
            "ACCESS2": "health_access",
            "ice_score": "economic_concentration",
            "poverty_rate": "economic_deprivation",
            "low_income_low_access": "food_insecurity",
            "snap_flag": "food_assistance",
        }
        mapped = [name_map.get(t, t) for t in top]
        name = "_".join(dict.fromkeys(mapped))  # deduplicate preserving order
        factor_names.append(name)

    return factor_names


def write_loadings(feature_names: list[str], factor_names: list[str],
                   loadings: np.ndarray) -> None:
    """Write factor loading matrix to CSV."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(LOADINGS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["feature"] + factor_names)
        for i, feat in enumerate(feature_names):
            writer.writerow([feat] + [f"{loadings[i, j]:.4f}" for j in range(loadings.shape[1])])
    print(f"  Wrote loadings to {LOADINGS_CSV}")


def write_scores(geoids: list[str], factor_names: list[str],
                 scores: np.ndarray) -> None:
    """Write per-tract factor scores to CSV."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(SCORES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["geoid"] + factor_names)
        for i, geoid in enumerate(geoids):
            writer.writerow([geoid] + [f"{scores[i, j]:.4f}" for j in range(scores.shape[1])])
    print(f"  Wrote {len(geoids)} tract scores to {SCORES_CSV}")


def load_to_db(geoids: list[str], factor_names: list[str],
               scores: np.ndarray) -> None:
    """Write factor scores to the factor_scores table."""
    sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "ingest"))
    from lib.db import get_conn

    print("Connecting to database...")
    conn = get_conn()
    cur = conn.cursor()

    vintage = "2023-efa-v1"
    count = 0
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
    cur.close()
    conn.close()
    print(f"  {count} factor score rows written to database (vintage={vintage})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Exploratory Factor Analysis on WI tract-level SDOH indicators.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show feature matrix stats without running EFA")
    parser.add_argument("--load", action="store_true",
                        help="Write factor scores to PostGIS after EFA")
    args = parser.parse_args()

    if args.dry_run:
        print_plan()
        return

    print("Building feature matrix...")
    geoids, feature_names, matrix = build_feature_matrix()
    print(f"  {matrix.shape[0]} tracts x {matrix.shape[1]} features")

    print("\nRunning Exploratory Factor Analysis...")
    n_factors, loadings, scores = run_efa(matrix, feature_names)

    factor_names = name_factors(loadings, feature_names)
    print(f"\n  Named factors: {', '.join(factor_names)}")

    # Print loading table
    print(f"\n  Factor Loading Matrix (|loading| > 0.30 highlighted):")
    header = f"  {'Feature':<25}" + "".join(f"{fn:>25}" for fn in factor_names)
    print(header)
    print("  " + "-" * (25 + 25 * n_factors))
    for i, feat in enumerate(feature_names):
        row = f"  {feat:<25}"
        for j in range(n_factors):
            val = loadings[i, j]
            marker = "*" if abs(val) > 0.30 else " "
            row += f"{val:>24.3f}{marker}"
        print(row)

    write_loadings(feature_names, factor_names, loadings)
    write_scores(geoids, factor_names, scores)

    if args.load:
        load_to_db(geoids, factor_names, scores)

    print("\nDone.")


if __name__ == "__main__":
    main()
