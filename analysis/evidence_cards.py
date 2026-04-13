#!/usr/bin/env python3
"""
Produce per-policy evidence cards by analyzing Wisconsin ACS data
against Francesca Hong's policy positions.

Reads: data/policies/francesca_hong_2026.csv
       analysis/output/wi_counties_acs.csv (run fetch_wi_counties.py first)

Outputs: analysis/output/evidence_cards.json
         analysis/output/evidence_summary.md

Usage:
    python analysis/evidence_cards.py
    python analysis/evidence_cards.py --dry-run    # print analysis plan
    python analysis/evidence_cards.py --min-relevance 0.6
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import statistics
from collections import Counter
from dataclasses import dataclass, asdict, field
from typing import Optional

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------

_REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
POLICIES_CSV = os.path.join(_REPO_ROOT, "data", "policies", "francesca_hong_2026.csv")
ACS_CSV = os.path.join(os.path.dirname(__file__), "output", "wi_counties_acs.csv")
CDC_PLACES_CSV = os.path.join(os.path.dirname(__file__), "output", "wi_health_cdc_places.csv")
USDA_FOOD_CSV = os.path.join(os.path.dirname(__file__), "output", "wi_food_access.csv")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
CARDS_JSON = os.path.join(OUTPUT_DIR, "evidence_cards.json")
SUMMARY_MD = os.path.join(OUTPUT_DIR, "evidence_summary.md")

# ---------------------------------------------------------------------------
# Equity dimension configuration
# ---------------------------------------------------------------------------

# Maps equity_dimension → (metric_fn_name, sort_ascending, data_quality, description)
# metric_fn_name references a function in METRIC_FUNCTIONS below.
DIMENSION_CONFIG: dict[str, dict] = {
    "food_access": {
        "metric": "usda_lila_pct",
        "ascending": False,  # higher % food desert tracts = more need
        "data_quality": "strong",
        "quality_note": "USDA 2019 low-income + low-access tract percentage (direct food desert measure)",
    },
    "childcare_access": {
        "metric": "poverty_rate",
        "ascending": False,
        "data_quality": "moderate",
        "quality_note": "poverty rate as childcare affordability proxy",
    },
    "education_funding": {
        "metric": "median_hh_income_inverse",
        "ascending": False,  # lower income = higher need
        "data_quality": "moderate",
        "quality_note": "median household income (inverse) as property tax burden proxy",
    },
    "education_equity": {
        "metric": "race_poverty_interaction",
        "ascending": False,
        "data_quality": "moderate",
        "quality_note": "pct_non_hispanic_black × poverty_rate interaction",
    },
    "housing_affordability": {
        "metric": "pct_cost_burdened",
        "ascending": False,
        "data_quality": "strong",
        "quality_note": "direct ACS housing cost burden indicator",
    },
    "housing_stability": {
        "metric": "pct_severely_cost_burdened",
        "ascending": False,
        "data_quality": "strong",
        "quality_note": "direct ACS severe housing cost burden indicator",
    },
    "eviction_prevention": {
        "metric": "burden_poverty_interaction",
        "ascending": False,
        "data_quality": "moderate",
        "quality_note": "pct_cost_burdened × poverty_rate interaction",
    },
    "transit_access": {
        "metric": "poverty_rate",
        "ascending": False,
        "data_quality": "moderate",
        "quality_note": "poverty rate as transit dependency proxy",
    },
    "health_access": {
        "metric": "health_vulnerability_composite",
        "ascending": False,
        "data_quality": "strong",
        "quality_note": "ACS uninsured rate + CDC PLACES no-healthcare-access composite",
    },
    "income_equity": {
        "metric": "median_hh_income_inverse",
        "ascending": False,
        "data_quality": "strong",
        "quality_note": "median household income (ascending = lowest income counties)",
    },
    "economic_equity": {
        "metric": "poverty_burden_composite",
        "ascending": False,
        "data_quality": "moderate",
        "quality_note": "poverty_rate + pct_cost_burdened composite",
    },
    "labor_rights": {
        "metric": "median_hh_income_inverse",
        "ascending": False,
        "data_quality": "moderate",
        "quality_note": "median household income (inverse) as wage vulnerability proxy",
    },
    "environmental_health": {
        "metric": "cdc_physical_health_composite",
        "ascending": False,
        "data_quality": "moderate",
        "quality_note": "CDC PLACES mean of obesity + diabetes + physical health burden rates",
    },
    "criminal_justice": {
        "metric": "race_poverty_interaction",
        "ascending": False,
        "data_quality": "moderate",
        "quality_note": "pct_non_hispanic_black × poverty_rate interaction",
    },
}

# Equity dimensions that have ACS-based analysis configured
ANALYZABLE_DIMENSIONS = set(DIMENSION_CONFIG.keys())

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class CountyEntry:
    """A county with its computed ranking metric value."""
    geoid: str
    county_name: str
    metric_value: Optional[float]


@dataclass
class EvidenceCard:
    policy_id: str
    policy_title: str
    category: str
    equity_dimension: str
    statewide_context: dict       # summary stats for relevant indicators
    county_variation: dict        # min/max/range/std across 72 counties
    top_need_counties: list       # top 5 counties where policy would have most impact
    bottom_need_counties: list    # 5 counties with least need
    key_finding: str              # one-sentence finding
    data_quality: str             # "strong" | "moderate" | "weak"


# ---------------------------------------------------------------------------
# Metric computation functions
# ---------------------------------------------------------------------------

def _metric_poverty_rate(row: dict) -> Optional[float]:
    return _safe_float(row.get("poverty_rate"))


def _metric_pct_cost_burdened(row: dict) -> Optional[float]:
    return _safe_float(row.get("pct_cost_burdened"))


def _metric_pct_severely_cost_burdened(row: dict) -> Optional[float]:
    return _safe_float(row.get("pct_severely_cost_burdened"))


def _metric_uninsured_rate(row: dict) -> Optional[float]:
    return _safe_float(row.get("uninsured_rate"))


def _metric_pct_poc(row: dict) -> Optional[float]:
    return _safe_float(row.get("pct_poc"))


def _metric_median_hh_income_inverse(row: dict) -> Optional[float]:
    """Returns negative of median income so that lower income = higher score."""
    val = _safe_float(row.get("median_hh_income"))
    if val is None:
        return None
    return -val


def _metric_race_poverty_interaction(row: dict) -> Optional[float]:
    """pct_non_hispanic_black × poverty_rate — measures compounded disadvantage."""
    nhb = _safe_float(row.get("pct_non_hispanic_black"))
    pov = _safe_float(row.get("poverty_rate"))
    if nhb is None or pov is None:
        return None
    return round(nhb * pov / 100.0, 4)  # divide by 100 to keep in reasonable range


def _metric_burden_poverty_interaction(row: dict) -> Optional[float]:
    """pct_cost_burdened × poverty_rate."""
    burden = _safe_float(row.get("pct_cost_burdened"))
    pov = _safe_float(row.get("poverty_rate"))
    if burden is None or pov is None:
        return None
    return round(burden * pov / 100.0, 4)


def _metric_poverty_burden_composite(row: dict) -> Optional[float]:
    """poverty_rate + pct_cost_burdened composite (simple sum, equal weight)."""
    pov = _safe_float(row.get("poverty_rate"))
    burden = _safe_float(row.get("pct_cost_burdened"))
    if pov is None and burden is None:
        return None
    return (pov or 0.0) + (burden or 0.0)


def _metric_usda_lila_pct(row: dict) -> Optional[float]:
    """% of tracts in county that are low-income + low-access (food deserts)."""
    return _safe_float(row.get("usda_lila_pct"))


def _metric_cdc_obesity_rate(row: dict) -> Optional[float]:
    return _safe_float(row.get("cdc_obesity_rate"))


def _metric_cdc_diabetes_rate(row: dict) -> Optional[float]:
    return _safe_float(row.get("cdc_diabetes_rate"))


def _metric_cdc_mental_health_rate(row: dict) -> Optional[float]:
    return _safe_float(row.get("cdc_mental_health_rate"))


def _metric_cdc_no_healthcare_access(row: dict) -> Optional[float]:
    """% adults with no healthcare access (no doctor visit in past year)."""
    return _safe_float(row.get("cdc_no_healthcare_access"))


def _metric_health_vulnerability_composite(row: dict) -> Optional[float]:
    """Composite of uninsured rate + no-healthcare-access CDC measure."""
    uninsured = _safe_float(row.get("uninsured_rate"))
    no_access = _safe_float(row.get("cdc_no_healthcare_access"))
    if uninsured is None and no_access is None:
        return None
    return (uninsured or 0.0) + (no_access or 0.0)


def _metric_cdc_physical_health_composite(row: dict) -> Optional[float]:
    """Sum of obesity + diabetes + physical health not good rates (CDC PLACES)."""
    obesity = _safe_float(row.get("cdc_obesity_rate"))
    diabetes = _safe_float(row.get("cdc_diabetes_rate"))
    phlth = _safe_float(row.get("cdc_physical_health_rate"))
    vals = [v for v in [obesity, diabetes, phlth] if v is not None]
    return round(sum(vals) / len(vals), 2) if vals else None


METRIC_FUNCTIONS = {
    "poverty_rate": _metric_poverty_rate,
    "pct_cost_burdened": _metric_pct_cost_burdened,
    "pct_severely_cost_burdened": _metric_pct_severely_cost_burdened,
    "uninsured_rate": _metric_uninsured_rate,
    "pct_poc": _metric_pct_poc,
    "median_hh_income_inverse": _metric_median_hh_income_inverse,
    "race_poverty_interaction": _metric_race_poverty_interaction,
    "burden_poverty_interaction": _metric_burden_poverty_interaction,
    "poverty_burden_composite": _metric_poverty_burden_composite,
    # CDC PLACES metrics
    "usda_lila_pct": _metric_usda_lila_pct,
    "cdc_obesity_rate": _metric_cdc_obesity_rate,
    "cdc_diabetes_rate": _metric_cdc_diabetes_rate,
    "cdc_mental_health_rate": _metric_cdc_mental_health_rate,
    "cdc_no_healthcare_access": _metric_cdc_no_healthcare_access,
    "health_vulnerability_composite": _metric_health_vulnerability_composite,
    "cdc_physical_health_composite": _metric_cdc_physical_health_composite,
}

# ---------------------------------------------------------------------------
# Summary stats helpers
# ---------------------------------------------------------------------------

def _safe_float(val) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


def _summary_stats(values: list[float]) -> dict:
    """Compute summary statistics for a list of non-null floats."""
    if not values:
        return {"count": 0, "mean": None, "median": None, "min": None, "max": None, "std": None}
    return {
        "count": len(values),
        "mean": round(statistics.mean(values), 2),
        "median": round(statistics.median(values), 2),
        "min": round(min(values), 2),
        "max": round(max(values), 2),
        "std": round(statistics.stdev(values), 2) if len(values) > 1 else 0.0,
    }


# ---------------------------------------------------------------------------
# Key finding generator
# ---------------------------------------------------------------------------

def _generate_key_finding(
    policy_title: str,
    equity_dimension: str,
    top_counties: list[dict],
    bottom_counties: list[dict],
    stats: dict,
    dim_config: dict,
) -> str:
    """Generate a one-sentence key finding for an evidence card."""
    if not top_counties:
        return f"Insufficient ACS data to identify county-level need for {policy_title}."

    top_names = [c["county_name"].replace(" County", "") for c in top_counties[:2]]
    metric_name = dim_config["metric"]
    quality = dim_config["data_quality"]
    mean_val = stats.get("mean")
    mean_str = f"{mean_val:.1f}%" if mean_val is not None else "N/A"

    top_1 = top_counties[0]
    top_val = top_1.get("display_value")
    top_val_str = f"{top_val:.1f}%" if top_val is not None else "highest need"

    # Dimension-specific phrasing
    DIMENSION_PHRASING = {
        "food_access": (
            f"Wisconsin's highest-poverty counties ({', '.join(top_names)} at {top_val_str}) "
            f"would benefit most from this food access policy "
            f"(statewide mean poverty rate: {mean_str})."
        ),
        "childcare_access": (
            f"Counties with the highest poverty rates ({', '.join(top_names)} at {top_val_str}) "
            f"face the greatest childcare affordability burden "
            f"(statewide mean poverty rate: {mean_str})."
        ),
        "education_funding": (
            f"The lowest-income counties ({', '.join(top_names)}) "
            f"face the highest relative property tax burden for school funding, "
            f"making them the primary beneficiaries of income-to-property tax reform."
        ),
        "education_equity": (
            f"Counties where racial and economic disadvantage compound — "
            f"particularly {', '.join(top_names)} — show the greatest need "
            f"for equitable education policy."
        ),
        "housing_affordability": (
            f"{', '.join(top_names)} lead Wisconsin with {top_val_str} of households "
            f"cost-burdened (statewide mean: {mean_str}), making them primary targets "
            f"for affordable housing investment."
        ),
        "housing_stability": (
            f"{', '.join(top_names)} have the highest rates of severe housing cost burden "
            f"({top_val_str}), indicating the greatest risk of housing instability "
            f"(statewide mean: {mean_str})."
        ),
        "eviction_prevention": (
            f"Counties combining high housing cost burden with high poverty — "
            f"led by {', '.join(top_names)} — face the greatest eviction risk "
            f"and would benefit most from right-to-counsel protections."
        ),
        "transit_access": (
            f"High-poverty counties like {', '.join(top_names)} ({top_val_str}) "
            f"have the highest transit dependency and would benefit most from "
            f"expanded transit options (statewide mean poverty: {mean_str})."
        ),
        "health_access": (
            f"{', '.join(top_names)} have the highest uninsured rates in Wisconsin "
            f"({top_val_str}), making them the strongest candidates for "
            f"BadgerCare expansion (statewide mean: {mean_str})."
        ),
        "income_equity": (
            f"The lowest-income counties in Wisconsin ({', '.join(top_names)}) "
            f"would see the greatest relative benefit from a $20 minimum wage indexed to cost of living."
        ),
        "economic_equity": (
            f"Counties with the highest combined poverty and housing cost burden — "
            f"{', '.join(top_names)} — face the most acute economic disadvantage "
            f"and stand to benefit most from structural economic equity reforms."
        ),
        "labor_rights": (
            f"Lower-wage counties like {', '.join(top_names)} have the most to gain "
            f"from stronger labor protections, collective bargaining, and wage standards."
        ),
        "environmental_health": (
            f"Counties with the highest concentrations of people of color — "
            f"{', '.join(top_names)} ({top_val_str} POC) — face disproportionate "
            f"environmental health burdens and are the primary beneficiaries of "
            f"environmental justice policies."
        ),
        "criminal_justice": (
            f"The intersection of race and poverty is most pronounced in "
            f"{', '.join(top_names)}, where criminal justice reform would have "
            f"the greatest equity impact."
        ),
    }

    return DIMENSION_PHRASING.get(
        equity_dimension,
        (
            f"{', '.join(top_names)} show the highest need relevant to this policy "
            f"based on ACS {dim_config['quality_note']}."
        ),
    )


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_policies(path: str) -> list[dict]:
    """Load policy CSV into list of dicts."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def load_acs(path: str) -> list[dict]:
    """Load ACS county CSV into list of dicts."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def load_cdc_places_by_county(path: str) -> dict[str, dict]:
    """
    Load CDC PLACES tract-level CSV and aggregate to county level.

    Returns dict keyed by 5-digit county GEOID:
    {
        "55025": {
            "cdc_obesity_rate": 28.4,   # mean crude prevalence across tracts
            "cdc_diabetes_rate": 8.7,
            "cdc_mental_health_rate": 14.2,
            "cdc_physical_health_rate": 11.1,
            "cdc_no_healthcare_access": 10.3,
            "cdc_asthma_rate": 9.1,
        },
        ...
    }
    """
    if not os.path.exists(path):
        return {}

    # Accumulate sum + count per county × measure
    county_data: dict[str, dict[str, list[float]]] = {}

    MEASURE_TO_FIELD = {
        "OBESITY":   "cdc_obesity_rate",
        "DIABETES":  "cdc_diabetes_rate",
        "MHLTH":     "cdc_mental_health_rate",
        "PHLTH":     "cdc_physical_health_rate",
        "ACCESS2":   "cdc_no_healthcare_access",
        "CASTHMA":   "cdc_asthma_rate",
        "CSMOKING":  "cdc_smoking_rate",
        "BPHIGH":    "cdc_hypertension_rate",
    }

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            geoid = str(row.get("geoid", "")).strip()
            if len(geoid) < 5:
                continue
            county_geoid = geoid[:5]
            measure = str(row.get("measure", "")).strip()
            field = MEASURE_TO_FIELD.get(measure)
            if not field:
                continue
            val = _safe_float(row.get("data_value"))
            if val is None:
                continue
            if county_geoid not in county_data:
                county_data[county_geoid] = {}
            if field not in county_data[county_geoid]:
                county_data[county_geoid][field] = []
            county_data[county_geoid][field].append(val)

    # Average tracts within each county
    result: dict[str, dict] = {}
    for county_geoid, measures in county_data.items():
        result[county_geoid] = {
            field: round(statistics.mean(vals), 2)
            for field, vals in measures.items()
            if vals
        }
    return result


def load_usda_food_by_county(path: str) -> dict[str, dict]:
    """
    Load USDA Food Access tract-level CSV and aggregate to county level.

    Returns dict keyed by 5-digit county GEOID:
    {
        "55025": {
            "usda_lila_pct":     12.3,  # % tracts with low income + low access
            "usda_low_access_1mi": 8.1, # avg population share with low access at 1 mi
            "usda_urban_pct":    85.0,  # % urban tracts
        },
        ...
    }
    """
    if not os.path.exists(path):
        return {}

    county_buckets: dict[str, dict[str, list]] = {}

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            geoid = str(row.get("geoid", "")).strip()
            if len(geoid) < 5:
                continue
            county_geoid = geoid[:5]
            if county_geoid not in county_buckets:
                county_buckets[county_geoid] = {
                    "lila": [], "low_access_1mi": [], "urban": [],
                    "snap_flag": [],
                }
            lila = _safe_float(row.get("low_income_low_access"))
            la1 = _safe_float(row.get("low_access_1mi"))
            urban = _safe_float(row.get("urban_flag"))
            snap = _safe_float(row.get("snap_flag"))

            if lila is not None:
                county_buckets[county_geoid]["lila"].append(lila)
            if la1 is not None:
                county_buckets[county_geoid]["low_access_1mi"].append(la1)
            if urban is not None:
                county_buckets[county_geoid]["urban"].append(urban)
            if snap is not None:
                county_buckets[county_geoid]["snap_flag"].append(snap)

    result: dict[str, dict] = {}
    for county_geoid, buckets in county_buckets.items():
        n = len(buckets["lila"]) if buckets["lila"] else 0
        lila_count = sum(1 for v in buckets["lila"] if v == 1)
        result[county_geoid] = {}
        if n > 0:
            result[county_geoid]["usda_lila_pct"] = round(lila_count / n * 100, 1)
        if buckets["low_access_1mi"]:
            result[county_geoid]["usda_low_access_1mi"] = round(
                statistics.mean(buckets["low_access_1mi"]), 2
            )
        if buckets["urban"]:
            urban_count = sum(1 for v in buckets["urban"] if v == 1)
            result[county_geoid]["usda_urban_pct"] = round(urban_count / len(buckets["urban"]) * 100, 1)
    return result


def merge_supplemental_data(
    acs_rows: list[dict],
    cdc_by_county: dict[str, dict],
    usda_by_county: dict[str, dict],
) -> list[dict]:
    """
    Merge CDC PLACES and USDA food access county aggregates into ACS county rows.
    Returns a new list of enriched dicts (original rows unchanged).
    """
    enriched = []
    for row in acs_rows:
        geoid = str(row.get("geoid", "")).strip()
        new_row = dict(row)
        # CDC PLACES fields
        if geoid in cdc_by_county:
            new_row.update(cdc_by_county[geoid])
        # USDA food access fields
        if geoid in usda_by_county:
            new_row.update(usda_by_county[geoid])
        enriched.append(new_row)
    return enriched


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def compute_ranking(
    acs_rows: list[dict],
    metric_name: str,
) -> list[dict]:
    """
    For each ACS county row, compute the metric value and return a sorted list
    (highest metric first — all metrics are defined so higher = more need).

    Returns list of dicts with keys: geoid, county_name, metric_value, display_value
    """
    metric_fn = METRIC_FUNCTIONS.get(metric_name)
    if metric_fn is None:
        raise ValueError(f"Unknown metric: {metric_name!r}")

    scored: list[dict] = []
    for row in acs_rows:
        val = metric_fn(row)
        # For display, find the most human-readable underlying value
        display_val = _pick_display_value(row, metric_name)
        scored.append({
            "geoid": row["geoid"],
            "county_name": row["county_name"],
            "metric_value": val,
            "display_value": display_val,
        })

    # Sort descending by metric (None values go last)
    scored.sort(key=lambda x: (x["metric_value"] is None, -(x["metric_value"] or 0)))
    return scored


def _pick_display_value(row: dict, metric_name: str) -> Optional[float]:
    """
    Choose a human-readable display value for a county entry.
    For inverse/interaction metrics, show the primary underlying indicator.
    """
    DISPLAY_MAP = {
        "poverty_rate": "poverty_rate",
        "pct_cost_burdened": "pct_cost_burdened",
        "pct_severely_cost_burdened": "pct_severely_cost_burdened",
        "uninsured_rate": "uninsured_rate",
        "pct_poc": "pct_poc",
        "median_hh_income_inverse": "median_hh_income",
        "race_poverty_interaction": "poverty_rate",
        "burden_poverty_interaction": "pct_cost_burdened",
        "poverty_burden_composite": "poverty_rate",
    }
    col = DISPLAY_MAP.get(metric_name, metric_name)
    return _safe_float(row.get(col))


def _statewide_context(acs_rows: list[dict], equity_dimension: str) -> tuple[dict, dict]:
    """
    Compute statewide summary stats and county variation for the indicators
    most relevant to the given equity dimension.
    Returns (statewide_context, county_variation).
    """
    dim_config = DIMENSION_CONFIG.get(equity_dimension, {})
    metric_name = dim_config.get("metric", "poverty_rate")

    # Pick the columns that are most relevant to display
    CONTEXT_COLUMNS = {
        "poverty_rate": ["poverty_rate"],
        "pct_cost_burdened": ["pct_cost_burdened", "pct_severely_cost_burdened"],
        "pct_severely_cost_burdened": ["pct_severely_cost_burdened", "pct_cost_burdened"],
        "uninsured_rate": ["uninsured_rate"],
        "pct_poc": ["pct_poc", "pct_non_hispanic_black", "pct_hispanic"],
        "median_hh_income_inverse": ["median_hh_income"],
        "race_poverty_interaction": ["pct_non_hispanic_black", "poverty_rate"],
        "burden_poverty_interaction": ["pct_cost_burdened", "poverty_rate"],
        "poverty_burden_composite": ["poverty_rate", "pct_cost_burdened"],
    }

    cols = CONTEXT_COLUMNS.get(metric_name, ["poverty_rate"])

    statewide: dict = {}
    variation: dict = {}
    for col in cols:
        values = [_safe_float(r.get(col)) for r in acs_rows]
        values = [v for v in values if v is not None]
        stats = _summary_stats(values)
        statewide[col] = stats
        variation[col] = {
            "min": stats["min"],
            "max": stats["max"],
            "range": round((stats["max"] or 0) - (stats["min"] or 0), 2) if stats["min"] is not None and stats["max"] is not None else None,
            "std": stats["std"],
            "counties_with_data": stats["count"],
        }

    return statewide, variation


def analyze_policy(
    policy: dict,
    acs_rows: list[dict],
) -> Optional[EvidenceCard]:
    """
    Produce an EvidenceCard for a single policy.
    Returns None if the equity_dimension is not in ANALYZABLE_DIMENSIONS.
    """
    dim = policy.get("equity_dimension", "").strip()
    if dim not in ANALYZABLE_DIMENSIONS:
        return None

    dim_config = DIMENSION_CONFIG[dim]
    metric_name = dim_config["metric"]

    ranked = compute_ranking(acs_rows, metric_name)

    # Extract top-5 and bottom-5 with non-null values
    ranked_valid = [r for r in ranked if r["metric_value"] is not None]
    top_5 = ranked_valid[:5]
    bottom_5 = ranked_valid[-5:][::-1] if len(ranked_valid) >= 5 else ranked_valid[::-1]

    statewide_context, county_variation = _statewide_context(acs_rows, dim)

    # Use the primary metric's stats for the key finding
    primary_col = list(statewide_context.keys())[0] if statewide_context else {}
    primary_stats = statewide_context.get(primary_col, {})

    key_finding = _generate_key_finding(
        policy_title=policy.get("policy_title", ""),
        equity_dimension=dim,
        top_counties=top_5,
        bottom_counties=bottom_5,
        stats=primary_stats,
        dim_config=dim_config,
    )

    return EvidenceCard(
        policy_id=policy["id"],
        policy_title=policy["policy_title"],
        category=policy["category"],
        equity_dimension=dim,
        statewide_context=statewide_context,
        county_variation=county_variation,
        top_need_counties=top_5,
        bottom_need_counties=bottom_5,
        key_finding=key_finding,
        data_quality=dim_config["data_quality"],
    )


# ---------------------------------------------------------------------------
# Output rendering
# ---------------------------------------------------------------------------

def _fmt_county_list(counties: list[dict], metric_name: str) -> str:
    """Format a list of county entries as a markdown bullet list."""
    lines = []
    for c in counties:
        name = c["county_name"]
        val = c.get("display_value")
        if val is not None:
            # Determine display label
            DISPLAY_LABELS = {
                "poverty_rate": "poverty rate",
                "pct_cost_burdened": "cost burdened",
                "pct_severely_cost_burdened": "severely cost burdened",
                "uninsured_rate": "uninsured",
                "pct_poc": "POC",
                "median_hh_income_inverse": "median income",
                "race_poverty_interaction": "poverty rate",
                "burden_poverty_interaction": "cost burdened",
                "poverty_burden_composite": "poverty rate",
            }
            label = DISPLAY_LABELS.get(metric_name, "value")
            if metric_name == "median_hh_income_inverse":
                lines.append(f"  - {name}: ${val:,.0f} {label}")
            else:
                lines.append(f"  - {name}: {val:.1f}% {label}")
        else:
            lines.append(f"  - {name}")
    return "\n".join(lines)


def _statewide_context_md(statewide_context: dict) -> str:
    """Format statewide context stats as markdown."""
    lines = []
    LABELS = {
        "poverty_rate": "Poverty rate",
        "uninsured_rate": "Uninsured rate",
        "pct_cost_burdened": "Cost-burdened households",
        "pct_severely_cost_burdened": "Severely cost-burdened households",
        "pct_poc": "People of color",
        "pct_non_hispanic_black": "Non-Hispanic Black",
        "pct_hispanic": "Hispanic/Latino",
        "median_hh_income": "Median household income",
    }
    for col, stats in statewide_context.items():
        label = LABELS.get(col, col)
        mean = stats.get("mean")
        mn = stats.get("min")
        mx = stats.get("max")
        count = stats.get("count", 0)
        if mean is not None:
            if col == "median_hh_income":
                lines.append(
                    f"- **{label}**: mean ${mean:,.0f} | range ${mn:,.0f}–${mx:,.0f} ({count} counties)"
                )
            else:
                lines.append(
                    f"- **{label}**: mean {mean:.1f}% | range {mn:.1f}%–{mx:.1f}% ({count} counties)"
                )
    return "\n".join(lines) if lines else "- No ACS indicators available for this dimension."


def render_markdown(
    cards: list[EvidenceCard],
    policies: list[dict],
    acs_rows: list[dict],
) -> str:
    """Render all evidence cards to a markdown summary document."""
    lines: list[str] = []
    lines.append("# Policy Evidence Summary: Francesca Hong for Governor (2026)")
    lines.append("")
    lines.append(
        "Generated from multi-source Wisconsin county data: "
        "ACS 2023 5-Year estimates, CDC PLACES 2023 health indicators (tract-level aggregated), "
        "and USDA 2019 Food Access Research Atlas (tract-level aggregated)."
    )
    lines.append(
        f"Policies with analyzable equity dimensions: {len(cards)} of {len(policies)}."
    )
    lines.append("")

    # Group cards by category
    categories: dict[str, list[EvidenceCard]] = {}
    for card in cards:
        categories.setdefault(card.category, []).append(card)

    for category, cat_cards in sorted(categories.items()):
        lines.append(f"## {category}")
        lines.append("")

        for card in cat_cards:
            dim_config = DIMENSION_CONFIG[card.equity_dimension]
            metric_name = dim_config["metric"]

            lines.append(f"### {card.policy_id}: {card.policy_title}")
            lines.append(f"**Equity dimension:** {card.equity_dimension}")
            lines.append(f"**Data quality:** {card.data_quality} ({dim_config['quality_note']})")
            lines.append(f"**Key finding:** {card.key_finding}")
            lines.append("")
            lines.append("**Statewide context:**")
            lines.append(_statewide_context_md(card.statewide_context))
            lines.append("")
            lines.append("**Most impacted counties (top 5 by need):**")
            lines.append(_fmt_county_list(card.top_need_counties, metric_name))
            lines.append("")
            lines.append("**Least impacted counties (bottom 5 by need):**")
            lines.append(_fmt_county_list(card.bottom_need_counties, metric_name))
            lines.append("")
            lines.append("---")
            lines.append("")

    # Summary section
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **Total policies analyzed:** {len(cards)}")

    quality_counts = Counter(c.data_quality for c in cards)
    lines.append(f"- **Direct indicators (strong):** {quality_counts.get('strong', 0)}")
    lines.append(f"- **Proxy indicators (moderate):** {quality_counts.get('moderate', 0)}")
    lines.append(f"- **Indirect indicators (weak):** {quality_counts.get('weak', 0)}")
    lines.append("")

    # Top 10 most-cited counties across all policies
    county_citation_counter: Counter = Counter()
    for card in cards:
        for entry in card.top_need_counties:
            county_citation_counter[entry["county_name"]] += 1

    top_10_counties = county_citation_counter.most_common(10)
    lines.append("### Top 10 Most-Cited High-Need Counties (across all policies)")
    lines.append("")
    lines.append("These counties appear most frequently in the top-5 need rankings across all analyzed policies.")
    lines.append("")
    for rank, (name, count) in enumerate(top_10_counties, 1):
        lines.append(f"{rank}. **{name}** — cited in {count} policy evidence cards")
    lines.append("")

    return "\n".join(lines)


def render_json(cards: list[EvidenceCard]) -> str:
    """Serialize evidence cards to JSON."""
    data = [asdict(card) for card in cards]
    return json.dumps(data, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Dry-run plan
# ---------------------------------------------------------------------------

def print_dry_run_plan(policies: list[dict]) -> None:
    """Print analysis plan without running the analysis."""
    analyzable = [p for p in policies if p.get("equity_dimension", "") in ANALYZABLE_DIMENSIONS]
    skipped = [p for p in policies if p.get("equity_dimension", "") not in ANALYZABLE_DIMENSIONS]

    print("=== DRY RUN: evidence_cards.py ===")
    print(f"Policies total:      {len(policies)}")
    print(f"Policies analyzable: {len(analyzable)} (equity dimension in DIMENSION_CONFIG)")
    print(f"Policies skipped:    {len(skipped)} (equity dimension not in ACS mapping)")
    print()
    print("Analyzable policies:")
    for p in analyzable:
        dim = p.get("equity_dimension", "")
        cfg = DIMENSION_CONFIG.get(dim, {})
        quality = cfg.get("data_quality", "?")
        metric = cfg.get("metric", "?")
        print(f"  [{quality:8s}] {p['id']:<20} dim={dim:<25} metric={metric}")
    print()
    print("Skipped policies (no ACS dimension mapping):")
    for p in skipped:
        print(f"  {p['id']:<20} dim={p.get('equity_dimension', '(none)')}")
    print()
    print(f"Outputs (not written in dry-run):")
    print(f"  {CARDS_JSON}")
    print(f"  {SUMMARY_MD}")
    print()
    print("[dry-run] No files written.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Produce per-policy evidence cards from ACS county data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print analysis plan without reading ACS data or writing output files.",
    )
    args = parser.parse_args()

    # Load policies
    if not os.path.exists(POLICIES_CSV):
        print(f"ERROR: Policies CSV not found: {POLICIES_CSV}", file=sys.stderr)
        sys.exit(1)
    policies = load_policies(POLICIES_CSV)
    print(f"Loaded {len(policies)} policies from {POLICIES_CSV}")

    if args.dry_run:
        print_dry_run_plan(policies)
        return

    # Load ACS data
    if not os.path.exists(ACS_CSV):
        print(
            f"ERROR: ACS CSV not found: {ACS_CSV}\n"
            f"Run 'python analysis/fetch_wi_counties.py' first.",
            file=sys.stderr,
        )
        sys.exit(1)
    acs_rows = load_acs(ACS_CSV)
    print(f"Loaded {len(acs_rows)} county rows from {ACS_CSV}")

    # Load supplemental data sources (optional — enriches metrics when present)
    cdc_by_county = load_cdc_places_by_county(CDC_PLACES_CSV)
    if cdc_by_county:
        print(f"Loaded CDC PLACES data for {len(cdc_by_county)} counties from {CDC_PLACES_CSV}")
    else:
        print("CDC PLACES data not found — health_access will use ACS uninsured rate only")

    usda_by_county = load_usda_food_by_county(USDA_FOOD_CSV)
    if usda_by_county:
        print(f"Loaded USDA food access data for {len(usda_by_county)} counties from {USDA_FOOD_CSV}")
    else:
        print("USDA food access data not found — food_access will fall back to poverty rate proxy")

    # Merge supplemental data into ACS rows
    enriched_rows = merge_supplemental_data(acs_rows, cdc_by_county, usda_by_county)

    # Fall back to poverty_rate for food_access if no USDA data available
    if not usda_by_county:
        DIMENSION_CONFIG["food_access"]["metric"] = "poverty_rate"
        DIMENSION_CONFIG["food_access"]["data_quality"] = "moderate"
        DIMENSION_CONFIG["food_access"]["quality_note"] = "poverty rate as food insecurity proxy (USDA data not found)"

    # Fall back to uninsured_rate if no CDC data available
    if not cdc_by_county:
        DIMENSION_CONFIG["health_access"]["metric"] = "uninsured_rate"
        DIMENSION_CONFIG["health_access"]["quality_note"] = "ACS uninsured rate (CDC PLACES not found)"
        DIMENSION_CONFIG["environmental_health"]["metric"] = "pct_poc"
        DIMENSION_CONFIG["environmental_health"]["data_quality"] = "weak"
        DIMENSION_CONFIG["environmental_health"]["quality_note"] = "pct_poc as proxy (CDC PLACES not found)"

    # Analyze each policy
    cards: list[EvidenceCard] = []
    skipped = 0
    for policy in policies:
        card = analyze_policy(policy, enriched_rows)
        if card is not None:
            cards.append(card)
        else:
            skipped += 1

    print(f"\nProduced {len(cards)} evidence cards ({skipped} policies skipped — no ACS dimension mapping)")

    # Write outputs
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(CARDS_JSON, "w", encoding="utf-8") as f:
        f.write(render_json(cards))
    print(f"Wrote {CARDS_JSON}")

    with open(SUMMARY_MD, "w", encoding="utf-8") as f:
        f.write(render_markdown(cards, policies, acs_rows))
    print(f"Wrote {SUMMARY_MD}")

    # Print top-cited counties to stdout as a quick summary
    county_counter: Counter = Counter()
    for card in cards:
        for entry in card.top_need_counties:
            county_counter[entry["county_name"]] += 1

    print("\nTop 10 highest-need counties across all policies:")
    for rank, (name, count) in enumerate(county_counter.most_common(10), 1):
        print(f"  {rank:2d}. {name:<30} cited in {count} evidence cards")

    print("\nDone.")


if __name__ == "__main__":
    main()
