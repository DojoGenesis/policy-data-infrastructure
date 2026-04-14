# PDI Statistical Refactor Plan

**Date:** 2026-04-14
**Status:** Research complete, implementation pending
**Grounding:** 30 sources across 4 research tracks

---

## Problem Statement

The current PDI statistical layer copies the Madison Equity Atlas approach: construct NARI (a percentile-ranked composite of 8 indicators with equal weights), assign arbitrary tier cutoffs, and use the composite to identify "priority tracts." This approach:

1. **Has no validation target.** NARI was never tested against an outcome it didn't contain.
2. **Uses equal weighting** despite evidence that 3-4 variables carry most variance (PMC 2025).
3. **Destroys diagnostic information** — a high NARI doesn't tell you which dimension to intervene on.
4. **Doesn't scale** — the 8 indicators and tier thresholds were calibrated for 125 Madison tracts, not 85K national tracts.

## Architecture Principle

**Raw data is the foundation. Composites are derived views, never stored truth.**

```
Layer 1: Raw Indicators     ← stored in PostgreSQL, CV-flagged
Layer 2: Validated Features  ← ICE, dissimilarity index, cost burden ratio
Layer 3: Factor Scores       ← EFA-derived, named, with loadings documented
Layer 4: Spatial Analysis    ← LISA clusters, GWR coefficients, MLM variance
Layer 5: Composite Views     ← query-time only, geometric mean, with sensitivity
```

## Implementation Phases

### Phase 1: Data Quality Foundation (Week 1)

**Goal:** Every indicator carries a reliability signal.

1. Add `cv` (coefficient of variation) column to `indicators` table
2. Compute CV = MOE / (1.645 × estimate) for every ACS-derived indicator
3. Add `reliability` enum: `high` (CV < 0.15), `moderate` (0.15-0.30), `low` (> 0.30)
4. API: include reliability in every indicator response
5. UI: gray out / hatch-mark low-reliability cells

**Code changes:**
- `pkg/store/migrations/007_indicator_reliability.sql` — add columns
- `pkg/datasource/acs.go` — compute CV during ingest
- `pkg/gateway/handlers.go` — include reliability in JSON responses

### Phase 2: Validated Features (Week 2)

**Goal:** Replace ad-hoc composites with literature-grounded features.

1. **ICE (Index of Concentration at the Extremes):** Compute as first-class indicator
   - Formula: (high-income-white - low-income-POC) / total
   - Source: Krieger et al. 2016
   - Store as `indicator_id: ice_income_race`

2. **Dissimilarity Index:** At county level for Black-white, Hispanic-white
   - Formula: D = 0.5 × Σ|bi/B - wi/W| (Massey & Denton 1988)
   - Store as county-level derived indicator

3. **Housing Cost Burden Ratio:** Already in ACS as B25070
   - Ensure stored as raw rate, not percentile-ranked

4. **Remove:** `CompositeIndex()` function from `pkg/stats/composite.go` — deprecate, do not delete yet. Replace with `ValidatedFeatures()` that computes ICE + dissimilarity.

5. **Remove:** `AssignTiers()` — arbitrary percentile cutoffs have no place in the foundation layer.

### Phase 3: Factor Analysis Pipeline (Week 3-4)

**Goal:** Let the data tell us what belongs together.

1. Python analysis script: `analysis/factor_analysis.py`
   - EFA with oblimin rotation on all tract-level indicators
   - Parallel analysis for factor count (expect 5-8)
   - Output: factor loadings matrix, named factors, factor scores per tract
   - Reference: Kolak et al. 2020 (4 factors, 71% variance at 72K tracts)

2. Store factor scores in PostgreSQL as derived indicators
   - `factor_name`, `factor_score`, `factor_loading_json`
   - Expose via API: `GET /v1/policy/geographies/:geoid/factors`

3. **Name factors by their loading profile**, not by number:
   - "Economic Distress" (poverty, unemployment, low education)
   - "Housing Burden" (cost burden, overcrowding, renter rate)
   - "Health Access Gap" (uninsured, HPSA, provider distance)
   - etc. — actual names from the data, not assumed

### Phase 4: Spatial Analysis Layer (Week 4-5)

**Goal:** Cluster maps and spatial relationships without arbitrary composites.

1. **Queen contiguity weights:** Build once from TIGER geometries, serialize
2. **LISA on each factor score:** Classify tracts as HH/LL/HL/LH/NS
3. **Three-level MLM:** Tract → county → state variance partitioning
4. **SKATER regionalization:** Within Census divisions, k=5-10 per division

All in Python (PySAL, esda, spopt). Results stored in PostgreSQL. Go serves them.

### Phase 5: Query-Time Composites (Week 5-6)

**Goal:** Users can compute custom composites, but the system doesn't impose them.

1. API endpoint: `POST /v1/policy/composite`
   - Input: list of indicator IDs, weights, aggregation method (geometric_mean, weighted_zscore)
   - Output: composite scores with sensitivity analysis (ranking stability under ±20% weight perturbation)
   - **Never stored** — computed fresh per request

2. Pre-built composite definitions as JSON configs (not code):
   - `composites/nari_v2.json` — if NARI is needed, it's a config file, not a hardcoded function
   - Each config must reference its validation target and cite its variable selection rationale

### Phase 6: Narrative Engine Update (Week 6-7)

**Goal:** Narratives reference raw indicators and factor profiles, not composite tiers.

1. Update `five_mornings.tmpl` to use factor profiles instead of NARI tiers
2. New template slot: `{{.FactorProfile}}` — "Economic Distress: 92nd percentile. Housing Burden: 67th percentile."
3. Replace tier badges ("Critical", "High Risk") with factor-specific language
4. Threshold callouts: "Above 35% poverty, diabetes prevalence increases 4x" (from segmented regression)

## What Gets Deleted

| Current | Replacement | Reason |
|---------|-------------|--------|
| `CompositeIndex()` | `ValidatedFeatures()` + query-time composites | Unvalidated equal-weight averaging |
| `AssignTiers()` | LISA cluster classification | Arbitrary percentile cutoffs |
| NARI as stored score | Factor scores + ICE | NARI was never cross-validated |
| `getMetricForCounty()` ad-hoc composites in HTML | Raw indicators + API queries | Min-max normalize + sum is not analysis |
| Tier badges in narrative templates | Factor profile descriptions | Tiers destroy diagnostic information |

## What Gets Kept

| Current | Why |
|---------|-----|
| `PercentileRank()` | Valid building block for individual indicators |
| `ZScore()` | Valid building block for standardization |
| `OLS()` | Solid implementation, well-tested |
| `BootstrapCI()` | Useful for uncertainty quantification |
| `TippingPoint()` | Aligns with threshold analysis recommendations |
| `BlinderOaxaca()` | Literature-grounded decomposition method |

## Success Criteria

1. No composite score appears in the stored data layer
2. Every indicator in the API carries a reliability flag
3. Factor analysis produces named, interpretable dimensions with documented loadings
4. LISA cluster maps replace tier-based priority tract identification
5. Any composite computed at query time includes sensitivity analysis
6. Narrative templates reference raw indicators and factor profiles, not tiers

---

*This plan is grounded in 30 peer-reviewed and technical sources documented in research/references.csv*
