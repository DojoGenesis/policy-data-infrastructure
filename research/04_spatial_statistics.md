# Spatial Statistics for 85K Census Tracts

**Date:** 2026-04-14 | **Confidence:** High | **Sources:** 14

## Compute Budget (4 vCPU / 8GB VPS)

| Task | Library | RAM | Runtime |
|------|---------|-----|---------|
| Queen contiguity weights (85K) | libpysal | ~100MB | 5-10 min |
| Global Moran's I (999 perms) | esda | ~500MB | 5-15 min |
| LISA (999 perms) | esda | ~1GB | 15-30 min |
| PCA (50 indicators) | sklearn | ~200MB | <1 min |
| EFA + parallel analysis | factor_analyzer / psych | ~300MB | 2-5 min |
| ScaGWR (fixed BW) | GWmodel (R) | ~2-4GB | 5-30 min |
| Three-level null MLM | lme4 / statsmodels | ~500MB | 2-10 min |
| SKATER (k=30, within division) | spopt | ~1-2GB | 10-30 min |
| HDBSCAN on factor scores | hdbscan | ~300MB | 2-10 min |

**Critical:** The 85K×85K matrix is a misconception. Queen contiguity = ~500K-600K non-zero entries in sparse CSR format, ~100MB.

## Methods

### 1. LISA Cluster Maps — Interpretability 5/5
Classifies each tract: High-High (concentrated disadvantage), Low-Low (concentrated advantage), High-Low (outlier), Low-High, Not Significant. The core equity atlas visual product.

### 2. Factor Analysis — Interpretability 5/5 (when named well)
- Use EFA with oblimin rotation (not PCA for construct discovery)
- Parallel analysis for factor count: expect 5-8 factors from 50 SDOH indicators
- Kolak et al. 2020 found 4 factors at 72K tracts explaining 71% variance:
  1. Socioeconomic Advantage
  2. Limited Mobility
  3. Urban Core Opportunity
  4. Immigrant Cohesion and Accessibility

### 3. GWR (Geographically Weighted Regression) — Interpretability 3/5
- Standard GWR is O(n²), intractable above ~15K
- **ScaGWR (Murakami 2020):** Linear-time, ~90s at N=80K
- Bandwidth selection adds 10-60 min; use theory-guided fixed bandwidth
- Reveals WHERE relationships change: poverty→mortality differs urban vs. rural

### 4. Multilevel Models — Interpretability 4/5
- Three levels: 85K tracts → 3,100 counties → 51 states
- ICC typically: tract 1-10%, county 5-15%, state 15-30% (for administrative outcomes)
- Key output: "27% of variation in uninsurance is between states — state Medicaid policy matters as much as local poverty"

### 5. SKATER Regionalization — Interpretability 4/5
- Builds minimum spanning tree on contiguity graph, prunes to form spatially contiguous clusters
- **Run within Census divisions, not nationally** (spanning tree Maine→Hawaii is meaningless)
- k=5-10 per division → 50-100 national regions, each named by factor profile

### 6. Go Limitation
No production spatial statistics in Go. Go covers geometry (GeoOS, gogeos), tile serving (Tegola), projections. **Analysis stays in Python/R; Go serves results.**

## Recommended Execution Sequence

1. Build queen contiguity weights → serialize (once)
2. PCA on 50 indicators → retain 5-8 components
3. EFA with oblimin → confirm structure, name factors
4. LISA on each factor score (999 perms) → cluster maps
5. Three-level null MLM on key outcomes → VPC report
6. SKATER within Census divisions → named typologies
7. ScaGWR on 2-3 key predictor-outcome pairs → local coefficient maps

**Steps 1-5 run overnight on VPS. Steps 6-7 add 1-4 hours.**

## Sources

- PySAL NARSC 2024 Workshop
- Kolak et al., JAMA Network Open (2020)
- JAMIA Open Housing SDOH Dimensionality Reduction (2025)
- ScaGWR — Murakami et al. (2020)
- SKATER — PySAL spopt
- Multilevel Variance Partitioning — NCBI Bookshelf
- Factor Retention Methods Review (2024)
- Walker: Analyzing US Census Data Ch. 7-8
