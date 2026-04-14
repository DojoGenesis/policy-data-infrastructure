# Cross-Tabulation and Disaggregated Analysis Methods

**Date:** 2026-04-14 | **Confidence:** High | **Sources:** 14

## Core Principle

Keep race, income, and geography as separate analytical dimensions. Do not collapse them into composites. The literature shows race and SES are too correlated in the US to "control for" one while studying the other — mutual adjustment removes the causal pathway, not noise (Graetz et al. 2022).

## Methods

### 1. Cross-Tabulation at Scale (85K tracts)
- **Problem:** 6 race groups × 5 poverty quintiles × 50 states × 5 health deciles = 7,500 cells, averaging 11 tracts per cell. Many cells empty.
- **Solution:** Two-way stratification first (race × poverty), then condition on geography as a fixed effect.
- **ACS MOE:** Combined MOE = √(MOE₁² + MOE₂²). Suppress cells where CV > 0.30.
- **Scalability:** PostgreSQL PARTITION BY + window functions handles 85K × 10 indicators in seconds.

### 2. Disaggregated Equity Analysis
- **Index of Concentration at the Extremes (ICE):** (high-income white - low-income POC) / total population. Validated, directional, interpretable. Krieger et al. 2016.
- **Rate ratios + rate differences:** Report both. A 2:1 ratio at 1% baseline differs from 2:1 at 20%.
- **Additive interaction testing:** Model with race main effect, income main effect, interaction term. Tests whether poverty's effect on health differs by race group.

### 3. Threshold / Tipping-Point Analysis
- **Segmented regression:** Identifies breakpoint where outcome slope changes. "Above 35% poverty, diabetes prevalence increases 4x faster."
- **Best practice:** Fit county-level first to find candidates, validate at tract level. Tract-level noise creates false thresholds.
- **Interpretability: 5/5** — threshold findings are directly actionable for policy.

### 4. Conditional Distributions (Quantile Regression)
- **What:** Instead of mean outcomes, estimate 10th/50th/90th percentile of health outcomes for a given poverty rate.
- **Key finding:** Black-white mortality disparities narrow at upper quantiles — mean regression misses this (PMC 2013).
- **Positive deviance:** Upper-quantile analysis identifies communities beating expectations — often more policy-relevant than cataloguing worst cases.
- **Spatial QR:** Add spatial lag term; 30-90 min at 85K tracts.

### 5. Small-Area Estimation
- **CV thresholds:** <0.15 reliable; 0.15-0.30 use with caution; >0.30 suppress or model.
- **BYM2 model:** Treat ACS standard error as known variance → apply spatial smoothing. Correctly propagates survey uncertainty.
- **INLA:** 200x faster than MCMC. 85K tracts in 15-30 min per model.
- **SSVD for inequality:** Bayesian shrinkage compresses variation; use SSVD when measuring inequality across tracts.

### 6. Validated Features vs. Ad-Hoc Composites

| Feature | Status | Source |
|---------|--------|--------|
| Dissimilarity Index | Canonical (35+ years) | Massey & Denton 1988 |
| Isolation Index (P*) | Canonical | Lieberson 1981 |
| ICE | Strong (growing adoption) | Krieger et al. 2016 |
| Housing Cost Burden (>30%) | Standard | HUD definition |
| ADI | Moderate (factor-analytic) | Singh 2003 |
| Child Opportunity Index | Moderate (domain-specific) | Acevedo-Garcia et al. |

**What to avoid:** Percentile-rank composites with equal weighting. Z-score sums without factor structure validation. Any composite without prospective cross-validation.

## Priority Implementation Order for PDI

1. **CV flagging** on every stored indicator (immediate)
2. **ICE** as first-class computed indicator (near-term)
3. **Standard quantile regression** as query-layer capability (near-term)
4. **BYM2 spatial smoothing** as optional view, not primary table (medium-term)
5. **Segmented regression** for threshold detection (medium-term)
6. **Reject composites without prospective cross-validation** (policy)

## Sources

- Models for Small Area Estimation — PMC (2020)
- Bayesian Models with Survey SE — Donegan et al. (2021)
- Factor Analysis vs SVI — PMC (2025)
- Quantifying Neighborhood SDOH — Kolak et al., JAMA (2020)
- Opportunity Atlas — Chetty, Hendren et al. (2018/2022)
- Census Quasi-Bayesian SAE Working Paper (2023)
- ACS Margin of Error Handbook — Census Bureau (2018)
- Massey & Denton Segregation Framework (1988/2003)
- Intersectionality in Quantitative Research — ScienceDirect (2021)
- Structural Racism and Causal Decomposition — Graetz et al. (2022)
- Quantile Regression for Mortality — PMC (2013)
- Walker: Analyzing US Census Data in R (2023)
- JMIR Bayesian Rate Stabilizing Tools (2026)
