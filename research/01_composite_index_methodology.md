# Validated Composite Index Methodologies

**Date:** 2026-04-14 | **Confidence:** High | **Sources:** 17

## Key Finding

Composites are appropriate for triage and communication, not for diagnosis or causal inference. Ad-hoc composites (percentile-rank + equal weight + sum) are the weakest form. Factor-analytic derivation (ADI) is better. Geometric mean aggregation (National Equity Atlas, HDI post-2010) prevents compensability. Raw disaggregated indicators should always be the primary layer.

## Index Comparison

| Index | Scale | Construction | Validation | Limitations |
|-------|-------|-------------|------------|-------------|
| CDC SVI | Tract | 16 ACS vars, equal-weight percentile rank, 4 expert themes | Expert panel + disaster-impact | Equal weights ignore correlations; 4 vars carry most variance; state heterogeneity |
| EPA EJScreen | Block group | Multiplicative: environmental × (demographic - national avg) × population | Theoretical justification only | Removed from EPA site Feb 2025; 2-var demographic index oversimplifies |
| UNDP HDI | National | 3 dimensions, geometric mean post-2010 | Sen capabilities framework | Not sub-national; equal dimension weights lack justification |
| ADI (Singh) | Block group/tract | Factor analysis on 21 ACS vars; 17 retained; loadings as weights | 1000+ peer-reviewed studies | Unstandardized versions: 98.8% explained by 2 vars (income + home value) |
| Opportunity Atlas | Tract | NO composite — raw mobility outcomes from IRS records | MTO experiment (causal) | Historical cohort (1978-83 births); economic outcomes only |
| National Equity Atlas | Metro/city | Geometric mean of Inclusion × Prosperity; 9 indicators | Academic partnership | Not tract-level; no public API |

## When Composites Fail

1. **Diagnostic use** — a high SVI doesn't tell you which of 16 variables to fix
2. **Causal inference** — composites conflate inputs, mediators, and moderators
3. **Ecological fallacy** — tract-level scores don't apply to individual households
4. **Weakly correlated components** — if variables don't measure a common construct, the composite is incoherent
5. **Methodological instability** — rankings shift 45+ places across alternative specifications (PMC 2023)

## The Stiglitz-Sen-Fitoussi Rule

"Weights embed hidden normative choices disguised as technical choices." Present composites alongside a dashboard of raw indicators so dimensions remain visible.

## Decision Framework

| Goal | Best approach |
|------|--------------|
| Disaster triage | SVI (designed for this) |
| Environmental + demographic co-occurrence | EJScreen (multiplicative) |
| Traditional socioeconomic deprivation | ADI (factor-derived, verify standardization) |
| Economic mobility | Opportunity Atlas (causal validation) |
| Equity communication | National Equity Atlas model (dashboard + geometric mean) |
| Diagnosing specific drivers | Raw disaggregated indicators — no composite |
| Estimating policy effects | Raw indicators + regression — no composite |

## Sources

- CDC/ATSDR SVI 2022 Documentation (2024)
- SVI Validity Assessment White Paper (2024)
- EJScreen Technical Documentation v2.3 (2024)
- Rethinking Vulnerability: Factor Analysis vs SVI — PMC (2025)
- ADI vs SVI Comparison — PLOS ONE (2023)
- ADI-3 Revised Neighborhood Risk Index — Springer (2021)
- Deciphering ADI: Consequences of Not Standardizing — Health Affairs Scholar (2023)
- Opportunity Atlas — AER (2020)
- National Equity Atlas Methodology (2023)
- UNDP HDI Methodology
- Stiglitz-Sen-Fitoussi Commission Report (2009)
- Composite Environmental Indices: Rickety Rankings — PMC (2023)
- Composite Indices: Weighting, Aggregation, Robustness — Social Indicators Research (2019)
