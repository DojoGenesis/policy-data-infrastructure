# Open-Source Policy Data Platform Landscape

**Date:** 2026-04-14 | **Confidence:** Medium-High | **Sources:** 19

## Platform Comparison

| Platform | Open Source | Scale | Composites? | API | Narrative Gen |
|----------|-----------|-------|------------|-----|--------------|
| Census Reporter | Yes | National/tract | No — raw ACS | Yes (api.censusreporter.org) | No |
| COI 3.0 (diversitydatakids) | Docs public | National/tract | Yes — 44 vars, ML-validated weights | Download only | No |
| IPUMS NHGIS | Partial | Historical/tract | No — raw census | Yes (v2) | No |
| Urban Institute SEDT | Yes (GitHub) | Upload-based | No — raw + equity analysis | Yes (2024) | No |
| National Equity Atlas | No | Metro/city/state | Yes — geometric mean REI | No | No |
| Opportunity Insights | Code public | National/tract | No — raw mobility outcomes | Download only | No |
| PolicyMap | No | National/tract | Aggregates existing indices | No | No |
| Social Explorer | No | National/tract | No — raw + visualization | Limited | No |
| Esri Community Analyst | No | National/tract | Yes — composite builder | ArcGIS ecosystem | No |
| Polco | No | Survey-based | No | No | Yes (only one) |
| **PDI (ours)** | **Yes** | **National/tract** | **Raw-first (refactoring)** | **Yes (REST + SSE)** | **Yes (Go templates)** |

## Key Gaps in the Landscape

| Gap | Impact | Feasibility | Best Current Approximation |
|-----|--------|-------------|---------------------------|
| Full-stack OSS (ingest + compute + viz) | High | Medium | Census Reporter + SEDT (partial) |
| Policy-to-indicator crosswalk | High | Medium | National Equity Atlas (manual, UI-only) |
| Narrative generation from data | High | High | Polco (commercial only) |
| Non-technical cross-tabulation | High | Medium | PolicyMap (commercial) |
| OMB 2024 disaggregation (MENA, multiracial) | Medium | Medium | None — all platforms predate standard |
| Open API for national equity time series | High | Low (governance) | None |

## PDI Differentiators

1. **Full-stack open-source** — ingest + compute + API + narrative + visualization in one deployable package
2. **Policy-to-indicator crosswalk as structured data** — no other platform exposes this as queryable schema
3. **Narrative generation pipeline** — Go template engine already built; only OSS option
4. **OMB 2024-ready from day one** — native MENA category + multiracial granularity
5. **Raw-data-first architecture** — composites only in query layer, never stored data

## Emerging Standards

- **OMB 2024 Race/Ethnicity:** MENA as separate category, expanded multiracial. Agencies comply by March 2029.
- **CARE Principles:** Indigenous data sovereignty (Collective Benefit, Authority, Responsibility, Ethics). Not yet operationalized in any platform.
- **Urban Institute Do No Harm Guide:** Equity-aware visualization standards.

## Sources

- Census Reporter GitHub, diversitydatakids.org COI 3.0 docs, IPUMS NHGIS API, Urban Institute SEDT,
  National Equity Atlas, Opportunity Insights, PolicyMap, Social Explorer, Esri Community Analyst,
  Polco AI Narrative blog, OMB Race/Ethnicity Standards, CARE Principles (GIDA), Urban Institute Do No Harm Guide
