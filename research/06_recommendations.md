# PDI Refactor: Recommendations & Next Steps

**Date:** 2026-04-14
**Session:** Statistical refactor + research + audit
**Status:** Refactor committed (9fada5c), audit complete, 4 medium issues identified

---

## Critical Path (Fix Before Generating Any Narrative)

### 1. Fix the narrative rendering chain
**Severity: Medium (broken prose in every generated document)**
The highest-risk chain: `AnalyzeStage` → `selector.buildProfile` (never sets `p.ICE`) → `engine.buildNarrative` (uses `NARITier=""`) → rendered HTML reads broken prose. Fix:
- `selector.go`: Assign `p.ICE = &scoreVal` when analysis type is `validated_features`
- `engine.go`: Guard NARI callout with `NARITier != ""` check; add ICE/factor callout branch
- `engine.go`: Update `buildNarrative` to use factor profiles when NARI fields are empty

### 2. Fix ICE approximation denominator
**Severity: Medium (biased results)**
Current: `(priv - dep) / total_pop` where `priv + dep != total_pop`. The approximation using independent poverty × race rates underestimates low-income POC counts. Fix:
- Short-term: Change denominator to `priv + dep` for internal consistency
- Medium-term: Ingest ACS B19001 (income by race) for true counts
- Document the approximation prominently in output metadata

### 3. Update hardcoded NARI text in templates
**Severity: Low (factually wrong but not crashing)**
`five_mornings.tmpl` lines 473-482 describe NARI as the methodology. Replace with conditional text driven by analysis type, or move to the `Methodology` template slot.

---

## Statistical Methodology (Research-Grounded)

### 4. Ingest ACS B19001 for true ICE computation
The ICE formula requires actual counts of high-income-white and low-income-POC populations. ACS table B19001 (income by household) crossed with B03002 (race/ethnicity) provides this. Add to `ingest/fetch_acs.py` variable list and `data/sources.toml`.

### 5. Implement CV flagging on all ACS indicators
Migration 007 added the `cv` and `reliability` columns. The pipeline doesn't yet compute CV during ingest. Add `stats.CoefficientOfVariation(estimate, moe)` calls to `pkg/datasource/acs.go` for every variable that has a MOE column (most ACS tables provide MOE).

### 6. Run factor analysis on current data
The `analysis/factor_analysis.py` script was written but needs execution. Requirements:
```
pip install -r analysis/requirements.txt
python3 analysis/factor_analysis.py --dry-run  # verify plan
python3 analysis/factor_analysis.py             # full EFA
python3 analysis/factor_analysis.py --load      # write to factor_scores table
```
Expect 5-8 factors from 50+ indicators (ref: Kolak et al. 2020).

### 7. Add Dissimilarity Index computation to pipeline
`stats.DissimilarityIndex()` exists but isn't called from the pipeline. Add to `AnalyzeStage` — compute Black-white and Hispanic-white dissimilarity at county level. Store as validated features.

### 8. Remove total_population from correlationVariables
`synthesize.go` computes Pearson correlations across `correlationVariables` which includes `total_population`. Population count correlated with rates produces spurious results. Remove `total_population` from the correlation list; keep it for ICE computation only.

---

## Store & API Layer

### 9. Add Store methods for validated_features and factor_scores tables
Migration 007 created the tables, but `Store` interface has no dedicated methods for them. Add:
- `PutValidatedFeatures(ctx, []ValidatedFeature) error`
- `QueryValidatedFeatures(ctx, ValidatedFeatureQuery) ([]ValidatedFeature, error)`
- `PutFactorScores(ctx, []FactorScore) error`
- `QueryFactorScores(ctx, geoid string) ([]FactorScore, error)`

### 10. Add `pdi query features` subcommand
The CLI needs a way to query validated features and factor scores. Add subcommands:
- `pdi query features --geoid 55025 --feature ice_income_race`
- `pdi query factors --geoid 55025000100`

### 11. Add composite query API endpoint
The refactor plan calls for `POST /v1/policy/composite` — query-time composite computation with sensitivity analysis. This is the key differentiator: users can compute custom composites, but the system doesn't impose them.

---

## Data Pipeline

### 12. Re-run BLS LAUS after rate limit reset
Script fixed (startyear/endyear params), but data still null due to rate limit. Run:
```
python3 ingest/fetch_bls_laus.py --year 2023
```
Wait until after UTC midnight for daily reset.

### 13. Add WI DPI attendance data to factor analysis inputs
449 districts with chronic_absence_rate (0% null) — this is high-quality data that should feed into the EFA as an outcome variable for validation.

### 14. Scale to national: parallel state ingestion
Current data covers Wisconsin only. For national scale:
```
pdi fetch --scope national --sources acs-5yr --parallel 5
```
This will populate 85K+ tracts. Run factor analysis on national data to validate whether Madison-calibrated patterns hold nationally.

---

## Website & Deployment

### 15. Build policydatainfrastructure.com
Domain accessible via cloudflared + Cloudflare. Options:
- **Cloudflare Pages** — static landing page with project docs, interactive explorer
- **Cloudflare Workers** — dynamic API proxy to VPS at pdi.trespies.dev
- **Combined** — Pages for static content, Workers for API routes

### 16. Deploy the HTML collaborator update
`CWD/outreach/pdi_collaborator_update.html` is ready — 8-section document with research findings, pipeline diagram, platform comparison. Deploy to policydatainfrastructure.com or share directly.

---

## Code Quality

### 17. Add test coverage for gateway handlers
`pkg/gateway/` has no test files. The handlers serve the REST API. Add at minimum:
- `handleGetGeography` — test with/without analysis_id param
- `handleGenerateNarrative` — test with validated_features analysis type
- `handlePipelineRun` — currently returns 501; test that it does

### 18. Consolidate analyze paths (CLI vs Pipeline)
`cmd/pdi/analyze.go` and `pkg/pipeline/analyze.go` are parallel implementations with divergent tier names. Consolidate: CLI should call the pipeline's `AnalyzeStage` internally rather than reimplementing the logic.

### 19. Add deprecation timeline to composite functions
`CompositeIndex()` and `AssignTiers()` are deprecated but still called from `cmd/pdi/analyze.go`. Set a removal target (e.g., v2.0) and add a `// Deprecated: Will be removed in v2.0` comment.

### 20. Write CONTRIBUTING.md
PDI is open-source (Apache-2.0) but has no contributor guide. Cover:
- How to add a new data source (checklist in CLAUDE.md)
- How to add a new statistical method to pkg/stats/
- How to write a narrative template
- Testing conventions (go test, --dry-run for Python)
