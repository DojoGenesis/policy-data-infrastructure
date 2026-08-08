# ADR-015: County Factor Profiles Are Population-Weighted Rollups of Tract Factor Scores

**Status:** Proposed — implemented behind the run queue, awaiting operator ratification
**Date:** 2026-08-08
**Deciders:** Cruz Morales
**Depends on:** ADR-014 (D2 withholding, D6 naive-aggregation removal, D7 class rules, D8 uncertainty, D9 cache key)

## Context

The tract-level factor model is live: EFA with parallel analysis and oblimin
rotation over the 9-variable canonical PLACES set plus two full-universe USDA
food-access shares (model v2 — the universe-limited 1-mile low-access share
was removed after it cost 290 tracts their scores through complete-case and
pushed Dane County under the rollup coverage threshold by 0.8 points),
yielding two interpretable factors (mental-health & health-access;
cardiovascular-metabolic), 1,525 scored tracts, identical model in every
environment. Model revisions bump the vintage string (v1→v2) because the
rollup's cache key carries it. County Factor Profiles, however, deliberately render an empty
state: the naive path that used to fill them — an unweighted tract average
with no minimum-N, at query time and again as persisted rows — was removed
under D6, and its persisted output purged by migration 018.

The county page still needs a factor answer. Three candidate methods:

1. **A separate county-level EFA** over county indicator rows. Rejected:
   n=72 is thin for EFA, the county PLACES values are themselves rollups
   (compounding), and a second model at a second grain means two "Factor
   Profiles" with different meanings sharing one UI.
2. **Population-weighted rollup of tract factor scores, through the run
   queue** — this decision. A weighted mean of standardized scores is the
   county population's average factor exposure: interpretable, single-model,
   and computable under every discipline ADR-014 established (weights from
   the same-vintage SVI tract population, bootstrap interval, coverage
   threshold recorded, withholding instead of approximation, D9 cache
   identity, refreshed by the launch warm).
3. **Distribution display** (min/median/max of the county's tracts).
   Not rejected — it is complementary and can layer onto the same rows —
   but alone it does not give the profile the page promises.

## Decision

County factor scores are produced exclusively by the `factor_rollup` run
type: for each factor and county, the svi_total_population-weighted mean of
tract factor scores, published only when tract coverage clears the recorded
threshold (default 0.8) and the bootstrap interval computes; withheld
otherwise (D2). Published rows are written to `factor_scores` with
`analysis_vintage = '<model-vintage>-popw'` and their provenance
(CI, coverage, n, denominator, method) in `loadings_json`, so
`GET /geographies/:geoid/factors` serves them with no schema change and the
`-popw` suffix distinguishes a rollup from a natively computed score
forever. The launch warm re-establishes them at boot; a new tract-model
vintage produces a new rollup vintage rather than an in-place mutation.

The un-weighted, un-thresholded, un-intervaled version of this same idea is
what D6 deleted. The difference is not cosmetic: weighting, coverage, and
intervals are what make an aggregate a measurement instead of an artifact.

## Consequences

- County Factor Profiles render again — from the one tract model, labeled
  as rollups, with uncertainty attached in the data.
- A county whose tract coverage fails the threshold shows the empty state,
  and that is the feature working.
- Ratification pending: if the operator prefers method 1 or 3, the executor
  and the `-popw` rows are cleanly removable (delete by vintage suffix).
