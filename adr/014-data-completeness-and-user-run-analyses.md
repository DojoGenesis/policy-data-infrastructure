# ADR-014: Data Completeness and User-Run Analyses

**Status:** Draft — recon in progress, Decision section not yet written
**Date:** 2026-07-30
**Deciders:** Cruz Morales
**Depends on:** ADR-002 (policy data in Postgres), ADR-003 (indicator metadata), ADR-008 (data source expansion), ADR-012 (integration architecture)

## Context

### The platform looks fuller than it is

The frontend reached a solid state on 2026-07-30 (ADR-011/013, PIP-116). The data
underneath it did not. Measured against the live API on 2026-07-30:

| Level | Source | Variables carrying data |
|---|---|---|
| County | American Community Survey 5-Year | **19** |
| County | CDC PLACES | **0** |
| County | USDA Food Access Research Atlas | **0** |
| County | CDC/ATSDR Social Vulnerability Index | **0** |
| County | FBI NIBRS | **0** |
| County | FCC Broadband (Form 477) | **0** |
| Tract | CDC PLACES | 14 (394 of 400 tracts sampled) |
| Tract | American Community Survey | **0** |

**32 of 51 catalogued variables return zero county values.** Coverage is binary — a
variable has all 72 counties or none of them. The catalog advertises a breadth the
database does not have, which is why `/composite` returns `score: null` for many
plausible variable combinations and why the Candidates page was able to report 0%
indicator coverage without anyone noticing it was also *structurally* true for most
dimensions.

### It is a level split, not missing data

This is the finding that reframes everything. ACS lives at county level. CDC PLACES
lives at tract level. **They never meet.** A tract carries 14 indicators, all health.
A county carries 19, all ACS.

The consequence is not cosmetic: a composite index blending health burden with
economic conditions — the platform's headline capability, the thing ADR-002 and the
Composite Index Builder exist to do — is **structurally impossible today**, because
those variables do not coexist at any single geography.

### Three stores that disagree

There is a third dataset nobody reconciles. The map draws
`cmd/pdi/frontend/tracts.geojson`, a static file whose per-tract properties carry 11
ACS indicators — `poverty_rate`, `median_hh_income`, `uninsured_rate`, `pct_poc`,
`pct_cost_burdened` and others. **None of that is in PostGIS.**

So:

1. **PostGIS county** — 19 ACS variables
2. **PostGIS tract** — 14 CDC PLACES variables
3. **Static geojson** — 11 ACS tract indicators, served to the map only

The map's dataset and the API's dataset are different datasets. The six stored LISA
analyses were computed against one of them while the API serves the other. Any claim
of consistency between what a user sees on the map and what they get from `/v1/policy`
is currently unverified.

### Analyses cannot be created through the API

The HTTP surface is read-only for analysis:

```
GET /v1/policy/analyses        GET /analyses/:id        GET /analyses/:id/scores
```

There is no `POST /analyses`. LISA, OLS and correlation are reachable only through
`cmd/pdi analyze`, which requires shell access to the VPS. The single computation a
user can trigger is `POST /v1/policy/composite`, and the router records its own
intent plainly:

```go
// Composite computation (query-time only, never stored).
```

Every visitor therefore recomputes from scratch and no one inherits anyone else's
work. There is no shared cache, because there is no persistence path at all.

### The stored analyses are hand-made and half of them are stale

| Type | Count | Computed | Vintage |
|---|---|---|---|
| LISA | 6 | 2026-07-27 | ACS-2024-5yr |
| composite_index | 11 | 2026-04-16 | **2022** |
| correlation_matrix | 1 | 2026-04-16 | **2022** |

Twelve of eighteen are three months old against a dataset two vintages behind, and
nothing in the system flags them as stale or regenerates them. A user reading those
results has no signal that they describe a different dataset than the one the
platform now serves.

### Why this matters now

PDI is grant-facing — Arnold Ventures ($591K, decision ~May 2026) and the MCF LOI.
For that audience **defensibility of method matters more than feature count**. A
platform that computes a composite index across a data matrix with 63% of its
catalogue empty, or that aggregates percentile ranks by averaging them, produces
numbers that look authoritative and are wrong. That failure mode is worse than
showing less.

This repository has been bitten by the same class four times in two days — hardcoded
county statistics that drifted, a hardcoded variable count, a hardcoded source count,
and a duplicated CSS fix that let a third page reproduce a solved bug. Each was a
value that was true once, kept asserting itself, and failed quietly. A results cache
is that same shape of risk with a larger blast radius, and this ADR has to address it
directly rather than assume correctness.

<!-- SECTIONS BELOW PENDING RECON
     Three read-only reconnaissance tracks are in flight:
       A. tract→county aggregation capability and the per-variable weighting problem
       B. ACS tract ingest cost, and whether a working tract fetch already exists
       C. analysis engine inventory, storage schema, cache key, auth and cost controls
     Decision, Consequences and the open questions are written once those land, so
     the record reflects measured capability rather than assumed capability. -->

## Operator decisions already taken (2026-07-30)

Recorded here before the full Decision is written, because they constrain it.

### D1 — Load in both directions

Aggregate tract data up to county **and** load ACS down to tract, rather than
choosing one. Rationale: either direction alone leaves a partial matrix, and the
platform's stated purpose is analysis across health, economic and environmental
dimensions at both geographies. Cost is accepted.

### D2 — Withhold unreliable aggregates rather than publishing them with a caveat

Where a tract→county rollup cannot be computed defensibly, the county value is
**absent**, not present-with-a-warning.

**This means the matrix stays partially empty by design, and that is the point.**
Aggregation will not light up all 24 dark variables. Expected to remain absent at
county level:

- **CDC SVI themes (5 variables)** — these are percentile ranks. The mean of a set
  of percentile ranks is not a percentile rank of anything; it has no defensible
  interpretation. A county SVI must come from CDC's own county-level product or not
  exist.
- **Any median** (e.g. `median_household_income` if sourced by rollup) — a median of
  medians is not a median. Where a true county value exists from ACS directly, that
  is used; it is never synthesised from tracts.
- **Counties whose tract coverage falls below threshold** — a rate aggregated from
  60% of a county's tracts is not that county's rate.

Rationale: a caveat is read by roughly nobody and survives no copy-paste. The number,
once rendered, travels without its footnote — into a grant application, a briefing, a
screenshot. For a grant-facing platform where defensibility of method matters more
than coverage, an absent value is honest and a caveated wrong value is a liability.
This also matches the precedent already set elsewhere in the codebase: the featured
county card renders "—" rather than a stale figure when its API call fails, on the
same reasoning that absent beats wrong.

Corollary: the API must distinguish **"no data loaded"** from **"cannot be computed
defensibly"**. Both are currently `null`, and conflating them means a user cannot
tell a gap from a refusal. This distinction needs to be explicit in the indicator
response.

### D3 — User-triggered analyses are queued, not synchronous

A run request returns a job handle immediately; results are retrieved when ready.
Rationale: LISA over 1,542 tracts and factor analysis are not request-latency
operations, a synchronous endpoint on an unauthenticated surface is an obvious
denial-of-service lever, and the codebase already has the pattern —
`GET /v1/policy/pipeline/events` is an existing SSE progress channel to model on.

## What recon found (2026-07-30, three read-only tracks)

Five findings reframed the problem. Each is measured, with a file reference.

### F1 — The mistake this ADR exists to prevent is already shipped, three times

| Where | What it does |
|---|---|
| `pkg/store/postgres.go:1299-1311` | `QueryFactorScores()` falls back to unweighted `AVG(factor_score) GROUP BY LEFT(geoid,5)`. **Serving production traffic now** — `/geographies/55001/factors` returns county values built this way. No minimum-N check: a county with 1 of 40 tracts reporting yields a "county" value from n=1. |
| `scripts/aggregate_factor_scores_county.sql` | Same formula, upserted. |
| `ingest/fetch_epa_ejscreen.py:302-311` | Docstring says "Population-weighted average aggregation"; the code is `sum(vals)/len(vals)`. The docstring concedes it and says to fix before production. Never resolved. |

And the correct argument is **already written in this repo**, at
`analysis/build_atlas_bundle.py:20-23`: *"County values come from the ACS county
estimates, NOT from aggregating tract values. Averaging tract medians does not
produce a county median... The county file is the Census's own county answer."*

The knowledge existed and did not reach the code. That is the same failure pattern as
the `x-show` evaluation trap (documented at `index.html:690`, then reproduced three
times) and the duplicated `table-layout` fix (fixed twice locally, reproduced a third
time). **A rule recorded in one file does not propagate.** This ADR is only useful if
its rules land somewhere enforced.

### F2 — Only 20 of the 24 dark variables are rollup candidates

FBI NIBRS (2) and FCC Broadband (2) are **natively county-level** — their own adapters
say so, and `fcc_multiple_providers_pct` is *defined by the source* as a county
aggregate. They have zero rows at every level. Their Go adapters are explicit
placeholder stubs. This is an **ingest failure, not an aggregation problem**; no
rollup work will fix them.

CDC SVI (5) has zero rows at any level *and* its themes are percentile ranks. CDC
publishes a county-level SVI file separately.

### F3 — Tract population is not loaded, and it is the denominator

`geographies.population` is 0 for every sampled tract. ACS `total_population` has
**zero** tract rows. The only tract population anywhere is `usda_population` — 82%
coverage, **2019** vintage, against CDC PLACES rates from **2022**.

So population-weighting the 8 CDC PLACES variables currently has no same-source,
same-vintage denominator. USDA's own rates are the one clean case, because USDA ships
its population count in the same table and vintage.

**This is why D1's two halves are load-bearing on each other.** Loading ACS at tract
level is not merely "more data" — it supplies the denominator that makes the rollup
defensible. Neither half works alone.

### F4 — A working statewide tract ACS fetch already exists, and there are three variable vocabularies

`analysis/fetch_wi_tracts.py` fetches all 1,542 WI tracts in **3 Census API calls,
~4.5 seconds** (commit `ef26cf0`). The hard part — statewide tract resolution — is
solved and proven. But that script **writes files only, never Postgres**; it exists to
build the static atlas bundle.

The sleeper problem is vocabulary. There is no canonical ACS variable-ID set:

| Path | Style | Example IDs |
|---|---|---|
| `pkg/datasource/acs.go` (**what is live at county**) | raw counts | `median_household_income`, `pop_white_non_hispanic`, `total_population_race` |
| `ingest/fetch_acs.py`, `analysis/fetch_wi_tracts.py` | derived percents | `median_hh_income`, `pct_poc`, `pct_cost_burdened` |

Loading tract data via the Python path would create a **third, non-matching set beside
the county data**. `pct_poc` at tract would have no `pct_poc` at county to compare
against — only `pop_white_non_hispanic` + `total_population_race`. Cross-level
analysis, which is the entire point of D1, would silently not work.

Also: county data is live at `ACS-2023-5yr`; the tract tooling defaults to 2024. And
`ingest/fetch_acs.py --dry-run` still performs all live Census calls — "dry run" there
means "skip the DB write", not "no API cost".

### F5 — The analysis layer has finished capability nobody can reach, and no dedup

- **Unreachable but complete and unit-tested:** `Bootstrap`, `BlinderOaxaca`,
  `InteractionOLS`, `SpearmanRho`, `IsolationIndex` — zero callers anywhere.
  `BlinderOaxaca` is a two-group gap decomposition, precisely the equity finding this
  platform exists to produce.
- **LISA is Python-only**, reads a static GeoJSON rather than the database, and writes
  via raw `psycopg` INSERTs that bypass `store.Store` entirely. Reachable from neither
  the Go CLI nor HTTP.
- **`ICE`, `DissimilarityIndex`, `TippingPoint`** are Go and wired — but only into
  stages 4-5 of the full 6-stage `pdi pipeline run`, a multi-hour operation that
  re-fetches ~15 external sources. There is no "recompute ICE for this county" path.
  `POST /pipeline/run` is a 501 stub.
- **`PutAnalysis` discards the caller's ID** (`postgres.go:690-709`) — always
  `gen_random_uuid()`, no `ON CONFLICT`, no lookup-before-write. The consequence is
  already in production: two identical `composite_index` rows, same scope, same
  vintage, same score count, 11.6 seconds apart.
- **Zero auth and zero rate limiting on all of `/v1/policy/*`.** The only cost-control
  precedent is `chatbudget.go` — reserve-then-settle, $1/day global, 5% per-client,
  client identity from `CF-Connecting-IP` — wired exclusively to `/v1/chat`.
- **`indicators_latest` is refreshed from exactly one place**, the pipeline fetch
  stage. Every analysis that does not pin a vintage reads that view by default, so
  analyses can silently compute on stale data today.
- **Two live defects worth fixing regardless of this ADR:** `ols` selects its outcome
  variable as whichever ID sorts first alphabetically (`analyze.go:280`) — add an
  indicator sorting earlier and the regression target silently changes. And
  `composite --weights` assigns weight 0 to any variable not listed
  (`analyze.go:184-189`), rather than erroring or redistributing.

## Decision

D1-D3 above stand. The following resolve what recon surfaced.

### D4 — One canonical variable vocabulary: the Go raw-count IDs

Tract ACS loads MUST emit the same `variable_id` values already live at county
(`pkg/datasource/acs.go`'s 19-entry set). Derived percentages are computed at read
time or by an explicit derivation step that registers its own IDs at **both** levels —
never as a parallel vocabulary at one level only.

Rationale: cross-level analysis is the purpose of D1, and it silently fails if the two
levels do not share identifiers. A third vocabulary would look like success — data
appears at tract level — while making the thing it was loaded for impossible.

### D5 — Load path: extend `ingest/fetch_acs.py`; do not use the Go adapter or the atlas script

`ingest/fetch_acs.py --state 55 --geo-level tract` already has both the statewide
Census clause and the DB write path. It needs the D4 vocabulary and MOE capture added.

Rejected: the **Go adapter** has never received the statewide-tract fix and would cost
216 calls instead of 3. The **atlas script** (`fetch_wi_tracts.py`) writes files only
and uses the wrong vocabulary; it stays as the map-bundle generator.

Vintage must be pinned to match county data. Loading tract at 2024 against county at
2023 would make every cross-level comparison a vintage comparison in disguise.

### D6 — Fix the three shipped naive aggregations before building the new one

`QueryFactorScores`'s unweighted county fallback is serving wrong values now and is
removed or corrected first. A new aggregation layer built alongside a live incorrect
one just adds a second answer to the same question.

### D7 — Aggregation rules by variable class, with withholding per D2

| Class | Rule | Result |
|---|---|---|
| Counts (`usda_population`, `usda_snap_authorized`) | Sum | Published |
| Rates with a same-source, same-vintage denominator (USDA rates) | Population-weighted mean | Published |
| Rates without one (8 CDC PLACES) | Population-weighted mean **once ACS tract population lands (D1/D5)** | Withheld until then |
| Percentile ranks (5 CDC SVI themes) | Never averaged | Withheld; ingest CDC's county file instead |
| Medians | Never derived from tracts | County value comes from ACS directly or not at all |
| Conditional companion fields (`usda_low_access_1mi`/`_10mi`) | Combine per tract before summing | Published once the COALESCE convention is defined |
| Any variable whose county tract-coverage falls below threshold | Not computed | Withheld |

Coverage threshold is a parameter, not a constant, and its value is recorded with the
result. No aggregate is published without one.

### D8 — Aggregates carry uncertainty, or they are not published

`stats.Bootstrap` is generic and parallel and already exists: resample tracts within a
county, apply the weighting function, return a CI. An aggregate without an interval is
withheld under D2.

Blocker to clear first: `indicators_latest` does not expose `cv`/`reliability` —
migration 007 added those columns to `indicators` after migration 003 defined the
view, and the view was never recreated. The aggregate read path cannot see reliability
today.

### D9 — Cache key is `(type, scope_geoid, scope_level, vintage, parameters)`

`PutAnalysis` stops discarding the caller's ID and gains a lookup-before-write against
that tuple. The schema already carries every field and has a GIN index on
`parameters`; nothing queries it.

**Vintage is part of the key, not metadata.** A cache keyed without it is the exact
failure this repo hit four times in two days — a value true once, still asserting
itself. When a vintage load lands, entries for the superseded vintage are invalidated,
not refreshed in place.

### D10 — Auth and a cost ceiling ship before `POST /analyses`, not after

Model on `chatbudget.go`: reserve-then-settle, a global daily ceiling, a per-client
sub-cap, client identity from `CF-Connecting-IP`. A queued run endpoint on an
unauthenticated surface with no ceiling is a denial-of-service lever with a database
behind it.

### D11 — Route the capability that already exists

`BlinderOaxaca`, `Bootstrap`, `InteractionOLS`, `SpearmanRho`, `IsolationIndex` are
finished and unreachable; LISA is shell-only; `ICE`/`Dissimilarity`/`TippingPoint`
require a multi-hour pipeline run. Exposing these through the queued-run API is a
larger capability gain than writing anything new, and it is what makes the analyses
"competent and complicated" without inventing new statistics.

LISA additionally moves off the static GeoJSON onto the database, or the map and the
analyses continue to describe different datasets.

## Consequences

### Positive

- **Cross-level analysis becomes possible.** A composite blending health burden with
  economic conditions — impossible today because the variables share no geography —
  works once D1/D4/D5 land.
- **One dataset.** Folding the static tract bundle into PostGIS ends the three-store
  split, so the map and the API stop describing different data.
- **Analyses become shareable.** The cache turns per-visitor recomputation into shared
  work, which is the actual gap; measured composite latency is ~100ms, so this is a
  sharing win, not a performance one.
- **Reachable statistics.** D11 surfaces five finished capabilities and LISA without
  new statistical work.
- **Wrong numbers stop shipping.** D6 removes a live incorrect aggregate.

### Negative and accepted

- **The matrix stays partially empty by design.** CDC SVI and any median will not have
  derived county values. Coverage looks worse than a naive rollup would make it look.
  That is D2 working, not failing.
- **Ingest work is unavoidable.** FBI NIBRS and FCC Broadband need real adapters; CDC
  SVI needs its county file. No aggregation work substitutes.
- **MOE capture must be added to the Python path**, which currently hardcodes
  `margin_of_error: None` for 10 of 11 indicators. Tract-level MOEs are proportionally
  much larger than county — loading tract data without them would quadruple the data
  while removing the reader's only signal of when not to trust it.
- **Auth on a previously open surface** is a breaking change for any existing caller.
- **Queued runs add operational surface** — a job table, status endpoint, worker
  lifecycle, failure semantics — that a synchronous endpoint would not.

### Risks

- **The cache silently serving stale conclusions** is the highest-severity risk and
  the reason vintage is in the key rather than beside it.
- **A defensible-looking wrong aggregate** is the second. D2 and D8 exist to make the
  system refuse rather than approximate.
- **Vocabulary drift** — if D4 is not enforced at ingest, the third vocabulary appears
  and cross-level features fail quietly rather than loudly.

## Open questions

1. **Coverage threshold value.** What fraction of a county's tracts must report before
   an aggregate is publishable? 80%? 90%? Needs a defensible basis, not a round number.
2. **Vintage alignment.** Pin the tract load to 2023 to match county, or upgrade county
   to 2024 and reload both? The latter is more work and more correct.
3. **`margin_of_error` discrepancy.** Recon measured 0 of 19 county ACS indicators
   carrying MOE, against a documented claim of 1 of 19. One county was checked. Resolve
   against the database before D8 leans on either number.
4. **Who may trigger a run** — fully open with a ceiling, or does an analysis run
   require identity? D10 sets the mechanism, not the policy.
5. **`usda_lila` / `usda_urban_flag` framing** — "% of tracts" or "% of population"?
   Both are legitimate and they are different questions; the label must say which.
