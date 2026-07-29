# CHANGELOG — policy-data-infrastructure

> Format: `## YYYY-MM-DD` sections, newest first. Update after every work session.
> Include row counts for data loads and root causes for fixes.

## 2026-07-29 — two blank pages, the narrative title, and two stale doc claims

Frontend usability pass toward the operator's 4/10 → 8/10 target (PIP-116). Everything below was measured from rendered pixels in headless Chromium across 10 pages × 2 themes × 3 viewports, not inferred from CSS — per `pdi-verify-per-page-not-shared-layer`, this repo's page-scoped `<style>` blocks make shared-layer reasoning produce inverted conclusions.

### P0 — `/evidence` and `/candidates` rendered as blank pages
Both showed nothing below the header. 70 of 71 `.reveal` elements sat at `opacity: 0` **permanently, even after a full scroll** — the content was in the DOM (~19,875 chars of text on /evidence), just invisible. Two independent causes:

- **The reveal observer never saw Alpine's nodes.** `motion.js`'s `reveals()` runs `querySelectorAll` once and observes only what exists at that moment. Both pages called it a single time at boot; Alpine then rendered the ~70 cards via `x-for` *afterward*, so those nodes were never observed and never got `.in`. Proof: `TPMotion.revealAll('.reveal')` took /candidates from 70 hidden to 0. Fixed by re-calling `reveals()` in a `$nextTick` after load — the pattern already precedented at `compare.html:2518` and `county.html:2224`, which the 2026-07-28 boot-time wiring never extended to dynamically-rendered content.
- **One JS error per page.** `candidates.html:576` referenced `idx` inside an `x-for` that only declared `pol`. `evidence.html:712` dereferenced `card.top_need_counties[0].geoid`, where exactly **1 of 70** cards has that array empty — a single card poisoned the whole page. The sibling `x-show` gates *visibility* only; Alpine still evaluates `:href` on hidden elements. `index.html:690` already documents this exact trap in a comment — the lesson was learned there and never propagated.

Verified: 0 hidden reveals, 0 pageerrors, known cards at opacity 1, on both pages in normal **and** reduced-motion modes.

### Reveal stagger compounded to 5.5 seconds
`--d` feeds motion.css's `transition-delay: calc(var(--d,0) * 1ms)` at an 80ms step. On flat lists using raw `idx * 80`, the 70th card's fade-in did not *begin* until 5.52s after it was marked `.in` — visually identical to the blanking bug, and the reason an initial measurement still showed 60 hidden after a forced reveal. Capped to `idx % 12` on `evidence.html` (grid) and `idx % 8` on `composites.html:915` (single-column stack that reaches 72 rows at county scope — latent, not yet reported). `candidates.html` left at raw `idx`: its index resets per equity dimension and is already bounded.

### P0 — every API-generated narrative read "Five Mornings in "
Root cause was **not** the template engine, as TODO recorded. `pkg/gateway/handlers.go` built `narrative.GenerateRequest` without ever setting `ScopeName`, so `defaultTitle()` concatenated an empty string. The CLI path sets it correctly — which is why this reproduced only through the API. Fixed with `resolveScopeName()`: GEOID → `store.GetGeography`, falling back to the analysis record's `ScopeGEOID`, then to the raw GEOID (traceable, unlike a blank), then to a generic label. Wired into **both** `handleGenerateNarrative` and `handleServeNarrative` — the latter had the identical bug and was not in the original report. `Engine.Generate` now also normalizes a blank `ScopeName`, covering every `{{.ScopeName}}` interpolation rather than the title alone. 5 regression tests added, each verified to fail against the pre-fix code.

### Landing page had no `<h1>`
`/` was the only page of ten without one — the hero was a `<p class="hero-statement">` and the first heading was an `<h2>`. No document-title landmark for screen readers, heading outline starting at h2, SEO penalty on the primary entry point. Promoted to `<h1>`; the `.hero-statement` class already set both `font-size` and full margin control and outranks the shared element selectors, so rendering is unchanged — verified pixel-identical (36px/600, 783×92 box, gradient `em` intact) in both themes.

### `/county` overflowed 118px on mobile — cause was an invisible table, not the nav
At 375px `scrollWidth` was 493 against a 375 `clientWidth`. The obvious suspects — nav anchors sitting past the right edge — were a red herring: `.site-nav` is a deliberate scroll rail (`overflow-x:auto; min-width:0`), so its children's `getBoundingClientRect()` legitimately extends past the viewport without contributing to document overflow. Walking the DOM for the ancestor whose *own* `scrollWidth > clientWidth` found `<section id="layer-3">`, and inside it the Layer 3 accessible data table (`<table class="visually-hidden">`) rendering at **477px instead of 1px**. Root cause is a CSS table quirk: `.visually-hidden` sets `width:1px`, but a `<table>` under default `table-layout:auto` computes its used width from column min-content and ignores the smaller explicit width. Being `position:absolute` inside a `position:relative` section, the oversized box still counted toward scrollable overflow while painting nothing. Fixed with `table.visually-hidden { table-layout: fixed }`, scoped to all three such tables on the page (Layers 1, 3, 4 share the latent defect; only Layer 3's was wide enough to overflow today). `display` untouched, so the implicit ARIA table/row/cell roles survive for screen readers.

### `.layer-indicator` contrast — worse in light theme than the audit reported
Dark theme li-3 (4.1:1) and li-5 (3.12:1) failed AA as expected. Light theme was reported clean by the initial sweep and was in fact **worse**: li-1 1.2:1, li-2 2.71:1, li-3 3.85:1, li-4 2.2:1 — four failures, saturated hues that read fine on the near-black ground and nearly vanish on cream. All six failing cells fixed via hue-preserving `color-mix()` (toward white on dark, toward black or `--tp-teal-deep` on light), gated behind the same `@supports (color-mix())` pattern the file already uses so non-supporting engines keep the original rule rather than an invalid declaration. Now measured 5.05–13.11:1 across all 5 indicators × 2 themes × 3 viewports.

**Why the sweep missed it — a measurement bug worth recording.** The audit's contrast probe parsed only `rgb()`/`rgba()`. Chromium computes `color-mix()` to **`color(srgb r g b)` with 0–1 floats**, which the regex silently skipped rather than flagged — so every element already using `color-mix()` was invisible to the audit and counted as "no failure." Any future contrast tooling in this repo must handle both notations; a parse failure must be reported, never treated as a pass.

### Two STATUS.md / TODO.md claims were stale — corrected
- "**Theme toggle renders nowhere** … ADR-013 §3C is NOT satisfied" — false. Measured present and *rendering* on **10/10** pages.
- "language toggle half-migrated: `lang-toggle.js` on index only, 10 stale `es/` twins remain" — false. Loaded on 10/10 pages, and the `es/` directory no longer exists.

Both originals were reasoned from the shared script layer instead of measured per page. Runtime i18n gaps are real and remain open (Alpine `x-text` bypasses the swap layer entirely).

### Tooling
Added a local dev proxy (scratchpad, not committed): serves `cmd/pdi/frontend/` from disk so edits are live, forwards `/v1/*` to the live API. Lets the frontend run against real data without a local PostGIS — the Docker DB on this machine is empty and an SSH tunnel on `[::1]:5433` shadows it for IPv6 localhost connections, which is why `PDI_DATABASE_URL` defaults fail here.

## 2026-07-28 (later) — stack capabilities, accessibility, light mode, chat drawer removed

### P0 — `.reveal` content was invisible for 2 seconds on every page load
`motion.css` declares `html.js:not(.no-motion) .reveal { opacity: 0 }`. The head snippet sets `.js` on every page, but **`motion.js` was loaded by zero pages**, and only it adds the `.in` class that reverses the rule. The 2000ms `__tpMotionFailsafe` timer was the sole thing making content appear. 26 elements across 6 pages (about 9, composites 7, index 5, evidence 2, candidates 2, narrative 1) blanked then snapped in. `motion.js` now loads on all 10 pages and initialises `TPMotion.reveals('.reveal')`, which clears the failsafe on execute.

### Dead stack capability activated
76KB of vendored casa-datos modules were embedded in the binary and referenced by nothing. `motion.js` (10/10 pages) and `charts.js` (4 pages) are now wired. `parametric.js`, `choropleth.js`, `bilingual-data.js` remain unused — `choropleth.js` deliberately: its `fitToViewBox()` is a linear scale-and-translate, **not a projection**, so it is not a drop-in for the Leaflet map's real GeoJSON.

### Backend capability surfaced
- `GET /v1/policy/geographies/:geoid/lisa-profile` worked and was never called. Now drives County Profile Layer 4 — ADR-011 §2B's "High-High clusters in [X] tracts" reads from `clusters[cluster=="HH"].count` (Dane: 81 of 738).
- Videos went from 3 embedded to all 6, placed contextually per ADR-011 §3B. `about.html` had 7 "Coming soon" placeholders and zero videos.
- Compare's statewide context became visual: standardized distribution strips, one panel per indicator, all 72 counties on a shared axis oriented so better is always right. Neutral-direction indicators excluded, never given an implied verdict.

### Accessibility — measured, not stylistic
- **`--subtle` (#6e6e80) failed WCAG AA as text**: 3.96:1 on `--bg`, 3.76:1 on `--surface` (AA-normal needs 4.5:1). It was used as a text colour **80 times across 9 files**. All swapped to `--quiet` (7.11:1 / 6.77:1 base; 5.74:1 / 5.46:1 enhanced). Non-text uses (backgrounds, borders, the `NS` polygon fill) deliberately preserved.
- **LISA map tiers were indistinguishable under colour blindness.** HH ember vs HL red measured dE **4.2** under deuteranopia — effectively identical — on the two most decision-relevant tiers, where a tract's only encoder is its fill until clicked. Swapped to HH=red (`--tp-cat-5`), HL=sun (`--tp-cat-1`): dE **29.0** deuteranopia, **34.4** protanopia, ΔL* **32.2** (survives greyscale). Warm=disadvantage / cool=advantage semantics preserved; HH now carries red, matching the standard LISA convention. Cool pair (LL/LH) was already fine at 43.7 and untouched.
- **Category tab colours collided with verdict colours.** `cat-health` rendered `--tp-cat-4` (emerald) and `cat-food` `--tp-cat-5` (red) on the same page where those tokens mean better/worse, and `cat-environment` used cyan, County B's identity. Resolved by separating *channel* (verdicts own text+bar-fill; categories own tab-fill+border) and drawing category hues only from the five non-verdict hues. `evidence.html` had a third, independent mapping — now unified.

### Light theme completed (ADR-013 §3C)
Token and shared layers already supported light; page-scoped `<style>` blocks were dark-only.
- `#fcd34d` measured **1.35:1** and `#c4b5fd` **1.72:1** on the cream ground — effectively invisible. Replaced with tokens that flip.
- `fix:` `.thread`'s `mix-blend-mode: multiply` was ungated. On an engine without `color-mix()`, forcing light theme yields the *dark* palette plus multiply — every spectrum stop landed at **1.01–1.04:1**. Not washed out; gone. Now inside the gate.
- `fix:` `var(--plane-raised, #f0f4ff)` — `--plane-raised` is defined **nowhere**, so the hardcoded light-blue fallback fired in *both* themes.
- `fix:` tract stroke was `rgba(255,255,255,0.06)` — white on cream, so tract boundaries vanished in light mode. Now theme-aware via `--tract-stroke`.
- `fix:` `select` chevron was a hardcoded dark-tuned hex at **2.45:1** on light, under the 3:1 floor for the only affordance signalling "dropdown".
- `narrative.html`'s `#fff`/`#000` confirmed inside `@media print` — correct, left alone.

### Chat drawer removed (operator decision)
Rationale: low expected adoption. `chat-drawer.js`/`.css` removed from all 10 pages and both assets deleted (24KB that would otherwise ship in the binary). It self-mounted to `document.body` with no pathname check, so `/chat` was rendering a floating drawer over a full chat page — removing it fixes that too. **`/chat` stays** as a destination with its own UI (`lib/api.js`, `lib/domain.js`, `lib/chat.js`, `lib/deeplink.js`), keeps its nav link, and the backend (`/v1/chat`, `pkg/grounding`, Gateway proxy) is untouched and still tested. ADR-011 carries a supersede block naming exactly what not to rebuild; ADR-005/006 got delivery-surface notes and remain valid.

### Hygiene
Two agent scratch files (`__preview_dist.html`, `_h_parsecheck.html`) were caught inside `cmd/pdi/frontend/`. `//go:embed all:frontend` reads the filesystem, not git, and the `all:` prefix includes underscore-prefixed names — both would have shipped in the binary despite never being committed. Directory verified at 36 entries.

### Gates
`go build` ✅ · `go vet` ✅ · `go test -short` 10/10 ✅ · all 10 pages: lang+theme toggle, footer, 9-link nav, 0 drawer refs, 0 `--subtle` text, `data-en`==`data-es`, `<div>` balanced ✅

## 2026-07-28

### Frontend rebuild — ADR-013 visual layer deployed (4 parallel tracks, PIP-115)

**Root cause found:** the frontend problem was not missing features. Waves 1–4 shipped the ADR-011/012 *structure* (chat drawer, deep links, PWA, videos, Leaflet — all present and working), but the ADR-013 *visual layer* was never ported from casa-datos. `tokens.css` was already byte-identical to casa-datos; only `styles.css` lagged (49,160 → 59,068 bytes).

- **`.thread` was used 19× in markup with zero style rules** — every section divider rendered as nothing. Now the full Rainbow spectrum ribbon (`--band-spectrum`), 1px, per ADR-013 §1D.
- **`.site-footer` was used 6× with zero style rules** — footers rendered as unstyled raw text. Now styled; contrast verified 5.76:1 dark / 6.25:1 light (AA).
- **Inline `<style>` blocks were defeating the shared stylesheet.** Page-level `<style>` loads *after* `/static/styles.css`, and 7 pages redefined `.thread` as the faded single-colour line ADR-013 §1D exists to replace. All stale inline `.thread`/`.site-footer` rules removed; markup preserved. Zero stale faded-thread rules remain in the built binary.
- `fix:` `.skeleton`'s `background` shorthand was clobbering `.card`'s `--edge` layer — cards silently lost their spectral edge *during loading*, exactly when ADR-013 §3B says atmosphere should hold. `.skeleton-card`/`.skeleton-row` now carry spectral edge + amber pulse, with a reduced-motion variant.
- `fix:` `@supports not (clip-path: inset())` fallback — without `clip-path`, `.section-band`'s 100vmax spread shadow was never trimmed and flooded the whole page.
- `feat:` light-theme `mix-blend-mode: multiply` on `.thread` — raw Rainbow hues wash out on cream.

### Chrome unification across 8 EN pages
- Footers added to `narrative`, `chat`, `map` (were absent); all 10 pages now carry one.
- Headers normalized byte-identical across 8 pages (differing only by `aria-current`); nav hrefs verified against the real route table in `cmd/pdi/serve.go` — Composites is served at `/composite` (singular).
- `theme-toggle.js` now loaded on all 8 pages. **Known incomplete — see TODO:** the script self-mounts only beside `[data-agent-lang-pair]`, which only `lang-toggle.js` creates, and only `index.html` loads that. The toggle button therefore still renders nowhere; ADR-013 §3C is not yet satisfied.
- `section-band` applied to all 9 sections of `about.html`. Deliberately **not** applied to `evidence`/`composites`/`narrative`/`chat`/`map` — those pages contain zero `<section>` elements (div-based Alpine conditional states), and ADR-011 §1A classifies Map/Chat/Composites as the **Interact** archetype, explicitly distinct from the section-band rhythm.
- Removed a dead `.chat-toggle` button from `index.html`'s header (verified zero click handlers; `chat-drawer.js` self-mounts its own toggle).
- **Corrected a false alarm:** `narrative.html`'s reported "4 headers" was a substring-grep artifact matching `.site-header-inner`, a print-media CSS selector, and a JS comment. Only one real `<header>` exists. No fix needed.

### County profile — composite builder promoted (ADR-011 §2B Act 3, ADR-012 §3)
- `<details class="composite-panel">` → a first-class section with an accessible button toggle (`aria-expanded`/`aria-controls`), open by default. `<details>`/`<summary>` count now 0.
- Deep-link `?composite=open` behaviour preserved, reimplemented via `$watch` instead of a DOM `toggle` listener (removes a `$nextTick` race).
- Alpine bindings 165 → 167, none lost.
- Assessment: the three-act narrative structure (ADR-011 §2B) **was already built** — ACT 1/2/3 headers, ICE-conditioned defining sentence, per-layer context prose, and both Act 3 CTAs already matched the ADR. The only real gap was Layer 5 being collapsed, which this fixes.

### Compare — statewide context replaces raw deltas
- Every gap is now positioned against all 72 WI counties rather than only the other county: statewide rank + percentile, standardized gap (`(v_A − v_B) / SD_state`, matching `pkg/stats.ZScore`), direction-aware verdict, materiality test, and an ADR-011 §2C scoreboard ("better off on 9 of 13 directional indicators").
- Polarity taken verbatim from the API's `direction` field; emerald/red per ADR-013 §2A. Across 51 variables: 33 `lower_is_better`, 4 `higher_is_better`, 12 `neutral`, 2 empty — the 14 undirected are rendered context-only, never coloured.
- Materiality uses the Census difference formula `√(MOE_A² + MOE_B²)` at 90% confidence where MOEs are published, otherwise a distributional band plus an assumption-free counties-between count.
- `fix:` **16 of 19 indicators are `unit: "count"`**, so raw comparison measured county *size*, not policy outcome. Counts are now normalized per-10k residents for rank/gap. This surfaced a genuine contradiction: 2 indicators (`owner_cost_burden_30pct_4`, `renter_cost_burden_30pct_4`) reverse which county is better off between raw-count and per-capita bases. Resolved with one canonical verdict; raw Δ columns left uncoloured on normalized rows.
- Reliability dots now populate from real `margin_of_error` (previously always null).
- Enabled by one `POST /v1/policy/query` returning all 72 counties in a single 225KB request. No Go changes.
- Alpine bindings 83 → 125, none lost.

### Repo hygiene
- `chore:` deleted the repo-root `/frontend/` orphan fork (27 tracked files). `cmd/pdi/serve.go:24`'s `//go:embed all:frontend` resolves relative to `cmd/pdi/`, so the root copy was never served; it had diverged 5 commits and was a trap for agents editing the wrong tree.

### Gates
`go build ./...` ✅ · `go vet ./...` ✅ · `go test ./... -short` 10/10 packages ✅ · HTML tag balance even across all 10 pages ✅ · JS syntax clean ✅ · new assets confirmed embedded in the built binary ✅

## 2026-07-27

### Wave 1: Frontend Reconciliation + Deploy
- Reconciled dual frontend directories (`frontend/` and `cmd/pdi/frontend/`) — 9 HTML files synced
  - index.html: merged root's rich county explorer (324 lines) with cmd's motion failsafe + theme detection → 332 lines
  - county.html: copied root's full dashboard (1,292 lines) → cmd (was 334-line skeleton)
  - about.html, compare.html, map.html, narrative.html: cmd versions were richer → kept + synced back to root
  - candidates.html, composites.html, evidence.html: identical
- Verified static assets in cmd/pdi/frontend/: tracts.geojson (1,542 features, 1.8MB), counties.geojson (72 features, 120KB), evidence_cards.json (70 cards, 166KB), composites/nari_v2.json
- Cross-compiled Linux binary (40MB), deployed to VPS (root@5.161.84.125), pdi.service restarted
- All 11 pages serving: index, county, compare, evidence, candidates, map, narrative, about, composite, chat, es/*
- VPS health confirmed: 6 LISA analyses (1,524 scores each), 70 evidence cards, 42 variables, Francesca Hong policies seeded
- API endpoints verified: geographies, indicators (Dane County $88,108 median income), analyses, variables, policies, sources

### Wave 2: Three New Data Sources (dispatch: deleg_3fe9e1c5)
- **CDC SVI** (Social Vulnerability Index): 5 variables (overall + 4 theme scores), county + tract level
  - `ingest/fetch_cdc_svi.py`, `pkg/datasource/cdc_svi.go`, `cdc_svi_test.go`
- **FBI NIBRS** (Crime Data): 2 variables (violent_crime_rate, property_crime_rate), county level
  - `ingest/fetch_fbi_nibrs.py`, `pkg/datasource/fbi_nibrs.go`, `fbi_nibrs_test.go`
  - Requires FBI_CDE_API_KEY (documented in --dry-run output)
- **FCC Broadband** (Form 477): 2 variables (broadband_access_pct, multiple_providers_pct), county + tract level
  - `ingest/fetch_fcc_broadband.py`, `pkg/datasource/fcc_broadband.go`, `fcc_broadband_test.go`
- 15 new files total, 9 new indicator variables, 3 migrations (009-011)
- Registrations in fetch.go + pipeline.go, seed_sources.sql updated
- All gates: go build ✅, go vet ✅, go test ./... -short (10/10 pass) ✅
- Post-dispatch fixes: FCC registration added to pipeline.go, fbi-nibrs added to seed_sources.sql, migrations renumbered sequentially

### Data Source Count
- Pre-Wave 2: 13 datasource adapters, 42 variables
- Post-Wave 2: 16 datasource adapters, 51 variables
- Sources: ACS, TIGER, CDC PLACES, EPA EJScreen, HRSA, GTFS, WI DPI, HUD CHAS, HMDA, EPA TRI, HUD PIT, USDA Food, BLS LAUS, **CDC SVI**, **FBI NIBRS**, **FCC Broadband**

### Statewide tract resolution — ACS 2024 (PIP-91)
- Raised the ACS default vintage 2023 → 2024 across the ACS-facing scripts
  (`analysis/fetch_wi_counties.py`, `ingest/fetch_acs.py`, `ingest/fetch_acs_b19001.py`,
  `ingest/Makefile`). ACS 2020-2024 5-Year was released 2025-12-11.
  - Deliberately NOT changed: `ingest/load_to_postgres.py`'s per-file vintage defaults.
    Those describe the vintage of already-generated atlas JSON files (2023 data), not a
    fetch target — bumping them would mislabel existing data.
  - Also unchanged: `fetch_wi_dpi.py`, `fetch_bls_laus.py`, `fetch_epa_ejscreen.py`.
    Different sources on different release cadences; bumping their defaults without
    verifying availability would just create broken defaults.
- New `analysis/fetch_wi_tracts.py` — statewide tract-level ACS fetch, two resolution
  modes (`statewide`, `county-drill`) plus `--compare` to diff them. Loaded 1,542 WI
  tracts, 11 indicators, ~1.2% null (normal small-tract suppression).
- New `analysis/fetch_tract_boundaries.py` — vintage-matched boundary GeoJSON.
  1,542 tract + 72 county features.
- New `analysis/build_atlas_bundle.py` — validated join + quantile class breaks.
  Join is a hard gate: any GEOID on one side and not the other exits non-zero.
- New `ingest/lib/tigerweb.py` — TIGERweb REST client (stdlib only).

### Fixes — three live bugs found while building the above
- **The Census API now requires a key.** A keyless request 302s to
  `/data/missing_key.html` (`X-DataWebAPI-KeyError: 1`); urllib follows it and the caller
  gets a JSONDecodeError with no hint of the cause. Added `lib/census.require_api_key()`
  and a landed-URL check in `fetch_acs_table()`. The repo's "45 req/min without key" note
  was stale — keyless now means zero.
- **`ingest/fetch_tiger.py` was entirely broken.** It downloaded from
  `https://www2.census.gov/geo/tiger/GENZ{year}/json/` — that directory returns HTTP 404
  (verified for 2024; the Census publishes these as shapefiles now, and this repo has no
  shapefile reader). Repointed at TIGERweb, which serves GeoJSON directly and is
  vintage-parameterized. Added `normalize_props()` because TIGERweb names attributes
  differently (`STATE`/`AREALAND`) than `lib/db.bulk_load_geographies()` reads
  (`STATEFP`/`ALAND`) — without it the load "succeeds" while writing NULL state_fips.
  Also deferred the `lib.db` import past the `--dry-run` return, so a dry run no longer
  requires psycopg to be installed.
- **`lib/census._geo_clause()` rejected statewide tract queries.** It raised unless a
  county was supplied, but `for=tract:*&in=state:55` works and returns all 1,542 tracts
  in one call — the restriction was inherited from an older API vintage and cost 72x the
  requests. Relaxed for `tract`; `block_group` still requires a county (unverified).

### Data health note
- Verified WI tract count is **1,542** (ACS 2024 API and TIGERweb `tigerWMS_ACS2024`
  agree exactly). `STATUS.md` said 1,652 and `CLAUDE.md` said ~1,929 — both corrected.

## 2026-04-15

### Datasource Adapter Expansion (9 new adapters)
- Added 9 new Go DataSource adapters: HRSA, GTFS, WI DPI, HUD CHAS, HMDA, EPA TRI, HUD PIT, USDA Food, BLS LAUS — `8957bd8`, `dcec5db`
  - Total adapter count: 13 (ACS, TIGER, CDC PLACES, EPA EJScreen + 9 new)
  - 170 test functions across 10 test files, 67.4% coverage on datasource package
  - 11,210 lines of Go across 16 adapter source files

### Post-Build Audit Fix Pass — `15cc5f9`, `68c86ac`
- Fixed 3 critical bugs found in audit:
  - BLS LAUS: prefix `LAUST` → `LAUCN` (county not state series); fill zeros 9 → 8
  - GTFS: geocoderResponse struct matched address endpoint, not coordinates endpoint
  - HUD PIT: default URL pointed to HTML page, not CSV; added Content-Type guard
- Fixed 2 medium bugs:
  - WI DPI: race label matching ("Black" → `strings.Contains("black or african american")`)
  - WI DPI: race-stratified variables never emitted; now emit as nil (data gap visible)
- Fixed HMDA minority classification: "2 or more minority races" was excluded from minority denial rate
- Fixed EPA TRI carcinogen semantics: renamed `epa_tri_carcinogen_releases` → `epa_tri_carcinogen_facility_count`
  - Root cause: tri_facility endpoint only provides CARCINOGEN=YES/NO flag, not per-chemical lbs
  - Prior approach summed all releases from flagged facilities (wrong units)
- Normalized variable ID prefixes across 4 adapters (21 variables total):
  - `bls_*` → `bls_laus_*` (4 vars), `dpi_*` → `wi_dpi_*` (7 vars)
  - `hud_*` → `hud_chas_*` (5 vars), `usda_*` → `usda_food_*` (4 vars, `usda_food_desert` already correct)
- Fixed USDA Food GEOID padding: `%011s` + byte loop → `ParseInt` + `%011d`
- Added TIGER registration to pipeline.go (was in fetch.go only)
- Updated sources.toml WI DPI variable names

### Scheduled Pipeline Verification
- All Apr 14 handoff targets confirmed complete — no new data loads required:
  - BLS LAUS: 72/72 WI counties, 0% null on unemployment_rate (rate limit cleared)
  - WI DPI attendance: 449 district rows, 0% null on chronic_absence_rate
  - Evidence cards: 70 cards generated (target was 50+; confirmed via `evidence_cards.json`)
  - VPS PostGIS geographies: 72 counties + 1,652 tracts loaded
  - VPS PostGIS indicators: 1,368 ACS, 12,200 CDC PLACES, 8,009 USDA (all above floor)
  - Gateway: `dojo-gateway.service` active since 2026-04-14, `/v1/models` and `/chat` both return 200
- Added `.wrangler/` to `.gitignore`
- Removed completed P1 BLS re-fetch item from TODO.md

## 2026-04-14

### BLS LAUS Fix
- Fixed series ID format: 8 fill zeros, not 7 — root cause of all-null data since Apr 14 — `1976e97`
  - LAUS county series IDs are 20 chars; script produced 19-char IDs
  - BLS accepted requests but returned empty data for non-existent series (silent drop)
  - Fixed registered batch size from 500 to 50 (actual BLS v2 limit)
  - 72/72 WI counties now return data: unemployment rate [2.1%, 5.8%], 0% null

### Factor Analysis
- New `analysis/factor_analysis.py`: EFA on 1,265 WI tracts with 12 features — `7739423`
  - 2 factors, 66.5% variance explained, KMO=0.833
  - Factor 1: Mental Health / Economic Deprivation (38.4%) — poverty, MHLTH, ICE, ACCESS2
  - Factor 2: Cardiovascular / Metabolic (28.1%) — BPHIGH, DIABETES, PHLTH, OBESITY
  - Outputs: factor_loadings.csv, factor_scores.csv (1,265 tracts)

### ACS B19001 ICE Ingest
- New `ingest/fetch_acs_b19001.py`: true ICE from cross-tabulated income-by-race data — `ba16b27`
  - 1,542 WI tracts, 1,524 with ICE scores (98.8%), range [-0.65, +0.82]
  - Replaces poverty×race approximation with Krieger 2016 methodology
- `analyze.go`: prefers true B19001 ICE scores when available, falls back to approximation

### Narrative Chain Fix
- Fixed 4 NARI→ICE rendering bugs blocking document generation — `d732c2b`
  - `selector.go`: now populates `p.ICE` field (was only setting deprecated NARI fields)
  - `engine.go`: "NARI Percentile" → "Equity Index Percentile", prose references ICE
  - 3 templates (`five_mornings`, `equity_profile`, `comparison_brief`): all user-facing NARI text replaced
  - Root cause: statistical refactor replaced NARI with ICE in pipeline but never updated narrative layer

### Health Audit & Infrastructure
- First comprehensive health audit completed — overall grade B-
- Rescued 6 research files from stale `policy-data-infrastructure/` clone
- Deleted stale clone — canonical directory is `policy-data-infra/`
- Created CLAUDE.md (300 lines, rewritten for Sonnet agents)
- Created TODO.md + CHANGELOG.md
- Added `.github/workflows/ci.yml` — go build + go vet + go test on push/PR

### P0 Fixes (all 7 resolved)
- `.gitignore`: added root `.venv/` and `analysis/output/` — `4b17097`
- Untracked 8 committed analysis output artifacts — `9fada5c`
- Makefile: ldflags PKG → `internal/version` (was `cmd/pdi`, silently broken) — `9fada5c`
- `go.mod`: version corrected via `go mod tidy` — `9fada5c`
- Pipeline: replaced deprecated NARI composite with ICE metric (Krieger et al. 2016) — `9fada5c`
- `analyze.go`: Percentile now uses `stats.PercentileRank()` instead of raw score — `9fada5c`
- `analyze.go`: removed `-1.0` sentinel, uses proper `*float64` nil — `9fada5c`

### P1 Fixes (8 of 9 resolved, 1 deferred)
- Gateway XSS: escape geography names with `html.EscapeString` — `f67d4dc`
- Gateway: log `LoadEmbeddedTemplates` errors instead of silent discard — `f67d4dc`
- Gateway: `errors.Is(err, pgx.ErrNoRows)` replaces string matching — `f67d4dc`
- Python `census.py`: `_clean_sentinel` handles float-string sentinel — `f67d4dc`
- `sources.toml`: cdc-places `api_key_env` → `CDC_PLACES_APP_TOKEN` — `9fada5c`
- `schemas/geography.schema.json`: county_fips 3-digit, `geo_level` → `level` — `9fada5c`
- Store: deleted dead `export.go`/`import.go`, promoted `PutIndicatorsBatch` to interface — `f67d4dc`
- **DEFERRED**: BLS LAUS re-run (rate limit, wait for UTC midnight reset)

### P2 Fixes (all 9 resolved)
- Gateway tests: 23 httptest handler tests (coverage: 0% → 23 tests) — `fc3a3e9`
- HTMLCraft tests: 41 tests across 6 groups (coverage: 0% → 41 tests) — `fc3a3e9`
- CI workflow: `.github/workflows/ci.yml` (build + vet + test) — `f67d4dc`
- Dead code: removed `buildURL()`, `buildStateURL()` from acs.go — `f67d4dc`
- Dead code: removed `geoLevelDisplay()` from query.go — `f67d4dc`
- CDC PLACES: 650ms rate limiting between paginated requests — `f67d4dc`
- README: marked 8 unimplemented sources as (planned), fixed Go version — `f67d4dc`
- `PutIndicatorsBatch` promoted to Store interface — `f67d4dc`

### P3 Fixes (2 of 5 resolved)
- Narrative: 12 magic numbers extracted to named consts with cited sources — `fc3a3e9`
- Narrative tests: 16 new tests (17 → 33 total) with table-driven + boundary — `fc3a3e9`

### New Features (from parallel orchestrator)
- `pkg/stats/features.go`: ICEIncomeRace + CoefficientOfVariation + ReliabilityLevel
- `pkg/stats/features_test.go`: test coverage for new stat functions
- `pkg/store/migrations/007`: cv, reliability columns + factor_scores + validated_features tables
- `pkg/narrative/slot.go`: FactorScores, FactorPercentiles, ICE, Reliability fields
- `pkg/narrative/template.go`: factor-based template helpers (factorLabel, factorColor, etc.)

### Session Stats
- 9 Sonnet agents dispatched across 2 waves (3+3+3)
- Total test count: 17 → 97 (+80 new tests across 3 packages)
- TODO items closed: 25 of 29 P0-P2 items (86%)
- Seed planted: Orchestrator Blindspot — stash don't revert parallel session work

### Data Pipeline
- Expanded evidence cards to 70 (all 70 policies, 0 skipped) — `32f5032`
- Added WI DPI attendance fetcher (449 districts, chronic_absence_rate 0% null)
- Fixed BLS LAUS script (startyear/endyear params) — data still null due to daily rate limit
- VPS PostGIS state: 72 counties + 1,652 tracts + 1,368 ACS + 12,200 CDC PLACES + 8,009 USDA = 22,949 indicator rows

## 2026-04-13

### Gateway & Narrative
- Wired narrative engine to gateway routes — `4edeebd`
- Fixed ACS FetchCounty: split detail/subject tables + SafeFloat string sentinel — `e205c08`

### Code Quality
- Deep code audit found 10 bugs across stats, pipeline, store — `3961a2d`
- Audit-driven sweep: 9 findings across 8 files — `b421cff`
- Fixed PostGIS-optional store + working analyze + narrative pipeline — `fc7dee6`

## 2026-04-12

### Analysis
- Multi-source evidence cards: CDC PLACES + USDA food access — `b110cb5`
- Fixed 5 data source fetchers, added WI output CSVs — `f3a1d61`
- Multi-source data ingest, idempotent migrations, county-level ACS fetch — `9b8ec01`

### Documentation
- Added README with architecture, data sources, API, and usage guide — `92311a9`

## 2026-04-11

### Infrastructure
- Phases 5+6: VPS deployment, national-scale fetch, CDC PLACES, EPA EJScreen — `8db0054`

## 2026-04-10

### Core Development
- v0.1 proof of concept: policy-to-evidence pipeline for Wisconsin — `f4b6ee7`
- Phases 3+4: pipeline engine, narrative generator, HTMLCraft bridge, gateway API, CLI wiring — `82d716a`

## 2026-04-09

### Foundation
- Policy record schema + Francesca Hong 2026 gubernatorial positions — `a724099`
- Phase 2 data ingest: Census API client, Python scripts, Store CRUD — `89a437d`
- Phase 1 foundation: pkg/geo, pkg/stats, pkg/store, CLI, PostGIS schema — `7680cfa`

---

## Update Conventions
- Update this file after every work session
- Use conventional commit categories: feat, fix, chore, docs, data
- Include commit hashes for traceability
- Include row counts and data state for pipeline changes
- For fixes: include the root cause, not just the symptom
- Agent sessions: append to the current date section or create a new one
