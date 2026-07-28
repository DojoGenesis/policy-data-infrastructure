# TODO — policy-data-infrastructure

> Updated: 2026-07-28 | Last audit: 2026-07-28 (frontend rebuild, 4 parallel tracks, PIP-115)
> Update this file after every work session. Move completed items to CHANGELOG.md.

## P0 — Blocking correctness (found 2026-07-28)

- [ ] **Theme toggle renders nowhere.** `theme-toggle.js` is now loaded on all 8 EN pages, but it self-mounts only beside a `[data-agent-lang-pair]` element, which is created solely by `lang-toggle.js` — and only `index.html` loads that. Net effect: no page shows a theme button. ADR-013 §3C is NOT satisfied despite the script tags. Fix alongside the i18n toggle track, since the two are coupled by that anchor element [source: Track B, verified by orchestrator]
- [ ] **Tract geometry/data vintage mismatch — the real "map quality" root cause.** Three counts are live at once: `tracts.geojson` (what the map draws) = **1,542**; live PostGIS (what LISA is computed on) = **1,669**; LISA analyses = **1,524/1,525** scores. So ~18 rendered tracts have no LISA score and ~127 DB tracts have no geometry. Presents as "the map looks patchy." Fix = reload tract geographies at ACS 2024, then recompute LISA. **Corrects the number recorded below**: the DB is at 1,669, not 1,652 [source: orchestrator, measured via offset paging on the live API]
- [ ] **`total` field breaks pagination.** `GET /v1/policy/geographies?level=tract&limit=1000` returns `total: 1000`; the true total is 1,669. `total` is being set to the page size, not the unfiltered count — any client paginating on it silently loses 40% of tracts. (`map.html` is unaffected: it reads static geojson and hits the API only for `/v1/policy/analyses`.) [source: orchestrator]

## P1 — High (grant-critical)

- [ ] **`lang-toggle.js` cannot translate `aria-label` / `title` attributes.** `swapDOM` supports only `data-en`/`data-es` (textContent) and `data-en-placeholder`/`data-es-placeholder`. Every `aria-label` and `title` on every page therefore stays English regardless of language — an accessibility gap for Spanish screen-reader users, not just a polish issue. Needs a `data-en-aria-label` convention (or equivalent) added to the mechanism [source: Wave 2B, verified by orchestrator]
- [ ] **Alpine `x-text` output is not translatable.** All runtime-generated strings bypass `swapDOM` entirely and stay English in Spanish mode: card titles/findings/categories (evidence), policy + dimension fields (candidates), method and stability labels (composites), source/variable/reference tables (about). Also several strings with English hardcoded *inside* the Alpine expression — e.g. `dim.policies.length + (…===1 ? ' policy' : ' policies')`, `:title="ind.available ? 'Data available in PDI' : …"`, `v.source_name + ' · ' + (v.unit || 'index')`, and two "No results" ternaries in about.html. Needs a JS-level i18n layer, not attribute pairs [source: Wave 2B]
- [ ] **Reconcile "evidence card" / "composite index" in Spanish copy.** 21 strings currently read as Spanglish ("No hay evidence cards que coincidan con tus filtros", "Constructor de Composite Index") because the dispatch instruction wrongly classified these product nouns as untranslatable domain terms of art alongside z-score/ICE/LISA. Decide the house rule and apply it consistently: `tarjeta de evidencia` / `índice compuesto` are the natural translations. Genuine terms of art (ICE, LISA, z-score, GEOID, NARI) correctly stay English [source: Wave 2B flagged it; orchestrator's instruction caused it]
- [ ] **Spanish copy needs a native-speaker review pass.** Specific flags: "Std Dev" → "Desv. Est." (unverified abbreviation), "Quantile Transformation" → "Transformación por Cuantiles", "Ranking stability" → "Estabilidad del ranking", nav "Evidence"→"Evidencia" / "Composites"→"Compuestos", and "seeded" translated loosely in "Aún no se han agregado candidatos" [source: Wave 2B]
- [ ] **about.html body prose remains English.** Headings, eyebrows, section intros, tables and the Funding/License sections are bilingual; the Overview paragraphs, all 5 Architecture layer blocks, and all 6 Methodology method-cards are not — deliberately deferred as the highest mistranslation-risk, lowest-first-scroll-visibility content. This is the bulk of the file's word count [source: Wave 2B]

- [ ] **`data/sources.toml` is 12 sources out of date.** It lists 5 (tiger, hmda, osm, hrsa, gtfs) against **17 Go adapters** and **12 Python ingest scripts** on disk. Missing: acs, bls_laus, cdc_places, cdc_svi, epa_ejscreen, epa_tri, fbi_nibrs, fcc_broadband, hud_chas, hud_pit, usda_food, wi_dpi. The repo's own "add a new source" checklist and the `api_key_env` lookup both treat this file as authoritative — it describes ~30% of reality. Blocks map-data-source expansion [source: orchestrator]
- [ ] **Emit ACS margins of error for all ACS-derived indicators.** Only `median_household_income` carries `margin_of_error` today (1 of 19). ACS publishes MOEs for `poverty_rate`, `uninsured_rate` and the B25xxx counts, but ingest drops them. Highest-value single fix for the new Compare page — turns materiality from a descriptive convention into a real uncertainty test on 18 more rows [source: Track D]
- [ ] **Add `direction` + `unit` to the `differences[]` array** in the compare API response. It currently omits both, so any client trusting that array alone will colour deltas without polarity — exactly what ADR-013 §2A calls misleading [source: Track D]
- [ ] **Verify polarity on `owner_/renter_cost_burden_30pct_1..5`.** All carry `lower_is_better` with an *empty* description, but the values look like ACS bracket components/denominators rather than outcome rates (Dane `renter_cost_burden_30pct_1` = 104,037 against 245,736 total housing units). They occupy 10 of 13 scoreboard slots and therefore dominate the Compare headline. Either document them or re-mark the components `neutral` [source: Track D]

## P1 — High (grant-critical, pre-existing)

- [ ] Verify `ingest/fetch_tiger.py` against a live PostGIS. The TIGERweb repoint is verified only via `--dry-run` (no psycopg on the build machine); `normalize_props()` -> `bulk_load_geographies()` has not been exercised against a real DB [source: PIP-91]
- [ ] Reload PostGIS tract geographies at the ACS 2024 vintage — the DB predates this and the tract count on record (1,652) does not match the verified 1,542 [source: PIP-91]
- [ ] Confirm whether `block_group` supports a statewide Census query. `lib/census._geo_clause()` still requires a county there; only `tract` was relaxed and verified [source: PIP-91]

- [ ] Build policydatainfrastructure.com — domain returns 200, needs real app content beyond static marketing page [source: roadmap]
- [ ] MCF LOI draft review with Justice — 9 open questions, due Jun 3 2026 (49 days) [source: grant]
- [ ] Arnold Ventures $591K — decision ~May 2026, no action required [source: grant]

## P2 — Medium (quality, testing)

- [ ] **`.site-nav` has no `flex-wrap` / mobile handling** and the nav just grew from 4–6 links to 9 on every page — verify it doesn't overflow on narrow viewports [source: Track B]
- [ ] **Decide `--grad-cta`.** ADR-013 §2B describes it as sun→ember→red; in dark theme the token actually renders amber→pink→purple (`#fcd34d → #f472b6 → #8b5cf6`). The token is byte-identical to casa-datos and casa-datos's own `.btn-primary` uses it, so it was left alone. Fixing this by editing `tokens.css` would break token fidelity with the reference system — decide at the ADR level [source: Track A]
- [ ] **Add a distribution/rank endpoint** (`GET /v1/policy/distribution?variable_id=&level=&state=`). `pkg/stats` already has `PercentileRank`, `ZScore`, `CoefficientOfVariation`, `ReliabilityLevel`. Would cut Compare's context request from ~225KB to ~10KB and move the math into the tested Go engine instead of a JS reimplementation [source: Track D]
- [ ] **Add `category` to `/v1/policy/variables`** — every page currently falls back to keyword-guessing in `inferCategory()` [source: Track D]
- [ ] **Full ADR-013 §1B section-band adoption on the 6 div-based pages** needs a dedicated content-architecture pass (those pages have zero `<section>` elements). Do this after the new `.section-band`/`.thread` CSS is visually verified on `about.html`/`index.html` [source: Track B]
- [ ] Orphaned `.chat-toggle` CSS remains in `composites.html` with no corresponding button (pre-existing) [source: Track B]
- [ ] `about.html`'s local `.about-section { margin-bottom: var(--section-gap) }` now stacks with the new `.section-band` padding on the same elements — check for doubled spacing [source: Track B]
- [ ] Dead element in county.html Levers section: `<span class="layer-indicator li-1" x-show="false">` — always-hidden, empty, likely cruft [source: Track C]
- [ ] Add `denominator_variable_id` to indicator metadata — count indicators currently normalize against `population`, but housing measures should use housing units [source: Track D]

- [x] ~~Add tests for `pkg/store/`~~ — DONE: 15 integration tests, 0% → 77.5% (`19c1242`)
- [x] ~~Add post-stage validation gate to pipeline~~ — DONE: ValidateStage + Config.Validate() (`19c1242`)
- [ ] Audit transitive deps: `go mod why github.com/quic-go/quic-go` and `go.mongodb.org/mongo-driver` [source: health-audit]
- [ ] `pkg/pipeline/` coverage at 30.4% — add stage-level unit tests [source: audit]
- [ ] `pkg/gateway/` coverage at 41.2% — add handler tests [source: audit]

## P3 — Low (polish, national-scale)

- [ ] Wire WI DPI attendance data into Go pipeline (currently Python-only) [source: architecture-gap]
- [ ] National-scale data pipeline (all 50 states) — 13 adapters ready, need orchestration + rate limit budget [source: roadmap]
- [ ] GTFS + EPA-TRI end-to-end tests with mock HTTP servers [source: adapter-audit]
- [ ] HTTP 500 error handling tests across all adapters [source: adapter-audit]
- [ ] HTMLCraft v3.5 Polish: animations, shortcuts, ARIA [source: session]

## Backlog — Future Phases

- [ ] Gateway protocol module merge (2→1) [source: deferred]
- [ ] Factor analysis pipeline integration (currently Python-only) [source: analysis]
- [ ] Tract-level EPA TRI attribution via PostGIS ST_Within [source: adapter-limitation]
