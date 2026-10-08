# One-Week MVP — "Does this reach my block?"

**One policy · one community · existing tools only**

> Status: proposed design, not built. Authored 2026-08-09 by the PDI orchestrator.
> Policy: **FH-TRANSIT-008 — Public groceries** (AB1221, 2025)
> Community: **Rock County's 12 USDA-designated tracts** (Beloit/Janesville), swappable by GEOID
> Claim type: **exposure and eligibility — never effect**

---

## 1. The decision, in one paragraph

Build one page — `/policy?id=FH-TRANSIT-008&geoid=55105` — that answers a single question a
Rock County resident can ask out loud: *"This candidate wants publicly-owned grocery stores in
food deserts. Does that include where I live, and who else here would it reach?"* The page is
place-scoped, printable for a community meeting, bilingual, and built entirely from data already
in the platform. It makes no claim about what a store would do. It states who is covered by the
policy's own eligibility test, how many people that is, and how confident we are.

---

## 2. Why this policy, out of 70

I tested every position in `data/policies/francesca_hong_2026.csv` against one question: **is the
policy's operative geographic unit the same object as a dataset PDI already holds?**

FH-TRANSIT-008 is the only clean pass. "Enable publicly-owned grocery stores in food deserts" is
keyed to *food-desert tracts*, and USDA FARA low-income/low-access designation is a **tract-level
published fact already loaded** (`usda_food_desert`, `usda_food_low_access_1mi/_10mi`,
`usda_food_snap_count`, `usda_food_grocery_count` — 6,438 rows, county + 2020-tract via the
population-weighted crosswalk, ADR-014 OQ6). Every other position needs data we do not have:

| Candidate | Why it loses |
|---|---|
| FH-HSG-005 Right to counsel | Needs eviction filings/outcomes. No court data in the stack, none acquirable this week. |
| FH-HSG-004 Protecting renters | Cost burden *is* loaded at tract level with MOE — strong second choice — but exposure is a proxy for a policy about enforcement. More caveat than signal. |
| FH-TAX-001 Property tax reform | Needs parcel-level tax and referendum data. Absent. |
| FH-CJ-003 Crime prevention | FBI NIBRS is **blocked on operator** until 2026-08-11. |
| FH-EDU-001 Free school meals | WI DPI adapter exists; participation data not loaded. Real, but a week of ingest before any product. |

The gap this closes is concrete. Today the evidence card for Public groceries
(`cmd/pdi/frontend/evidence_cards.json`) is backed by **`poverty_rate` alone, at county level,
statewide**, with a templated finding ("Wisconsin's highest-poverty counties would benefit most").
A policy explicitly about food deserts is currently explained with a poverty number and does not
touch the food-access data sitting in the same database. No surface in the product is scoped
small enough for a person to recognize as home.

---

## 3. Why this community

133 of 1,392 tracts statewide carry the designation, across 46 counties. Ranked by designated
population, Rock County is first — and unlike Milwaukee or Dane it is small enough that the
designated tracts *are* the community, not a fragment of it.

| | Rock County | Rusk County (rural contrast) |
|---|---|---|
| Designated tracts | **12** | **4 of 5** |
| Population in them | **41,768** | **12,510** |
| Share of county | **26.1%** | **84.8%** |
| SNAP households | **4,754** | **814** |
| Access standard | all 12 urban (1-mile) | 3 rural (10-mile) + 1 urban |
| County profile (ACS 2020-2024) | 164,350 people · 9.3% poverty · 10.1% Hispanic · 20.4% POC · 32.0% cost-burdened | 14,179 people · 13.1% poverty · 16.9% BA+ |

Designated tracts (2010 vintage, as USDA publishes them): `55105000100, 000300, 000600, 001000,
001100, 001600, 001700, 002000, 002100, 002300, 002400, 002500`.

Rock County is the pilot: largest designated population, one access standard (simpler first
build), 10.1% Hispanic county-wide so the existing ES lane earns its keep. **Rusk is the week-two
contrast** — it stresses the 10-mile rural standard and a place where four of five tracts qualify.

*Operator call:* which community you actually build **with** is yours, not the data's. Swapping
the pilot is a GEOID change, not a rebuild. A brief about a place should be made with someone
from it — line up the partner before the page ships, not after.

---

## 4. What PDI may say, and what it may not

This is the spine of the design. The platform has no counterfactual, no store-location data, and
no way to observe a policy that has not passed.

**It answers:**
1. Does my area meet the policy's own eligibility test, and by how much?
2. Who lives in the areas that do — how many people, how many SNAP households, what else is true here?
3. How does my county compare to the rest of Wisconsin on this one measure?

**It refuses:**
- "A store here would cut food insecurity by X%" — no counterfactual exists.
- "This policy would help/harm you" — an assessment, not a measurement.
- Siting simulation ("put a store at 4th & Grand and Y people gain access") — we hold access *flags* at 1 and 10 miles, not distances or store locations.
- Any binary "you live in a food desert." See below.

**The vintage rule that governs every number on the page.** The designation is a 2010-tract fact
(FARA 2019, the newest published release). The people-data is 2020 tracts (ACS 2020-2024). The
crosswalk allocates by population, which turns a categorical flag into a **0–1 population share**
— `fetch_usda_food.py` says so explicitly, and the metadata records it. The repo holds **no 2010
tract geometry**, so the map must be drawn on 2020 tracts.

Therefore: the map layer is *"share of this tract's population living in an area USDA designated
low-income and low-access"* — worded that way in the legend, in both languages. The 12 designated
tracts are listed separately, at their native vintage, labeled as such. The page never prints
"your tract is a food desert" from a crosswalked share. Where the two vintages disagree about a
place, the page says so rather than smoothing it.

---

## 5. The artifact

**One page. One question at the top. Print-clean.**

```
┌──────────────────────────────────────────────────────────┐
│  Public groceries — Rock County          [EN/ES] [◐]     │
│  A position by Francesca Hong · AB1221 (2025)            │
├──────────────────────────────────────────────────────────┤
│  Does this reach my block?                               │
│  ┌────────────────────┐  41,768 people — about 1 in 4    │
│  │   [tract map]      │  Rock County residents — live    │
│  │   click your area  │  where USDA's test is met.       │
│  │                    │  4,754 households use SNAP.      │
│  └────────────────────┘  12 of 41 tracts. All urban.     │
├──────────────────────────────────────────────────────────┤
│  WHAT THE POLICY SAYS   verbatim position + bill, linked │
│  WHAT THE TEST IS       LILA in plain words + vintage    │
│  WHO LIVES HERE         per-tract: people, SNAP, poverty,│
│                         uninsured, cost burden (+ MOE)   │
│  WISCONSIN CONTEXT      Rock vs 72 counties, one chart   │
│  WHAT WE CANNOT TELL YOU  the refusals, stated plainly   │
│  WHERE EVERY NUMBER CAME FROM  source, vintage, method   │
└──────────────────────────────────────────────────────────┘
```

- **Route:** `/policy?id=<policy_id>&geoid=<geoid>` (extensionless — `.html` 404s in this stack).
  Policy-parameterized from day one; only FH-TRANSIT-008 is populated this week.
- **Endpoint:** `GET /v1/policy/briefs/:policy_id?geoid=` → one JSON payload: position record,
  eligibility layer, per-tract rows with MOE, statewide context, provenance block. Read-only,
  no token, cache-friendly.
- **Aggregates** go through the existing run layer (`POST /v1/policy/analyses`,
  `tract_rollup`) so coverage thresholds, CI, and withholding apply for free. No new statistics.
- **Selection is click-the-map plus a tract dropdown.** No address geocoding — that is a new
  external dependency and it is cut.
- **Print stylesheet** is a requirement, not a nicety: the artifact's real venue is a folding
  table at a community meeting.
- **ES parity is a gate, not a follow-up.** Note the known Alpine `x-text` i18n gap (TODO P1) —
  static strings swap, Alpine-bound strings do not. Build this page's dynamic strings through the
  swap layer or keep them out of Alpine bindings.

---

## 6. Why this fits in a week

| Component | Status |
|---|---|
| USDA food access data | ✅ loaded, 6,438 rows, drift-checked |
| Tract geometry + ACS attributes | ✅ `tracts.geojson`, 1,542 features, 41 in Rock County, already carries poverty/cost-burden/uninsured/renter |
| Policy positions | ✅ `policies` table + CSV of record |
| Population-weighted rollup, CI, coverage withholding | ✅ ADR-015, shipped |
| Leaflet map, choropleth, charts | ✅ `choropleth.js`, `charts.js`, vendored Leaflet |
| Design system, motion, theme + language toggles | ✅ vendored; do not edit `tokens.css` |
| Bilingual scaffolding | ✅ `lang-toggle.js` on 10/10 pages |
| **New: `/v1/policy/briefs` handler** | 🔨 ~1 day — assembles existing queries |
| **New: `policy.html` + view module** | 🔨 ~2 days |
| **New: plain-language + ES copy, print CSS** | 🔨 ~1 day |
| **New: provenance block** | 🔨 ~0.5 day |

Zero new ingest. Zero blocked dependencies — FBI NIBRS, FCC BDC, and IPUMS are all irrelevant to
this build, so the operator's Monday 2pm unblock session is not on the critical path.

---

## 7. The week

| Day | Work | Done means |
|---|---|---|
| **Mon** | Verify AB1221's actual text and eligibility language (do not design around a CSV summary). Confirm which municipalities the 12 tracts fall in against 2020 geometry. Freeze copy deck EN. Reconcile the 70-vs-85 policy count between the CSV and STATUS.md:142. | Bill text in hand; tract→place mapping written down; count discrepancy explained |
| **Tue** | `GET /v1/policy/briefs/:policy_id?geoid=` — payload, MOE passthrough, provenance block, table tests. Rollups routed through the run layer. | Endpoint returns Rock County; `go test -short` green |
| **Wed** | `policy.html`: map + eligibility layer, tract table, statewide context. Reuse the county page's layer structure. | Page renders live data at 1280/768/375, both themes |
| **Thu** | Plain-language pass, "what we cannot tell you" section, print CSS, ES parity, a11y (contrast, focus order, keyboard path to the map). | ES page complete; print preview clean; contrast ≥4.5:1 measured |
| **Fri** | Rebuild binary (embedded frontend — see §8), deploy, verify on prod, smoke test, screenshots, CHANGELOG + STATUS. | Live and verified; artifacts attached |
| **Buffer** | Sat/Sun untouched. If Thursday slips, ES parity and print survive; the statewide-context chart is the first cut. | — |

---

## 8. How this gets verified (the part that usually gets faked)

Every item below exists because this repo has been burned by its absence.

1. **Rebuild the binary.** The frontend is `//go:embed`-ed at compile time. A running `pdi serve`
   serves the HTML/JS from whenever it was built — frontend edits will look like no-ops. Restart
   against a fresh build before believing anything.
2. **Verify this page, not the shared layer.** Page-scoped `<style>` overrides `styles.css`.
   Grepping the shared layer and generalizing has produced inverted conclusions here. Measure
   `/policy` itself, in a browser.
3. **Verify by loading the page.** A green test suite, a 200 from the API, and correct SQL have
   all coexisted with a page that rendered nothing. Load it and look.
4. **Screenshots at scroll 0, tall viewport.** Browser-pane JS probes run with rAF dead, so
   motion reads as stuck at opacity 0; scrolled states paint blank.
5. **No self-certification.** "✓ COMPLETE / Real Data" from a builder is not evidence — this
   repo has shipped fabricated spike numbers to a grant-facing page. Every number on the brief
   must be traceable to a query someone else can re-run.
6. **A known-bad control on every check.** If the page renders the same with the data source
   pointed at nothing, the check proved nothing.

---

## 9. Acceptance criteria

- [ ] `/policy?id=FH-TRANSIT-008&geoid=55105` returns 200 on prod; `.html` variant 404s (route form is correct)
- [ ] The map shades 41 Rock County tracts; legend states the crosswalked-share wording verbatim
- [ ] Every displayed figure carries source, vintage, and method; ACS figures carry MOE
- [ ] The words "food desert" never appear as an unqualified statement about a 2020 tract
- [ ] "What we cannot tell you" is present, above the fold on print
- [ ] EN and ES pages have identical figures and no untranslated dynamic strings
- [ ] 0 JS errors, 0 horizontal overflow at 375/768/1280 × both themes
- [ ] Contrast ≥4.5:1 on every text/indicator pair, measured in-browser
- [ ] `go build` ✅ `go vet` ✅ `go test -short` ✅ · smoke test green against live
- [ ] One person who lives in Rock County and does not work in policy reads it and says what it says

The last one is the only criterion that actually matters. The rest are how we avoid embarrassing
ourselves in front of it.

---

## 10. Explicitly out of scope

Address geocoding · store siting or simulation · any forecast of the policy's effect · other 69
positions · other counties (Rusk is week two) · new data sources · authentication or accounts ·
mobile app · the tract-count mismatch (see §11) · regenerating the full evidence-card set.

---

## 11. Risks

| Risk | Severity | Mitigation |
|---|---|---|
| **Vintage collision.** Designation is 2010-tract; everything else is 2020-tract; the crosswalk yields a share, not a flag. | **High** — it is the core claim | §4 wording rule, enforced as an acceptance criterion. If the wording cannot be made honest, the map layer is cut before the page ships wrong. |
| **Tract-count mismatch.** geojson 1,542 (correct) vs PostGIS 1,669 (stale) vs LISA 1,524. A tract map walks straight into this. | **High** | Drive the map from `tracts.geojson` + `indicators`, not PostGIS. Assert the 41-feature Rock County count in a test. Do not attempt the P0 fix inside this week. |
| **AB1221 text unverified.** Designed off a CSV summary; a bill may define eligibility differently than USDA does. | **Medium** | Monday task 1. If the bill's test differs, the page presents USDA's test *as USDA's*, not as the bill's. |
| **PLACES vintage divergence** prod (`2022`) vs local (`CDC-PLACES-2023`). | Low here | This brief does not depend on PLACES. Do not let it creep in. |
| **ES parity slips** on the Alpine `x-text` gap. | Medium | Keep dynamic strings out of Alpine bindings on this page; verify by loading `/policy` with ES active, not by grepping the toggle. |
| **Deploy severs traffic.** | Low, now | Fixed 2026-08-08 — `systemctl stop` drains up to 45s. Expect the stop to block; that is correct behavior. |
| **The community is a subject, not a partner.** | **High, non-technical** | Do not ship a brief about Beloit without someone from Beloit reading it first. This is a gate on the launch, not on the build. |

---

## 12. For the operator

1. **Pilot community** — Rock County (recommended), or Rusk if you want the rural standard first.
   Do you have a partner org in either?
2. **Ship or hold?** Publish at `/policy` on the live site this week, or keep it unlisted until a
   community partner has read it?
3. **Policy count** — CSV has 70 positions, STATUS.md:142 says 85 seeded. Which is canonical?

---

### Provenance

Grounded 2026-08-09 against `main` in `policy-data-infra/`: `data/policies/francesca_hong_2026.csv`
(70 positions), `cmd/pdi/frontend/evidence_cards.json` (70 cards), `analysis/output/wi_food_access.csv`
(1,392 tracts, 2010 vintage — county totals above are computed from this file's own population
column and are indicative pending re-derivation from the DB's crosswalked 2020-tract rows),
`analysis/output/wi_counties_acs.csv`, `cmd/pdi/frontend/tracts.geojson` (1,542 features, 2020),
`ingest/fetch_usda_food.py` (flag→share rule), `pkg/datasource/*.go` (variable vocabulary),
`STATUS.md`, `TODO.md`, ADR-014/015.
