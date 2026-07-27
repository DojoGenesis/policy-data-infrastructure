# ADR-007: Frontend Architecture — Full Feature Surface & TresPies Stack

**Status:** Proposed
**Date:** 2026-07-27
**Deciders:** Cruz Morales, Hermes Agent

## Context

PDI was designed as a full-stack open-source policy data platform — ingest, compute, API, narrative, and visualization in one deployable artifact. The original vision (ADR-004, April 2026) described a multi-page data explorer: county browser, indicator dashboard, compare tool, evidence cards, candidate policy tracker, and map visualization. The README positioned PDI against an 11-platform landscape as the only full-stack open-source option.

Between April and July 2026, the backend grew substantially — 13 datasource adapters, 42 variables, ICE/dissimilarity validated features, factor analysis, LISA spatial statistics, query-time composites, and a grounded chat system. But the frontend never advanced beyond ADR-004's initial sketch. The current `frontend/index.html` is a functional shell: 4 views (explorer, profile, compare, evidence), navy/cream light theme, no map, no chat, no composites, no LISA, no narrative generation, no candidate tracker.

The rebuild (Waves 1-4, July 27) completed the backend. This ADR defines the frontend that surfaces everything the platform can do — and aligns it with the TresPies Night Shift v4 design language.

## Decision: Multi-Page Data Platform with TresPies Night Shift Design

Build the frontend as a set of distinct, purpose-built HTML pages — not a collapsed single-page shell — each serving a specific audience task. All pages share the Night Shift v4 design system and are embedded in the Go binary via `//go:embed frontend/*`. Navigation is hash-routed through Alpine.js, per ADR-001.

### Design Stack

PDI adopts the **`casa-datos`** stack from [`trespies-stacks/`](../../trespies-stacks/) — the build-stack palette's data-viz template, purpose-built for indicator dashboards, public maps, and data explorers. The stack provides token discipline, chart/map primitives, i18n scaffolding, and a verification gate suite — all with zero build step, zero package manager, and zero external dependencies.

| Concern | Choice | Source |
|---|---|---|
| Design tokens | Night Shift v4 + Rainbow categorical | `tokens.css` (vendored from `spine/kits/tokens/`) |
| Surfaces | Near-black canvas (#0a0a0f), cards (#111118), raised (#16161f) | Context A (Night Shift) |
| Typography | System font stack: `-apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif` | Stack constraint — no webfonts |
| Accent | Amber (#fbbf24) | `--accent` |
| Signature gradient | amber→pink→purple (#fbbf24 → #ec4899 → #8b5cf6) | `--grad` |
| Data viz categorical | Rainbow 8-color scale (`--tp-cat-1` through `--tp-cat-8`) | Context D (Rainbow) |
| Chart rendering | `charts.js` — hand-rolled SVG bar/dot/line primitives, zero dependencies | `casa-datos/charts.js` |
| Map rendering | `choropleth.js` — hand-rolled SVG choropleth, token-driven palette | `casa-datos/choropleth.js` |
| CSS approach | `tokens.css` (vendored verbatim) + stack-owned `styles.css` per page | TresPies stack pattern |
| Framework | None. Alpine.js is NOT vendored here — the stack's own JS handles reactivity through DOM manipulation | Stack constraint |
| Delivery | Embedded via `//go:embed frontend/*` in Go binary | ADR-001 |
| Routing | Multi-page: each view is an independent HTML file. Navigation is native `<a href>` between pages, not a hash-routed SPA | Stack pattern |
| i18n | EN/ES twin pages with reciprocal `hreflang`, flipper from `spine/kits/i18n/` | Stack constraint |
| Accessibility | WCAG AA including computed Rainbow rules. JS-off: every page is complete and legible. Reduced motion honored. | Stack gate suite |

### Stack Constraint Relaxations for PDI

The `trespies-stacks` constraint "no external anything" was written for static sites. PDI is an API-driven data platform — it MUST call its own backend. Two relaxations:

1. **Same-origin API calls are NOT "external."** `fetch('/v1/policy/geographies')` hits the Go binary that served the page. This is the same process, not a third party. The `no-external` gate is updated to allow `fetch()` to same-origin paths under `/v1/`.
2. **Alpine.js is a framework, not external content.** The stack's own `charts.js` and `parametric.js` already use the IIFE + `window` namespace pattern. Alpine.js serves the same role — a JS enhancement that makes the page interactive, not a remote dependency that could break the page. It is vendored as a single file (`alpine.js`) alongside the other vendored kits, loaded from the same embedded filesystem. No CDN.

Everything else — no webfonts, no remote images, no third-party scripts, system font stack, JS-off legibility, WCAG AA, Spanish twins — holds without modification.

### File Structure (aligned with casa-datos)

```
frontend/
  tokens.css              ← vendored from spine/kits/tokens/ (@spine-kit tag)
  theme-toggle.js         ← vendored from spine/kits/tokens/
  motion.css              ← vendored from spine/kits/motion/
  motion.js               ← vendored from spine/kits/motion/
  parametric.js           ← vendored from spine/kits/parametric/
  bilingual-data.js       ← vendored from spine/kits/i18n/
  alpine.js               ← vendored (single file, no CDN)
  charts.js               ← vendored from casa-datos/charts.js
  choropleth.js            ← vendored from casa-datos/choropleth.js
  styles.css              ← stack-owned: PDI layout over tokens

  index.html              ← County Explorer
  county.html             ← County Profile + Dashboard
  compare.html            ← Compare Tool
  evidence.html           ← Evidence Card Gallery
  candidates.html         ← Candidate Policy Tracker
  map.html                ← LISA Cluster Map
  narrative.html          ← Narrative Reader
  about.html              ← About + Methodology
  composites.html         ← Composite Index Builder
  chat.html               ← Grounded Chat (loadable drawer from any page)

  es/                     ← Spanish twins (every .html above)
  site-manifest.json      ← agent-nav: structured data for LLM consumption
  llms.txt                ← agent-nav: plain-text guide for LLMs
```

### Page Architecture — 8 Pages

Each page is a complete, self-contained HTML document — not a fragment within a single-page shell. Navigation between pages uses native `<a href>` links. Within each page, Alpine.js handles interactivity (search, filter, sort, toggle) scoped to that page's data. This matches the `casa-datos` pattern: independent pages, vendored scripts, no build step.

Pages are served from the Go binary at their natural paths: `GET /` → `index.html`, `GET /county` → `county.html`, etc. The server maps clean URLs to files. No hash routing.

### Page 1: County Explorer (`index.html`)

**Audience task:** "Show me which Wisconsin counties have the data I need."

- Fetches `GET /geographies?level=county&state_fips=55&limit=72`
- 72 county cards in a responsive grid (6→4→2→1 columns)
- Each card shows: county name, population, top-line indicator spark (poverty rate or median income)
- Search bar filters by name
- Click → navigates to `/county?geoid=:geoid`
- States: loading skeleton (6 ghost cards), empty (no match), error (retry)

### Page 2: County Profile + Dashboard (`county.html`)

**Audience task:** "What does this county look like across every dimension we track?"

This is the richest page — it surfaces all five statistical layers.

**Header section:**
- County name, GEOID, population, land area
- Quick stats row: ICE score, poverty rate, median income, uninsured rate
- "Generate Brief" button → `POST /generate/narrative` → opens Narrative Reader

**Indicator dashboard (Layer 1):**
- Fetches `GET /geographies/:geoid/indicators`
- All 42 variables grouped by category (demographic, health, housing, environment, economic, education, food, transit)
- Each indicator row: name, value with unit, percentile bar relative to state, reliability badge (high/moderate/low — green/amber/red)
- CV shown on hover for ACS-derived indicators

**Validated features card (Layer 2):**
- ICE score with percentile rank and interpretation text
- County-level dissimilarity index (Black-white, Hispanic-white) if available
- CV/reliability summary

**Factor profile card (Layer 3):**
- Fetches `GET /geographies/:geoid/factors`
- Factor scores displayed as a radar or horizontal bar chart
- Factor names with loading descriptions

**Spatial cluster badge (Layer 4):**
- LISA cluster classification for key indicators (poverty, income, cost burden)
- HH/HL/LL/LH badge with color coding

**Composite widget (Layer 5):**
- "Build Custom Index" expandable panel
- Variable picker with weight sliders
- Calls `POST /composite`
- Shows score + sensitivity analysis (ranking stability under ±20% weight perturbation)
- Results displayed inline with perturbation scenario comparison

**Children section:**
- Fetches `GET /geographies/:geoid/children` (tracts within county)
- Tract list linking to tract profile (future: tract-level page)

### Page 3: Compare Tool (`compare.html`)

**Audience task:** "How do these two counties differ across every indicator?"

- Two county selectors (autocomplete dropdown from `/geographies`)
- Calls `POST /compare` with both GEOIDs
- Side-by-side indicator table: variable name, County A value, County B value, absolute delta, percent delta
- Delta highlighting: green (A better), red (B better), gray (neutral)
- Filter by category tabs
- Sort by delta magnitude
- States: county selectors empty until search, loading spinner during fetch, error per county

### Page 4: Evidence Card Gallery (`evidence.html`)

**Audience task:** "What policy positions are backed by data for these equity dimensions?"

- Fetches `/static/evidence_cards.json` (70 cards)
- Filterable gallery: by category (housing, health, education, etc.), by equity dimension, by candidate
- Card layout: title, policy position summary, linked indicators with values, evidence sources
- Search by keyword
- Click a card → expands inline with full detail (indicator values, methodology, citations)
- States: loading skeleton (card grid), empty (no filters match), error

### Page 5: Candidate Policy Tracker (`candidates.html`)

**Audience task:** "Which candidates have policy positions linked to measurable outcomes?"

- Fetches `GET /policies` (from policies table, ADR-002)
- Candidate selector tabs (Francesca Hong, + future candidates)
- Policy positions grouped by equity dimension
- Each position shows: title, description, linked indicators, evidence card count
- Visual: indicator match bar — how many of the dimension's indicators have data for the candidate's stated geography
- States: loading, empty (no candidates seeded), error

### Page 6: LISA Cluster Map (`map.html`)

**Audience task:** "Where are the spatial clusters of disadvantage and advantage?"

- Leaflet map centered on Wisconsin, 72 counties + 1,542 tracts
- Fetches tract GeoJSON from `/static/tracts.geojson` (or API endpoint)
- Fetches LISA analysis scores from `GET /analyses/:id/scores`
- Variable selector dropdown (poverty, income, cost burden, uninsured, POC%)
- Choropleth with diverging color scale:
  - HH (High-High): deep amber/orange — concentrated disadvantage
  - LL (Low-Low): teal — concentrated advantage
  - HL (High-Low): purple — outlier (worse than neighbors)
  - LH (Low-High): amber — outlier (better than neighbors)
  - NS (Not Significant): muted gray
- Click a tract → popup with tract name, indicator value, cluster classification, ICE score
- County overlay toggle (boundaries with labels)
- "Download as PNG" button for reports
- States: loading (gray map placeholder), empty (no LISA data computed), error

### Page 7: Narrative Reader (`narrative.html`)

**Audience task:** "Read a data-driven narrative about structural conditions in this county."

- Accessed via "Generate Brief" button from County Profile, or directly via `/narrative?analysis_id=:analysis_id`
- Fetches `GET /generate/narrative/:analysis_id` (or renders from POST response)
- Full HTML narrative with chapter navigation
- Each chapter: tract name, indicator profile, stat callouts, policy lever recommendations with evidence citations
- "Download PDF" (opens print dialog with print stylesheet)
- "Share link" (copy hash URL)
- States: loading (prose skeleton), error (generation failed), empty (no analysis available)

### Page 8: About + Methodology (`about.html`)

**Audience task:** "Where does this data come from and how is it analyzed?"

- Platform overview (from README)
- Five-layer architecture diagram (visual, not just text)
- Data sources table with vintage, frequency, geographic level (from `GET /sources`)
- Variable catalog (from `GET /variables`) — searchable, grouped by source
- Statistical methodology: ICE (Krieger 2016), Dissimilarity (Massey & Denton 1988), LISA (Anselin 1995), Factor Analysis (Kolak 2020)
- Research references (30 sources from `research/references.csv`)
- Grant acknowledgments (Arnold Ventures, MCF)
- License (Apache-2.0)

### Grounded Chat Drawer (available from all pages)

**Audience task:** "Ask a question about the data and get a verified answer."

- Triggered by a floating chat button (bottom-right) or nav link
- Slides in from right (400px desktop, full-screen mobile)
- Calls `POST /v1/chat` — model plans, Go executes, Go verifies (ADR-006)
- Displays verified answer with source indicator values
- Conversation history within session
- States: initial (example questions), thinking (typing indicator), answered (verified), refused (unanswerable — "I can't answer that from the data I have. Try asking about poverty rates, income, or health outcomes.")

### Cross-Cutting Concerns

**Navigation shell (`nav.js`):**
- Sticky top bar with Night Shift styling: PDI amber logo mark, nav links, chat toggle
- Hash-based routing: parses `window.location.hash`, loads the correct page
- Active page indicator
- Shared across all pages via `//go:embed`

**Token system (`tokens.css`):**
- Night Shift v4 custom properties
- Rainbow categorical scale for data viz
- Imported once, referenced everywhere
- No Tailwind dependency — pure CSS custom properties eliminate the CDN config script

**Responsive breakpoints:**
- Mobile (< 768px): single column, stacked cards, full-width tables with horizontal scroll, chat overlay
- Tablet (768-1024px): 2-3 column grids, side-by-side compare
- Desktop (> 1024px): full multi-column, chat drawer

**Accessibility:**
- All text meets WCAG AA contrast ratios (Night Shift tokens pass: 15.3:1 body)
- Focus indicators on all interactive elements
- Screen reader labels on charts and maps
- Keyboard-navigable county grid and evidence cards

## File Ownership (avoid conflicts)

| File | Owned By | Depends On |
|---|---|---|
| `tokens.css` | Design agent (shared) | Nothing |
| `nav.js` | Shell agent (shared) | `tokens.css` |
| `index.html` | Explorer agent | `tokens.css`, `nav.js` |
| `county.html` | Profile agent | `tokens.css`, `nav.js` |
| `compare.html` | Compare agent | `tokens.css`, `nav.js` |
| `evidence.html` | Evidence agent | `tokens.css`, `nav.js` |
| `candidates.html` | Candidates agent | `tokens.css`, `nav.js` |
| `map.html` | Map agent | `tokens.css`, `nav.js`, Leaflet CDN |
| `narrative.html` | Narrative agent | `tokens.css`, `nav.js` |
| `about.html` | About agent | `tokens.css`, `nav.js` |
| `composites.html` | Composites agent | `tokens.css`, `nav.js` |
| `chat.js` | Chat agent | Nothing |

## Build & Deploy

All files live in `frontend/`. The Go binary embeds them via:

```go
//go:embed all:frontend
var frontendFS embed.FS
```

Served from `GET /` (index.html) and `GET /static/*` (all other assets). Deploy to VPS via cross-compiled binary + systemd restart.

## Effort Estimate

| Track | Deliverable | Effort |
|---|---|---|
| 7A | `tokens.css` + `nav.js` — shared design system + navigation shell | 2h |
| 7B | `index.html` — County Explorer | 2h |
| 7C | `county.html` — County Profile + full dashboard | 4h |
| 7D | `compare.html` — Compare Tool | 2h |
| 7E | `evidence.html` — Evidence Card Gallery | 2h |
| 7F | `candidates.html` — Candidate Policy Tracker | 2h |
| 7G | `map.html` — LISA Cluster Map (Leaflet) | 3h |
| 7H | `narrative.html` — Narrative Reader | 2h |
| 7I | `about.html` — About + Methodology | 2h |
| 7J | `composites.html` — Composite Builder | 2h |
| 7K | `chat.js` — Grounded Chat Drawer | 2h |
| 7L | Embed + build + deploy + verify | 1h |
| **Total** | | **~30h** |

## Chat Revamp: Intelligent Grounded Conversation

ADR-006 established the three-stage pipeline (Plan → Execute → Verify) and proved it works against the Atlas static bundle. This revamp extends the chat from a proof-of-concept into an intelligent assistant that works across the full platform, not just a static snapshot.

### Current Limitations (ADR-006 as built)

| Limitation | Impact |
|---|---|
| Queries only the Atlas static bundle | Stale data — new ACS vintages, LISA results, and composite scores are invisible |
| Single-turn only | Can't ask "what about the neighboring county?" after a lookup |
| No page context | Chat doesn't know you're looking at Dane County's profile |
| Plain text responses only | Tables, charts, and stat callouts would make answers more useful |
| No action triggers | Can't say "generate a narrative" or "build a composite" from chat |
| No Spanish support | Bilingual capability exists in the data but not in the chat |
| Refusals are generic | "I can't answer that" doesn't guide the user to what they CAN ask |

### Revamp: 9 Capabilities

#### 1. Live API Backend

Replace the static Atlas bundle with live PostGIS queries through the existing API endpoints. The grounding pipeline stays — Plan → Execute → Verify — but Execute now hits `GET /geographies/:geoid/indicators`, `POST /compare`, `GET /analyses/:id/scores`, etc. instead of a static JSON file.

A question like "what's the poverty rate in Dane County?" becomes: Plan = `{op: lookup, indicator: poverty_rate, place: Dane County}` → Execute = `GET /geographies/55025/indicators?variable_id=poverty_rate` → Verify = check response value against model's prose.

This means new data (ACS 2024, LISA results, composite scores) is available immediately without re-bundling.

#### 2. Multi-Turn Conversation

Maintain a conversation history within the chat session. The model receives the last N exchanges as context, allowing:

- Follow-ups: "What about Milwaukee?" → resolves "Milwaukee" from context (the previous answer was about Dane County)
- Refinement: "Show me just the health indicators" → adds filter to previous query
- Drill-down: "Which tracts in that county are worst?" → switches from county to tract level with rank operation

No server-side session storage needed — the conversation lives in the browser and is sent with each request. The grounding pipeline still verifies every number independently per turn.

#### 3. Page Context Awareness

When the chat opens, it receives the current page context:

- On County Profile: `{page: "county", geoid: "55025", name: "Dane County"}`
- On Compare: `{page: "compare", geoid1: "55025", geoid2: "55079"}`
- On Map: `{page: "map", indicator: "poverty_rate"}`
- On Explorer: `{page: "explorer", state: "WI"}`

The model uses this to:
- Default geography to the current page ("how does this compare to the state average?" → knows which county)
- Suggest relevant questions in the empty state
- Provide more specific refusals ("I don't have tract-level data for that indicator, but I can show you the county values")

#### 4. Rich Responses

Beyond plain text, the chat can render:

- **Inline stat callouts:** Big number + label + direction arrow, matching the County Profile stat cards
- **Mini bar charts:** 5-bar horizontal comparison (e.g., "top 5 counties by poverty rate")
- **Comparison tables:** Side-by-side for two geographies
- **Reliability badges:** Green/amber/red indicator next to values with low CV
- **Trend sparklines:** When asking about vintage changes (future)

These use the same Night Shift design tokens as the rest of the platform — the chat doesn't have its own visual language.

#### 5. Action Triggers

The model can suggest — and the chat UI can render — clickable actions:

| User says | Chat responds with |
|---|---|
| "Generate a narrative for Dane County" | Button: "Open Five Mornings Narrative" → navigates to `/narrative?analysis_id=:analysis_id` |
| "Build a composite index from poverty and income" | Button: "Open Composite Builder" → navigates to `/composite?vars=poverty_rate,median_household_income` |
| "Compare Dane and Milwaukee" | Button: "Open Compare Tool" → navigates to `/compare?geoid1=55025&geoid2=55079` |
| "Show me on a map" | Button: "Open LISA Map" → navigates to `/map?indicator=poverty_rate` |

Actions are suggested by the model in the Compose stage, rendered as buttons in the chat UI, and trigger native page navigation. The grounding pipeline verifies that the action parameters match the data context.

#### 6. Expanded Operation Schema

Extend the 6 current operations (lookup, rank, compare, aggregate, threshold, representation) with:

| Operation | Example | API call |
|---|---|---|
| `time_series` | "How has poverty changed since 2019?" | `GET /indicators?geoid=X&variable_id=Y&vintage=2019,2024` |
| `distribution` | "What's the range of median incomes across WI tracts?" | `POST /query` with all tracts |
| `correlation` | "Is poverty correlated with cost burden?" | `POST /composite` or compute client-side |
| `explain` | "Why is this tract's poverty rate so high?" | Prose retrieval from methodology docs + factor scores + LISA cluster |
| `what_if` | "What would the composite look like if I doubled the weight on income?" | `POST /composite` with custom weights |

The `explain` operation is the key addition — it combines the numeric answer with definitional prose from the documentation corpus (ADR-006's "prose path"), factor profile interpretation, and LISA cluster context. "Why is tract 55025001700's poverty rate 35%?" → "This tract sits at the 92nd percentile for poverty statewide. It is classified as a High-High LISA cluster (concentrated disadvantage), meaning neighboring tracts also have elevated poverty. Its factor profile shows high loadings on Economic Distress and Housing Burden. [citation: ACS 2020-2024 5-Year, ICE score, LISA analysis]"

#### 7. Methodology Transparency

Every numeric answer includes a disclosure path:

- **Source:** Which API endpoint produced this number
- **Vintage:** ACS 2020-2024 5-Year (released Dec 2025)
- **Reliability:** CV-based flag (high/moderate/low) for ACS-derived indicators
- **Computation:** If derived (ICE, composite, percentile), show the formula on request
- **Limitation:** If the indicator suppresses data for small populations, surface that

The user can ask "how did you get that number?" and receive the full chain: indicator → API endpoint → source table → vintage → reliability → computation method.

#### 8. Spanish Language Parity

The Plan and Compose stages work in Spanish with the same verification pipeline. The data already carries Spanish labels (`labelEs`, `unitEs`, `descriptionEs`) in the Atlas bundle. The model receives bilingual system prompts and can switch languages mid-conversation.

Spanish queries use the same Intent schema — the indicator IDs are language-neutral. The verify stage works identically (numbers have no language). The Compose stage generates Spanish prose from Spanish-labeled data.

#### 9. Suggested Questions

The empty chat state shows context-aware prompts instead of a blank input:

- County Explorer: "Which county has the highest poverty rate?" "Show me counties above the state median income"
- County Profile (Dane County): "How does Dane County compare to the state average?" "What are the biggest disparities within Dane County?" "Generate a Five Mornings narrative"
- Compare (Dane vs. Milwaukee): "Which county has better health outcomes?" "Show me the biggest differences between these counties"
- Map: "Where are the LISA High-High clusters?" "Which tracts are outliers?"
- Evidence: "Which policies address housing affordability?" "Show me evidence for education equity policies"

Suggestions change with the page context. Clicking one sends it as a message. The chat learns — after a user asks a novel question, that question type can appear in suggestions for other users (future: analytics-driven suggestion ranking).

### Chat Architecture Diagram

```
┌──────────────────────────────────────────────────┐
│  USER MESSAGE                                     │
│  "What's the poverty rate in Dane County?"         │
└────────────────────┬─────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────┐
│  CONTEXT ASSEMBLY                                 │
│  + page context (county profile, geoid=55025)     │
│  + conversation history (last N exchanges)         │
│  + available operations + indicator catalog        │
└────────────────────┬─────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────┐
│  STAGE 1: PLAN (model)                            │
│  Intent: {op: lookup, indicator: poverty_rate,    │
│           place: "55025"}                          │
│  Validate against closed schema → pass/fail        │
└────────────────────┬─────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────┐
│  STAGE 2: EXECUTE (deterministic Go)               │
│  GET /v1/policy/geographies/55025/indicators       │
│       ?variable_id=poverty_rate                    │
│  Result: {value: 10.3, vintage: ACS-2023-5yr,     │
│           reliability: high, cv: 0.08}             │
└────────────────────┬─────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────┐
│  STAGE 3: COMPOSE + VERIFY (model + Go)            │
│  Model writes prose from result + Spanish labels   │
│  Go extracts every number → checks against result  │
│  Mismatch → refuse. Match → render.                │
│  Add action buttons if applicable.                 │
└────────────────────┬─────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────┐
│  RENDERED RESPONSE                                 │
│  "Dane County's poverty rate is 10.3%              │
│   (ACS 2023 5-Year, high reliability).             │
│   [Stat callout: 10.3% poverty rate]               │
│   [Button: Compare to state average ▸]             │
│   [Button: See on LISA map ▸]"                    │
└──────────────────────────────────────────────────┘
```

### Effort (added to frontend build)

| Track | Deliverable | Effort |
|---|---|---|
| 7M | Live API backend for Execute stage (replace Atlas bundle) | 2h |
| 7N | Multi-turn conversation + page context injection | 2h |
| 7O | Rich response rendering (stat callouts, mini charts, tables) | 2h |
| 7P | Action trigger system (nav + parameter passing) | 1h |
| 7Q | Expanded operations (time_series, distribution, correlation, explain, what_if) | 3h |
| 7R | Methodology transparency (source/vintage/reliability/computation chain) | 1h |
| 7S | Spanish language parity in Plan + Compose stages | 2h |
| 7T | Suggested questions engine (context-aware, page-specific) | 1h |
| **Chat subtotal** | | **14h** |

### Chat Consequences

- **Positive:** The chat becomes the platform's intelligent interface — not a toy bolted on, but the primary way many users will interact with the data. Every page gains an assistant that knows where you are and what you can ask.
- **Positive:** Spanish parity makes the platform accessible to the communities the data describes. The bilingual labels already exist in the data; the chat is the last piece.
- **Positive:** Action triggers bridge chat to navigation — the chat doesn't just answer questions, it moves you through the platform. A user who starts in chat can end up on the LISA map, the Composite Builder, or the Narrative Reader without knowing those pages exist.
- **Negative:** The expanded operation schema (time_series, correlation, what_if) requires new API endpoints or client-side computation. Some operations may be deferred to a second chat wave.
- **Negative:** The model must now handle more context (page state, conversation history, expanded schema), which increases token usage. Mitigated by keeping the intent schema compact and the conversation window bounded (last 6 exchanges).

## Consequences

### Positive

- **Full platform surface.** Every backend capability is accessible from a browser — no API docs required. Grant reviewers, policy analysts, and community stakeholders can explore the data directly.
- **Audience-aligned pages.** Each page serves one clear task. A county commissioner comparing their county to the state average goes to Compare. A policy researcher looking for evidence-backed positions goes to Evidence. A data journalist looking for spatial patterns goes to Map.
- **Coherent brand.** Night Shift v4 across every page. PDI reads as a TresPies product.
- **Extensible.** Adding a new page means one new HTML file + one nav link. The hash router handles it. No framework migration.
- **Shared infrastructure.** Tokens and navigation are written once, shared everywhere. A design change to the accent color updates one file.

### Negative

- **More files to maintain.** 20+ files vs. 1 file in the current shell. Mitigated by clear file ownership, shared vendored kits (never edited in place), and the stack's verification gate suite (`verify.sh`).
- **Alpine.js ceiling for complex interactions.** The composite builder (weight sliders), compare table (delta sorting), and map (cluster toggles) push Alpine.js to its practical limit. Acceptable for v2; revisit if user feedback demands richer interactivity.
- **`casa-datos` constraint tension.** The stack's `charts.js` and `choropleth.js` are designed for static SVG rendering. PDI's live API data requires bridging the static-to-dynamic gap — fetching JSON, transforming into chart input, and re-rendering on navigation. This is manageable (the same pattern `tool.html` uses) but adds ~200 lines of glue JS per charted page.

## Supersedes

- **ADR-004** Wave 2 (Data Explorer Frontend) — this ADR replaces the original 4-page hash-routed SPA with an 8-page native-navigation architecture anchored in the `casa-datos` stack, surfacing every statistical layer, spatial analysis, and the grounded chat system. The embedded delivery decision from ADR-001 still holds; the Alpine.js SPA approach does not.
- **ADR-001** — partially superseded: embedded frontend in Go binary is reaffirmed. The single-page Alpine.js hash-routed architecture is replaced with multi-page native navigation following the `casa-datos` stack pattern.

## Related

- ADR-001: Embedded frontend decision (partially superseded)
- ADR-002: Policy data in Postgres
- ADR-004: v1 launch plan (superseded frontend wave)
- ADR-006: Grounded chat architecture
- `research/05_refactor_plan.md`: Five-layer statistical architecture
- `research/07_live_platform_plan.md`: Live platform vision
- `trespies-stacks/casa-datos/`: Canonical data-viz stack (tokens, charts, choropleth, gates)
- `trespies-stacks/spine/kits/tokens/`: Night Shift + Rainbow token system
- `trespies-stacks/spine/kits/i18n/`: EN/ES twin page scaffolding
- `trespies-stacks/spine/gates/`: Verification gate suite (no-external, a11y, js-off, i18n-parity, spine-drift, token-fidelity)
- `design-systems/TresPies/colors.md`: Canonical color contexts (A/B/C/D)
