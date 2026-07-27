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

| Concern | Choice | Source |
|---|---|---|
| Design tokens | Night Shift v4 | `design-systems/TresPies/web-tokens.css` |
| Surfaces | Near-black canvas (#0a0a0f), cards (#111118), raised (#16161f) | Context A |
| Typography | Inter 400-900 + JetBrains Mono | `design-systems/TresPies/typography.md` |
| Accent | Amber (#fbbf24) | `--accent` |
| Signature gradient | amber→pink→purple (#fbbf24 → #ec4899 → #8b5cf6) | `--grad` |
| Data viz categorical | Rainbow 8-color scale | `design-systems/TresPies/rainbow-tokens.css` |
| CSS approach | Inline `<style>` per page with shared token `<link>` | TresPies house pattern |
| Framework | Alpine.js 3.x (CDN, no build step) | ADR-001 |
| Delivery | Embedded via `//go:embed frontend/*` in Go binary | ADR-001 |
| Mapping | Leaflet 1.9 (CDN, ~40KB) | ADR-004 Wave 4E |
| Design SoT | Night Shift directly from `design-systems/TresPies/` until `trespies-stacks` ships | |

### Page Architecture — 8 Pages

Each page is a distinct HTML file in `frontend/`, sharing a common token import and navigation shell. All pages serve a specific audience task and pull from live API endpoints.

```
frontend/
  tokens.css              ← Night Shift v4 token import (shared)
  nav.js                  ← Hash-router + navigation shell (shared)
  index.html              ← #/        County Explorer
  county.html             ← #/county/:geoid  County Profile + Dashboard
  compare.html            ← #/compare  Compare Tool
  evidence.html           ← #/evidence  Evidence Card Gallery
  candidates.html         ← #/candidates  Candidate Policy Tracker
  map.html                ← #/map       LISA Cluster Map
  narrative.html          ← #/narrative/:id  Narrative Reader
  about.html              ← #/about     Methodology + Sources
  composites.html         ← #/composite  Composite Index Builder
  chat.js                 ← Chat drawer (available from all pages)
```

### Page 1: County Explorer (`index.html`)

**Audience task:** "Show me which Wisconsin counties have the data I need."

- Fetches `GET /geographies?level=county&state_fips=55&limit=72`
- 72 county cards in a responsive grid (6→4→2→1 columns)
- Each card shows: county name, population, top-line indicator spark (poverty rate or median income)
- Search bar filters by name
- Click → navigates to `#/county/:geoid`
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

- Accessed via "Generate Brief" button from County Profile, or directly via `#/narrative/:analysis_id`
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
| **Total** | | **~26h** |

## Consequences

### Positive

- **Full platform surface.** Every backend capability is accessible from a browser — no API docs required. Grant reviewers, policy analysts, and community stakeholders can explore the data directly.
- **Audience-aligned pages.** Each page serves one clear task. A county commissioner comparing their county to the state average goes to Compare. A policy researcher looking for evidence-backed positions goes to Evidence. A data journalist looking for spatial patterns goes to Map.
- **Coherent brand.** Night Shift v4 across every page. PDI reads as a TresPies product.
- **Extensible.** Adding a new page means one new HTML file + one nav link. The hash router handles it. No framework migration.
- **Shared infrastructure.** Tokens and navigation are written once, shared everywhere. A design change to the accent color updates one file.

### Negative

- **More files to maintain.** 13 files vs. 1 file in the current shell. Mitigated by clear file ownership and shared token/nav dependencies.
- **Alpine.js ceiling for complex interactions.** The composite builder (weight sliders), compare table (delta sorting), and map (cluster toggles) push Alpine.js to its practical limit. Acceptable for v2; revisit Svelte if user feedback demands richer interactivity — same escape hatch as ADR-004.
- **Leaflet dependency.** Adds ~40KB to the map page. Acceptable for a spatial data platform. Lazy-loaded only on `map.html`.
- **`trespies-stacks` not yet stabilized.** Tokens come directly from `design-systems/TresPies/` as interim SoT. Update paths when `trespies-stacks` ships.

## Supersedes

- **ADR-004** Wave 2 (Data Explorer Frontend) — this ADR replaces the original 4-page plan with an 8-page architecture that surfaces every statistical layer, spatial analysis, and the grounded chat system. The Alpine.js + embedded delivery decisions from ADR-001 and ADR-004 still hold.
- **ADR-001** — reaffirmed: embedded Alpine.js frontend in Go binary. This ADR extends the file structure from a single page to a multi-page architecture with shared nav and tokens.

## Related

- ADR-001: Embedded frontend decision
- ADR-002: Policy data in Postgres
- ADR-004: v1 launch plan (superseded frontend wave)
- ADR-006: Grounded chat architecture
- `research/05_refactor_plan.md`: Five-layer statistical architecture
- `research/07_live_platform_plan.md`: Live platform vision
- `design-systems/TresPies/web-tokens.css`: Night Shift v4 canonical tokens
- `design-systems/TresPies/rainbow-tokens.css`: Categorical data viz scale
- `design-systems/TresPies/colors.md`: Complete palette documentation
- `design-systems/TresPies/typography.md`: Font stacks and scale
