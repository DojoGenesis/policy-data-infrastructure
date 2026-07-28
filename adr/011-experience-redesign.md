# ADR-011: PDI Experience Design — From Toolkit to Product

**Status:** Proposed
**Date:** 2026-07-28
**Deciders:** Cruz Morales, Hermes Agent
**Supersedes:** ADR-007 (frontend architecture), ADR-004 (v1 launch plan)

## Context — Honest Assessment

PDI's technical foundation is real and verified:

| Layer | Status |
|---|---|
| Data ingest (13 sources, 42 variables) | ✅ |
| Statistical engine (ICE, Dissimilarity, LISA, EFA, composites) | ✅ |
| API (20+ endpoints, all 200) | ✅ |
| Frontend (10 EN + 10 ES pages) | ✅ |
| Chat (grounded AI, page context) | ✅ |
| Map (Leaflet, 1,543 tracts colored with real LISA) | ✅ |
| Education (6 Manim stats videos) | ✅ |
| Build + test (12/12 clean) | ✅ |

But the frontend is a **2/10 experience**. The pages exist as disconnected tools — a county explorer, a profile page, a map, a chat — each built by a different agent at a different time with a different visual language. They share CSS classes but not a design intention. The experience is:

- **Arrive at Explorer** → see 72 county cards → click one → land on a data wall → leave
- **No journey.** No guidance. No emotional arc. No "aha moment."
- **Chat is isolated.** It knows which page you're on but doesn't feel integrated.
- **The map is a separate app.** It doesn't connect back to the profile or the chat.
- **The videos are files on disk.** They're not woven into the narrative.
- **ES pages are translations.** Not a bilingual experience.

The product exists. It does not feel like a product.

## Decision: Three-Phase Experience Redesign

### Phase 1 — Unify (atmosphere + IA)
Make the platform feel like ONE thing, not ten things glued together.

### Phase 2 — Guide (journeys + narratives)
Give users a reason to stay, explore, and return.

### Phase 3 — Deepen (integration + intelligence)
Make every piece amplify every other piece.

---

## Phase 1: Unify

### 1A — Visual Language Reset

**Problem:** 10 pages, 10 different visual languages despite sharing CSS classes. The County Profile has a Monitor-surface density. The Explorer has a Decide-surface hero. The Map has tool-like chrome. The Chat has message-bubble styling. They don't feel like siblings.

**Solution:** Define a single visual language with three page archetypes, not ten unique designs:

| Archetype | Examples | Pattern |
|---|---|---|
| **Browse** | Explorer, Evidence, Candidates | Card grid, search, filter — the "finding things" pattern |
| **Inspect** | County Profile, Narrative | Data-dense, stat-forward, layered reading — the "understanding one thing deeply" pattern |
| **Interact** | Map, Compare, Chat, Composites | Tool-like, direct manipulation, immediate feedback — the "doing things" pattern |

Every page inherits from one archetype. Each archetype has ONE layout template, not per-page deviations.

**Specific changes:**
- **Unified header** across all 10 pages — identical nav, identical height, identical behavior. Currently each page has subtle differences.
- **Unified footer** on every page — PDI attribution, data freshness timestamp, license link. Currently only some pages have footers.
- **Consistent card system** — Browse pages use `.card-grid`. Inspect pages use `.chart-card`. Interact pages use a tool-panel pattern. No page mixes all three.
- **Section rhythm rule** — every page section has identical vertical rhythm: section-gap + thread ribbon. No exceptions.
- **The accent color does ONE job**: it marks the primary action on every page. Currently amber is used for eyebrow text, badges, buttons, stat values, and chart fills — it means everything, so it means nothing.

### 1B — Information Architecture Reset

**Problem:** 10 pages accessed via a horizontal nav bar. No hierarchy, no relationship between pages. A user on the County Profile doesn't know the Map exists unless they scan the nav. The Chat is a separate page rather than a persistent assistant.

**Solution:** Restructure into a 3-section experience with persistent chat:

```
┌─────────────────────────────────────────────────────┐
│  HEADER: PDI logo · [Browse ▼] [Analyze ▼] [Learn]  │
├─────────────────────────────────────────────────────┤
│                                                     │
│  MAIN CONTENT AREA                                  │
│  (page content — explorer, profile, map, etc.)      │
│                                                     │
├─────────────────────────────────────────────────────┤
│  CHAT DRAWER (collapsible, persistent across pages) │
│  ┌─────────────────────────────────────────────┐    │
│  │ 💬 Ask about this county...          [Send] │    │
│  └─────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

**Browse** (finding things):
- County Explorer → `/`
- Evidence Cards → `/evidence`
- Candidate Tracker → `/candidates`

**Analyze** (understanding deeply):
- County Profile → `/county?geoid=X`
- Compare → `/compare`
- Composites → `/composite`

**Explore** (visual + interactive):
- LISA Map → `/map`
- Narrative Reader → `/narrative`
- Methodology Videos → embedded in About

**Persistent:**
- Chat Drawer → always accessible, slides up from bottom
- About → `/about` (footer link, not main nav)

### 1C — Spanish as a Mode, Not a Mirror

**Problem:** ES pages are literal translations. They duplicate every file, double the maintenance burden, and feel like an afterthought.

**Solution:** Language toggle in the header. One HTML file per page. All visible strings are data attributes (`data-en`, `data-es`). A small JS module swaps text on toggle. Same URL, same page, different language.

---

## Phase 2: Guide

### 2A — The Landing Experience

**Problem:** The current Explorer is a grid of 72 county cards. It answers "what counties exist" but not "why should I care."

**Solution:** The landing page tells a story before it shows data:

1. **Hero statement** — one sentence that declares what PDI does and why it matters. "Public data, privately held, is the quietest form of exclusion. We've made it public again."
2. **Three cards** — each describing a different way to use PDI: "I want to understand my county," "I want to compare places," "I want to see patterns on a map."
3. **Featured county** — one highlighted county (rotates) with the most interesting story. "Dane County has the highest median income in Wisconsin — but 10.5% of residents still live below the poverty line. The story is in the gap."
4. **County grid below** — the 72 cards, now contextualized by the story above.

### 2B — The County Profile as a Narrative

**Problem:** The current County Profile is a data wall — five sections of indicators stacked vertically. It's informationally complete but emotionally flat.

**Solution:** The County Profile tells a story in three acts:

**Act 1: The Portrait** (above the fold)
- County name, population, a single stat that defines this county
- "Dane County is Wisconsin's economic engine — but not everyone is along for the ride."
- Quick stats row: poverty, income, uninsured, POC%

**Act 2: The Layers** (scroll-driven narrative)
- Each statistical layer appears as you scroll, with a contextual sentence
- Layer 1: "Here's what the Census tells us about Dane County."
- Layer 2: "ICE score: 0.931. That means Dane County is among Wisconsin's most polarized — prosperity and poverty live close together."
- Layer 3: "Factor analysis reveals two underlying patterns: cardiovascular-metabolic health burden, and mental-health economic deprivation."
- Layer 4: "LISA clusters show where these patterns concentrate spatially. The map reveals High-High clusters in [X] tracts."
- Layer 5: "Build your own index to weigh what matters most to your community."

**Act 3: The Levers** (what to do about it)
- Evidence cards relevant to this county's highest-burden dimensions
- "Generate a Five Mornings narrative" button
- "Compare with a neighboring county" suggestion

### 2C — The Chat as Guide, Not Tool

**Problem:** The chat exists but doesn't guide. It answers questions but doesn't suggest them. It's reactive, not proactive.

**Solution:** The chat drawer is always present. It changes its tone and suggestions based on context:

- **On Explorer:** "Not sure where to start? Ask me: 'Which county has the biggest gap between rich and poor?' or 'Show me the healthiest and least healthy counties.'"
- **On County Profile:** "I can see you're looking at Dane County. Want me to compare it to the state average? Or explain what ICE 0.931 means?"
- **On Map:** "You're viewing poverty clusters. Tap any tract to see its story, or ask me: 'Why is this area a High-High cluster?'"
- **On Compare:** "Dane leads Milwaukee in 12 of 19 indicators. The biggest gap is in [X]. Want me to explain why?"

The chat becomes a companion — it notices where you are and offers to help, rather than waiting for you to figure out what to ask.

---

## Phase 3: Deepen

### 3A — Cross-Page Integration

Every page should surface connections to every other page:

- **County Profile** → "View on Map" chip for each LISA variable → "Compare with neighbors" → "See evidence cards for this county's dimensions"
- **Map** → clicking a tract opens a mini-profile panel → "View full profile" link
- **Evidence Cards** → "Which counties does this affect most?" link → auto-filters to affected counties
- **Chat** → can trigger navigation: "Show me on the map" → opens Map with the right indicator selected

### 3B — The Education Layer

The 6 Manim videos should be embedded contextually, not gated behind an About page:

- **Z-score** → appears when a user first sees standardized scores (Layer 1)
- **ICE** → appears when a user first encounters the ICE score (Layer 2)
- **Bootstrap CI** → appears when reliability is discussed (Layer 2)
- **Quantile Classification** → appears when category breaks are shown (Layer 1)
- **LISA** → appears when spatial clusters are shown (Layer 4)
- **Pipeline DAG** → appears on the About page as system architecture

Each video is a 60-second explainer that plays inline — not a link to a separate page.

### 3C — Bilingual as Design, Not Feature

Spanish should feel native, not translated:

- Language toggle is prominent, not buried
- All prose is written in both languages from the start — not translated after
- Data labels, units, and descriptions are bilingual at the database level (already partially done)
- The chat responds in the user's chosen language
- Videos have Spanish audio/subtitle variants

---

## Highest-Leverage Changes (2/10 → 6/10)

These five changes alone would transform the experience:

1. **Persistent chat drawer** on every page — the single biggest UX upgrade. Makes the AI feel like part of the product, not a separate feature.

2. **Landing page redesign** with the three-card navigation pattern — gives users a reason to stay and a clear path forward.

3. **Unified header + footer** — makes the 10 pages feel like one product instead of ten.

4. **County Profile narrative structure** — transforms the data wall into a story with emotional arc and clear next actions.

5. **Language toggle replacing ES twins** — cuts maintenance in half and makes Spanish feel intentional, not bolted-on.

## Consequences

### Positive
- **Coherent product.** PDI feels like something designed, not assembled.
- **Guided experience.** Users don't need to know what's available — the platform shows them.
- **Chat as companion.** The AI becomes the connective tissue between pages.
- **Maintainable.** Three archetypes, not ten unique designs. Language toggle, not 2× files.
- **Educates while informing.** Manim videos appear contextually, teaching as you explore.

### Negative
- **Significant rebuild.** The chat drawer requires JS architecture changes. The language toggle replaces 10 files. The narrative County Profile is a complete rewrite.
- **Chat cost.** A persistent AI companion means more API calls.
- **Video weight.** 6 videos at ~16MB total — embedding them contextually means more page weight for Browse/Inspect pages.

## Effort Estimate

| Track | Deliverable | Effort |
|---|---|---|
| 8A | Unified header + footer across all pages | 2h |
| 8B | Three-archetype visual language (stylesheet consolidation) | 3h |
| 8C | Persistent chat drawer (JS + CSS + page integration) | 4h |
| 8D | Landing page redesign (hero + three-card nav + featured county) | 3h |
| 8E | County Profile narrative restructure (three-act story) | 4h |
| 8F | Cross-page integration (links between profile/map/evidence/chat) | 2h |
| 8G | Language toggle replacing ES twins | 3h |
| 8H | Contextual video embedding | 2h |
| 8I | Chat companion mode (proactive suggestions per page) | 3h |
| **Total** | | **~26h** |

## Supersedes

- ADR-007 (frontend architecture) — replaces the 8-page hash-routed architecture with a 3-section experience + persistent chat
- ADR-004 (v1 launch plan) — replaces the 4-wave plan with this 3-phase experience redesign
