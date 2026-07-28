# ADR-012: Integration Architecture — From Product to Platform

**Status:** Proposed
**Date:** 2026-07-28
**Deciders:** Cruz Morales, Hermes Agent
**Depends on:** ADR-011 (experience redesign)

## Context

ADR-011 defined 5 moves to take PDI from 2/10 to 6/10: persistent chat, landing redesign, unified chrome, narrative profile, language toggle. These make PDI a **product** — coherent, guided, designed.

ADR-012 defines what makes it a **platform** — integrated, intelligent, indispensable. The difference between 6/10 and 11/10 is not more features. It's features that amplify each other. Every piece should make every other piece better.

## Integration Map

```
                    ┌──────────┐
                    │   CHAT   │ ← persistent companion, spatial narrator
                    └────┬─────┘
                         │ cross-references everything
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
   ┌─────────┐    ┌───────────┐    ┌──────────┐
   │  MAP    │    │  PROFILE  │    │ EVIDENCE │
   │ (space) │◄──▶│  (depth)  │◄──▶│ (action) │
   └────┬────┘    └─────┬─────┘    └────┬─────┘
        │               │               │
        └───────────────┼───────────────┘
                        │
                   ┌────▼─────┐
                   │  VIDEOS  │ ← personalized education
                   │ (learn)  │
                   └──────────┘
```

Every arrow is a two-way integration. Click a tract on the map → see its profile in a panel. See an indicator in the profile → view it spatially on the map. Ask the chat about a county → it cites evidence cards. Watch a video → it uses YOUR county's data.

## Integration 1: Chat as Spatial Narrator

**Current:** Chat answers questions about specific counties. It's reactive.

**11/10:** Chat is a spatial narrator. It doesn't just answer — it guides, compares, and tells stories.

**Capabilities:**
- "Walk me through Dane County" → chat narrates the county profile, section by section, scrolling the page to each layer as it speaks
- "Compare this county to its neighbors" → chat computes neighbor averages on-the-fly and narrates the differences
- "Why is this tract a High-High cluster?" → chat explains the LISA methodology in context, citing the tract's specific indicator values and its neighbors
- "What's changed since 2019?" → chat queries multi-vintage data and narrates the trend
- "What should I do about this?" → chat cross-references the county's highest-burden dimensions with matching evidence cards and recommends policy levers

**Architecture:** ChatAdapter gains a `narrate(pageContext, command)` method. The model receives the full page state — indicators, factors, LISA clusters, evidence cards — and composes a narrative response. The chat UI can trigger page navigation (scroll to section, open map at location, highlight card).

## Integration 2: Personalized Education

**Current:** 6 Manim videos explain statistical concepts generically. They're files on a page.

**11/10:** Every video uses YOUR data. The z-score animation shows YOUR county's position on the distribution. The ICE video visualizes YOUR county's polarization with your actual numbers. The LISA video highlights YOUR county's tracts.

**Architecture:** The Manim scripts accept a `--geoid` parameter. When embedded on a County Profile, the page passes the current GEOID. The server renders a personalized variant (cached). Videos become "this county's z-score story" not "what is a z-score."

**Implementation:**
- `GET /v1/policy/videos/:name?geoid=X` → returns personalized MP4
- Server-side: Manim renders with county-specific data injected into the scene
- Cache: first render per county per video is slow (~60s), subsequent requests serve cached MP4
- Fallback: generic video with overlay: "Your county's data highlighted in amber"

## Integration 3: Real-Time Composite Sensitivity

**Current:** Composite builder is a separate page. You select variables, set weights, click compute, see a result.

**11/10:** Composite sensitivity is live. As you drag weight sliders, the county's score updates in real-time. You can see which variables drive the score most. You can compare scenarios side-by-side ("what if I double the weight on poverty?").

**Architecture:** The composite computation is fast enough for client-side preview. The API still validates and stores results for sharing. The UI shows:
- Live score as sliders move
- Sensitivity sparkline: how the score changes across weight ranges
- Variable influence: which indicators contribute most to the current score
- Scenario comparison: save scenario A, tweak to scenario B, see the difference

**Integration:** Composite results appear on the County Profile, not just a separate page. "Build your own index" is a panel within the profile, not a destination.

## Integration 4: Deep-Linkable Everything

**Current:** You can link to a county profile (`/county?geoid=55025`). You can't link to a specific indicator, a specific map view, or a specific chat query.

**11/10:** Every state is a URL. Every view is shareable. PDI becomes embeddable infrastructure.

**Deep-link schema:**
- `/county?geoid=55025&layer=3` → County Profile, scrolled to Layer 3 (Factor Profile)
- `/county?geoid=55025&indicator=poverty_rate` → County Profile, filtered to poverty indicator
- `/map?indicator=poverty_rate&lat=44.5&lng=-89.5&zoom=9` → Map, specific view
- `/map?geoid=55025001700` → Map, zoomed to a specific tract with popup open
- `/compare?geoid1=55025&geoid2=55079&highlight=median_household_income` → Compare, specific row highlighted
- `/chat?q=What+is+the+poverty+rate+in+Dane+County` → Chat, pre-loaded query
- `/evidence?dimension=housing_affordability` → Evidence, filtered to housing cards

**Embed support:**
- `<iframe src="/embed/map?indicator=poverty_rate">` → embeddable map
- `<iframe src="/embed/stat?geoid=55025&indicator=poverty_rate">` → single stat card
- News sites, policy blogs, county websites can embed PDI views

## Integration 5: Temporal Analysis

**Current:** PDI shows a single vintage (ACS 2023 5-Year). There's no way to see change over time.

**11/10:** Every indicator has a temporal dimension. The County Profile shows trend sparklines. The Map can scrub through vintages. The Chat can answer "how has this changed?"

**Architecture:**
- Store multiple vintages in the indicators table (already supported via `vintage` column)
- `GET /v1/policy/geographies/:geoid/indicators?vintage=2019,2023` → multi-vintage response
- County Profile: trend sparkline next to each indicator value (mini SVG line chart)
- Map: vintage slider — scrub from 2015 to 2023, tracts recolor as values change
- Chat: "How has poverty changed in Dane County since 2015?" → trend data + narrative

**Data requirement:** Run the ACS fetch pipeline for multiple vintages. Currently only ACS 2023 is loaded.

## Integration 6: Alert & Return System

**Current:** PDI is static. You visit, you explore, you leave. No reason to return.

**11/10:** PDI notifies you when data changes. Your county gets new ACS data — PDI tells you what changed. A new evidence card matches your county's profile — PDI surfaces it.

**Architecture:**
- Client-side: localStorage tracks which counties a user has viewed
- On return: "Since your last visit, Dane County's poverty rate changed from 10.1% to 10.5%"
- No server-side accounts needed — browser-local, privacy-respecting
- Optional: email alerts for grant reviewers or policy analysts who need to track specific counties

## Integration 7: Offline-Ready & Installable

**Current:** PDI requires an internet connection. Every page load hits the API.

**11/10:** PDI is a Progressive Web App. Core data (WI county profiles) is cached for offline access. The map works without internet. The chat degrades gracefully.

**Architecture:**
- Service worker caches static assets + API responses
- `/v1/policy/geographies` and `/v1/policy/geographies/:geoid/indicators` responses cached for 24h
- Map tiles cached for offline use
- Chat shows "offline — showing cached data" when disconnected
- Installable: "Add PDI to your home screen" on mobile

## Effort Summary

| # | Integration | Effort | Dependencies |
|---|---|---|---|
| I1 | Chat as spatial narrator | 6h | I4 (deep links) |
| I2 | Personalized education (videos) | 8h | Manim scripts, server-side rendering |
| I3 | Real-time composite sensitivity | 3h | Client-side compute |
| I4 | Deep-linkable everything | 3h | URL schema, embed support |
| I5 | Temporal analysis | 5h | Multi-vintage data pipeline |
| I6 | Alert & return system | 2h | localStorage, client-side only |
| I7 | Offline-ready PWA | 3h | Service worker, cache strategy |
| **Total** | | **~30h** | |

## Priority Order

ADR-011's 5 moves (→ 6/10) should ship first. Then integrations in this order:

1. **I4 — Deep links** (unlocks all other integrations, low effort, high leverage)
2. **I1 — Chat as narrator** (the flagship integration, makes PDI feel intelligent)
3. **I3 — Real-time composites** (immediate wow factor, low effort)
4. **I5 — Temporal analysis** (requires data pipeline work, high value)
5. **I2 — Personalized videos** (computationally expensive, deferred)
6. **I6 — Alerts** (nice-to-have, low effort)
7. **I7 — PWA** (infrastructure work, lowest leverage for policy audience)
