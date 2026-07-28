# ADR-013: Visual Design — Deploying the Full Stack Vocabulary

**Status:** Proposed
**Date:** 2026-07-28
**Deciders:** Cruz Morales, Hermes Agent
**Depends on:** ADR-011 (experience), ADR-012 (integrations)

## Context

PDI uses the casa-datos stack's CSS tokens at the surface level — `--bg`, `--surface`, `--text`, `--accent`, `--border`. This delivers a functional dark theme. But the stack ships a much richer visual vocabulary that we've never deployed:

| Stack Capability | What it does | We use it? |
|---|---|---|
| Spectral card edges | Warm-to-cool gradient border per card | ❌ Flat `--border` |
| Edge warmth scaling | `color-mix()` tuned per plane level | ❌ |
| Section band atmosphere | Warm peach atmospheric tint per section | ❌ |
| Hero bloom | Radial gradient glows for depth | ❌ |
| Section ribbons (threads) | Full Rainbow spectrum dividers | ❌ Faded `--border` |
| Rainbow categorical scale | 8-color data viz palette | ❌ Amber-only |
| Lift shadows | Warm-cream inner glow + deep drop | ❌ Flat shadow |
| CTA gradient | Amber→ember→red gradient buttons | ❌ Flat amber |
| Light theme | Full light-mode via `@media` + toggle | ❌ Dark only |
| Parametric generation | Seeded randomness for visual variety | ❌ Never used |
| Grain/texture | Subtle noise overlay for depth | ❌ |
| Motion FLIP | Layout animations on state change | ❌ Only reveal-on-scroll |

We're using ~15% of the stack's visual vocabulary. The pages are technically correct but atmospherically flat.

## Decision: Three-Layer Visual Deployment

### Layer 1 — Atmosphere (every page)

These are ambient changes that affect the entire platform without per-page work:

**1A — Spectral Card Edges**
Every `.card`, `.chart-card`, and `.card-grid` item gets the stack's spectral edge gradient instead of flat `--border`. A card's top-left edge reads warm (amber-cream), its bottom-right reads cool (cyan). This creates the stack's signature "lit from above-left" lighting model.

Implementation: Update `.card` and `.chart-card` in styles.css to use `border-image: var(--edge) 1` or the gradient border technique from casa-datos/styles.css.

**1B — Section Atmosphere**
Every `<section>` gets a subtle warm tint. The `--band-atmos` variable applies a radial gradient overlay — peach warmth that's barely visible but gives sections depth. The stack's approach: each section band has a background that pulls slightly toward peach, creating a warm/cool rhythm as you scroll.

Implementation: Add `.section-band` class to styles.css. Apply to each `<section>` in every page.

**1C — Hero Bloom**
The landing page hero gets atmospheric glows. The stack technique: `::before` with a warm radial gradient + `::after` with a cool counter-glow behind it. Creates the impression that light is coming from somewhere — not just a flat dark canvas.

Implementation: Add `.hero-glow` class to styles.css with warm + cool pseudo-elements. Apply to landing page hero.

**1D — Thread Ribbons**
The `.thread` divider between sections gets the full Rainbow spectrum: sun→ember→red→teal→cyan. Currently it's a faded single-color line. The stack ships this as `--band-spectrum`.

Implementation: Update `.thread` in styles.css to use `background: var(--band-spectrum); height: 1px;`.

### Layer 2 — Data Visualization (data-heavy pages)

**2A — Rainbow Categorical Scale**
Every chart, map, and data visualization uses the 8-color Rainbow scale (`--tp-cat-1` through `--tp-cat-8`) instead of amber-only. The scale is designed for categorical data — each color is perceptually distinct and WCAG AA compliant.

| Token | Color | Use |
|---|---|---|
| `--tp-cat-1` | #FBCE0B (sun yellow) | Primary highlight |
| `--tp-cat-2` | #0E6690 (ocean blue) | Secondary category |
| `--tp-cat-3` | #E7630B (ember orange) | Tertiary category |
| `--tp-cat-4` | #37BB8F (emerald green) | Positive/improvement |
| `--tp-cat-5` | #E73B1F (red) | Negative/concern |
| `--tp-cat-6` | #0FA6E6 (cyan) | Quaternary category |
| `--tp-cat-7` | #84BB2C (lime green) | Success/good |
| `--tp-cat-8` | #6B4C8A (purple) | Accent/other |

Implementation:
- County Profile: indicator category tabs use different Rainbow colors
- Map: LISA tiers use the Rainbow scale (already partially done)
- Evidence cards: equity dimensions mapped to Rainbow colors
- Chat: stat callouts colored by indicator category
- Compare: delta bars use emerald (better) and red (worse)

**2B — Chart Bar Gradient**
Chart bars use the stack's CTA gradient (`--grad-cta`: sun→ember→red) instead of flat `--accent`. Gives bars a luminous quality — they look like they're glowing from within.

Implementation: Update `.chart-bar-fill` to use `background: var(--grad-cta)`.

**2C — Lift Shadows**
Cards get the stack's proper lift shadow — an inner warm highlight (cream) plus a deep drop shadow. Currently our `--lift` is a flat color-mix of amber.

Implementation: Port casa-datos's `--lift` and `--lift-hover` values into styles.css.

### Layer 3 — Motion & States (interactive elements)

**3A — FLIP Layout Animations**
When data changes (new county selected, variable switched on map, category tab clicked), elements animate to their new positions. The stack's `motion.js` provides FLIP (First, Last, Invert, Play) — elements smoothly transition between states.

Implementation: Wire `motion.js` FLIP helpers into Alpine.js state changes. When `activeCategory` changes, indicator rows animate to new positions.

**3B — Loading States as Atmosphere**
Instead of generic spinners, loading states use the stack's visual language:
- Skeleton cards with spectral edge shimmer
- Data-placeholder glow (amber pulse)
- Section atmosphere holds during loading (the tint is present even when content isn't)

Implementation: Add `.skeleton-card` and `.skeleton-row` classes that use the spectral edge + subtle amber pulse.

**3C — Light Theme**
Enable the light theme toggle. The tokens.css already has a complete light theme defined. It needs:
- `theme-toggle.js` loaded on every page
- Toggle button in the site header
- `data-theme` attribute handled by the theme restore script (already in our pages)

The light theme transforms the platform: near-black canvas becomes cream paper. The amber accent reads differently on light — warmer, more approachable. This single toggle gives PDI two complete visual identities.

Implementation: Add `theme-toggle.js` script tag and toggle button to all pages.

## Visual Principles

1. **Warmth is information.** The warm→cool gradient across a card edge isn't decoration — it's wayfinding. "This is the start of this card" (warm) and "this is the end" (cool).

2. **Color carries meaning.** Amber (`--accent`) is the primary action color — buttons, links, CTAs. The Rainbow scale carries categorical meaning — each color maps to a specific concept (emerald = improvement, red = concern, ocean = secondary). Never use a Rainbow color as decoration on non-data elements.

3. **Depth is earned.** Cards lift (--lift shadow) only when they contain actionable content. Flat sections don't lift. The elevation scale (`--plane-base` → `--plane-1` → `--plane-2`) matches information hierarchy, not arbitrary decoration.

4. **Motion clarifies.** Reveal-on-scroll draws attention to new content. FLIP animations show relationships between states. Never animate purely for spectacle.

5. **Both themes are real.** The light theme isn't a "light mode version" of the dark theme — it's a complete visual identity. Text that works on dark must be verified on light. Charts that use opacity on dark must use different techniques on light.

## Implementation Strategy

**Phase A — Global styles.css changes (affects all pages):**
- Spectral card edges (`.card`, `.chart-card`)
- Section atmosphere (`.section-band`)
- Thread ribbons (`.thread`)
- Lift shadows (`--lift`, `--lift-hover`)
- CTA gradient (`--grad-cta` for `.btn-primary`, `.chart-bar-fill`)

**Phase B — Per-page atmosphere:**
- Hero bloom on landing page
- Rainbow categorical on County Profile, Map, Evidence, Compare
- Chart bar gradient on all charted pages

**Phase C — Motion + States:**
- FLIP animations on interactive elements
- Skeleton loading with spectral shimmer
- Light theme toggle + theme-restore

## Effort

| Track | Deliverable | Effort |
|---|---|---|
| 9A | Spectral card edges + section atmosphere + thread ribbons | 2h |
| 9B | Lift shadows + CTA gradient | 1h |
| 9C | Hero bloom on landing page | 1h |
| 9D | Rainbow categorical across data-viz pages | 2h |
| 9E | Light theme toggle (all pages) | 2h |
| 9F | FLIP animations + skeleton states | 2h |
| **Total** | | **~10h** |

## Consequences

### Positive
- **Atmospheric depth.** Pages feel designed, not assembled. The warm→cool gradient gives every card a unique identity.
- **Data tells stories in color.** The Rainbow scale makes categorical differences immediately visible — you don't need to read numbers to see patterns.
- **Two identities.** Light/dark toggle gives PDI two complete visual languages for different contexts (late-night research vs. daytime presentation).
- **Motion that clarifies.** FLIP animations show relationships between states — the data changes, and you see how.

### Negative
- **Color discipline required.** The Rainbow scale has 8 colors — every new data visualization must map to them consistently. A color used for "health indicators" on the County Profile must mean "health indicators" on the Map and in the Chat.
- **Light theme verification.** Every new page must be checked in both themes. The `@supports` layer with `color-mix()` behaves differently on light backgrounds.
- **Atmosphere can't be tested programmatically.** The spectral edge gradient, section atmosphere, and hero bloom are visual effects — they require human review, not automated gates.
