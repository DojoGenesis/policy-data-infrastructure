# Policy Data Infrastructure — Design System

## Intent

**Who:** Grant program officers (5 minutes, scanning for credibility) and policy analysts (pulling numbers for reports and talking points). Secondary: advocates looking up what candidates propose and what the data says.

**Feel:** Activist data journalism meets clean data tool. ProPublica's editorial punch with Census Reporter's utility. Data tells stories, but the tool gets out of the way when you need a number. An institution, not a startup.

## Typography

- **Serif:** Merriweather — editorial voice, headings, prose, narrative content, chat assistant messages
- **Sans:** Source Sans 3 — ALL data, UI chrome, tables, stats, bars, badges, buttons, nav, overlines, filter pills
- **Body:** Merriweather 17px / 1.75 line-height on cream (#faf8f4)
- **Rule:** Two fonts = two modes. Reading mode (serif) and working mode (sans).

## Palette

| Token | Hex | Semantic Role |
|-------|-----|--------------|
| navy | #1a2744 | Structure: nav, cover, headers, deep text |
| teal | #0e7490 | Interactive: links, buttons, selected states, overlines |
| amber | #d97706 | Accent: section rules, warning states, highlight marker |
| cream | #faf8f4 | Reading surface (warm) |
| white | #ffffff | Data surface (cool — cards, inputs) |
| red | #b91c1c | Critical: worst-tier indicators, negative diffs |
| green | #15803d | Positive: best-tier indicators, positive diffs |

## Depth

Flat. No drop shadows anywhere. Cards use `1px solid var(--border)` + `10px border-radius`. The data IS the depth — numbers, bars, and color-coding create visual hierarchy, not elevation.

## Signature Element

The **amber underline** — a `40px x 3px` bar (`.rule`) under section titles. Every section of the site uses it. It's the "you are here" marker. Like a highlighter pen on a printed report.

## Spacing

8px base unit. Rhythm: 8 / 16 / 20 / 24 / 32 / 40 / 48 / 64.

## Component Patterns

### Section Opening
```
<div class="overline">SECTION NAME</div>
<h2>Title</h2>
<hr class="rule">
```

### Stat Blocks (`.ns`)
Left-border mini-callouts. Three severities:
- Default (teal): neutral data
- `.ns.warning` (amber): moderate concern
- `.ns.critical` (red): critical threshold

### Filter Pills (`.pill`)
Full-radius pills for category filtering. `.pill.active` = teal fill.

### Data Tables (`.data-table`)
Navy header row, white text. 11px uppercase sans headers. Row hover.

### County Cards (`.county-card`)
Card with border-color hover (not shadow). Contains: county name (serif), population (sans muted), 3 stat blocks.

### Bar Charts
Horizontal bars with `.bar-fill-teal` / `.bar-fill-amber` / `.bar-fill-red` / `.bar-fill-green`. Direction-coded: lower_better = amber, higher_better = teal.

### Wiki Links (`.wiki-links`)
Bottom-of-page section connecting to related pages. Pill-shaped links.

### Chat Messages
User = navy background, sans font, right-aligned.
Assistant = white card, serif font, left-aligned.

## Files

- `cmd/pdi/frontend/style.css` — Design system (source of truth)
- `cmd/pdi/frontend/index.html` — App shell with all page templates
- `cmd/pdi/frontend/lib/` — Hexagonal adapters (api, domain, chat, router)
- `cmd/pdi/frontend/pages/` — Alpine.js page components
