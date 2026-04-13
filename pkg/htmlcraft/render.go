package htmlcraft

import "strings"

// RenderFull assembles a complete HTML5 document from its constituent parts.
// head is injected into <head> after the base styles; scripts are injected
// after the CDN script tags at the bottom of <body>.
//
// The document includes:
//   - Leaflet 1.9.4 CSS/JS from unpkg
//   - Chart.js 4 from jsDelivr
//   - Base responsive styles + print styles in the navy/amber design language
func RenderFull(title string, head string, body string, scripts string) string {
	// Use string concatenation rather than fmt.Sprintf so that literal % in
	// CSS values (e.g. "width: 100%") are not misinterpreted as format verbs.
	css := strings.Join([]string{
		"/* ── Reset & base ───────────────────────────────────── */",
		"*, *::before, *::after { box-sizing: border-box; }",
		"html { font-size: 16px; }",
		"body {",
		"  margin: 0;",
		"  font-family: Georgia, 'Times New Roman', serif;",
		"  background: #f5f6f8;",
		"  color: #1a1a2e;",
		"  line-height: 1.6;",
		"}",
		"a { color: #1b4a7a; }",
		"a:hover { color: #d97706; }",
		"",
		"/* ── Header ─────────────────────────────────────────── */",
		".pdi-header {",
		"  background: #1b4a7a;",
		"  color: #fff;",
		"  padding: 1.25rem 2rem;",
		"  display: flex;",
		"  align-items: center;",
		"  justify-content: space-between;",
		"}",
		".pdi-header h1 { margin: 0; font-size: 1.35rem; font-weight: 700; }",
		".pdi-header .pdi-subtitle { font-size: 0.85rem; color: #fbbf24; margin-top: 0.2rem; }",
		"",
		"/* ── Layout ──────────────────────────────────────────── */",
		".pdi-container { max-width: 1100px; margin: 0 auto; padding: 2rem 1.5rem; }",
		".pdi-section {",
		"  background: #fff;",
		"  border-radius: 6px;",
		"  padding: 1.5rem 2rem;",
		"  margin-bottom: 1.5rem;",
		"  box-shadow: 0 1px 4px rgba(0,0,0,0.07);",
		"}",
		".pdi-section h2 {",
		"  margin-top: 0;",
		"  font-size: 1.1rem;",
		"  color: #1b4a7a;",
		"  border-bottom: 2px solid #fbbf24;",
		"  padding-bottom: 0.4rem;",
		"  margin-bottom: 1rem;",
		"}",
		"",
		"/* ── Tables ─────────────────────────────────────────── */",
		".pdi-table-wrap { overflow-x: auto; }",
		".pdi-table { width: 100%; border-collapse: collapse; font-size: 0.875rem; }",
		".pdi-table th {",
		"  background: #1b4a7a; color: #fff;",
		"  padding: 0.55rem 0.75rem;",
		"  text-align: left; font-weight: 600; white-space: nowrap;",
		"}",
		".pdi-table th:hover { background: #1e5c99; }",
		".pdi-table td { padding: 0.45rem 0.75rem; border-bottom: 1px solid #e8ecf0; vertical-align: top; }",
		".pdi-table tr:nth-child(even) td { background: #f7f9fb; }",
		".pdi-table tr:hover td { background: #eef2f7; }",
		"",
		"/* ── Charts ─────────────────────────────────────────── */",
		".pdi-chart-title { font-size: 0.9rem; font-weight: 600; color: #1b4a7a; margin-bottom: 0.5rem; }",
		"canvas { max-width: 100%; }",
		"",
		"/* ── Metric cards ────────────────────────────────────── */",
		".pdi-metrics-row { display: flex; flex-wrap: wrap; gap: 1rem; margin-bottom: 1rem; }",
		".pdi-metric-card {",
		"  background: #fff; border: 1px solid #dde3ea; border-radius: 6px;",
		"  padding: 1rem 1.25rem; min-width: 160px; flex: 1 1 160px;",
		"}",
		".pdi-metric-label { font-size: 0.75rem; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.25rem; }",
		".pdi-metric-value { font-size: 1.75rem; font-weight: 700; color: #1b4a7a; line-height: 1.1; }",
		".pdi-metric-unit { font-size: 1rem; margin-left: 0.15rem; color: #6b7280; }",
		".pdi-metric-trend { font-size: 0.75rem; margin-top: 0.25rem; }",
		".pdi-trend-up { color: #059669; }",
		".pdi-trend-down { color: #dc2626; }",
		"",
		"/* ── Stat callouts ───────────────────────────────────── */",
		".pdi-stat-callout { text-align: center; padding: 1.25rem; background: #1b4a7a; color: #fff; border-radius: 6px; }",
		".pdi-stat-number { font-size: 2.5rem; font-weight: 800; color: #fbbf24; line-height: 1; }",
		".pdi-stat-label { font-size: 0.9rem; margin-top: 0.4rem; color: #e0e7ef; }",
		".pdi-stat-note { font-size: 0.75rem; margin-top: 0.3rem; color: #9ca3af; }",
		"",
		"/* ── Map ─────────────────────────────────────────────── */",
		".pdi-map { height: 420px; width: 100%; border-radius: 4px; border: 1px solid #dde3ea; }",
		"",
		"/* ── Utility ─────────────────────────────────────────── */",
		".pdi-empty { color: #9ca3af; font-style: italic; font-size: 0.875rem; }",
		"",
		"/* ── Responsive ──────────────────────────────────────── */",
		"@media (max-width: 640px) {",
		"  .pdi-container { padding: 1rem; }",
		"  .pdi-section { padding: 1rem; }",
		"  .pdi-header { padding: 1rem; flex-direction: column; align-items: flex-start; }",
		"  .pdi-metric-card { min-width: 120px; }",
		"}",
		"",
		"/* ── Print ───────────────────────────────────────────── */",
		"@media print {",
		"  body { background: #fff; font-size: 11pt; }",
		"  .pdi-header { background: #fff !important; color: #000 !important; border-bottom: 2pt solid #1b4a7a; }",
		"  .pdi-header h1 { color: #000 !important; }",
		"  .pdi-header .pdi-subtitle { color: #555 !important; }",
		"  .pdi-section { box-shadow: none; border: 1pt solid #ccc; page-break-inside: avoid; }",
		"  .pdi-section h2 { color: #000; }",
		"  .pdi-map { height: 300px; }",
		"  .pdi-stat-callout { background: #fff !important; color: #000 !important; border: 1pt solid #1b4a7a; }",
		"  .pdi-stat-number { color: #1b4a7a !important; }",
		"  a { color: #000; text-decoration: none; }",
		"}",
	}, "\n")

	return "<!DOCTYPE html>\n" +
		"<html lang=\"en\">\n" +
		"<head>\n" +
		"    <meta charset=\"UTF-8\">\n" +
		"    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
		"    <title>" + title + "</title>\n" +
		"    <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\" />\n" +
		"    <style>\n" + css + "\n    </style>\n" +
		"    " + head + "\n" +
		"</head>\n" +
		"<body>\n" +
		"    " + body + "\n" +
		"    <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"></script>\n" +
		"    <script src=\"https://cdn.jsdelivr.net/npm/chart.js@4\"></script>\n" +
		"    " + scripts + "\n" +
		"</body>\n" +
		"</html>"
}
