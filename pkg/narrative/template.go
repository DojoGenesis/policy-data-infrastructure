package narrative

import (
	"fmt"
	"html/template"
	"math"
	"strings"
)

// FuncMap is the set of custom template functions available in all narrative templates.
var FuncMap = template.FuncMap{
	"formatCurrency": formatCurrency,
	"formatPct":      formatPct,
	"formatOrdinal":  formatOrdinal,
	"formatRate":     formatRate,
	"formatInt":      formatInt,
	"severity":       severityClass,
	"tierColor":      tierColor,
	"tierLabel":      tierLabel,
	"hasData":        hasData,
	"upper":          strings.ToUpper,
	"lower":          strings.ToLower,
	"join":           strings.Join,
	"add":            func(a, b int) int { return a + b },
	"sub":            func(a, b int) int { return a - b },
	"safeHTML":       func(s string) template.HTML { return template.HTML(s) }, //nolint:gosec
}

// formatCurrency renders a *float64 as a US dollar amount, e.g. "$52,000".
// Returns "—" if the pointer is nil.
func formatCurrency(v *float64) string {
	if v == nil {
		return "—"
	}
	n := int(math.Round(*v))
	return "$" + commaSep(n)
}

// formatPct renders a *float64 as a percentage with one decimal, e.g. "23.4%".
// Returns "—" if the pointer is nil.
func formatPct(v *float64) string {
	if v == nil {
		return "—"
	}
	return fmt.Sprintf("%.1f%%", *v)
}

// formatOrdinal renders a *float64 percentile as an ordinal string, e.g. "87th".
// Returns "—" if the pointer is nil.
func formatOrdinal(v *float64) string {
	if v == nil {
		return "—"
	}
	n := int(math.Round(*v))
	return ordinal(n)
}

// formatRate renders a *float64 as a rate "per 100", e.g. "1.2 per 100".
// Returns "—" if the pointer is nil.
func formatRate(v *float64) string {
	if v == nil {
		return "—"
	}
	return fmt.Sprintf("%.1f per 100", *v)
}

// formatInt renders a *float64 as a comma-separated integer, e.g. "52,000".
// Returns "—" if nil.
func formatInt(v *float64) string {
	if v == nil {
		return "—"
	}
	return commaSep(int(math.Round(*v)))
}

// severityClass maps a tier name to a CSS severity class: "critical", "warning",
// "neutral", or "positive".
func severityClass(tier string) string {
	switch strings.ToLower(tier) {
	case "very_high", "critical":
		return "critical"
	case "high":
		return "warning"
	case "moderate":
		return "neutral"
	case "low", "on_track", "minimal":
		return "positive"
	default:
		return "neutral"
	}
}

// tierColor maps a tier to its CSS variable name (for inline style use).
func tierColor(tier string) string {
	switch strings.ToLower(tier) {
	case "very_high", "critical":
		return "var(--red)"
	case "high":
		return "var(--amber)"
	case "moderate":
		return "var(--teal)"
	case "low", "on_track", "minimal":
		return "var(--green)"
	default:
		return "var(--slate)"
	}
}

// tierLabel maps a tier to a human-readable label.
func tierLabel(tier string) string {
	switch strings.ToLower(tier) {
	case "very_high":
		return "Very High Risk"
	case "critical":
		return "Critical"
	case "high":
		return "High Risk"
	case "moderate":
		return "Moderate Risk"
	case "low":
		return "Low Risk"
	case "on_track":
		return "On Track"
	case "minimal":
		return "Minimal Risk"
	default:
		return tier
	}
}

// hasData returns true if the *float64 pointer is non-nil.
func hasData(v *float64) bool {
	return v != nil
}

// ─── Helpers ───────────────────────────────────────────────────────────────────

// commaSep formats an integer with comma separators for thousands.
func commaSep(n int) string {
	negative := n < 0
	if negative {
		n = -n
	}
	s := fmt.Sprintf("%d", n)
	// Insert commas every 3 digits from the right.
	result := make([]byte, 0, len(s)+len(s)/3)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	if negative {
		return "-" + string(result)
	}
	return string(result)
}

// ordinal converts an integer to its English ordinal representation.
func ordinal(n int) string {
	suffix := "th"
	switch n % 100 {
	case 11, 12, 13:
		// 11th, 12th, 13th
	default:
		switch n % 10 {
		case 1:
			suffix = "st"
		case 2:
			suffix = "nd"
		case 3:
			suffix = "rd"
		}
	}
	return fmt.Sprintf("%d%s", n, suffix)
}
