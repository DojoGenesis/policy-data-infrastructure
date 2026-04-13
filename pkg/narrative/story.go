package narrative

import "time"

// Document is the full narrative output ready for rendering.
type Document struct {
	Title       string
	Subtitle    string
	Template    string
	Chapters    []Chapter
	Methodology string
	DataSources []string
	GeneratedAt time.Time
	ScopeGEOID  string
	ScopeName   string
	// Optional scope-level summary stats shown in the introduction.
	ScopeStats []StatCallout
}

// Chapter is a single section of the narrative, typically centered on one geography.
type Chapter struct {
	Number       int
	Title        string
	Geography    GeographyProfile
	Indicators   []IndicatorValue
	StatCallouts []StatCallout
	PolicyLevers []PolicyLever
	// Narrative is a rendered prose block (may contain HTML markup).
	Narrative string
}

// defaultTitle returns a reasonable document title for the given scope and template.
func defaultTitle(scopeName, tmplName string) string {
	switch tmplName {
	case "five_mornings":
		return "Five Mornings in " + scopeName
	case "equity_profile":
		return "Equity Profile: " + scopeName
	case "comparison_brief":
		return "Comparison Brief: " + scopeName
	default:
		return "Policy Brief: " + scopeName
	}
}

// defaultSubtitle returns a subtitle for the given template.
func defaultSubtitle(tmplName string) string {
	switch tmplName {
	case "five_mornings":
		return "Five neighborhoods. Five alarm clocks. What does structural risk look like at 6:45 on a Tuesday morning?"
	case "equity_profile":
		return "A data-driven portrait of one community's structural conditions and policy levers."
	case "comparison_brief":
		return "A side-by-side analysis of structural conditions across two communities."
	default:
		return ""
	}
}
