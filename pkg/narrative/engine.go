package narrative

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// ─── Policy & severity thresholds ─────────────────────────────────────────────
//
// Keep all numeric parameters here so they are visible, citable, and easy to
// update when the underlying data vintage changes.

const (
	// evictionPolicyThreshold is the eviction filing rate above which Eviction
	// Diversion is recommended. Set at the national average filing rate per the
	// Princeton Eviction Lab (2018 data, most recent published national figure).
	evictionPolicyThreshold = 3.0

	// transitScorePolicyThreshold is the Walk Score transit index below which
	// Transit Frequency Investment is recommended. A score < 30 indicates
	// "minimal transit" per Walk Score's published methodology.
	transitScorePolicyThreshold = 30.0

	// chronicAbsencePolicyThreshold is the chronic absenteeism rate above which
	// School-Based Wraparound Services are recommended. 25% is the federal
	// threshold distinguishing "widespread chronic absence" per the U.S.
	// Department of Education's Every Student Succeeds Act reporting guidance.
	chronicAbsencePolicyThreshold = 25.0

	// uninsuredPolicyThreshold is the uninsured rate above which targeted
	// Medicaid outreach is recommended. 15% exceeds the pre-ACA national average
	// and is used as the trigger for enrollment campaigns in Urban Institute
	// impact analyses (2021).
	uninsuredPolicyThreshold = 15.0

	// absenceCriticalThreshold marks chronic absence rates that are "critical"
	// (at or above 30%). The U.S. Dept. of Education flags ≥30% as the severe
	// tier in ESSA state report cards.
	absenceCriticalThreshold = 30.0

	// absenceWarningThreshold marks chronic absence rates in the warning band
	// (20–30%). This is the "high" tier in ESSA reporting guidance.
	absenceWarningThreshold = 20.0

	// absenceNeutralThreshold marks the lower bound of the neutral band
	// (10–20%). Below 10% is considered on-track nationally.
	absenceNeutralThreshold = 10.0

	// incomeCriticalThreshold is the median household income below which a
	// tract is classified "critical." $35,000 approximates 60% of the 2023
	// U.S. median household income ($74,580, U.S. Census ACS 2023 1-year).
	incomeCriticalThreshold = 35000.0

	// incomeWarningThreshold is the income below which a tract is classified
	// "warning." $50,000 ≈ 67% of the 2023 U.S. median household income.
	incomeWarningThreshold = 50000.0

	// incomeNeutralThreshold is the income below which a tract is classified
	// "neutral." $75,000 ≈ the U.S. median household income (ACS 2023).
	incomeNeutralThreshold = 75000.0

	// evictionCriticalThreshold is the eviction filing rate above which a
	// tract is classified "critical" (more than double the national average).
	// Derived from Princeton Eviction Lab national mean of ~2.5 per 100 (2018).
	evictionCriticalThreshold = 5.0

	// evictionWarningThreshold is the eviction filing rate above which a tract
	// is classified "warning" — at or above the national average filing rate
	// (Princeton Eviction Lab, 2018).
	evictionWarningThreshold = 3.0

	// evictionNeutralThreshold is the eviction filing rate above which a tract
	// is classified "neutral." Rates below this threshold are below the national
	// median and are classified "positive."
	evictionNeutralThreshold = 1.5
)

//go:embed templates/*.tmpl
var embeddedTemplates embed.FS

// Engine generates narrative documents from analysis data.
type Engine struct {
	store     store.Store
	templates map[string]*template.Template
}

// NewEngine creates a new Engine backed by the given store.
func NewEngine(s store.Store) *Engine {
	return &Engine{
		store:     s,
		templates: make(map[string]*template.Template),
	}
}

// LoadTemplates loads all *.tmpl files from dir into the engine.
// Template names are the base filename without extension.
func (e *Engine) LoadTemplates(dir string) error {
	entries, err := filepath.Glob(filepath.Join(dir, "*.tmpl"))
	if err != nil {
		return fmt.Errorf("narrative: LoadTemplates glob %s: %w", dir, err)
	}
	for _, path := range entries {
		name := strings.TrimSuffix(filepath.Base(path), ".tmpl")
		t, err := template.New(name).Funcs(FuncMap).ParseFiles(path)
		if err != nil {
			return fmt.Errorf("narrative: LoadTemplates parse %s: %w", path, err)
		}
		// ParseFiles sets the root template name to the file base name.
		// Re-associate under our short name.
		e.templates[name] = t
	}
	return nil
}

// LoadEmbeddedTemplates loads the built-in templates compiled into the binary.
func (e *Engine) LoadEmbeddedTemplates() error {
	return fs.WalkDir(embeddedTemplates, "templates", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".tmpl") {
			return nil
		}
		name := strings.TrimSuffix(filepath.Base(path), ".tmpl")
		data, err := embeddedTemplates.ReadFile(path)
		if err != nil {
			return fmt.Errorf("narrative: LoadEmbeddedTemplates read %s: %w", path, err)
		}
		t, err := template.New(name).Funcs(FuncMap).Parse(string(data))
		if err != nil {
			return fmt.Errorf("narrative: LoadEmbeddedTemplates parse %s: %w", path, err)
		}
		e.templates[name] = t
		return nil
	})
}

// GenerateRequest specifies what kind of narrative document to produce.
type GenerateRequest struct {
	// Template is the template name (e.g. "five_mornings", "equity_profile").
	Template string
	// ScopeGEOID is the GEOID of the bounding geography (county, state, national).
	ScopeGEOID string
	// ScopeName is a human-readable name for the scope (e.g. "Dane County, WI").
	ScopeName string
	// AnalysisID identifies which analysis to use for tier/score selection.
	AnalysisID string
	// ChapterCount controls how many chapters/vignettes to include. Default 5.
	ChapterCount int
	// Selection controls which geographies are chosen. One of:
	//   "by_tier"      — one geography per tier, severity-first
	//   "outliers"     — positive outliers (over-performing given structural risk)
	//   "by_indicator" — extremes on VariableID
	Selection string
	// VariableID is used only when Selection == "by_indicator".
	VariableID string
	// Methodology is an optional methodology blurb for the document footer.
	Methodology string
	// DataSources is an optional list of data source citations.
	DataSources []string
}

// Generate builds a Document from the given request. The Document can then be
// passed to RenderHTML to produce the final HTML output.
func (e *Engine) Generate(ctx context.Context, req GenerateRequest) (*Document, error) {
	if req.ChapterCount <= 0 {
		req.ChapterCount = 5
	}
	if req.Template == "" {
		req.Template = "five_mornings"
	}
	if req.Selection == "" {
		req.Selection = "by_tier"
	}

	// 1. Select geographies.
	var profiles []GeographyProfile
	var err error
	switch req.Selection {
	case "by_tier":
		profiles, err = SelectByTier(ctx, e.store, req.AnalysisID, req.ChapterCount)
	case "outliers":
		profiles, err = SelectOutliers(ctx, e.store, req.AnalysisID, "positive", req.ChapterCount)
	case "by_indicator":
		profiles, err = SelectByIndicator(ctx, e.store, req.VariableID, "highest", req.ChapterCount)
	default:
		return nil, fmt.Errorf("narrative: unknown selection %q (want by_tier, outliers, by_indicator)", req.Selection)
	}
	if err != nil {
		return nil, fmt.Errorf("narrative: selection %s: %w", req.Selection, err)
	}

	// Patch scope name/level onto each profile.
	for i := range profiles {
		if profiles[i].ScopeName == "" {
			profiles[i].ScopeName = req.ScopeName
		}
	}

	// 2. Build chapters.
	chapters := make([]Chapter, 0, len(profiles))
	for i, prof := range profiles {
		ch := Chapter{
			Number:    i + 1,
			Title:     chapterTitle(i, prof),
			Geography: prof,
		}
		ch.Indicators = buildIndicators(prof)
		ch.StatCallouts = buildStatCallouts(prof)
		ch.PolicyLevers = buildPolicyLevers(prof)
		ch.Narrative = buildNarrative(i, prof)
		chapters = append(chapters, ch)
	}

	doc := &Document{
		Title:       defaultTitle(req.ScopeName, req.Template),
		Subtitle:    defaultSubtitle(req.Template),
		Template:    req.Template,
		Chapters:    chapters,
		Methodology: req.Methodology,
		DataSources: req.DataSources,
		GeneratedAt: time.Now().UTC(),
		ScopeGEOID:  req.ScopeGEOID,
		ScopeName:   req.ScopeName,
		ScopeStats:  buildScopeStats(profiles),
	}

	return doc, nil
}

// RenderHTML renders a Document to an HTML string using the named template.
func (e *Engine) RenderHTML(doc *Document) (string, error) {
	t, ok := e.templates[doc.Template]
	if !ok {
		return "", fmt.Errorf("narrative: template %q not loaded", doc.Template)
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, doc); err != nil {
		return "", fmt.Errorf("narrative: RenderHTML: %w", err)
	}
	return buf.String(), nil
}

// ─── Chapter construction helpers ──────────────────────────────────────────────

var chapterNames = []string{"One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine", "Ten"}

func chapterTitle(i int, p GeographyProfile) string {
	if i < len(chapterNames) {
		return "Chapter " + chapterNames[i] + ": " + p.Name
	}
	return fmt.Sprintf("Chapter %d: %s", i+1, p.Name)
}

// buildIndicators converts GeographyProfile fields into IndicatorValue slices.
func buildIndicators(p GeographyProfile) []IndicatorValue {
	var ivs []IndicatorValue
	add := func(name string, v *float64, dir, fmt_, bench string) {
		if v == nil {
			return
		}
		var fmtd string
		switch fmt_ {
		case "currency":
			fmtd = formatCurrency(v)
		case "pct":
			fmtd = formatPct(v)
		case "rate":
			fmtd = formatRate(v)
		default:
			fmtd = formatPct(v)
		}
		ivs = append(ivs, IndicatorValue{
			Name:      name,
			Value:     v,
			Formatted: fmtd,
			Direction: dir,
			Benchmark: bench,
		})
	}
	add("Median Household Income", p.MedianIncome, "higher_better", "currency", "")
	add("Poverty Rate", p.PovertyRate, "lower_better", "pct", "")
	add("Uninsured Rate", p.UninsuredRate, "lower_better", "pct", "")
	add("Cost Burden Rate", p.CostBurdenRate, "lower_better", "pct", "")
	add("Eviction Filing Rate", p.EvictionRate, "lower_better", "rate", "")
	add("Chronic Absence Rate", p.ChronicAbsence, "lower_better", "pct", "")
	add("% People of Color", p.PctPOC, "neutral", "pct", "")
	return ivs
}

// buildStatCallouts produces up to 4 big-number callouts from the most important indicators.
func buildStatCallouts(p GeographyProfile) []StatCallout {
	var callouts []StatCallout
	if p.NARIPercentile != nil {
		callouts = append(callouts, StatCallout{
			Value:    formatOrdinal(p.NARIPercentile),
			Label:    "NARI Percentile",
			Context:  tierLabel(p.NARITier),
			Severity: severityClass(p.NARITier),
		})
	}
	if p.ChronicAbsence != nil {
		callouts = append(callouts, StatCallout{
			Value:    formatPct(p.ChronicAbsence),
			Label:    "Chronic Absence Rate",
			Context:  "",
			Severity: absenceSeverity(*p.ChronicAbsence),
		})
	}
	if p.MedianIncome != nil {
		callouts = append(callouts, StatCallout{
			Value:    formatCurrency(p.MedianIncome),
			Label:    "Median Household Income",
			Context:  "",
			Severity: incomeSeverity(*p.MedianIncome),
		})
	}
	if p.EvictionRate != nil {
		callouts = append(callouts, StatCallout{
			Value:    formatRate(p.EvictionRate),
			Label:    "Eviction Filing Rate",
			Context:  "",
			Severity: evictionSeverity(*p.EvictionRate),
		})
	}
	return callouts
}

// buildScopeStats produces summary callouts for the intro section.
func buildScopeStats(profiles []GeographyProfile) []StatCallout {
	if len(profiles) == 0 {
		return nil
	}
	return []StatCallout{
		{
			Value:    fmt.Sprintf("%d", len(profiles)),
			Label:    "neighborhoods profiled",
			Context:  "selected by structural risk tier",
			Severity: "neutral",
		},
	}
}

// buildPolicyLevers returns a generic set of policy levers if the geography
// shows high-risk characteristics; in production these would come from a
// curated evidence database.
func buildPolicyLevers(p GeographyProfile) []PolicyLever {
	var levers []PolicyLever

	if p.EvictionRate != nil && *p.EvictionRate > evictionPolicyThreshold {
		levers = append(levers, PolicyLever{
			Title:       "Eviction Diversion Program",
			Description: "Expand eviction diversion and legal counsel to all high-risk tracts. Evidence shows eviction effects on school attendance persist for two years after the filing.",
			Evidence:    []string{"University of Michigan Poverty Solutions (2022)", "National Low Income Housing Coalition (2023)"},
			Impact:      "high",
			Category:    "housing",
		})
	}
	if p.TransitScore != nil && *p.TransitScore < transitScorePolicyThreshold {
		levers = append(levers, PolicyLever{
			Title:       "Transit Frequency Investment",
			Description: "Increase AM-peak bus frequency to 15-minute headways in transit-poor tracts. Free student transit passes address cost; frequency addresses availability.",
			Evidence:    []string{"Federal Transit Administration (2024) — transit as chronic absenteeism intervention", "MIT/Michigan regression-discontinuity study (2024)"},
			Impact:      "moderate",
			Category:    "transit",
		})
	}
	if p.ChronicAbsence != nil && *p.ChronicAbsence > chronicAbsencePolicyThreshold {
		levers = append(levers, PolicyLever{
			Title:       "School-Based Wraparound Services",
			Description: "Co-locate childcare, health, and social services inside school buildings serving high-absence neighborhoods.",
			Evidence:    []string{"Learning Policy Institute — California Community Schools evaluation (2023)"},
			Impact:      "high",
			Category:    "education",
		})
	}
	if p.UninsuredRate != nil && *p.UninsuredRate > uninsuredPolicyThreshold {
		levers = append(levers, PolicyLever{
			Title:       "Medicaid Outreach & Enrollment",
			Description: "Targeted Medicaid and CHIP enrollment campaigns in tracts with high uninsured rates reduce health-related absences.",
			Evidence:    []string{"Urban Institute — Medicaid expansion and school attendance (2021)"},
			Impact:      "moderate",
			Category:    "health",
		})
	}

	return levers
}

// buildNarrative produces a short prose intro for a chapter.
// In production this would be templated or LLM-generated from actual data.
func buildNarrative(i int, p GeographyProfile) string {
	tier := tierLabel(p.NARITier)
	pctStr := "—"
	if p.NARIPercentile != nil {
		pctStr = formatOrdinal(p.NARIPercentile)
	}
	return fmt.Sprintf(
		"%s is a %s community in the %s, sitting at the %s percentile on the Neighborhood "+
			"Attendance Risk Index. Its structural profile shapes daily life for families here in "+
			"ways the aggregate numbers cannot fully capture.",
		p.Name, strings.ToLower(tier), p.ScopeName, pctStr,
	)
}

// ─── Severity helpers ──────────────────────────────────────────────────────────

func absenceSeverity(v float64) string {
	switch {
	case v >= absenceCriticalThreshold:
		return "critical"
	case v >= absenceWarningThreshold:
		return "warning"
	case v >= absenceNeutralThreshold:
		return "neutral"
	default:
		return "positive"
	}
}

func incomeSeverity(v float64) string {
	switch {
	case v < incomeCriticalThreshold:
		return "critical"
	case v < incomeWarningThreshold:
		return "warning"
	case v < incomeNeutralThreshold:
		return "neutral"
	default:
		return "positive"
	}
}

func evictionSeverity(v float64) string {
	switch {
	case v > evictionCriticalThreshold:
		return "critical"
	case v > evictionWarningThreshold:
		return "warning"
	case v > evictionNeutralThreshold:
		return "neutral"
	default:
		return "positive"
	}
}
