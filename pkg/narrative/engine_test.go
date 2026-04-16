package narrative

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// ─── Template function tests ──────────────────────────────────────────────────

func TestFormatCurrency(t *testing.T) {
	tests := []struct {
		input *float64
		want  string
	}{
		{ptr(52000.0), "$52,000"},
		{ptr(1234567.0), "$1,234,567"},
		{ptr(0.0), "$0"},
		{ptr(999.0), "$999"},
		{nil, "—"},
	}
	for _, tc := range tests {
		got := formatCurrency(tc.input)
		if got != tc.want {
			t.Errorf("formatCurrency(%v) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestFormatPct(t *testing.T) {
	tests := []struct {
		input *float64
		want  string
	}{
		{ptr(23.4), "23.4%"},
		{ptr(0.0), "0.0%"},
		{ptr(100.0), "100.0%"},
		{nil, "—"},
	}
	for _, tc := range tests {
		got := formatPct(tc.input)
		if got != tc.want {
			t.Errorf("formatPct(%v) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestFormatOrdinal(t *testing.T) {
	tests := []struct {
		input *float64
		want  string
	}{
		{ptr(1.0), "1st"},
		{ptr(2.0), "2nd"},
		{ptr(3.0), "3rd"},
		{ptr(4.0), "4th"},
		{ptr(11.0), "11th"},
		{ptr(12.0), "12th"},
		{ptr(13.0), "13th"},
		{ptr(21.0), "21st"},
		{ptr(22.0), "22nd"},
		{ptr(87.0), "87th"},
		{nil, "—"},
	}
	for _, tc := range tests {
		got := formatOrdinal(tc.input)
		if got != tc.want {
			t.Errorf("formatOrdinal(%v) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestFormatRate(t *testing.T) {
	tests := []struct {
		input *float64
		want  string
	}{
		{ptr(1.2), "1.2 per 100"},
		{ptr(5.12), "5.1 per 100"},
		{nil, "—"},
	}
	for _, tc := range tests {
		got := formatRate(tc.input)
		if got != tc.want {
			t.Errorf("formatRate(%v) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestSeverityClass(t *testing.T) {
	tests := []struct {
		tier string
		want string
	}{
		{"very_high", "critical"},
		{"critical", "critical"},
		{"high", "warning"},
		{"moderate", "neutral"},
		{"low", "positive"},
		{"on_track", "positive"},
		{"", "neutral"},
	}
	for _, tc := range tests {
		got := severityClass(tc.tier)
		if got != tc.want {
			t.Errorf("severityClass(%q) = %q, want %q", tc.tier, got, tc.want)
		}
	}
}

func TestHasData(t *testing.T) {
	if hasData(nil) {
		t.Error("hasData(nil) should be false")
	}
	v := 1.0
	if !hasData(&v) {
		t.Error("hasData(&v) should be true")
	}
}

func TestCommaSep(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{1234567, "1,234,567"},
		{-52000, "-52,000"},
	}
	for _, tc := range tests {
		got := commaSep(tc.n)
		if got != tc.want {
			t.Errorf("commaSep(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

// ─── Mock store ───────────────────────────────────────────────────────────────

type mockStore struct {
	geographies map[string]*geo.Geography
	scores      []store.AnalysisScore
	indicators  []store.Indicator
}

func newMockStore() *mockStore {
	return &mockStore{
		geographies: map[string]*geo.Geography{
			"55025000100": {
				GEOID:  "55025000100",
				Level:  geo.Tract,
				Name:   "South Madison Tract 1",
				StateFIPS: "55",
				CountyFIPS: "025",
				Population: 4200,
			},
			"55025000200": {
				GEOID:  "55025000200",
				Level:  geo.Tract,
				Name:   "Allied Drive Tract 2",
				StateFIPS: "55",
				CountyFIPS: "025",
				Population: 3800,
			},
			"55025000300": {
				GEOID:  "55025000300",
				Level:  geo.Tract,
				Name:   "West Side Tract 3",
				StateFIPS: "55",
				CountyFIPS: "025",
				Population: 5200,
			},
		},
		scores: []store.AnalysisScore{
			{AnalysisID: "test-analysis", GEOID: "55025000100", Score: 92.0, Rank: 1, Percentile: 97.0, Tier: "critical"},
			{AnalysisID: "test-analysis", GEOID: "55025000200", Score: 78.0, Rank: 2, Percentile: 85.0, Tier: "high"},
			{AnalysisID: "test-analysis", GEOID: "55025000300", Score: 18.0, Rank: 3, Percentile: 3.0, Tier: "on_track"},
		},
		indicators: []store.Indicator{
			{GEOID: "55025000100", VariableID: "median_household_income", Value: ptr(42000.0)},
			{GEOID: "55025000100", VariableID: "poverty_rate", Value: ptr(28.5)},
			{GEOID: "55025000100", VariableID: "eviction_rate", Value: ptr(5.12)},
			{GEOID: "55025000100", VariableID: "chronic_absence_rate", Value: ptr(29.6)},
			{GEOID: "55025000100", VariableID: "transit_score", Value: ptr(22.0)},
			{GEOID: "55025000200", VariableID: "median_household_income", Value: ptr(38000.0)},
			{GEOID: "55025000200", VariableID: "eviction_rate", Value: ptr(3.8)},
			{GEOID: "55025000300", VariableID: "median_household_income", Value: ptr(165000.0)},
			{GEOID: "55025000300", VariableID: "chronic_absence_rate", Value: ptr(13.1)},
		},
	}
}

func (m *mockStore) PutGeographies(_ context.Context, _ []geo.Geography) error { return nil }
func (m *mockStore) GetGeography(_ context.Context, geoid string) (*geo.Geography, error) {
	g, ok := m.geographies[geoid]
	if !ok {
		return nil, nil
	}
	return g, nil
}
func (m *mockStore) QueryGeographies(_ context.Context, _ store.GeoQuery) ([]geo.Geography, error) {
	out := make([]geo.Geography, 0, len(m.geographies))
	for _, g := range m.geographies {
		out = append(out, *g)
	}
	return out, nil
}
func (m *mockStore) PutIndicators(_ context.Context, _ []store.Indicator) error { return nil }
func (m *mockStore) QueryIndicators(_ context.Context, q store.IndicatorQuery) ([]store.Indicator, error) {
	if len(q.GEOIDs) == 0 {
		return m.indicators, nil
	}
	geoSet := make(map[string]bool, len(q.GEOIDs))
	for _, g := range q.GEOIDs {
		geoSet[g] = true
	}
	var out []store.Indicator
	for _, ind := range m.indicators {
		if geoSet[ind.GEOID] {
			out = append(out, ind)
		}
	}
	return out, nil
}
func (m *mockStore) Aggregate(_ context.Context, _ store.AggregateQuery) (*store.AggregateResult, error) {
	return &store.AggregateResult{Value: 0, Count: 0}, nil
}
func (m *mockStore) PutAnalysis(_ context.Context, _ store.AnalysisResult) (string, error) {
	return "mock-analysis-id", nil
}
func (m *mockStore) PutAnalysisScores(_ context.Context, _ []store.AnalysisScore) error { return nil }
func (m *mockStore) QueryAnalysisScores(_ context.Context, analysisID, tier string) ([]store.AnalysisScore, error) {
	var out []store.AnalysisScore
	for _, s := range m.scores {
		if s.AnalysisID != analysisID {
			continue
		}
		if tier != "" && s.Tier != tier {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}
func (m *mockStore) PutIndicatorsBatch(_ context.Context, _ []store.Indicator, _ int) error {
	return nil
}
func (m *mockStore) QueryVariables(_ context.Context) ([]store.VariableMeta, error) {
	return nil, nil
}
func (m *mockStore) ListAnalyses(_ context.Context) ([]store.AnalysisSummary, error) {
	return nil, nil
}
func (m *mockStore) GetAnalysis(_ context.Context, _ string) (*store.AnalysisResult, error) {
	return nil, nil
}
func (m *mockStore) PutPolicies(_ context.Context, _ []store.PolicyRecord) error { return nil }
func (m *mockStore) QueryPolicies(_ context.Context, _ store.PolicyQuery) ([]store.PolicyRecord, error) {
	return nil, nil
}
func (m *mockStore) GetPolicy(_ context.Context, _ string) (*store.PolicyRecord, error) {
	return nil, nil
}
func (m *mockStore) Ping(_ context.Context) error          { return nil }
func (m *mockStore) Migrate(_ context.Context) error       { return nil }
func (m *mockStore) RefreshViews(_ context.Context) error  { return nil }
func (m *mockStore) Close() error                          { return nil }

// ─── Engine tests ─────────────────────────────────────────────────────────────

func TestEngineGenerate(t *testing.T) {
	ms := newMockStore()
	eng := NewEngine(ms)
	if err := eng.LoadEmbeddedTemplates(); err != nil {
		t.Fatalf("LoadEmbeddedTemplates: %v", err)
	}

	doc, err := eng.Generate(context.Background(), GenerateRequest{
		Template:     "five_mornings",
		ScopeGEOID:   "55025",
		ScopeName:    "Dane County, WI",
		AnalysisID:   "test-analysis",
		ChapterCount: 3,
		Selection:    "by_tier",
		Methodology:  "ICE income-race metric (Krieger et al. 2016) with percentile ranking across 8 indicators.",
		DataSources:  []string{"US Census ACS 2023", "Princeton Eviction Lab"},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	if doc.Title == "" {
		t.Error("doc.Title should not be empty")
	}
	if len(doc.Chapters) == 0 {
		t.Error("doc.Chapters should not be empty")
	}
	if doc.ScopeName != "Dane County, WI" {
		t.Errorf("doc.ScopeName = %q, want %q", doc.ScopeName, "Dane County, WI")
	}
	if doc.GeneratedAt.IsZero() {
		t.Error("doc.GeneratedAt should not be zero")
	}

	// Chapters should be ordered by tier severity.
	for i, ch := range doc.Chapters {
		if ch.Number != i+1 {
			t.Errorf("ch.Number = %d, want %d", ch.Number, i+1)
		}
		if ch.Geography.GEOID == "" {
			t.Errorf("chapter %d: GEOID is empty", i)
		}
	}
}

func TestEngineRenderHTML_FiveMornings(t *testing.T) {
	ms := newMockStore()
	eng := NewEngine(ms)
	if err := eng.LoadEmbeddedTemplates(); err != nil {
		t.Fatalf("LoadEmbeddedTemplates: %v", err)
	}

	doc, err := eng.Generate(context.Background(), GenerateRequest{
		Template:     "five_mornings",
		ScopeGEOID:   "55025",
		ScopeName:    "Dane County, WI",
		AnalysisID:   "test-analysis",
		ChapterCount: 3,
		Selection:    "by_tier",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	html, err := eng.RenderHTML(doc)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}

	required := []string{
		"<!DOCTYPE html>",
		"<html",
		"Five Mornings in",
		"Dane County, WI",
		"Index of Concentration at the Extremes",
		"chapter-1",
	}
	for _, want := range required {
		if !strings.Contains(html, want) {
			t.Errorf("rendered HTML missing expected string: %q", want)
		}
	}
}

func TestEngineRenderHTML_EquityProfile(t *testing.T) {
	ms := newMockStore()
	eng := NewEngine(ms)
	if err := eng.LoadEmbeddedTemplates(); err != nil {
		t.Fatalf("LoadEmbeddedTemplates: %v", err)
	}

	doc, err := eng.Generate(context.Background(), GenerateRequest{
		Template:     "equity_profile",
		ScopeGEOID:   "55025",
		ScopeName:    "Dane County, WI",
		AnalysisID:   "test-analysis",
		ChapterCount: 1,
		Selection:    "by_tier",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	html, err := eng.RenderHTML(doc)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}

	if !strings.Contains(html, "<!DOCTYPE html>") {
		t.Error("equity_profile output missing DOCTYPE")
	}
	if !strings.Contains(html, "Equity Profile") {
		t.Error("equity_profile output missing 'Equity Profile'")
	}
}

func TestEngineRenderHTML_ComparisonBrief(t *testing.T) {
	ms := newMockStore()
	eng := NewEngine(ms)
	if err := eng.LoadEmbeddedTemplates(); err != nil {
		t.Fatalf("LoadEmbeddedTemplates: %v", err)
	}

	doc, err := eng.Generate(context.Background(), GenerateRequest{
		Template:     "comparison_brief",
		ScopeGEOID:   "55025",
		ScopeName:    "Dane County, WI",
		AnalysisID:   "test-analysis",
		ChapterCount: 2,
		Selection:    "by_tier",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	html, err := eng.RenderHTML(doc)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}

	if !strings.Contains(html, "<!DOCTYPE html>") {
		t.Error("comparison_brief output missing DOCTYPE")
	}
	if !strings.Contains(html, "Comparison Brief") {
		t.Error("comparison_brief output missing 'Comparison Brief'")
	}
}

func TestEngineUnknownTemplate(t *testing.T) {
	ms := newMockStore()
	eng := NewEngine(ms)
	if err := eng.LoadEmbeddedTemplates(); err != nil {
		t.Fatalf("LoadEmbeddedTemplates: %v", err)
	}

	doc := &Document{
		Title:       "Test",
		Template:    "nonexistent_template",
		GeneratedAt: time.Now(),
		ScopeName:   "Test County",
	}
	_, err := eng.RenderHTML(doc)
	if err == nil {
		t.Error("expected error for unknown template, got nil")
	}
}

func TestSelectByTier(t *testing.T) {
	ms := newMockStore()
	profiles, err := SelectByTier(context.Background(), ms, "test-analysis", 3)
	if err != nil {
		t.Fatalf("SelectByTier: %v", err)
	}
	if len(profiles) == 0 {
		t.Error("SelectByTier returned no profiles")
	}
	// First profile should be the highest-severity tier.
	if profiles[0].NARITier != "critical" && profiles[0].NARITier != "very_high" {
		t.Errorf("first profile tier = %q, want critical or very_high", profiles[0].NARITier)
	}
}

func TestSelectByTierNoResults(t *testing.T) {
	ms := newMockStore()
	profiles, err := SelectByTier(context.Background(), ms, "nonexistent-analysis", 5)
	if err != nil {
		t.Fatalf("SelectByTier with empty results should not error: %v", err)
	}
	if len(profiles) != 0 {
		t.Errorf("expected 0 profiles for nonexistent analysis, got %d", len(profiles))
	}
}

func TestBuildIndicators(t *testing.T) {
	income := 52000.0
	poverty := 18.5
	p := GeographyProfile{
		MedianIncome: &income,
		PovertyRate:  &poverty,
	}
	ivs := buildIndicators(p)
	if len(ivs) == 0 {
		t.Error("buildIndicators returned empty slice for non-nil fields")
	}
	// Check that income is first and has correct formatting.
	found := false
	for _, iv := range ivs {
		if iv.Name == "Median Household Income" {
			found = true
			if iv.Formatted != "$52,000" {
				t.Errorf("income formatted = %q, want $52,000", iv.Formatted)
			}
			break
		}
	}
	if !found {
		t.Error("buildIndicators: Median Household Income not found in output")
	}
}

func TestBuildStatCallouts(t *testing.T) {
	pct := 97.0
	ca := 29.6
	income := 42000.0
	p := GeographyProfile{
		NARIPercentile: &pct,
		NARITier:       "critical",
		ChronicAbsence: &ca,
		MedianIncome:   &income,
	}
	callouts := buildStatCallouts(p)
	if len(callouts) == 0 {
		t.Error("buildStatCallouts returned empty slice")
	}
	if callouts[0].Value != "97th" {
		t.Errorf("first callout value = %q, want 97th", callouts[0].Value)
	}
}

func TestDefaultTitle(t *testing.T) {
	if got := defaultTitle("Dane County, WI", "five_mornings"); !strings.Contains(got, "Dane County") {
		t.Errorf("defaultTitle = %q, want to contain scope name", got)
	}
}

// ─── Table-driven Generate tests ─────────────────────────────────────────────

func TestGenerateTableDriven(t *testing.T) {
	type tc struct {
		name        string
		req         GenerateRequest
		wantChapMin int    // at least this many chapters expected
		wantTitle   string // substring that must appear in doc.Title
	}

	cases := []tc{
		{
			name: "minimal — only required fields, no extra indicators",
			req: GenerateRequest{
				// Template and Selection intentionally omitted to exercise defaults.
				ScopeGEOID:   "55025",
				ScopeName:    "Dane County, WI",
				AnalysisID:   "test-analysis",
				ChapterCount: 1,
			},
			wantChapMin: 1,
			wantTitle:   "Dane County",
		},
		{
			name: "full — all optional fields populated",
			req: GenerateRequest{
				Template:     "equity_profile",
				ScopeGEOID:   "55025",
				ScopeName:    "Dane County, WI",
				AnalysisID:   "test-analysis",
				ChapterCount: 3,
				Selection:    "by_tier",
				Methodology:  "ICE income-race metric (Krieger et al. 2016) with percentile ranking.",
				DataSources:  []string{"US Census ACS 2023", "Princeton Eviction Lab"},
			},
			wantChapMin: 1,
			wantTitle:   "Equity Profile",
		},
		{
			name: "all-nil indicator values — graceful handling, no panic",
			req: GenerateRequest{
				Template:     "five_mornings",
				ScopeGEOID:   "55025",
				ScopeName:    "Null County, WI",
				AnalysisID:   "test-analysis",
				ChapterCount: 1,
				Selection:    "by_tier",
			},
			wantChapMin: 1,
			wantTitle:   "Null County",
		},
	}

	// For the all-nil case, stand up a store whose indicators are all nil-valued.
	nullStore := newMockStore()
	for i := range nullStore.indicators {
		nullStore.indicators[i].Value = nil
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ms := newMockStore()
			if tc.name == "all-nil indicator values — graceful handling, no panic" {
				ms = nullStore
			}

			eng := NewEngine(ms)
			if err := eng.LoadEmbeddedTemplates(); err != nil {
				t.Fatalf("LoadEmbeddedTemplates: %v", err)
			}

			doc, err := eng.Generate(context.Background(), tc.req)
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			if doc == nil {
				t.Fatal("Generate returned nil Document")
			}
			if !strings.Contains(doc.Title, tc.wantTitle) {
				t.Errorf("doc.Title = %q, want substring %q", doc.Title, tc.wantTitle)
			}
			if len(doc.Chapters) < tc.wantChapMin {
				t.Errorf("got %d chapters, want at least %d", len(doc.Chapters), tc.wantChapMin)
			}
			if doc.GeneratedAt.IsZero() {
				t.Error("doc.GeneratedAt should not be zero")
			}
			// Chapter numbers must be sequential starting at 1.
			for i, ch := range doc.Chapters {
				if ch.Number != i+1 {
					t.Errorf("chapter index %d: Number = %d, want %d", i, ch.Number, i+1)
				}
			}
		})
	}
}

// ─── buildPolicyLevers focused tests ─────────────────────────────────────────

func TestBuildPolicyLevers_NoLeversWhenAllNil(t *testing.T) {
	p := GeographyProfile{} // all indicator pointers nil
	levers := buildPolicyLevers(p)
	if len(levers) != 0 {
		t.Errorf("expected 0 levers for profile with all-nil indicators, got %d", len(levers))
	}
}

func TestBuildPolicyLevers_EvictionTrigger(t *testing.T) {
	// Rate just above threshold → eviction lever present.
	above := evictionPolicyThreshold + 0.1
	p := GeographyProfile{EvictionRate: &above}
	levers := buildPolicyLevers(p)
	if !containsCategory(levers, "housing") {
		t.Errorf("expected housing lever for eviction rate %.1f, got %v", above, leverCategories(levers))
	}

	// Rate at or below threshold → no eviction lever.
	at := evictionPolicyThreshold
	p2 := GeographyProfile{EvictionRate: &at}
	levers2 := buildPolicyLevers(p2)
	if containsCategory(levers2, "housing") {
		t.Errorf("expected no housing lever for eviction rate %.1f (at threshold), got one", at)
	}
}

func TestBuildPolicyLevers_TransitTrigger(t *testing.T) {
	// Score just below threshold → transit lever present.
	below := transitScorePolicyThreshold - 1
	p := GeographyProfile{TransitScore: &below}
	levers := buildPolicyLevers(p)
	if !containsCategory(levers, "transit") {
		t.Errorf("expected transit lever for transit score %.1f, got %v", below, leverCategories(levers))
	}

	// Score at threshold → no transit lever.
	at := transitScorePolicyThreshold
	p2 := GeographyProfile{TransitScore: &at}
	levers2 := buildPolicyLevers(p2)
	if containsCategory(levers2, "transit") {
		t.Errorf("expected no transit lever for transit score %.1f (at threshold), got one", at)
	}
}

func TestBuildPolicyLevers_ChronicAbsenceTrigger(t *testing.T) {
	above := chronicAbsencePolicyThreshold + 0.1
	p := GeographyProfile{ChronicAbsence: &above}
	levers := buildPolicyLevers(p)
	if !containsCategory(levers, "education") {
		t.Errorf("expected education lever for absence rate %.1f, got %v", above, leverCategories(levers))
	}
}

func TestBuildPolicyLevers_UninsuredTrigger(t *testing.T) {
	above := uninsuredPolicyThreshold + 0.1
	p := GeographyProfile{UninsuredRate: &above}
	levers := buildPolicyLevers(p)
	if !containsCategory(levers, "health") {
		t.Errorf("expected health lever for uninsured rate %.1f, got %v", above, leverCategories(levers))
	}
}

func TestBuildPolicyLevers_AllLevers(t *testing.T) {
	evict := evictionPolicyThreshold + 1
	transit := transitScorePolicyThreshold - 5
	absence := chronicAbsencePolicyThreshold + 1
	uninsured := uninsuredPolicyThreshold + 1
	p := GeographyProfile{
		EvictionRate:  &evict,
		TransitScore:  &transit,
		ChronicAbsence: &absence,
		UninsuredRate: &uninsured,
	}
	levers := buildPolicyLevers(p)
	if len(levers) != 4 {
		t.Errorf("expected 4 levers when all thresholds exceeded, got %d", len(levers))
	}
}

// ─── Severity helper focused tests ───────────────────────────────────────────

func TestAbsenceSeverity(t *testing.T) {
	cases := []struct {
		v    float64
		want string
	}{
		{absenceCriticalThreshold, "critical"},
		{absenceCriticalThreshold + 1, "critical"},
		{absenceWarningThreshold, "warning"},
		{absenceWarningThreshold + 1, "warning"},
		{absenceNeutralThreshold, "neutral"},
		{absenceNeutralThreshold + 1, "neutral"},
		{absenceNeutralThreshold - 0.1, "positive"},
		{0, "positive"},
	}
	for _, tc := range cases {
		got := absenceSeverity(tc.v)
		if got != tc.want {
			t.Errorf("absenceSeverity(%.1f) = %q, want %q", tc.v, got, tc.want)
		}
	}
}

func TestIncomeSeverity(t *testing.T) {
	cases := []struct {
		v    float64
		want string
	}{
		{incomeCriticalThreshold - 1, "critical"},
		{incomeCriticalThreshold, "warning"}, // not < critical threshold
		{incomeWarningThreshold - 1, "warning"},
		{incomeWarningThreshold, "neutral"},
		{incomeNeutralThreshold - 1, "neutral"},
		{incomeNeutralThreshold, "positive"},
		{incomeNeutralThreshold + 10000, "positive"},
	}
	for _, tc := range cases {
		got := incomeSeverity(tc.v)
		if got != tc.want {
			t.Errorf("incomeSeverity(%.0f) = %q, want %q", tc.v, got, tc.want)
		}
	}
}

func TestEvictionSeverity(t *testing.T) {
	cases := []struct {
		v    float64
		want string
	}{
		{evictionCriticalThreshold + 0.1, "critical"},
		{evictionCriticalThreshold, "warning"}, // not > critical threshold
		{evictionWarningThreshold + 0.1, "warning"},
		{evictionWarningThreshold, "neutral"}, // not > warning threshold
		{evictionNeutralThreshold + 0.1, "neutral"},
		{evictionNeutralThreshold, "positive"}, // not > neutral threshold
		{0, "positive"},
	}
	for _, tc := range cases {
		got := evictionSeverity(tc.v)
		if got != tc.want {
			t.Errorf("evictionSeverity(%.1f) = %q, want %q", tc.v, got, tc.want)
		}
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func ptr(v float64) *float64 { return &v }

func containsCategory(levers []PolicyLever, cat string) bool {
	for _, l := range levers {
		if l.Category == cat {
			return true
		}
	}
	return false
}

func leverCategories(levers []PolicyLever) []string {
	cats := make([]string, len(levers))
	for i, l := range levers {
		cats[i] = l.Category
	}
	return cats
}
