package htmlcraft

import (
	"context"
	"strings"
	"testing"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// ─── stub store ──────────────────────────────────────────────────────────────

// stubStore implements store.Store for in-process tests. All methods return
// minimal valid data; none hit a database.
type stubStore struct {
	geography  *geo.Geography
	children   []geo.Geography
	indicators []store.Indicator
}

func (s *stubStore) PutGeographies(_ context.Context, _ []geo.Geography) error { return nil }
func (s *stubStore) GetGeography(_ context.Context, _ string) (*geo.Geography, error) {
	return s.geography, nil
}
func (s *stubStore) QueryGeographies(_ context.Context, _ store.GeoQuery) ([]geo.Geography, error) {
	return s.children, nil
}
func (s *stubStore) PutIndicators(_ context.Context, _ []store.Indicator) error { return nil }
func (s *stubStore) PutIndicatorsBatch(_ context.Context, _ []store.Indicator, _ int) error {
	return nil
}
func (s *stubStore) QueryIndicators(_ context.Context, _ store.IndicatorQuery) ([]store.Indicator, error) {
	return s.indicators, nil
}
func (s *stubStore) Aggregate(_ context.Context, _ store.AggregateQuery) (*store.AggregateResult, error) {
	return &store.AggregateResult{}, nil
}
func (s *stubStore) PutAnalysis(_ context.Context, _ store.AnalysisResult) (string, error) {
	return "stub-id", nil
}
func (s *stubStore) PutAnalysisScores(_ context.Context, _ []store.AnalysisScore) error { return nil }
func (s *stubStore) QueryAnalysisScores(_ context.Context, _ string, _ string) ([]store.AnalysisScore, error) {
	return nil, nil
}
func (s *stubStore) Ping(_ context.Context) error         { return nil }
func (s *stubStore) Migrate(_ context.Context) error     { return nil }
func (s *stubStore) RefreshViews(_ context.Context) error { return nil }
func (s *stubStore) Close() error                         { return nil }

// ─── helpers ──────────────────────────────────────────────────────────────────

func countyGeo() *geo.Geography {
	return &geo.Geography{
		GEOID:       "55025",
		Level:       geo.County,
		ParentGEOID: "55",
		Name:        "Dane County",
		StateFIPS:   "55",
		CountyFIPS:  "025",
		Lat:         43.07,
		Lon:         -89.40,
	}
}

// ─── EmbedData ────────────────────────────────────────────────────────────────

func TestEmbedData_SingleDataset(t *testing.T) {
	ds := []DataSet{
		{Key: "indicators", Data: []map[string]interface{}{{"geoid": "55025", "value": 42.5}}},
	}
	out, err := EmbedData(ds)
	if err != nil {
		t.Fatalf("EmbedData returned error: %v", err)
	}
	if !strings.Contains(out, "<script>") {
		t.Error("output missing <script> tag")
	}
	if !strings.Contains(out, "window.__DATA__") {
		t.Error("output missing window.__DATA__ assignment")
	}
	if !strings.Contains(out, `"indicators"`) {
		t.Error("output missing 'indicators' key")
	}
	if !strings.Contains(out, "55025") {
		t.Error("output missing geoid value '55025'")
	}
}

func TestEmbedData_MultipleDatasets(t *testing.T) {
	f := 1.5
	ds := []DataSet{
		{Key: "scope", Data: map[string]interface{}{"name": "Dane County"}},
		{Key: "indicators", Data: []store.Indicator{{GEOID: "55025", VariableID: "B19013", Value: &f}}},
	}
	out, err := EmbedData(ds)
	if err != nil {
		t.Fatalf("EmbedData returned error: %v", err)
	}
	if !strings.Contains(out, `"scope"`) {
		t.Error("output missing 'scope' key")
	}
	if !strings.Contains(out, `"indicators"`) {
		t.Error("output missing 'indicators' key")
	}
}

func TestEmbedData_EmptySlice(t *testing.T) {
	out, err := EmbedData([]DataSet{})
	if err != nil {
		t.Fatalf("EmbedData(empty) returned error: %v", err)
	}
	if !strings.Contains(out, "window.__DATA__") {
		t.Error("empty dataset embed still needs window.__DATA__")
	}
}

func TestEmbedData_NilData(t *testing.T) {
	ds := []DataSet{{Key: "empty", Data: nil}}
	out, err := EmbedData(ds)
	if err != nil {
		t.Fatalf("EmbedData with nil data returned error: %v", err)
	}
	if !strings.Contains(out, `"empty"`) {
		t.Error("output missing 'empty' key")
	}
}

func TestEmbedData_OutputFormat(t *testing.T) {
	ds := []DataSet{{Key: "x", Data: 1}}
	out, err := EmbedData(ds)
	if err != nil {
		t.Fatalf("EmbedData returned error: %v", err)
	}
	// Must be a single self-contained <script> tag.
	if !strings.HasPrefix(out, "<script>") {
		t.Error("output must start with <script>")
	}
	if !strings.HasSuffix(out, "</script>") {
		t.Error("output must end with </script>")
	}
}

// ─── RenderFull ───────────────────────────────────────────────────────────────

func TestRenderFull_BasicStructure(t *testing.T) {
	out := RenderFull("Test Title", "<meta/>", "<p>body</p>", "")
	checks := []string{
		"<!DOCTYPE html>",
		"<html lang=\"en\">",
		"<head>",
		"<body>",
		"</html>",
		"Test Title",
		"<p>body</p>",
	}
	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q", want)
		}
	}
}

func TestRenderFull_EmbedsCSSStyles(t *testing.T) {
	out := RenderFull("T", "", "", "")
	// Design-language classes that must be present in the embedded CSS.
	cssMarkers := []string{
		".pdi-header",
		".pdi-container",
		".pdi-section",
		".pdi-table",
		".pdi-metric-card",
		".pdi-stat-callout",
		".pdi-map",
		"@media print",
	}
	for _, cls := range cssMarkers {
		if !strings.Contains(out, cls) {
			t.Errorf("CSS missing class/rule %q", cls)
		}
	}
}

func TestRenderFull_CDNLinks(t *testing.T) {
	out := RenderFull("T", "", "", "")
	cdnLinks := []string{
		"leaflet@1.9.4",
		"chart.js@4",
	}
	for _, link := range cdnLinks {
		if !strings.Contains(out, link) {
			t.Errorf("output missing CDN reference %q", link)
		}
	}
}

func TestRenderFull_HeadInjected(t *testing.T) {
	head := `<script>window.__DATA__={};</script>`
	out := RenderFull("T", head, "", "")
	if !strings.Contains(out, head) {
		t.Error("custom head content not injected into <head>")
	}
}

func TestRenderFull_ScriptsInjected(t *testing.T) {
	scripts := `<script>console.log("test");</script>`
	out := RenderFull("T", "", "", scripts)
	if !strings.Contains(out, scripts) {
		t.Error("custom scripts not injected into <body>")
	}
}

func TestRenderFull_TitleInHeadAndBody(t *testing.T) {
	out := RenderFull("Policy Brief 2024", "", "", "")
	// Title must appear in the <title> element.
	if !strings.Contains(out, "<title>Policy Brief 2024</title>") {
		t.Error("title not set in <title> element")
	}
}

func TestRenderFull_EmptyStrings_NoSingleFilePanic(t *testing.T) {
	// All-empty call must return valid non-empty HTML without panicking.
	out := RenderFull("", "", "", "")
	if out == "" {
		t.Error("RenderFull returned empty string for empty inputs")
	}
	if !strings.Contains(out, "<!DOCTYPE html>") {
		t.Error("output not a valid HTML document")
	}
}

func TestRenderFull_PercentInCSS_NoFormatVerb(t *testing.T) {
	// Regression: CSS values like "width: 100%" must not be misinterpreted
	// as fmt format verbs. Verify literal % survives in the output.
	out := RenderFull("T", "", "", "")
	if !strings.Contains(out, "100%") {
		t.Error("percent sign in CSS was dropped or corrupted")
	}
}

// ─── StandardComponents ───────────────────────────────────────────────────────

func TestStandardComponents_ReturnsFour(t *testing.T) {
	comps := StandardComponents()
	if len(comps) != 4 {
		t.Errorf("expected 4 standard components, got %d", len(comps))
	}
}

func TestStandardComponents_KnownTags(t *testing.T) {
	wantTags := []string{"data-table", "chart-bar", "metric-card", "stat-callout"}
	comps := StandardComponents()
	got := make(map[string]bool, len(comps))
	for _, c := range comps {
		got[c.Tag] = true
	}
	for _, tag := range wantTags {
		if !got[tag] {
			t.Errorf("standard components missing tag %q", tag)
		}
	}
}

func TestStandardComponents_NonEmptyJSSource(t *testing.T) {
	for _, c := range StandardComponents() {
		if strings.TrimSpace(c.JSSource) == "" {
			t.Errorf("component %q has empty JSSource", c.Tag)
		}
	}
}

func TestStandardComponents_CustomElementsDefine(t *testing.T) {
	// Each component must call customElements.define to register itself.
	for _, c := range StandardComponents() {
		needle := "customElements.define('" + c.Tag + "'"
		if !strings.Contains(c.JSSource, needle) {
			t.Errorf("component %q JSSource missing customElements.define call", c.Tag)
		}
	}
}

// ─── InlineComponents ─────────────────────────────────────────────────────────

func TestInlineComponents_KnownTag(t *testing.T) {
	out := InlineComponents([]string{"data-table"})
	if !strings.Contains(out, "<script>") {
		t.Error("InlineComponents output missing <script> tag")
	}
	if !strings.Contains(out, "data-table") {
		t.Error("InlineComponents output missing data-table element")
	}
}

func TestInlineComponents_MultipleKnownTags(t *testing.T) {
	out := InlineComponents([]string{"data-table", "metric-card", "stat-callout"})
	for _, tag := range []string{"data-table", "metric-card", "stat-callout"} {
		if !strings.Contains(out, tag) {
			t.Errorf("InlineComponents output missing %q", tag)
		}
	}
}

func TestInlineComponents_UnknownTagSkipped(t *testing.T) {
	out := InlineComponents([]string{"nonexistent-component"})
	// Unknown tags should produce no output.
	if strings.Contains(out, "nonexistent-component") {
		t.Error("unknown component tag should be silently skipped")
	}
}

func TestInlineComponents_EmptyList(t *testing.T) {
	out := InlineComponents([]string{})
	// Empty request = empty string (no panic).
	if out != "" {
		t.Errorf("InlineComponents([]) expected empty string, got %q", out)
	}
}

func TestInlineComponents_NilList(t *testing.T) {
	out := InlineComponents(nil)
	if out != "" {
		t.Errorf("InlineComponents(nil) expected empty string, got %q", out)
	}
}

func TestInlineComponents_MixedKnownAndUnknown(t *testing.T) {
	out := InlineComponents([]string{"chart-bar", "bogus"})
	if !strings.Contains(out, "chart-bar") {
		t.Error("known tag 'chart-bar' should still appear when mixed with unknown tags")
	}
	if strings.Contains(out, "bogus") {
		t.Error("unknown tag 'bogus' should not appear in output")
	}
}

func TestInlineComponents_AllStandardTags(t *testing.T) {
	tags := make([]string, 0, 4)
	for _, c := range StandardComponents() {
		tags = append(tags, c.Tag)
	}
	out := InlineComponents(tags)
	for _, tag := range tags {
		if !strings.Contains(out, tag) {
			t.Errorf("InlineComponents output missing expected tag %q", tag)
		}
	}
}

// ─── NewBridge ────────────────────────────────────────────────────────────────

func TestNewBridge_NotNil(t *testing.T) {
	b := NewBridge(&stubStore{})
	if b == nil {
		t.Error("NewBridge returned nil")
	}
}

// ─── Bridge.BuildDeliverable ──────────────────────────────────────────────────

func TestBuildDeliverable_BasicOutput(t *testing.T) {
	s := &stubStore{
		geography:  countyGeo(),
		children:   nil,
		indicators: nil,
	}
	b := NewBridge(s)
	out, err := b.BuildDeliverable(context.Background(), "<p>Narrative</p>", "55025", DeliverableOpts{
		Title: "Dane County Brief",
	})
	if err != nil {
		t.Fatalf("BuildDeliverable returned error: %v", err)
	}
	checks := []string{
		"<!DOCTYPE html>",
		"Dane County Brief",
		"<p>Narrative</p>",
		"window.__DATA__",
		"pdi-header",
		"pdi-container",
	}
	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Errorf("deliverable missing %q", want)
		}
	}
}

func TestBuildDeliverable_DefaultTitle(t *testing.T) {
	s := &stubStore{geography: countyGeo()}
	b := NewBridge(s)
	out, err := b.BuildDeliverable(context.Background(), "", "55025", DeliverableOpts{})
	if err != nil {
		t.Fatalf("BuildDeliverable returned error: %v", err)
	}
	if !strings.Contains(out, "Policy Data Brief") {
		t.Error("default title 'Policy Data Brief' not applied when Title is empty")
	}
}

func TestBuildDeliverable_IndicatorsEmbedded(t *testing.T) {
	v := 15.3
	s := &stubStore{
		geography: countyGeo(),
		indicators: []store.Indicator{
			{GEOID: "55025", VariableID: "B19013", Vintage: "2022", Value: &v, RawValue: "15.3"},
		},
	}
	b := NewBridge(s)
	out, err := b.BuildDeliverable(context.Background(), "", "55025", DeliverableOpts{})
	if err != nil {
		t.Fatalf("BuildDeliverable returned error: %v", err)
	}
	if !strings.Contains(out, "B19013") {
		t.Error("deliverable missing indicator variable ID 'B19013'")
	}
}

func TestBuildDeliverable_WithMapOption(t *testing.T) {
	s := &stubStore{
		geography: countyGeo(),
		children: []geo.Geography{
			{GEOID: "55025000100", Level: geo.Tract, Name: "Tract 1", ParentGEOID: "55025"},
		},
	}
	b := NewBridge(s)
	out, err := b.BuildDeliverable(context.Background(), "<p>Hello</p>", "55025", DeliverableOpts{
		Title:      "Map Test",
		IncludeMap: true,
		TileLayer:  "dark",
	})
	if err != nil {
		t.Fatalf("BuildDeliverable with map returned error: %v", err)
	}
	if !strings.Contains(out, "pdi-map") {
		t.Error("deliverable missing map container 'pdi-map'")
	}
	if !strings.Contains(out, "L.map") {
		t.Error("deliverable missing Leaflet initialisation")
	}
	if !strings.Contains(out, "geojson") {
		t.Error("deliverable missing geojson data key")
	}
}

func TestBuildDeliverable_WithComponents(t *testing.T) {
	s := &stubStore{geography: countyGeo()}
	b := NewBridge(s)
	out, err := b.BuildDeliverable(context.Background(), "", "55025", DeliverableOpts{
		Components: []string{"data-table", "metric-card"},
	})
	if err != nil {
		t.Fatalf("BuildDeliverable with components returned error: %v", err)
	}
	if !strings.Contains(out, "data-table") {
		t.Error("deliverable missing data-table component script")
	}
	if !strings.Contains(out, "metric-card") {
		t.Error("deliverable missing metric-card component script")
	}
}

func TestBuildDeliverable_HTMLEscapingInTitle(t *testing.T) {
	s := &stubStore{geography: countyGeo()}
	b := NewBridge(s)
	const dangerousTitle = `<script>alert('xss')</script>`
	out, err := b.BuildDeliverable(context.Background(), "", "55025", DeliverableOpts{
		Title: dangerousTitle,
	})
	if err != nil {
		t.Fatalf("BuildDeliverable returned error: %v", err)
	}
	// buildBody passes the title through htmlEscape before embedding it in the
	// visible <h1> header, so the escaped form must appear in the document.
	if !strings.Contains(out, "&lt;script&gt;") {
		t.Error("title was not HTML-escaped in the page body <h1> header")
	}
	// Note: RenderFull passes the title raw into <title> (standard browser
	// behaviour — <title> is text content, not executed). The body header
	// (<h1>) is the user-visible surface and must always use htmlEscape.
}

func TestBuildDeliverable_ScopeGeoEmbedded(t *testing.T) {
	s := &stubStore{geography: countyGeo()}
	b := NewBridge(s)
	out, err := b.BuildDeliverable(context.Background(), "", "55025", DeliverableOpts{})
	if err != nil {
		t.Fatalf("BuildDeliverable returned error: %v", err)
	}
	// Scope geography is embedded under the "scope" key.
	if !strings.Contains(out, `"scope"`) {
		t.Error("deliverable missing embedded 'scope' data key")
	}
	if !strings.Contains(out, "Dane County") {
		t.Error("deliverable missing scope geography name 'Dane County'")
	}
}

func TestBuildDeliverable_IndicatorWithNilValue(t *testing.T) {
	s := &stubStore{
		geography: countyGeo(),
		indicators: []store.Indicator{
			{GEOID: "55025", VariableID: "B25003", Vintage: "2022", Value: nil, RawValue: ""},
		},
	}
	b := NewBridge(s)
	out, err := b.BuildDeliverable(context.Background(), "", "55025", DeliverableOpts{})
	if err != nil {
		t.Fatalf("BuildDeliverable with nil indicator value returned error: %v", err)
	}
	// Null value should appear as JSON null in the embedded data.
	if !strings.Contains(out, "B25003") {
		t.Error("deliverable missing indicator with nil value")
	}
}

func TestBuildDeliverable_TileLayerSatellite(t *testing.T) {
	s := &stubStore{geography: countyGeo()}
	b := NewBridge(s)
	out, err := b.BuildDeliverable(context.Background(), "", "55025", DeliverableOpts{
		IncludeMap: true,
		TileLayer:  "satellite",
		MapCenter:  [2]float64{43.07, -89.40},
		MapZoom:    12,
	})
	if err != nil {
		t.Fatalf("BuildDeliverable satellite returned error: %v", err)
	}
	if !strings.Contains(out, "arcgisonline") {
		t.Error("satellite tile layer missing Esri ArcGIS Online URL")
	}
}

func TestBuildDeliverable_MapCenterFallback(t *testing.T) {
	s := &stubStore{geography: countyGeo()}
	b := NewBridge(s)
	// MapCenter is zero — must fall back to scope Lat/Lon.
	out, err := b.BuildDeliverable(context.Background(), "", "55025", DeliverableOpts{
		IncludeMap: true,
	})
	if err != nil {
		t.Fatalf("BuildDeliverable returned error: %v", err)
	}
	// countyGeo has Lat=43.07 — verify it appears in the Leaflet setView call.
	if !strings.Contains(out, "43.07") {
		t.Error("scope Lat 43.07 not used as map centre fallback")
	}
}

// ─── htmlEscape (via BuildDeliverable/buildBody) ──────────────────────────────

func TestHTMLEscape_SpecialChars(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"<b>bold</b>", "&lt;b&gt;bold&lt;/b&gt;"},
		{"a & b", "a &amp; b"},
		{`say "hi"`, "say &#34;hi&#34;"},
		{"it's", "it&#39;s"},
		{"no special chars", "no special chars"},
	}
	for _, tc := range cases {
		got := htmlEscape(tc.input)
		if got != tc.want {
			t.Errorf("htmlEscape(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestHTMLEscape_Empty(t *testing.T) {
	if got := htmlEscape(""); got != "" {
		t.Errorf("htmlEscape(\"\") = %q, want \"\"", got)
	}
}

// ─── DeliverableOpts defaults ─────────────────────────────────────────────────

func TestDeliverableOpts_ZeroValuesSafe(t *testing.T) {
	s := &stubStore{geography: countyGeo()}
	b := NewBridge(s)
	// Zero-value opts: must not panic, must produce valid HTML.
	out, err := b.BuildDeliverable(context.Background(), "", "55025", DeliverableOpts{})
	if err != nil {
		t.Fatalf("zero-value opts returned error: %v", err)
	}
	if !strings.Contains(out, "<!DOCTYPE html>") {
		t.Error("zero-value opts: output is not a valid HTML document")
	}
}
