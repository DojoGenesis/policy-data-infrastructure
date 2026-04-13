// Package narrative provides the narrative generation engine for the policy data
// infrastructure. It selects geographies from analysis results, renders prose using
// Go templates, and outputs self-contained HTML documents suitable for policy audiences.
package narrative

// SlotType defines the kind of data a template slot expects.
type SlotType string

const (
	SlotGeographyProfile SlotType = "geography_profile"
	SlotIndicatorValue   SlotType = "indicator_value"
	SlotComparisonPair   SlotType = "comparison_pair"
	SlotRegressionResult SlotType = "regression_result"
	SlotPolicyLever      SlotType = "policy_lever"
	SlotMapEmbed         SlotType = "map_embed"
	SlotChartEmbed       SlotType = "chart_embed"
	SlotDataTable        SlotType = "data_table"
	SlotStatCallout      SlotType = "stat_callout"
)

// GeographyProfile contains all data for one tract/area in a narrative.
type GeographyProfile struct {
	GEOID          string
	Name           string
	Level          string
	NARIScore      *float64
	NARIPercentile *float64
	NARITier       string
	Population     int
	MedianIncome   *float64
	PovertyRate    *float64
	PctPOC         *float64
	UninsuredRate  *float64
	CostBurdenRate *float64
	EvictionRate   *float64
	TransitScore   *float64
	ChronicAbsence *float64
	// Computed context
	IncomePercentile  *float64
	PovertyPercentile *float64
	ScopeLevel        string // "county", "state", "national"
	ScopeName         string // "Dane County", "Wisconsin", "United States"
}

// IndicatorValue holds a single named measurement with formatting metadata.
type IndicatorValue struct {
	Name       string
	Value      *float64
	Formatted  string   // pre-formatted: "$52,000", "23.4%", "1.2 per 100"
	Percentile *float64
	Direction  string   // "higher_better", "lower_better"
	Benchmark  string   // "county median: $48,000"
}

// PolicyLever describes an evidence-backed intervention.
type PolicyLever struct {
	Title       string
	Description string
	Evidence    []string // citations
	Impact      string   // "high", "moderate"
	Category    string   // "housing", "transit", "education", "health"
}

// StatCallout is a big-number highlight for display in a stat row.
type StatCallout struct {
	Value    string // "23.4%"
	Label    string // "chronic absence rate"
	Context  string // "3rd highest in county"
	Severity string // "critical", "warning", "neutral", "positive"
}
