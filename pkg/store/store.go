// Package store defines the primary data-access interface and supporting types
// for the policy data infrastructure. The Store interface is the single point
// of contact between the application layer and the database; callers depend
// only on this interface, not on any concrete implementation.
package store

import (
	"context"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
)

// VariableMeta holds human-readable metadata for one indicator variable,
// joined with its source name from indicator_sources.
type VariableMeta struct {
	VariableID  string
	SourceID    string
	SourceName  string
	Name        string
	Description string
	Unit        string
	Direction   string
}

// Indicator represents a single data point for a geography.
type Indicator struct {
	GEOID         string
	VariableID    string
	Vintage       string
	Value         *float64
	MarginOfError *float64
	RawValue      string
}

// IndicatorQuery filters indicators.
type IndicatorQuery struct {
	GEOIDs      []string
	VariableIDs []string
	Vintage     string
	LatestOnly  bool
}

// AnalysisSummary is a lightweight summary of an analysis run for listing.
type AnalysisSummary struct {
	ID         string
	Type       string
	ScopeGEOID string
	ScopeLevel string
	Vintage    string
	ComputedAt string // ISO 8601 timestamp
	ScoreCount int    // number of scores in this analysis
}

// AnalysisResult represents a computed analysis.
type AnalysisResult struct {
	ID         string
	Type       string
	ScopeGEOID string
	ScopeLevel string
	Parameters map[string]interface{}
	Results    map[string]interface{}
	Vintage    string
}

// AnalysisScore is a per-geography score from an analysis.
type AnalysisScore struct {
	AnalysisID string
	GEOID      string
	Score      float64
	Rank       int
	Percentile float64
	Tier       string
	Details    map[string]interface{}
}

// GeoQuery filters geographies.
type GeoQuery struct {
	Level       geo.Level
	ParentGEOID string
	StateFIPS   string
	CountyFIPS  string
	NameSearch  string // fuzzy search via pg_trgm
	Limit       int
	Offset      int
}

// AggregateQuery specifies an aggregation.
type AggregateQuery struct {
	VariableID string
	Level      geo.Level
	StateFIPS  string
	Function   string // "avg", "sum", "min", "max", "stddev", "count"
}

// AggregateResult is the result of an aggregation.
type AggregateResult struct {
	Value   float64
	Count   int
	GroupBy string
}

// PolicyRecord represents a single policy position for a candidate.
type PolicyRecord struct {
	ID                string
	Candidate         string
	Office            string
	State             string
	Category          string
	Title             string
	Description       string
	BillReferences    string
	ClaimsEmpirical   string
	EquityDimension   string
	GeographicScope   string
	DataSourcesNeeded string
	SourceURL         string
}

// PolicyQuery filters policy records.
type PolicyQuery struct {
	Candidate string
	Category  string
	State     string
	Limit     int
	Offset    int
}

// Store is the primary data access interface.
type Store interface {
	// Geography operations
	PutGeographies(ctx context.Context, geos []geo.Geography) error
	GetGeography(ctx context.Context, geoid string) (*geo.Geography, error)
	QueryGeographies(ctx context.Context, q GeoQuery) ([]geo.Geography, error)

	// Indicator operations
	PutIndicators(ctx context.Context, indicators []Indicator) error
	PutIndicatorsBatch(ctx context.Context, indicators []Indicator, batchSize int) error
	QueryIndicators(ctx context.Context, q IndicatorQuery) ([]Indicator, error)
	Aggregate(ctx context.Context, q AggregateQuery) (*AggregateResult, error)

	// Analysis operations
	// PutAnalysis persists an AnalysisResult and returns the database-generated UUID.
	PutAnalysis(ctx context.Context, result AnalysisResult) (string, error)
	GetAnalysis(ctx context.Context, id string) (*AnalysisResult, error)
	PutAnalysisScores(ctx context.Context, scores []AnalysisScore) error
	QueryAnalysisScores(ctx context.Context, analysisID string, tier string) ([]AnalysisScore, error)
	ListAnalyses(ctx context.Context) ([]AnalysisSummary, error)

	// Policy operations
	PutPolicies(ctx context.Context, policies []PolicyRecord) error
	QueryPolicies(ctx context.Context, q PolicyQuery) ([]PolicyRecord, error)
	GetPolicy(ctx context.Context, id string) (*PolicyRecord, error)

	// Metadata operations
	QueryVariables(ctx context.Context) ([]VariableMeta, error)

	// Lifecycle
	Ping(ctx context.Context) error
	Migrate(ctx context.Context) error
	RefreshViews(ctx context.Context) error
	Close() error
}
