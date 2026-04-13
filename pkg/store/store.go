// Package store defines the primary data-access interface and supporting types
// for the policy data infrastructure. The Store interface is the single point
// of contact between the application layer and the database; callers depend
// only on this interface, not on any concrete implementation.
package store

import (
	"context"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
)

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

// Store is the primary data access interface.
type Store interface {
	// Geography operations
	PutGeographies(ctx context.Context, geos []geo.Geography) error
	GetGeography(ctx context.Context, geoid string) (*geo.Geography, error)
	QueryGeographies(ctx context.Context, q GeoQuery) ([]geo.Geography, error)

	// Indicator operations
	PutIndicators(ctx context.Context, indicators []Indicator) error
	QueryIndicators(ctx context.Context, q IndicatorQuery) ([]Indicator, error)
	Aggregate(ctx context.Context, q AggregateQuery) (*AggregateResult, error)

	// Analysis operations
	PutAnalysis(ctx context.Context, result AnalysisResult) error
	PutAnalysisScores(ctx context.Context, scores []AnalysisScore) error
	QueryAnalysisScores(ctx context.Context, analysisID string, tier string) ([]AnalysisScore, error)

	// Lifecycle
	Migrate(ctx context.Context) error
	RefreshViews(ctx context.Context) error
	Close() error
}
