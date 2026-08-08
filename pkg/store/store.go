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

// SourceMeta describes a data source, one row of indicator_sources.
//
// URL and Description are richer in migrations/seed_sources.sql than anything a
// DataSource adapter can report, so RegisterSource does not overwrite an
// existing row with them — it only guarantees the row exists so the
// indicator_meta foreign key can be satisfied.
type SourceMeta struct {
	SourceID    string
	Name        string
	Category    string
	URL         string
	Description string
}

// Indicator represents a single data point for a geography.
type Indicator struct {
	GEOID         string
	VariableID    string
	Vintage       string
	Value         *float64
	MarginOfError *float64
	CV            *float64 // coefficient of variation
	Reliability   string   // "high", "moderate", "low", or ""
	RawValue      string
}

// IndicatorQuery filters indicators.
type IndicatorQuery struct {
	GEOIDs      []string
	VariableIDs []string
	Vintage     string   // single vintage (legacy; parsed from query param)
	Vintages    []string // multiple vintages (comma-separated query param)
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

// AnalysisKey is the ADR-014 D9 cache identity of an analysis:
// (type, scope_geoid, scope_level, vintage, parameters). Vintage is part of
// the key, never metadata — two runs over different vintages are different
// analyses. Empty ScopeGEOID/ScopeLevel denote NULL scope columns.
type AnalysisKey struct {
	Type       string
	ScopeGEOID string
	ScopeLevel string
	Vintage    string
	Parameters map[string]interface{}
}

// AnalysisScore is a per-geography score from an analysis.
type AnalysisScore struct {
	AnalysisID string
	GEOID      string
	Score      float64
		Rank       *int
	Percentile float64
	Tier       string
	Details    map[string]interface{}
}

// AnalysisRun is one row of the DB-backed run queue (ADR-014 D3). Status
// moves queued → running → done|failed; AnalysisID points at the cache entry
// once the run completes.
type AnalysisRun struct {
	ID          string
	RunType     string
	ScopeGEOID  string
	ScopeLevel  string
	Vintage     string
	Parameters  map[string]interface{}
	Status      string
	Error       string
	AnalysisID  string
	ClientKey   string
	RequestedAt string
	StartedAt   string
	FinishedAt  string
}

// GeoQuery filters geographies.
//
// Retirement filtering (migration 013): geographies carry an explicit
// retired_at marker. A retired geography is one that no longer exists in the
// current census vintage (e.g. a 2010-vintage tract superseded by 2020
// redistricting) but whose historical indicator data is still needed for
// temporal analysis (ADR-012 §Integration 5). The zero value of GeoQuery
// therefore describes the CURRENT world: retired rows are excluded unless a
// caller opts in.
type GeoQuery struct {
	Level       geo.Level
	ParentGEOID string
	StateFIPS   string
	CountyFIPS  string
	NameSearch  string // fuzzy search via pg_trgm

	// IncludeRetired returns retired geographies alongside current ones.
	// Default (false) returns current geographies only.
	IncludeRetired bool
	// RetiredOnly restricts the result to retired geographies. It implies
	// IncludeRetired and takes precedence over it when both are set.
	RetiredOnly bool

	Limit  int
	Offset int
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

// FactorScore represents one factor score for a geography from a statistical
// factor analysis (e.g. PCA). LoadingsJSON carries per-variable loadings as raw JSON.
type FactorScore struct {
	GEOID            string
	FactorName       string
	FactorScore      *float64
	FactorPercentile *float64
	LoadingsJSON     string
	AnalysisVintage  string
}

// ValidatedFeature represents a verified data point with an explicit source
// citation, providing an audited alternative to raw indicators.
type ValidatedFeature struct {
	GEOID           string
	FeatureName     string
	FeatureValue    *float64
	SourceCitation  string
	AnalysisVintage string
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

// EvidenceCard pairs a policy position with indicator-derived evidence,
// including findings, linked indicators, and statewide context as JSONB.
// Primary key is a SERIAL id; policy_id links to the policies table.
type EvidenceCard struct {
	ID                 int
	PolicyID           string
	PolicyTitle        string
	Category           string
	EquityDimension    string
	Title              string
	KeyFinding         string
	DataQuality        string
	Findings           []byte // JSONB
	Indicators         []byte // JSONB
	StatewideContext   []byte // JSONB
	CountyVariation    []byte // JSONB
	TopNeedCounties    []byte // JSONB
	BottomNeedCounties []byte // JSONB
}

// LISACountyProfile summarizes LISA cluster membership for tracts
// within a county. Each entry holds the cluster label and the count
// of tracts in that cluster.
type LISAClusterEntry struct {
	Cluster string `json:"cluster"`
	Count   int    `json:"count"`
}

// LISACountyProfile provides a county-level summary of LISA clusters
// computed from tract-level analysis_scores.
type LISACountyProfile struct {
	GEOID   string            `json:"geoid"`
	Clusters []LISAClusterEntry `json:"clusters"`
	TotalTracts int           `json:"total_tracts"`
}

// EvidenceCardFilter provides optional query-parameter filters for
// listing evidence cards.
type EvidenceCardFilter struct {
	Category        string
	EquityDimension string
	PolicyID        string
	Limit           int
	Offset          int
}

// Store is the primary data access interface.
type Store interface {
	// Geography operations
	PutGeographies(ctx context.Context, geos []geo.Geography) error
	// GetGeography looks up one geography by exact GEOID. Naming a GEOID is
	// itself an explicit request for that row, so retired geographies are
	// returned here — this is what keeps historical profiles and deep links
	// resolvable. Use QueryGeographies for lifecycle-filtered listings.
	GetGeography(ctx context.Context, geoid string) (*geo.Geography, error)
	// QueryGeographies lists geographies matching q. Retired geographies are
	// excluded unless q.IncludeRetired or q.RetiredOnly is set.
	QueryGeographies(ctx context.Context, q GeoQuery) ([]geo.Geography, error)
	// CountGeographies returns the number of geographies matching q's filters,
	// ignoring q.Limit and q.Offset. Callers use it to report an accurate
	// pagination total rather than the size of the current page.
	CountGeographies(ctx context.Context, q GeoQuery) (int, error)

	// Indicator operations
	PutIndicators(ctx context.Context, indicators []Indicator) error
	PutIndicatorsBatch(ctx context.Context, indicators []Indicator, batchSize int) error
	QueryIndicators(ctx context.Context, q IndicatorQuery) ([]Indicator, error)
	Aggregate(ctx context.Context, q AggregateQuery) (*AggregateResult, error)

	// Analysis operations
	// PutAnalysis persists an AnalysisResult and returns its UUID. Identity
	// is the AnalysisKey tuple (ADR-014 D9): an identical re-run refreshes
	// the existing row and returns the same UUID rather than duplicating.
	PutAnalysis(ctx context.Context, result AnalysisResult) (string, error)
	// FindAnalysisByKey returns the analysis with exactly this cache key, or
	// nil (with nil error) when none exists.
	FindAnalysisByKey(ctx context.Context, key AnalysisKey) (*AnalysisSummary, error)
	GetAnalysis(ctx context.Context, id string) (*AnalysisResult, error)
	PutAnalysisScores(ctx context.Context, scores []AnalysisScore) error
	QueryAnalysisScores(ctx context.Context, analysisID string, tier string) ([]AnalysisScore, error)
	ListAnalyses(ctx context.Context) ([]AnalysisSummary, error)

	// Run queue operations (ADR-014 D3). CreateAnalysisRun enqueues and
	// returns the run id. ClaimNextAnalysisRun atomically claims the oldest
	// queued run (nil, nil when the queue is empty). CompleteAnalysisRun
	// marks done when errMsg is empty, failed otherwise. CountActiveRuns
	// counts queued+running rows for queue-depth admission.
	CreateAnalysisRun(ctx context.Context, run AnalysisRun) (string, error)
	GetAnalysisRun(ctx context.Context, id string) (*AnalysisRun, error)
	ClaimNextAnalysisRun(ctx context.Context) (*AnalysisRun, error)
	CompleteAnalysisRun(ctx context.Context, id, analysisID, errMsg string) error
	CountActiveRuns(ctx context.Context) (int, error)
	// LatestVintageForVariable resolves "latest" to a concrete vintage at
	// enqueue time ("" when the variable has no data).
	LatestVintageForVariable(ctx context.Context, variableID string) (string, error)

	// Factor & validated feature operations
	PutFactorScores(ctx context.Context, scores []FactorScore) error
	QueryFactorScores(ctx context.Context, geoid string) ([]FactorScore, error)
	QueryValidatedFeatures(ctx context.Context, scopeGEOID string) ([]ValidatedFeature, error)

	// Policy operations
	PutPolicies(ctx context.Context, policies []PolicyRecord) error
	QueryPolicies(ctx context.Context, q PolicyQuery) ([]PolicyRecord, error)
	GetPolicy(ctx context.Context, id string) (*PolicyRecord, error)

	// Evidence card operations
	PutEvidenceCards(ctx context.Context, cards []EvidenceCard) error
	QueryEvidenceCards(ctx context.Context, filter EvidenceCardFilter) ([]EvidenceCard, error)
	SeedEvidenceCardsFromJSON(ctx context.Context, jsonData []byte) error

	// LISA county profile
	QueryLISACountyProfile(ctx context.Context, countyGEOID string) (*LISACountyProfile, error)

	// Metadata operations
	QueryVariables(ctx context.Context) ([]VariableMeta, error)

	// CountSourcesWithData returns how many distinct sources have at least one
	// stored indicator row. This is smaller than the number of registered
	// sources, and much smaller than the number of shipped adapters: a source
	// can be catalogued, and have every one of its variables described, while
	// holding no data at all. Distinguishing the two is ADR-014's central point.
	CountSourcesWithData(ctx context.Context) (int, error)

	// RegisterSource upserts a source row and its variable definitions in one
	// transaction, in FK order. Indicators reference indicator_meta, which
	// references indicator_sources, so a fetch that writes indicators without
	// this having run first fails on a constraint violation that reads like a
	// broken fetcher rather than a missing registration.
	RegisterSource(ctx context.Context, src SourceMeta, vars []VariableMeta) error

	// Lifecycle
	Ping(ctx context.Context) error
	Migrate(ctx context.Context) error
	RefreshViews(ctx context.Context) error
	Close() error
}
