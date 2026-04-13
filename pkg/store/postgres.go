package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// PostgresStore is the PostgreSQL implementation of Store backed by a pgxpool.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore creates a pgxpool from connString, runs all pending
// migrations, and returns a ready-to-use *PostgresStore.
func NewPostgresStore(ctx context.Context, connString string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("store: open pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}

	s := &PostgresStore{pool: pool}
	if err := s.Migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: migrate: %w", err)
	}

	return s, nil
}

// Close releases all connections in the pool.
func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

// Migrate reads all *.up.sql files from the embedded migrations directory,
// sorts them lexicographically, and executes each one inside a transaction.
// Migrations are idempotent by design (IF NOT EXISTS guards in every file).
func (s *PostgresStore) Migrate(ctx context.Context) error {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("store: read migrations dir: %w", err)
	}

	// Collect and sort .up.sql files only.
	var upFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			upFiles = append(upFiles, e.Name())
		}
	}
	sort.Strings(upFiles)

	for _, name := range upFiles {
		data, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("store: read migration %s: %w", name, err)
		}

		sql := strings.TrimSpace(string(data))
		if sql == "" {
			continue
		}

		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("store: begin tx for %s: %w", name, err)
		}

		if _, err := tx.Exec(ctx, sql); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("store: exec migration %s: %w", name, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("store: commit migration %s: %w", name, err)
		}
	}

	return nil
}

// RefreshViews refreshes any materialized views maintained by the schema.
// Currently a no-op stub until the view layer is implemented.
func (s *PostgresStore) RefreshViews(ctx context.Context) error {
	return fmt.Errorf("store: RefreshViews: not implemented")
}

// --- Geography operations ---

// PutGeographies upserts a batch of Geography records.
func (s *PostgresStore) PutGeographies(ctx context.Context, geos []geo.Geography) error {
	return fmt.Errorf("store: PutGeographies: not implemented")
}

// GetGeography retrieves a single Geography by GEOID.
func (s *PostgresStore) GetGeography(ctx context.Context, geoid string) (*geo.Geography, error) {
	return nil, fmt.Errorf("store: GetGeography: not implemented")
}

// QueryGeographies returns geographies matching the given filter.
func (s *PostgresStore) QueryGeographies(ctx context.Context, q GeoQuery) ([]geo.Geography, error) {
	return nil, fmt.Errorf("store: QueryGeographies: not implemented")
}

// --- Indicator operations ---

// PutIndicators upserts a batch of Indicator records.
func (s *PostgresStore) PutIndicators(ctx context.Context, indicators []Indicator) error {
	return fmt.Errorf("store: PutIndicators: not implemented")
}

// QueryIndicators returns indicators matching the given filter.
func (s *PostgresStore) QueryIndicators(ctx context.Context, q IndicatorQuery) ([]Indicator, error) {
	return nil, fmt.Errorf("store: QueryIndicators: not implemented")
}

// Aggregate runs a statistical aggregation over a variable at a given level.
func (s *PostgresStore) Aggregate(ctx context.Context, q AggregateQuery) (*AggregateResult, error) {
	return nil, fmt.Errorf("store: Aggregate: not implemented")
}

// --- Analysis operations ---

// PutAnalysis persists an AnalysisResult record.
func (s *PostgresStore) PutAnalysis(ctx context.Context, result AnalysisResult) error {
	return fmt.Errorf("store: PutAnalysis: not implemented")
}

// PutAnalysisScores persists a batch of AnalysisScore records.
func (s *PostgresStore) PutAnalysisScores(ctx context.Context, scores []AnalysisScore) error {
	return fmt.Errorf("store: PutAnalysisScores: not implemented")
}

// QueryAnalysisScores returns scores for an analysis, optionally filtered by tier.
func (s *PostgresStore) QueryAnalysisScores(ctx context.Context, analysisID string, tier string) ([]AnalysisScore, error) {
	return nil, fmt.Errorf("store: QueryAnalysisScores: not implemented")
}
