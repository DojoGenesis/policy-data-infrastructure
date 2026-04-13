package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// PostgresStore is the PostgreSQL implementation of Store backed by a pgxpool.
type PostgresStore struct {
	pool       *pgxpool.Pool
	hasPostGIS bool // true when PostGIS extension is installed and centroid column exists
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

	// Detect PostGIS: check if centroid column exists on geographies table.
	var hasCentroid bool
	_ = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'geographies' AND column_name = 'centroid'
		)`).Scan(&hasCentroid)
	s.hasPostGIS = hasCentroid

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

// RefreshViews refreshes the indicators_latest materialized view concurrently
// so reads are not blocked during the refresh.
func (s *PostgresStore) RefreshViews(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, "REFRESH MATERIALIZED VIEW CONCURRENTLY indicators_latest")
	if err != nil {
		return fmt.Errorf("store: RefreshViews: %w", err)
	}
	return nil
}

// --- Geography operations ---

// PutGeographies upserts a batch of Geography records using a pgx Batch.
// Each row is upserted via ON CONFLICT (geoid) DO UPDATE. The boundary column
// is populated from a GeoJSON string when present; centroid is derived from
// the boundary geometry using ST_Centroid.
func (s *PostgresStore) PutGeographies(ctx context.Context, geos []geo.Geography) error {
	if len(geos) == 0 {
		return nil
	}

	var upsertSQL string
	if s.hasPostGIS {
		upsertSQL = `
INSERT INTO geographies
    (geoid, level, parent_geoid, name, state_fips, county_fips, population, land_area_m2, boundary, centroid)
VALUES (
    $1, $2::geo_level,
    NULLIF($3, ''),
    $4,
    NULLIF($5, ''),
    NULLIF($6, ''),
    $7, $8,
    CASE WHEN $9 = '' THEN NULL ELSE ST_GeomFromGeoJSON($9) END,
    CASE WHEN $9 = '' THEN NULL ELSE ST_Centroid(ST_GeomFromGeoJSON($9)) END
)
ON CONFLICT (geoid) DO UPDATE SET
    level        = EXCLUDED.level,
    parent_geoid = EXCLUDED.parent_geoid,
    name         = EXCLUDED.name,
    state_fips   = EXCLUDED.state_fips,
    county_fips  = EXCLUDED.county_fips,
    population   = EXCLUDED.population,
    land_area_m2 = EXCLUDED.land_area_m2,
    boundary     = EXCLUDED.boundary,
    centroid     = EXCLUDED.centroid,
    updated_at   = now()`
	} else {
		upsertSQL = `
INSERT INTO geographies
    (geoid, level, parent_geoid, name, state_fips, county_fips, population, land_area_m2)
VALUES (
    $1, $2::geo_level,
    NULLIF($3, ''),
    $4,
    NULLIF($5, ''),
    NULLIF($6, ''),
    $7, $8
)
ON CONFLICT (geoid) DO UPDATE SET
    level        = EXCLUDED.level,
    parent_geoid = EXCLUDED.parent_geoid,
    name         = EXCLUDED.name,
    state_fips   = EXCLUDED.state_fips,
    county_fips  = EXCLUDED.county_fips,
    population   = EXCLUDED.population,
    land_area_m2 = EXCLUDED.land_area_m2,
    updated_at   = now()`
	}

	batch := &pgx.Batch{}
	for _, g := range geos {
		if s.hasPostGIS {
			batch.Queue(upsertSQL,
				g.GEOID,
				string(g.Level),
				g.ParentGEOID,
				g.Name,
				g.StateFIPS,
				g.CountyFIPS,
				g.Population,
				g.LandAreaM2,
				"", // boundary GeoJSON placeholder
			)
		} else {
			batch.Queue(upsertSQL,
				g.GEOID,
				string(g.Level),
				g.ParentGEOID,
				g.Name,
				g.StateFIPS,
				g.CountyFIPS,
				g.Population,
				g.LandAreaM2,
			)
		}
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := range geos {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("store: PutGeographies[%d] geoid=%s: %w", i, geos[i].GEOID, err)
		}
	}
	return nil
}

// GetGeography retrieves a single Geography by GEOID, returning the boundary
// as a GeoJSON string in the BoundaryGeoJSON field of the returned record.
// The Lat/Lon fields are populated from the stored centroid.
func (s *PostgresStore) GetGeography(ctx context.Context, geoid string) (*geo.Geography, error) {
	var q string
	if s.hasPostGIS {
		q = `
SELECT
    geoid, level, COALESCE(parent_geoid,''), name,
    COALESCE(state_fips,''), COALESCE(county_fips,''),
    COALESCE(population, 0), COALESCE(land_area_m2, 0),
    COALESCE(ST_Y(centroid), 0),
    COALESCE(ST_X(centroid), 0)
FROM geographies
WHERE geoid = $1`
	} else {
		q = `
SELECT
    geoid, level, COALESCE(parent_geoid,''), name,
    COALESCE(state_fips,''), COALESCE(county_fips,''),
    COALESCE(population, 0), COALESCE(land_area_m2, 0),
    0::float8, 0::float8
FROM geographies
WHERE geoid = $1`
	}

	row := s.pool.QueryRow(ctx, q, geoid)

	var g geo.Geography
	var level string
	err := row.Scan(
		&g.GEOID, &level, &g.ParentGEOID, &g.Name,
		&g.StateFIPS, &g.CountyFIPS,
		&g.Population, &g.LandAreaM2,
		&g.Lat, &g.Lon,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("store: GetGeography: geoid %q not found", geoid)
		}
		return nil, fmt.Errorf("store: GetGeography: %w", err)
	}
	g.Level = geo.Level(level)
	return &g, nil
}

// QueryGeographies returns geographies matching the given filter. All filter
// fields are optional; an empty GeoQuery returns everything up to Limit rows.
func (s *PostgresStore) QueryGeographies(ctx context.Context, q GeoQuery) ([]geo.Geography, error) {
	args := []interface{}{}
	idx := 1

	var where []string

	if q.Level != "" {
		where = append(where, fmt.Sprintf("level = $%d::geo_level", idx))
		args = append(args, string(q.Level))
		idx++
	}
	if q.ParentGEOID != "" {
		where = append(where, fmt.Sprintf("parent_geoid = $%d", idx))
		args = append(args, q.ParentGEOID)
		idx++
	}
	if q.StateFIPS != "" {
		where = append(where, fmt.Sprintf("state_fips = $%d", idx))
		args = append(args, q.StateFIPS)
		idx++
	}
	if q.CountyFIPS != "" {
		where = append(where, fmt.Sprintf("county_fips = $%d", idx))
		args = append(args, q.CountyFIPS)
		idx++
	}
	if q.NameSearch != "" {
		where = append(where, fmt.Sprintf("name ILIKE '%%' || $%d || '%%'", idx))
		args = append(args, q.NameSearch)
		idx++
	}

	whereClause := ""
	if len(where) > 0 {
		whereClause = "WHERE " + strings.Join(where, " AND ")
	}

	limit := q.Limit
	if limit <= 0 {
		limit = 1000
	}
	offset := q.Offset

	latLonExpr := "0::float8, 0::float8"
	if s.hasPostGIS {
		latLonExpr = "COALESCE(ST_Y(centroid), 0), COALESCE(ST_X(centroid), 0)"
	}
	sql := fmt.Sprintf(`
SELECT
    geoid, level, COALESCE(parent_geoid,''), name,
    COALESCE(state_fips,''), COALESCE(county_fips,''),
    COALESCE(population, 0), COALESCE(land_area_m2, 0),
    %s
FROM geographies
%s
ORDER BY geoid
LIMIT %d OFFSET %d`, latLonExpr, whereClause, limit, offset)

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("store: QueryGeographies: %w", err)
	}
	defer rows.Close()

	var result []geo.Geography
	for rows.Next() {
		var g geo.Geography
		var level string
		if err := rows.Scan(
			&g.GEOID, &level, &g.ParentGEOID, &g.Name,
			&g.StateFIPS, &g.CountyFIPS,
			&g.Population, &g.LandAreaM2,
			&g.Lat, &g.Lon,
		); err != nil {
			return nil, fmt.Errorf("store: QueryGeographies scan: %w", err)
		}
		g.Level = geo.Level(level)
		result = append(result, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: QueryGeographies rows: %w", err)
	}
	return result, nil
}

// --- Indicator operations ---

// PutIndicators bulk-upserts Indicator records using a two-step COPY+INSERT
// approach: rows are COPYed into a temp table, then merged into the indicators
// table with ON CONFLICT DO UPDATE. This is the fastest path for large batches
// while still supporting upsert semantics.
func (s *PostgresStore) PutIndicators(ctx context.Context, indicators []Indicator) error {
	if len(indicators) == 0 {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: PutIndicators begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	_, err = tx.Exec(ctx, `
CREATE TEMP TABLE indicators_stage (
    geoid          TEXT,
    variable_id    TEXT,
    vintage        TEXT,
    value          DOUBLE PRECISION,
    margin_of_error DOUBLE PRECISION,
    raw_value      TEXT
) ON COMMIT DROP`)
	if err != nil {
		return fmt.Errorf("store: PutIndicators create temp table: %w", err)
	}

	rows := make([][]interface{}, 0, len(indicators))
	for _, ind := range indicators {
		rows = append(rows, []interface{}{
			ind.GEOID,
			ind.VariableID,
			ind.Vintage,
			ind.Value,
			ind.MarginOfError,
			ind.RawValue,
		})
	}

	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"indicators_stage"},
		[]string{"geoid", "variable_id", "vintage", "value", "margin_of_error", "raw_value"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("store: PutIndicators COPY: %w", err)
	}

	_, err = tx.Exec(ctx, `
INSERT INTO indicators (geoid, variable_id, vintage, value, margin_of_error, raw_value)
SELECT geoid, variable_id, vintage, value, margin_of_error, raw_value
FROM indicators_stage
ON CONFLICT (geoid, variable_id, vintage) DO UPDATE SET
    value           = EXCLUDED.value,
    margin_of_error = EXCLUDED.margin_of_error,
    raw_value       = EXCLUDED.raw_value,
    fetched_at      = now()`)
	if err != nil {
		return fmt.Errorf("store: PutIndicators merge: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: PutIndicators commit: %w", err)
	}
	return nil
}

// PutIndicatorsBatch handles large national-scale inserts by splitting indicators
// into chunks of batchSize and calling PutIndicators for each chunk. This keeps
// per-transaction memory bounded during national fetches that may produce
// hundreds of thousands of rows.
//
// batchSize <= 0 defaults to 10,000 rows per transaction.
func (s *PostgresStore) PutIndicatorsBatch(ctx context.Context, indicators []Indicator, batchSize int) error {
	if len(indicators) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = 10_000
	}

	for start := 0; start < len(indicators); start += batchSize {
		end := start + batchSize
		if end > len(indicators) {
			end = len(indicators)
		}
		if err := s.PutIndicators(ctx, indicators[start:end]); err != nil {
			return fmt.Errorf("store: PutIndicatorsBatch chunk [%d:%d]: %w", start, end, err)
		}
	}
	return nil
}

// QueryIndicators returns indicators matching the given filter. When LatestOnly
// is set, the query reads from the indicators_latest materialized view which
// holds only the most recent vintage per (geoid, variable_id) pair.
func (s *PostgresStore) QueryIndicators(ctx context.Context, q IndicatorQuery) ([]Indicator, error) {
	args := []interface{}{}
	idx := 1
	var where []string

	table := "indicators"
	if q.LatestOnly {
		table = "indicators_latest"
	}

	if len(q.GEOIDs) > 0 {
		where = append(where, fmt.Sprintf("geoid = ANY($%d)", idx))
		args = append(args, q.GEOIDs)
		idx++
	}
	if len(q.VariableIDs) > 0 {
		where = append(where, fmt.Sprintf("variable_id = ANY($%d)", idx))
		args = append(args, q.VariableIDs)
		idx++
	}
	if q.Vintage != "" {
		where = append(where, fmt.Sprintf("vintage = $%d", idx))
		args = append(args, q.Vintage)
		idx++
	}

	_ = idx // suppress unused warning after last use

	whereClause := ""
	if len(where) > 0 {
		whereClause = "WHERE " + strings.Join(where, " AND ")
	}

	rawValueExpr := "COALESCE(raw_value, '')"
	if q.LatestOnly {
		rawValueExpr = "''" // indicators_latest materialized view has no raw_value column
	}
	sql := fmt.Sprintf(`
SELECT geoid, variable_id, vintage, value, margin_of_error, %s
FROM %s
%s
ORDER BY geoid, variable_id, vintage`, rawValueExpr, table, whereClause)

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("store: QueryIndicators: %w", err)
	}
	defer rows.Close()

	var result []Indicator
	for rows.Next() {
		var ind Indicator
		if err := rows.Scan(
			&ind.GEOID, &ind.VariableID, &ind.Vintage,
			&ind.Value, &ind.MarginOfError, &ind.RawValue,
		); err != nil {
			return nil, fmt.Errorf("store: QueryIndicators scan: %w", err)
		}
		result = append(result, ind)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: QueryIndicators rows: %w", err)
	}
	return result, nil
}

// Aggregate runs a statistical aggregation over a variable across all
// geographies at the given level. Supported functions: avg, sum, min, max,
// stddev, count. The query targets the indicators_latest materialized view.
func (s *PostgresStore) Aggregate(ctx context.Context, q AggregateQuery) (*AggregateResult, error) {
	allowed := map[string]bool{
		"avg": true, "sum": true, "min": true,
		"max": true, "stddev": true, "count": true,
	}
	fn := strings.ToLower(q.Function)
	if !allowed[fn] {
		return nil, fmt.Errorf("store: Aggregate: unsupported function %q", q.Function)
	}

	args := []interface{}{q.VariableID, string(q.Level)}
	var stateFIPSClause string
	if q.StateFIPS != "" {
		stateFIPSClause = "AND g.state_fips = $3"
		args = append(args, q.StateFIPS)
	}

	// stddev uses stddev_samp; count operates on non-NULL values via COUNT(value).
	var aggExpr string
	switch fn {
	case "stddev":
		aggExpr = "stddev_samp(il.value)"
	case "count":
		aggExpr = "COUNT(il.value)"
	default:
		aggExpr = fn + "(il.value)"
	}

	sql := fmt.Sprintf(`
SELECT %s, COUNT(il.value)
FROM indicators_latest il
JOIN geographies g ON g.geoid = il.geoid
WHERE il.variable_id = $1
  AND g.level = $2::geo_level
  %s`, aggExpr, stateFIPSClause)

	row := s.pool.QueryRow(ctx, sql, args...)

	var aggVal *float64
	var cnt int
	if err := row.Scan(&aggVal, &cnt); err != nil {
		return nil, fmt.Errorf("store: Aggregate: %w", err)
	}

	result := &AggregateResult{Count: cnt}
	if aggVal != nil {
		result.Value = *aggVal
	}
	return result, nil
}

// --- Analysis operations ---

// PutAnalysis persists an AnalysisResult record to the analyses table and
// returns the database-generated UUID. Callers must use this UUID (not any
// caller-generated ID) as the analysis_id in PutAnalysisScores.
func (s *PostgresStore) PutAnalysis(ctx context.Context, result AnalysisResult) (string, error) {
	const q = `
INSERT INTO analyses (type, scope_geoid, scope_level, parameters, results, vintage)
VALUES ($1, NULLIF($2,''), NULLIF($3,'')::geo_level, $4, $5, $6)
RETURNING id`

	var id string
	err := s.pool.QueryRow(ctx, q,
		result.Type,
		result.ScopeGEOID,
		result.ScopeLevel,
		marshalJSONB(result.Parameters),
		marshalJSONB(result.Results),
		result.Vintage,
	).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("store: PutAnalysis: %w", err)
	}
	return id, nil
}

// PutAnalysisScores bulk-upserts AnalysisScore records using a pgx Batch.
// ON CONFLICT (analysis_id, geoid) DO UPDATE refreshes all mutable columns.
func (s *PostgresStore) PutAnalysisScores(ctx context.Context, scores []AnalysisScore) error {
	if len(scores) == 0 {
		return nil
	}

	const upsertSQL = `
INSERT INTO analysis_scores (analysis_id, geoid, score, rank, percentile, tier, details)
VALUES ($1, $2, $3, $4, $5, NULLIF($6,''), $7)
ON CONFLICT (analysis_id, geoid) DO UPDATE SET
    score      = EXCLUDED.score,
    rank       = EXCLUDED.rank,
    percentile = EXCLUDED.percentile,
    tier       = EXCLUDED.tier,
    details    = EXCLUDED.details`

	batch := &pgx.Batch{}
	for _, sc := range scores {
		batch.Queue(upsertSQL,
			sc.AnalysisID,
			sc.GEOID,
			sc.Score,
			sc.Rank,
			sc.Percentile,
			sc.Tier,
			marshalJSONB(sc.Details),
		)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i, sc := range scores {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("store: PutAnalysisScores[%d] analysis_id=%s geoid=%s: %w",
				i, sc.AnalysisID, sc.GEOID, err)
		}
	}
	return nil
}

// QueryAnalysisScores returns all scores for the given analysis, optionally
// filtered to a single tier. Results are ordered by rank ascending.
func (s *PostgresStore) QueryAnalysisScores(ctx context.Context, analysisID string, tier string) ([]AnalysisScore, error) {
	args := []interface{}{analysisID}
	tierClause := ""
	if tier != "" {
		tierClause = "AND tier = $2"
		args = append(args, tier)
	}

	sql := fmt.Sprintf(`
SELECT analysis_id, geoid, score, rank, percentile, COALESCE(tier,''), details
FROM analysis_scores
WHERE analysis_id = $1
  %s
ORDER BY rank ASC`, tierClause)

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("store: QueryAnalysisScores: %w", err)
	}
	defer rows.Close()

	var result []AnalysisScore
	for rows.Next() {
		var sc AnalysisScore
		var detailsJSON []byte
		if err := rows.Scan(
			&sc.AnalysisID, &sc.GEOID, &sc.Score, &sc.Rank, &sc.Percentile, &sc.Tier,
			&detailsJSON,
		); err != nil {
			return nil, fmt.Errorf("store: QueryAnalysisScores scan: %w", err)
		}
		sc.Details = unmarshalJSONB(detailsJSON)
		result = append(result, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: QueryAnalysisScores rows: %w", err)
	}
	return result, nil
}
