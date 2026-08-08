package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
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
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'geographies' AND column_name = 'centroid'
		)`).Scan(&hasCentroid); err != nil {
		// Non-fatal: default to false (no spatial queries).
		hasCentroid = false
	}
	s.hasPostGIS = hasCentroid

	return s, nil
}

// Ping verifies database connectivity by executing a lightweight query.
func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
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
// Each row is upserted via ON CONFLICT (geoid) DO UPDATE. Boundary and centroid
// columns are only written when PostGIS is available; to load boundaries, use
// ogr2ogr or the TIGER loader which writes geometry directly via SQL.
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
//
// Retired geographies (migration 013) are NOT filtered out here. Supplying an
// exact GEOID is already an explicit request for that specific row, and hiding
// it would both 404 existing deep links and make the historical profiles that
// ADR-012 §Integration 5 depends on unreachable. Lifecycle filtering belongs
// to the listing paths (QueryGeographies / CountGeographies), where the caller
// asks an open question and expects an answer about the present.
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

// geoWhereClause builds the WHERE fragment and positional arguments shared by
// QueryGeographies and CountGeographies, so that a listing and its total can
// never disagree about which rows match. The returned clause is either empty
// or begins with "WHERE ".
//
// The lifecycle predicate is always present: retired geographies (migration
// 013) are filtered out unless the caller opts in, which keeps every default
// read describing the current census vintage.
func geoWhereClause(q GeoQuery) (string, []interface{}) {
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

	switch {
	case q.RetiredOnly:
		where = append(where, "retired_at IS NOT NULL")
	case q.IncludeRetired:
		// No lifecycle predicate — current and retired rows both match.
	default:
		where = append(where, "retired_at IS NULL")
	}

	if len(where) == 0 {
		return "", args
	}
	return "WHERE " + strings.Join(where, " AND "), args
}

// QueryGeographies returns geographies matching the given filter. All filter
// fields are optional; an empty GeoQuery returns every CURRENT geography up to
// Limit rows — retired geographies require q.IncludeRetired or q.RetiredOnly.
func (s *PostgresStore) QueryGeographies(ctx context.Context, q GeoQuery) ([]geo.Geography, error) {
	whereClause, args := geoWhereClause(q)

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

// CountGeographies returns how many geographies match q's filters, ignoring
// q.Limit and q.Offset. It shares geoWhereClause with QueryGeographies so the
// count always describes the same row set the listing pages through — a client
// paginating on this number will not stop early.
func (s *PostgresStore) CountGeographies(ctx context.Context, q GeoQuery) (int, error) {
	whereClause, args := geoWhereClause(q)

	sql := "SELECT COUNT(*) FROM geographies " + whereClause

	var n int
	if err := s.pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: CountGeographies: %w", err)
	}
	return n, nil
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
    cv             DOUBLE PRECISION,
    reliability    TEXT,
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
			ind.CV,
			ind.Reliability,
			ind.RawValue,
		})
	}

	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"indicators_stage"},
		[]string{"geoid", "variable_id", "vintage", "value", "margin_of_error", "cv", "reliability", "raw_value"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("store: PutIndicators COPY: %w", err)
	}

	// reliability is an enum (reliability_level) in indicators but text in the
	// staging table, and Postgres will not implicitly cast text to an enum — so
	// this needs an explicit cast. Without it every write carrying a reliability
	// value fails; only writes that leave it empty got through, which is why the
	// existing tests never caught it. NULLIF keeps "" (Indicator.Reliability's
	// zero value) out of the enum, where it is not a valid label.
	_, err = tx.Exec(ctx, `
INSERT INTO indicators (geoid, variable_id, vintage, value, margin_of_error, cv, reliability, raw_value)
SELECT geoid, variable_id, vintage, value, margin_of_error, cv,
       NULLIF(reliability, '')::reliability_level, raw_value
FROM indicators_stage
ON CONFLICT (geoid, variable_id, vintage) DO UPDATE SET
    value           = EXCLUDED.value,
    margin_of_error = EXCLUDED.margin_of_error,
    cv              = EXCLUDED.cv,
    reliability     = EXCLUDED.reliability,
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
// is set and no vintage filter is provided, the query reads from the
// indicators_latest materialized view which holds only the most recent vintage
// per (geoid, variable_id) pair.
func (s *PostgresStore) QueryIndicators(ctx context.Context, q IndicatorQuery) ([]Indicator, error) {
	args := []interface{}{}
	idx := 1
	var where []string

	table := "indicators"
	if q.LatestOnly && q.Vintage == "" && len(q.Vintages) == 0 {
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
	if q.Vintage != "" && len(q.Vintages) == 0 {
		q.Vintages = []string{q.Vintage}
	}
	if len(q.Vintages) > 0 {
		where = append(where, fmt.Sprintf("vintage = ANY($%d)", idx))
		args = append(args, q.Vintages)
		idx++
	}

	_ = idx // suppress unused warning after last use

	whereClause := ""
	if len(where) > 0 {
		whereClause = "WHERE " + strings.Join(where, " AND ")
	}

	rawValueExpr := "COALESCE(raw_value, '')"
	cvExpr := "cv"
	reliabilityExpr := "COALESCE(reliability::text, '')"
	if table == "indicators_latest" {
		// The view has no raw_value column; cv and reliability it HAS since
		// migration 014. This branch used to null them out too — a leftover
		// from the pre-014 view that silently stripped reliability from every
		// latest-vintage read, which is why the dashboard's badges showed "—"
		// while the data sat present in both tables (found 2026-08-08).
		rawValueExpr = "''"
	}
	sql := fmt.Sprintf(`
SELECT geoid, variable_id, vintage, value, margin_of_error, %s, %s, %s
FROM %s
%s
ORDER BY geoid, variable_id, vintage`, cvExpr, reliabilityExpr, rawValueExpr, table, whereClause)

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
			&ind.Value, &ind.MarginOfError, &ind.CV, &ind.Reliability, &ind.RawValue,
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
//
// Retired geographies are excluded. indicators_latest resolves to the newest
// vintage each geography has, so a retired geography contributes its final
// pre-retirement value — leaving them in would silently dilute a current-vintage
// statistic with rows for places that no longer exist, and would put the
// aggregate's denominator out of step with the geography counts the API
// reports. Temporal aggregates, when needed, should opt in via a new
// AggregateQuery field rather than by removing this predicate.
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
  AND g.retired_at IS NULL
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

// --- Metadata operations ---

// QueryVariables returns all indicator_meta rows joined with their source name.
// Results are ordered by variable_id. An empty table returns an empty slice, not an error.
// CountSourcesWithData counts distinct sources holding at least one indicator
// row. It joins through indicator_meta because indicators carry a variable_id,
// not a source_id.
func (s *PostgresStore) CountSourcesWithData(ctx context.Context) (int, error) {
	const q = `
SELECT count(DISTINCT im.source_id)
FROM indicators i
JOIN indicator_meta im ON im.variable_id = i.variable_id`

	var n int
	if err := s.pool.QueryRow(ctx, q).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: CountSourcesWithData: %w", err)
	}
	return n, nil
}

// RegisterSource upserts one indicator_sources row and its indicator_meta rows
// inside a single transaction, parent first.
//
// The source row is ON CONFLICT DO NOTHING: seed_sources.sql carries better
// prose than an adapter can produce, and this must not clobber it. Variable
// rows do update, because the adapter's Schema() is the authority on a
// variable's name, unit and direction.
func (s *PostgresStore) RegisterSource(ctx context.Context, src SourceMeta, vars []VariableMeta) error {
	if src.SourceID == "" {
		return fmt.Errorf("store: RegisterSource: empty source_id")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: RegisterSource begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, `
INSERT INTO indicator_sources (source_id, name, category, url, description)
VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''))
ON CONFLICT (source_id) DO NOTHING`,
		src.SourceID, src.Name, src.Category, src.URL, src.Description,
	); err != nil {
		return fmt.Errorf("store: RegisterSource source %q: %w", src.SourceID, err)
	}

	for _, v := range vars {
		if v.VariableID == "" {
			return fmt.Errorf("store: RegisterSource %q: variable with empty id", src.SourceID)
		}
		sourceID := v.SourceID
		if sourceID == "" {
			sourceID = src.SourceID
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO indicator_meta (variable_id, source_id, name, description, unit, direction)
VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''))
ON CONFLICT (variable_id) DO UPDATE SET
    source_id   = EXCLUDED.source_id,
    name        = EXCLUDED.name,
    description = EXCLUDED.description,
    unit        = EXCLUDED.unit,
    direction   = EXCLUDED.direction`,
			v.VariableID, sourceID, v.Name, v.Description, v.Unit, v.Direction,
		); err != nil {
			return fmt.Errorf("store: RegisterSource variable %q: %w", v.VariableID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: RegisterSource commit: %w", err)
	}
	return nil
}

func (s *PostgresStore) QueryVariables(ctx context.Context) ([]VariableMeta, error) {
	const q = `
SELECT im.variable_id, im.source_id, COALESCE(src.name, '') AS source_name,
       im.name, COALESCE(im.description, ''), COALESCE(im.unit, ''),
       COALESCE(im.direction, '')
FROM indicator_meta im
LEFT JOIN indicator_sources src ON src.source_id = im.source_id
ORDER BY im.variable_id`

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: QueryVariables: %w", err)
	}
	defer rows.Close()

	var result []VariableMeta
	for rows.Next() {
		var v VariableMeta
		if err := rows.Scan(
			&v.VariableID, &v.SourceID, &v.SourceName,
			&v.Name, &v.Description, &v.Unit, &v.Direction,
		); err != nil {
			return nil, fmt.Errorf("store: QueryVariables scan: %w", err)
		}
		result = append(result, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: QueryVariables rows: %w", err)
	}
	return result, nil
}

// --- Analysis operations ---

// PutAnalysis persists an AnalysisResult and returns its UUID. Identity is
// the ADR-014 D9 cache key — (type, scope_geoid, scope_level, vintage,
// parameters) — enforced by uq_analyses_cache_key (migration 015): re-running
// an identical analysis refreshes results/computed_at on the existing row and
// returns the SAME id instead of minting a duplicate. result.ID is ignored on
// write; callers must use the returned UUID as the analysis_id in
// PutAnalysisScores. Existing scores for a refreshed analysis remain until
// the caller upserts replacements (PutAnalysisScores upserts by
// (analysis_id, geoid)).
func (s *PostgresStore) PutAnalysis(ctx context.Context, result AnalysisResult) (string, error) {
	const q = `
INSERT INTO analyses (type, scope_geoid, scope_level, parameters, results, vintage)
VALUES ($1, NULLIF($2,''), NULLIF($3,'')::geo_level, $4, $5, $6)
ON CONFLICT (type, scope_geoid, scope_level, vintage, parameters)
DO UPDATE SET results = EXCLUDED.results, computed_at = now()
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

// FindAnalysisByKey returns the analysis matching the exact ADR-014 D9 cache
// key, or nil (no error) when none exists. Empty ScopeGEOID/ScopeLevel match
// rows whose columns are NULL, mirroring how PutAnalysis stores them.
func (s *PostgresStore) FindAnalysisByKey(ctx context.Context, key AnalysisKey) (*AnalysisSummary, error) {
	const q = `
SELECT a.id, a.type,
       COALESCE(a.scope_geoid, ''), COALESCE(a.scope_level::text, ''),
       COALESCE(a.vintage, ''), a.computed_at::text,
       (SELECT COUNT(*) FROM analysis_scores sc WHERE sc.analysis_id = a.id)
FROM analyses a
WHERE a.type = $1
  AND a.scope_geoid IS NOT DISTINCT FROM NULLIF($2, '')
  AND a.scope_level IS NOT DISTINCT FROM NULLIF($3, '')::geo_level
  AND a.vintage IS NOT DISTINCT FROM $4
  AND a.parameters IS NOT DISTINCT FROM $5
LIMIT 1`

	var as AnalysisSummary
	err := s.pool.QueryRow(ctx, q,
		key.Type, key.ScopeGEOID, key.ScopeLevel, key.Vintage,
		marshalJSONB(key.Parameters),
	).Scan(&as.ID, &as.Type, &as.ScopeGEOID, &as.ScopeLevel,
		&as.Vintage, &as.ComputedAt, &as.ScoreCount)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("store: FindAnalysisByKey: %w", err)
	}
	return &as, nil
}

// GetAnalysis retrieves a single analysis by ID.
func (s *PostgresStore) GetAnalysis(ctx context.Context, id string) (*AnalysisResult, error) {
	const q = `
SELECT id, type, COALESCE(scope_geoid, ''), COALESCE(scope_level::text, ''),
       COALESCE(parameters, '{}'), COALESCE(results, '{}'), COALESCE(vintage, '')
FROM analyses WHERE id = $1`

	var r AnalysisResult
	var paramsJSON, resultsJSON []byte
	err := s.pool.QueryRow(ctx, q, id).Scan(
		&r.ID, &r.Type, &r.ScopeGEOID, &r.ScopeLevel,
		&paramsJSON, &resultsJSON, &r.Vintage,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("store: GetAnalysis: id %q not found", id)
		}
		return nil, fmt.Errorf("store: GetAnalysis: %w", err)
	}
	r.Parameters = unmarshalJSONB(paramsJSON)
	r.Results = unmarshalJSONB(resultsJSON)
	return &r, nil
}

// --- Run queue operations (ADR-014 D3) ---

// analysisRunColumns is the shared SELECT list for AnalysisRun scans.
const analysisRunColumns = `
    id::text, run_type,
    COALESCE(scope_geoid, ''), COALESCE(scope_level::text, ''),
    COALESCE(vintage, ''), COALESCE(parameters, '{}'::jsonb),
    status, COALESCE(error, ''), COALESCE(analysis_id::text, ''),
    COALESCE(client_key, ''), requested_at::text,
    COALESCE(started_at::text, ''), COALESCE(finished_at::text, '')`

func scanAnalysisRun(row pgx.Row) (*AnalysisRun, error) {
	var r AnalysisRun
	var paramsJSON []byte
	if err := row.Scan(
		&r.ID, &r.RunType, &r.ScopeGEOID, &r.ScopeLevel,
		&r.Vintage, &paramsJSON, &r.Status, &r.Error, &r.AnalysisID,
		&r.ClientKey, &r.RequestedAt, &r.StartedAt, &r.FinishedAt,
	); err != nil {
		return nil, err
	}
	r.Parameters = unmarshalJSONB(paramsJSON)
	return &r, nil
}

// CreateAnalysisRun enqueues a run (status=queued) and returns its id.
func (s *PostgresStore) CreateAnalysisRun(ctx context.Context, run AnalysisRun) (string, error) {
	const q = `
INSERT INTO analysis_runs (run_type, scope_geoid, scope_level, vintage, parameters, client_key)
VALUES ($1, NULLIF($2,''), NULLIF($3,'')::geo_level, NULLIF($4,''), COALESCE($5, '{}'::jsonb), NULLIF($6,''))
RETURNING id::text`

	var id string
	err := s.pool.QueryRow(ctx, q,
		run.RunType, run.ScopeGEOID, run.ScopeLevel, run.Vintage,
		marshalJSONB(run.Parameters), run.ClientKey,
	).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("store: CreateAnalysisRun: %w", err)
	}
	return id, nil
}

// GetAnalysisRun retrieves one run by id.
func (s *PostgresStore) GetAnalysisRun(ctx context.Context, id string) (*AnalysisRun, error) {
	q := `SELECT` + analysisRunColumns + ` FROM analysis_runs WHERE id = $1`
	r, err := scanAnalysisRun(s.pool.QueryRow(ctx, q, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("store: GetAnalysisRun: id %q not found", id)
		}
		return nil, fmt.Errorf("store: GetAnalysisRun: %w", err)
	}
	return r, nil
}

// ClaimNextAnalysisRun atomically claims the oldest queued run, marking it
// running. Returns (nil, nil) when the queue is empty. FOR UPDATE SKIP LOCKED
// makes concurrent workers claim disjoint rows.
func (s *PostgresStore) ClaimNextAnalysisRun(ctx context.Context) (*AnalysisRun, error) {
	q := `
UPDATE analysis_runs SET status = 'running', started_at = now()
WHERE id = (
    SELECT id FROM analysis_runs
    WHERE status = 'queued'
    ORDER BY requested_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED)
RETURNING` + analysisRunColumns

	r, err := scanAnalysisRun(s.pool.QueryRow(ctx, q))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("store: ClaimNextAnalysisRun: %w", err)
	}
	return r, nil
}

// CompleteAnalysisRun finishes a run: done when errMsg is empty, failed
// otherwise. analysisID may be empty (failed runs have no cache entry).
func (s *PostgresStore) CompleteAnalysisRun(ctx context.Context, id, analysisID, errMsg string) error {
	const q = `
UPDATE analysis_runs
SET status      = CASE WHEN $2 = '' THEN 'done' ELSE 'failed' END,
    error       = NULLIF($2, ''),
    analysis_id = NULLIF($3, '')::uuid,
    finished_at = now()
WHERE id = $1`

	tag, err := s.pool.Exec(ctx, q, id, errMsg, analysisID)
	if err != nil {
		return fmt.Errorf("store: CompleteAnalysisRun: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: CompleteAnalysisRun: id %q not found", id)
	}
	return nil
}

// CountActiveRuns counts queued and running rows, for queue-depth admission.
func (s *PostgresStore) CountActiveRuns(ctx context.Context) (int, error) {
	const q = `SELECT count(*) FROM analysis_runs WHERE status IN ('queued', 'running')`
	var n int
	if err := s.pool.QueryRow(ctx, q).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: CountActiveRuns: %w", err)
	}
	return n, nil
}

// QueryStateRanks returns, for one geography, each variable's percentile
// rank (0–100, value-ascending) among all same-level geographies in
// indicators_latest. This replaces the dashboard's hardcoded 50% stub with
// the real statewide position, computed by the database in one window pass
// rather than by a JS reimplementation shipping every peer's value to the
// client (TODO's distribution-endpoint recommendation, narrowed to ranks).
func (s *PostgresStore) QueryStateRanks(ctx context.Context, geoid string) (map[string]float64, error) {
	const q = `
SELECT variable_id, pct FROM (
    SELECT geoid, variable_id,
           percent_rank() OVER (PARTITION BY variable_id ORDER BY value) AS pct
    FROM indicators_latest
    WHERE length(geoid) = length($1) AND value IS NOT NULL
) ranked
WHERE geoid = $1`

	rows, err := s.pool.Query(ctx, q, geoid)
	if err != nil {
		return nil, fmt.Errorf("store: QueryStateRanks: %w", err)
	}
	defer rows.Close()

	out := map[string]float64{}
	for rows.Next() {
		var v string
		var pct float64
		if err := rows.Scan(&v, &pct); err != nil {
			return nil, fmt.Errorf("store: QueryStateRanks scan: %w", err)
		}
		out[v] = pct * 100
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: QueryStateRanks rows: %w", err)
	}
	return out, nil
}

// LatestVintageForVariable resolves the newest vintage carrying data for a
// variable ("" when the variable has no rows). Used at enqueue time so an
// omitted request vintage becomes a CONCRETE vintage in the cache key —
// "latest" is resolved once, never stored (ADR-014 D9: a key without a
// pinned vintage is the stale-value failure class).
func (s *PostgresStore) LatestVintageForVariable(ctx context.Context, variableID string) (string, error) {
	const q = `SELECT COALESCE(MAX(vintage), '') FROM indicators WHERE variable_id = $1`
	var v string
	if err := s.pool.QueryRow(ctx, q, variableID).Scan(&v); err != nil {
		return "", fmt.Errorf("store: LatestVintageForVariable: %w", err)
	}
	return v, nil
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

// --- Policy operations ---

// PutPolicies upserts a slice of PolicyRecord rows into the policies table.
// ON CONFLICT (id) DO UPDATE replaces all mutable columns so repeated CSV loads
// are idempotent.
func (s *PostgresStore) PutPolicies(ctx context.Context, policies []PolicyRecord) error {
	if len(policies) == 0 {
		return nil
	}

	const upsertSQL = `
INSERT INTO policies (id, candidate, office, state, category, title, description,
    bill_references, claims_empirical, equity_dimension, geographic_scope,
    data_sources_needed, source_url)
VALUES ($1, $2, NULLIF($3,''), NULLIF($4,''), $5, $6, NULLIF($7,''),
    NULLIF($8,''), NULLIF($9,''), NULLIF($10,''), NULLIF($11,''),
    NULLIF($12,''), NULLIF($13,''))
ON CONFLICT (id) DO UPDATE SET
    candidate          = EXCLUDED.candidate,
    office             = EXCLUDED.office,
    state              = EXCLUDED.state,
    category           = EXCLUDED.category,
    title              = EXCLUDED.title,
    description        = EXCLUDED.description,
    bill_references    = EXCLUDED.bill_references,
    claims_empirical   = EXCLUDED.claims_empirical,
    equity_dimension   = EXCLUDED.equity_dimension,
    geographic_scope   = EXCLUDED.geographic_scope,
    data_sources_needed = EXCLUDED.data_sources_needed,
    source_url         = EXCLUDED.source_url`

	batch := &pgx.Batch{}
	for _, p := range policies {
		batch.Queue(upsertSQL,
			p.ID, p.Candidate, p.Office, p.State, p.Category, p.Title, p.Description,
			p.BillReferences, p.ClaimsEmpirical, p.EquityDimension, p.GeographicScope,
			p.DataSourcesNeeded, p.SourceURL,
		)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i, p := range policies {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("store: PutPolicies[%d] id=%s: %w", i, p.ID, err)
		}
	}
	return nil
}

// QueryPolicies returns policy rows filtered by the optional fields in q.
// Results are ordered by category, id. An empty result set returns an empty
// slice, not an error.
func (s *PostgresStore) QueryPolicies(ctx context.Context, q PolicyQuery) ([]PolicyRecord, error) {
	args := []interface{}{}
	clauses := []string{}

	if q.Candidate != "" {
		args = append(args, q.Candidate)
		clauses = append(clauses, fmt.Sprintf("candidate = $%d", len(args)))
	}
	if q.Category != "" {
		args = append(args, q.Category)
		clauses = append(clauses, fmt.Sprintf("category = $%d", len(args)))
	}
	if q.State != "" {
		args = append(args, q.State)
		clauses = append(clauses, fmt.Sprintf("state = $%d", len(args)))
	}

	where := ""
	if len(clauses) > 0 {
		where = "WHERE " + strings.Join(clauses, " AND ")
	}

	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit)
	limitClause := fmt.Sprintf("LIMIT $%d", len(args))

	args = append(args, q.Offset)
	offsetClause := fmt.Sprintf("OFFSET $%d", len(args))

	qSQL := fmt.Sprintf(`
SELECT id, candidate, COALESCE(office,''), COALESCE(state,''), category, title,
    COALESCE(description,''), COALESCE(bill_references,''), COALESCE(claims_empirical,''),
    COALESCE(equity_dimension,''), COALESCE(geographic_scope,''),
    COALESCE(data_sources_needed,''), COALESCE(source_url,'')
FROM policies
%s
ORDER BY category, id
%s %s`, where, limitClause, offsetClause)

	rows, err := s.pool.Query(ctx, qSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("store: QueryPolicies: %w", err)
	}
	defer rows.Close()

	var result []PolicyRecord
	for rows.Next() {
		var p PolicyRecord
		if err := rows.Scan(
			&p.ID, &p.Candidate, &p.Office, &p.State, &p.Category, &p.Title,
			&p.Description, &p.BillReferences, &p.ClaimsEmpirical,
			&p.EquityDimension, &p.GeographicScope, &p.DataSourcesNeeded, &p.SourceURL,
		); err != nil {
			return nil, fmt.Errorf("store: QueryPolicies scan: %w", err)
		}
		result = append(result, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: QueryPolicies rows: %w", err)
	}
	return result, nil
}

// GetPolicy returns the policy record with the given id, or an error wrapping
// pgx.ErrNoRows when no record exists.
func (s *PostgresStore) GetPolicy(ctx context.Context, id string) (*PolicyRecord, error) {
	const q = `
SELECT id, candidate, COALESCE(office,''), COALESCE(state,''), category, title,
    COALESCE(description,''), COALESCE(bill_references,''), COALESCE(claims_empirical,''),
    COALESCE(equity_dimension,''), COALESCE(geographic_scope,''),
    COALESCE(data_sources_needed,''), COALESCE(source_url,'')
FROM policies
WHERE id = $1`

	var p PolicyRecord
	err := s.pool.QueryRow(ctx, q, id).Scan(
		&p.ID, &p.Candidate, &p.Office, &p.State, &p.Category, &p.Title,
		&p.Description, &p.BillReferences, &p.ClaimsEmpirical,
		&p.EquityDimension, &p.GeographicScope, &p.DataSourcesNeeded, &p.SourceURL,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("store: GetPolicy id=%s: %w", id, pgx.ErrNoRows)
		}
		return nil, fmt.Errorf("store: GetPolicy id=%s: %w", id, err)
	}
	return &p, nil
}

// --- Evidence card operations ---

// PutEvidenceCards bulk-upserts EvidenceCard records using a pgx Batch.
// ON CONFLICT (policy_id) DO UPDATE refreshes all columns except id so the
// same policy_id always maps to its most recent card.
func (s *PostgresStore) PutEvidenceCards(ctx context.Context, cards []EvidenceCard) error {
	if len(cards) == 0 {
		return nil
	}

	const upsertSQL = `
INSERT INTO evidence_cards (policy_id, policy_title, category, equity_dimension, title, key_finding, data_quality, findings, indicators, statewide_context, county_variation, top_need_counties, bottom_need_counties)
VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''), NULLIF($7, ''), $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb)
ON CONFLICT (policy_id) DO UPDATE SET
    policy_title        = EXCLUDED.policy_title,
    category            = EXCLUDED.category,
    equity_dimension    = EXCLUDED.equity_dimension,
    title               = EXCLUDED.title,
    key_finding         = EXCLUDED.key_finding,
    data_quality        = EXCLUDED.data_quality,
    findings            = EXCLUDED.findings,
    indicators          = EXCLUDED.indicators,
    statewide_context   = EXCLUDED.statewide_context,
    county_variation    = EXCLUDED.county_variation,
    top_need_counties   = EXCLUDED.top_need_counties,
    bottom_need_counties = EXCLUDED.bottom_need_counties`

	batch := &pgx.Batch{}
	for _, card := range cards {
		batch.Queue(upsertSQL,
			card.PolicyID,
			card.PolicyTitle,
			card.Category,
			card.EquityDimension,
			card.Title,
			card.KeyFinding,
			card.DataQuality,
			card.Findings,
			card.Indicators,
			card.StatewideContext,
			card.CountyVariation,
			card.TopNeedCounties,
			card.BottomNeedCounties,
		)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i, card := range cards {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("store: PutEvidenceCards[%d] policy_id=%s: %w", i, card.PolicyID, err)
		}
	}
	return nil
}

// QueryEvidenceCards returns evidence cards matching the optional filters in f.
// Results are ordered by category, policy_id. An empty result set returns an
// empty slice, not an error.
func (s *PostgresStore) QueryEvidenceCards(ctx context.Context, f EvidenceCardFilter) ([]EvidenceCard, error) {
	args := []interface{}{}
	clauses := []string{}

	if f.Category != "" {
		args = append(args, f.Category)
		clauses = append(clauses, fmt.Sprintf("category = $%d", len(args)))
	}
	if f.EquityDimension != "" {
		args = append(args, f.EquityDimension)
		clauses = append(clauses, fmt.Sprintf("equity_dimension = $%d", len(args)))
	}
	if f.PolicyID != "" {
		args = append(args, f.PolicyID)
		clauses = append(clauses, fmt.Sprintf("policy_id = $%d", len(args)))
	}

	where := ""
	if len(clauses) > 0 {
		where = "WHERE " + strings.Join(clauses, " AND ")
	}

	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	args = append(args, limit)
	limitClause := fmt.Sprintf("LIMIT $%d", len(args))

	args = append(args, f.Offset)
	offsetClause := fmt.Sprintf("OFFSET $%d", len(args))

	qSQL := fmt.Sprintf(`
SELECT id, policy_id, policy_title, category, COALESCE(equity_dimension, ''),
    COALESCE(title, ''), COALESCE(key_finding, ''), COALESCE(data_quality, ''),
    COALESCE(findings::text, '[]'), COALESCE(indicators::text, '[]'),
    COALESCE(statewide_context::text, '{}'), COALESCE(county_variation::text, '{}'),
    COALESCE(top_need_counties::text, '[]'), COALESCE(bottom_need_counties::text, '[]')
FROM evidence_cards
%s
ORDER BY category, policy_id
%s %s`, where, limitClause, offsetClause)

	rows, err := s.pool.Query(ctx, qSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("store: QueryEvidenceCards: %w", err)
	}
	defer rows.Close()

	var result []EvidenceCard
	for rows.Next() {
		var card EvidenceCard
		if err := rows.Scan(
			&card.ID, &card.PolicyID, &card.PolicyTitle, &card.Category,
			&card.EquityDimension, &card.Title, &card.KeyFinding, &card.DataQuality,
			&card.Findings, &card.Indicators, &card.StatewideContext,
			&card.CountyVariation, &card.TopNeedCounties, &card.BottomNeedCounties,
		); err != nil {
			return nil, fmt.Errorf("store: QueryEvidenceCards scan: %w", err)
		}
		result = append(result, card)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: QueryEvidenceCards rows: %w", err)
	}
	return result, nil
}

// SeedEvidenceCardsFromJSON parses a JSON array of evidence card objects and
// bulk-upserts them into the evidence_cards table. It skips seeding if the
// table already has at least one row. The method accepts the raw JSON bytes
// (typically from //go:embed) directly.
func (s *PostgresStore) SeedEvidenceCardsFromJSON(ctx context.Context, jsonData []byte) error {
	// Check if table already has data.
	var count int
	if err := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM evidence_cards").Scan(&count); err != nil {
		return fmt.Errorf("store: SeedEvidenceCardsFromJSON count: %w", err)
	}
	if count > 0 {
		return nil // already seeded
	}

	// Parse JSON array into intermediate struct.
	type jsonCard struct {
		PolicyID           string          `json:"policy_id"`
		PolicyTitle        string          `json:"policy_title"`
		Category           string          `json:"category"`
		EquityDimension    string          `json:"equity_dimension"`
		KeyFinding         string          `json:"key_finding"`
		DataQuality        string          `json:"data_quality"`
		Title              string          `json:"title"`
		Findings           json.RawMessage `json:"findings"`
		Indicators         json.RawMessage `json:"indicators"`
		StatewideContext   json.RawMessage `json:"statewide_context"`
		CountyVariation    json.RawMessage `json:"county_variation"`
		TopNeedCounties    json.RawMessage `json:"top_need_counties"`
		BottomNeedCounties json.RawMessage `json:"bottom_need_counties"`
	}

	var rawCards []jsonCard
	if err := json.Unmarshal(jsonData, &rawCards); err != nil {
		return fmt.Errorf("store: SeedEvidenceCardsFromJSON parse: %w", err)
	}

	cards := make([]EvidenceCard, 0, len(rawCards))
	for _, rc := range rawCards {
		// Default empty JSON arrays/objects for missing fields.
		findings := rc.Findings
		if findings == nil || string(findings) == "null" {
			findings = json.RawMessage("[]")
		}
		indicators := rc.Indicators
		if indicators == nil || string(indicators) == "null" {
			indicators = json.RawMessage("[]")
		}
		statewideCtx := rc.StatewideContext
		if statewideCtx == nil || string(statewideCtx) == "null" {
			statewideCtx = json.RawMessage("{}")
		}
		countyVar := rc.CountyVariation
		if countyVar == nil || string(countyVar) == "null" {
			countyVar = json.RawMessage("{}")
		}
		topNeed := rc.TopNeedCounties
		if topNeed == nil || string(topNeed) == "null" {
			topNeed = json.RawMessage("[]")
		}
		bottomNeed := rc.BottomNeedCounties
		if bottomNeed == nil || string(bottomNeed) == "null" {
			bottomNeed = json.RawMessage("[]")
		}

		cards = append(cards, EvidenceCard{
			PolicyID:           rc.PolicyID,
			PolicyTitle:        rc.PolicyTitle,
			Category:           rc.Category,
			EquityDimension:    rc.EquityDimension,
			Title:              rc.Title,
			KeyFinding:         rc.KeyFinding,
			DataQuality:        rc.DataQuality,
			Findings:           findings,
			Indicators:         indicators,
			StatewideContext:   statewideCtx,
			CountyVariation:    countyVar,
			TopNeedCounties:    topNeed,
			BottomNeedCounties: bottomNeed,
		})
	}

	return s.PutEvidenceCards(ctx, cards)
}

// ListAnalyses returns a summary of all analysis runs ordered by computed_at
// descending (most recent first). ScoreCount is populated via a correlated
// subquery so no JOIN fanout occurs on the result set.
func (s *PostgresStore) ListAnalyses(ctx context.Context) ([]AnalysisSummary, error) {
	const q = `
SELECT a.id, a.type,
       COALESCE(a.scope_geoid, ''), COALESCE(a.scope_level::text, ''),
       COALESCE(a.vintage, ''), a.computed_at::text,
       (SELECT COUNT(*) FROM analysis_scores sc WHERE sc.analysis_id = a.id)
FROM analyses a
ORDER BY a.computed_at DESC`

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: ListAnalyses: %w", err)
	}
	defer rows.Close()

	var result []AnalysisSummary
	for rows.Next() {
		var as AnalysisSummary
		if err := rows.Scan(
			&as.ID, &as.Type,
			&as.ScopeGEOID, &as.ScopeLevel,
			&as.Vintage, &as.ComputedAt,
			&as.ScoreCount,
		); err != nil {
			return nil, fmt.Errorf("store: ListAnalyses scan: %w", err)
		}
		result = append(result, as)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: ListAnalyses rows: %w", err)
	}
	return result, nil
}

// --- Factor & validated feature operations ---

// PutFactorScores bulk-upserts FactorScore records using a pgx Batch.
// ON CONFLICT (geoid, factor_name, analysis_vintage) DO UPDATE refreshes
// all mutable columns.
func (s *PostgresStore) PutFactorScores(ctx context.Context, scores []FactorScore) error {
	if len(scores) == 0 {
		return nil
	}

	const upsertSQL = `
INSERT INTO factor_scores (geoid, factor_name, factor_score, factor_percentile, loadings_json, analysis_vintage)
VALUES ($1, $2, $3, $4, NULLIF($5, '')::jsonb, NULLIF($6, ''))
ON CONFLICT (geoid, factor_name, analysis_vintage) DO UPDATE SET
    factor_score      = EXCLUDED.factor_score,
    factor_percentile = EXCLUDED.factor_percentile,
    loadings_json     = EXCLUDED.loadings_json`

	batch := &pgx.Batch{}
	for _, fs := range scores {
		batch.Queue(upsertSQL,
			fs.GEOID,
			fs.FactorName,
			fs.FactorScore,
			fs.FactorPercentile,
			fs.LoadingsJSON,
			fs.AnalysisVintage,
		)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i, fs := range scores {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("store: PutFactorScores[%d] geoid=%s factor=%s: %w",
				i, fs.GEOID, fs.FactorName, err)
		}
	}
	return nil
}

// QueryFactorScores returns all factor scores for a single geography,
// ordered by factor_name. An empty result set returns an empty slice.
//
// Exact-match only. A county GEOID with no stored county-level scores
// returns empty rather than an on-the-fly tract average: the unweighted
// fallback this method used to carry had no minimum-N check (a county with
// 1 of 40 tracts reporting produced a "county" value from n=1) and no
// defensible weighting, so per ADR-014 D2/D6 the value is absent, not
// approximated. County factor scores must be written by an explicit,
// weighted, coverage-checked aggregation (the tract_rollup analysis type).
func (s *PostgresStore) QueryFactorScores(ctx context.Context, geoid string) ([]FactorScore, error) {
	const eq = `
SELECT geoid, factor_name, factor_score, factor_percentile,
       COALESCE(loadings_json::text, ''), COALESCE(analysis_vintage, '')
FROM factor_scores
WHERE geoid = $1
ORDER BY factor_name`

	rows, err := s.pool.Query(ctx, eq, geoid)
	if err != nil {
		return nil, fmt.Errorf("store: QueryFactorScores: %w", err)
	}

	var result []FactorScore
	for rows.Next() {
		var fs FactorScore
		if err := rows.Scan(
			&fs.GEOID, &fs.FactorName, &fs.FactorScore, &fs.FactorPercentile,
			&fs.LoadingsJSON, &fs.AnalysisVintage,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: QueryFactorScores scan: %w", err)
		}
		result = append(result, fs)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: QueryFactorScores rows: %w", err)
	}

	return result, nil
}

// QueryValidatedFeatures returns all validated features for a given scope
// GEOID, ordered by feature_name. An empty result set returns an empty slice.
func (s *PostgresStore) QueryValidatedFeatures(ctx context.Context, scopeGEOID string) ([]ValidatedFeature, error) {
	const q = `
SELECT geoid, feature_name, feature_value,
       COALESCE(source_citation, ''), COALESCE(analysis_vintage, '')
FROM validated_features
WHERE geoid = $1
ORDER BY feature_name`

	rows, err := s.pool.Query(ctx, q, scopeGEOID)
	if err != nil {
		return nil, fmt.Errorf("store: QueryValidatedFeatures: %w", err)
	}
	defer rows.Close()

	var result []ValidatedFeature
	for rows.Next() {
		var vf ValidatedFeature
		if err := rows.Scan(
			&vf.GEOID, &vf.FeatureName, &vf.FeatureValue,
			&vf.SourceCitation, &vf.AnalysisVintage,
		); err != nil {
			return nil, fmt.Errorf("store: QueryValidatedFeatures scan: %w", err)
		}
		result = append(result, vf)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: QueryValidatedFeatures rows: %w", err)
	}
	return result, nil
}

// QueryLISACountyProfile returns a county-level summary of LISA spatial
// autocorrelation clusters by aggregating from tract-level analysis_scores.
// The county GEOID is matched via prefix (tracts whose GEOID starts with
// the county prefix). Only LISA analyses (type='lisa' in the analyses table)
// are considered. The profile includes per-cluster counts and total tracts.
func (s *PostgresStore) QueryLISACountyProfile(ctx context.Context, countyGEOID string) (*LISACountyProfile, error) {
	const q = `
SELECT
    ascores.tier AS cluster,
    COUNT(*) AS count
FROM analysis_scores ascores
JOIN analyses a ON a.id = ascores.analysis_id
WHERE a.type = 'lisa'
  AND ascores.geoid LIKE $1 || '%'
  AND LENGTH(ascores.geoid) = 11
  AND ascores.tier IS NOT NULL
  AND ascores.tier != ''
GROUP BY ascores.tier
ORDER BY ascores.tier`

	rows, err := s.pool.Query(ctx, q, countyGEOID)
	if err != nil {
		return nil, fmt.Errorf("store: QueryLISACountyProfile: %w", err)
	}
	defer rows.Close()

	profile := &LISACountyProfile{
		GEOID:   countyGEOID,
		Clusters: make([]LISAClusterEntry, 0),
	}

	for rows.Next() {
		var entry LISAClusterEntry
		if err := rows.Scan(&entry.Cluster, &entry.Count); err != nil {
			return nil, fmt.Errorf("store: QueryLISACountyProfile scan: %w", err)
		}
		profile.Clusters = append(profile.Clusters, entry)
		profile.TotalTracts += entry.Count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: QueryLISACountyProfile rows: %w", err)
	}
	return profile, nil
}
