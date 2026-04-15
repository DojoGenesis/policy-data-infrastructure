package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
)

// fp returns a pointer to v — convenience for Indicator.Value fields.
func fp(v float64) *float64 { return &v }

// testStore opens a PostgresStore against the test database, skipping if the
// database is unavailable or if -short is passed. Tables are cleaned after
// the store is created so every test starts from a blank slate.
func testStore(t *testing.T) *PostgresStore {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	connStr := os.Getenv("PDI_TEST_DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://pdi:pdi@localhost:5432/pdi_test?sslmode=disable"
	}
	ctx := context.Background()
	s, err := NewPostgresStore(ctx, connStr)
	if err != nil {
		t.Skipf("skipping: cannot connect to test database: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	cleanTables(t, s)
	return s
}

// cleanTables deletes all rows in dependency order so FK constraints are not
// violated. Missing tables (first run before migration) are silently ignored.
func cleanTables(t *testing.T, s *PostgresStore) {
	t.Helper()
	ctx := context.Background()
	tables := []string{
		"analysis_scores",
		"analyses",
		"indicators",
		"indicator_meta",
		"indicator_sources",
		"geographies",
	}
	for _, tbl := range tables {
		if _, err := s.pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s", tbl)); err != nil {
			// Table might not exist yet (first run) — skip silently.
			continue
		}
	}
}

// seedIndicatorDeps inserts the FK chain that indicators depend on:
// indicator_sources → indicator_meta for three test variables.
func seedIndicatorDeps(t *testing.T, s *PostgresStore) {
	t.Helper()
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO indicator_sources (source_id, name, category)
		VALUES ('test-source', 'Test Source', 'test')
		ON CONFLICT DO NOTHING`)
	if err != nil {
		t.Fatalf("seed indicator_sources: %v", err)
	}
	for _, v := range []string{"test_var_a", "test_var_b", "test_var_c"} {
		_, err = s.pool.Exec(ctx, `
			INSERT INTO indicator_meta (variable_id, source_id, name, description, unit, direction)
			VALUES ($1, 'test-source', $1, 'test', 'count', 'neutral')
			ON CONFLICT DO NOTHING`, v)
		if err != nil {
			t.Fatalf("seed indicator_meta %s: %v", v, err)
		}
	}
}

// sampleCounty returns a county-level Geography for Dane County WI.
func sampleCounty(geoid, name string) geo.Geography {
	return geo.Geography{
		GEOID:      geoid,
		Level:      geo.County,
		Name:       name,
		StateFIPS:  "55",
		CountyFIPS: geoid[2:],
		Population: 550000,
		LandAreaM2: 3_126_000_000,
	}
}

// sampleTract returns a tract-level Geography.
func sampleTract(geoid string) geo.Geography {
	return geo.Geography{
		GEOID:      geoid,
		Level:      geo.Tract,
		Name:       "Census Tract " + geoid,
		StateFIPS:  "55",
		CountyFIPS: geoid[2:5],
		Population: 4200,
		LandAreaM2: 8_500_000,
	}
}

// ── Geography tests ───────────────────────────────────────────────────────────

func TestPutGetGeography(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	g := sampleCounty("55025", "Dane County")
	if err := s.PutGeographies(ctx, []geo.Geography{g}); err != nil {
		t.Fatalf("PutGeographies: %v", err)
	}

	got, err := s.GetGeography(ctx, "55025")
	if err != nil {
		t.Fatalf("GetGeography: %v", err)
	}

	if got.GEOID != g.GEOID {
		t.Errorf("GEOID: got %q, want %q", got.GEOID, g.GEOID)
	}
	if got.Level != g.Level {
		t.Errorf("Level: got %q, want %q", got.Level, g.Level)
	}
	if got.Name != g.Name {
		t.Errorf("Name: got %q, want %q", got.Name, g.Name)
	}
	if got.StateFIPS != g.StateFIPS {
		t.Errorf("StateFIPS: got %q, want %q", got.StateFIPS, g.StateFIPS)
	}
	if got.CountyFIPS != g.CountyFIPS {
		t.Errorf("CountyFIPS: got %q, want %q", got.CountyFIPS, g.CountyFIPS)
	}
	if got.Population != g.Population {
		t.Errorf("Population: got %d, want %d", got.Population, g.Population)
	}
}

func TestQueryGeographies_ByLevel(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	geos := []geo.Geography{
		sampleCounty("55025", "Dane County"),
		sampleCounty("55079", "Milwaukee County"),
		sampleTract("55025000100"),
	}
	if err := s.PutGeographies(ctx, geos); err != nil {
		t.Fatalf("PutGeographies: %v", err)
	}

	got, err := s.QueryGeographies(ctx, GeoQuery{Level: geo.County})
	if err != nil {
		t.Fatalf("QueryGeographies: %v", err)
	}

	if len(got) != 2 {
		t.Errorf("expected 2 county results, got %d", len(got))
	}
	for _, g := range got {
		if g.Level != geo.County {
			t.Errorf("unexpected level %q in county results", g.Level)
		}
	}
}

func TestQueryGeographies_ByNameSearch(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	geos := []geo.Geography{
		sampleCounty("55025", "Dane County"),
		sampleCounty("55079", "Milwaukee County"),
	}
	if err := s.PutGeographies(ctx, geos); err != nil {
		t.Fatalf("PutGeographies: %v", err)
	}

	got, err := s.QueryGeographies(ctx, GeoQuery{NameSearch: "dane"})
	if err != nil {
		t.Fatalf("QueryGeographies: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 result for 'dane', got %d", len(got))
	}
	if got[0].GEOID != "55025" {
		t.Errorf("expected GEOID 55025, got %q", got[0].GEOID)
	}
}

// ── Indicator tests ───────────────────────────────────────────────────────────

func TestPutGetIndicators(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if err := s.PutGeographies(ctx, []geo.Geography{sampleTract("55025000100")}); err != nil {
		t.Fatalf("PutGeographies: %v", err)
	}
	seedIndicatorDeps(t, s)

	indicators := []Indicator{
		{GEOID: "55025000100", VariableID: "test_var_a", Vintage: "2023", Value: fp(1.1)},
		{GEOID: "55025000100", VariableID: "test_var_a", Vintage: "2022", Value: fp(2.2)},
		{GEOID: "55025000100", VariableID: "test_var_b", Vintage: "2023", Value: fp(3.3)},
		{GEOID: "55025000100", VariableID: "test_var_b", Vintage: "2022", Value: fp(4.4)},
		{GEOID: "55025000100", VariableID: "test_var_c", Vintage: "2023", Value: fp(5.5)},
	}

	if err := s.PutIndicators(ctx, indicators); err != nil {
		t.Fatalf("PutIndicators: %v", err)
	}

	got, err := s.QueryIndicators(ctx, IndicatorQuery{GEOIDs: []string{"55025000100"}})
	if err != nil {
		t.Fatalf("QueryIndicators: %v", err)
	}

	if len(got) != 5 {
		t.Errorf("expected 5 indicators, got %d", len(got))
	}
}

func TestPutIndicators_Upsert(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if err := s.PutGeographies(ctx, []geo.Geography{sampleTract("55025000100")}); err != nil {
		t.Fatalf("PutGeographies: %v", err)
	}
	seedIndicatorDeps(t, s)

	first := []Indicator{
		{GEOID: "55025000100", VariableID: "test_var_a", Vintage: "2023", Value: fp(10.0)},
	}
	if err := s.PutIndicators(ctx, first); err != nil {
		t.Fatalf("PutIndicators (first): %v", err)
	}

	// Same key, different value — second write should win.
	second := []Indicator{
		{GEOID: "55025000100", VariableID: "test_var_a", Vintage: "2023", Value: fp(99.9)},
	}
	if err := s.PutIndicators(ctx, second); err != nil {
		t.Fatalf("PutIndicators (second): %v", err)
	}

	got, err := s.QueryIndicators(ctx, IndicatorQuery{
		GEOIDs:      []string{"55025000100"},
		VariableIDs: []string{"test_var_a"},
		Vintage:     "2023",
	})
	if err != nil {
		t.Fatalf("QueryIndicators: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 indicator, got %d", len(got))
	}
	if got[0].Value == nil || *got[0].Value != 99.9 {
		t.Errorf("expected upserted value 99.9, got %v", got[0].Value)
	}
}

func TestPutIndicatorsBatch(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	// Seed one geography and the three variable FKs.
	if err := s.PutGeographies(ctx, []geo.Geography{sampleTract("55025000100")}); err != nil {
		t.Fatalf("PutGeographies: %v", err)
	}
	seedIndicatorDeps(t, s)

	const total = 500
	inds := make([]Indicator, total)
	vars := []string{"test_var_a", "test_var_b", "test_var_c"}
	for i := 0; i < total; i++ {
		v := fp(float64(i))
		inds[i] = Indicator{
			GEOID:      "55025000100",
			VariableID: vars[i%3],
			Vintage:    fmt.Sprintf("%04d", 1600+i), // unique vintage per row
			Value:      v,
		}
	}

	if err := s.PutIndicatorsBatch(ctx, inds, 100); err != nil {
		t.Fatalf("PutIndicatorsBatch: %v", err)
	}

	got, err := s.QueryIndicators(ctx, IndicatorQuery{GEOIDs: []string{"55025000100"}})
	if err != nil {
		t.Fatalf("QueryIndicators: %v", err)
	}
	if len(got) != total {
		t.Errorf("expected %d indicators, got %d", total, len(got))
	}
}

func TestQueryIndicators_FilterByVariable(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if err := s.PutGeographies(ctx, []geo.Geography{sampleTract("55025000100")}); err != nil {
		t.Fatalf("PutGeographies: %v", err)
	}
	seedIndicatorDeps(t, s)

	inds := []Indicator{
		{GEOID: "55025000100", VariableID: "test_var_a", Vintage: "2023", Value: fp(1.0)},
		{GEOID: "55025000100", VariableID: "test_var_b", Vintage: "2023", Value: fp(2.0)},
		{GEOID: "55025000100", VariableID: "test_var_c", Vintage: "2023", Value: fp(3.0)},
	}
	if err := s.PutIndicators(ctx, inds); err != nil {
		t.Fatalf("PutIndicators: %v", err)
	}

	got, err := s.QueryIndicators(ctx, IndicatorQuery{
		GEOIDs:      []string{"55025000100"},
		VariableIDs: []string{"test_var_b"},
	})
	if err != nil {
		t.Fatalf("QueryIndicators: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 result for test_var_b, got %d", len(got))
	}
	if got[0].VariableID != "test_var_b" {
		t.Errorf("expected variable test_var_b, got %q", got[0].VariableID)
	}
}

func TestRefreshViews_LatestOnly(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if err := s.PutGeographies(ctx, []geo.Geography{sampleTract("55025000100")}); err != nil {
		t.Fatalf("PutGeographies: %v", err)
	}
	seedIndicatorDeps(t, s)

	inds := []Indicator{
		{GEOID: "55025000100", VariableID: "test_var_a", Vintage: "2022", Value: fp(10.0)},
		{GEOID: "55025000100", VariableID: "test_var_a", Vintage: "2023", Value: fp(20.0)},
		{GEOID: "55025000100", VariableID: "test_var_b", Vintage: "2022", Value: fp(30.0)},
		{GEOID: "55025000100", VariableID: "test_var_b", Vintage: "2023", Value: fp(40.0)},
	}
	if err := s.PutIndicators(ctx, inds); err != nil {
		t.Fatalf("PutIndicators: %v", err)
	}

	if err := s.RefreshViews(ctx); err != nil {
		t.Fatalf("RefreshViews: %v", err)
	}

	got, err := s.QueryIndicators(ctx, IndicatorQuery{
		GEOIDs:     []string{"55025000100"},
		LatestOnly: true,
	})
	if err != nil {
		t.Fatalf("QueryIndicators (LatestOnly): %v", err)
	}

	// Should return only the 2023 vintage for each variable → 2 rows.
	if len(got) != 2 {
		t.Errorf("LatestOnly: expected 2 results (one per variable), got %d", len(got))
	}
	for _, ind := range got {
		if ind.Vintage != "2023" {
			t.Errorf("LatestOnly: expected vintage 2023, got %q", ind.Vintage)
		}
	}
}

// ── Aggregate tests ───────────────────────────────────────────────────────────

func TestAggregate_Avg(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	// Seed two county geographies with known values.
	geos := []geo.Geography{
		sampleCounty("55025", "Dane County"),
		sampleCounty("55079", "Milwaukee County"),
	}
	if err := s.PutGeographies(ctx, geos); err != nil {
		t.Fatalf("PutGeographies: %v", err)
	}
	seedIndicatorDeps(t, s)

	inds := []Indicator{
		{GEOID: "55025", VariableID: "test_var_a", Vintage: "2023", Value: fp(10.0)},
		{GEOID: "55079", VariableID: "test_var_a", Vintage: "2023", Value: fp(20.0)},
	}
	if err := s.PutIndicators(ctx, inds); err != nil {
		t.Fatalf("PutIndicators: %v", err)
	}
	if err := s.RefreshViews(ctx); err != nil {
		t.Fatalf("RefreshViews: %v", err)
	}

	res, err := s.Aggregate(ctx, AggregateQuery{
		VariableID: "test_var_a",
		Level:      geo.County,
		Function:   "avg",
	})
	if err != nil {
		t.Fatalf("Aggregate avg: %v", err)
	}

	// avg(10, 20) = 15
	if res.Value != 15.0 {
		t.Errorf("avg: expected 15.0, got %v", res.Value)
	}
	if res.Count != 2 {
		t.Errorf("avg count: expected 2, got %d", res.Count)
	}
}

func TestAggregate_Count(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	geos := []geo.Geography{
		sampleCounty("55025", "Dane County"),
		sampleCounty("55079", "Milwaukee County"),
		sampleCounty("55001", "Adams County"),
	}
	if err := s.PutGeographies(ctx, geos); err != nil {
		t.Fatalf("PutGeographies: %v", err)
	}
	seedIndicatorDeps(t, s)

	inds := []Indicator{
		{GEOID: "55025", VariableID: "test_var_a", Vintage: "2023", Value: fp(1.0)},
		{GEOID: "55079", VariableID: "test_var_a", Vintage: "2023", Value: fp(2.0)},
		{GEOID: "55001", VariableID: "test_var_a", Vintage: "2023", Value: fp(3.0)},
	}
	if err := s.PutIndicators(ctx, inds); err != nil {
		t.Fatalf("PutIndicators: %v", err)
	}
	if err := s.RefreshViews(ctx); err != nil {
		t.Fatalf("RefreshViews: %v", err)
	}

	res, err := s.Aggregate(ctx, AggregateQuery{
		VariableID: "test_var_a",
		Level:      geo.County,
		Function:   "count",
	})
	if err != nil {
		t.Fatalf("Aggregate count: %v", err)
	}

	// COUNT returns as Value (COUNT(il.value) expr), Count field == COUNT(il.value) too.
	if res.Count != 3 {
		t.Errorf("count: expected Count=3, got %d", res.Count)
	}
}

func TestAggregate_InvalidFunction(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	_, err := s.Aggregate(ctx, AggregateQuery{
		VariableID: "test_var_a",
		Level:      geo.County,
		Function:   "invalid_fn",
	})
	if err == nil {
		t.Fatal("expected error for invalid aggregate function, got nil")
	}
	if !strings.Contains(err.Error(), "invalid_fn") {
		t.Errorf("error should mention the invalid function name, got: %v", err)
	}
}

// ── Analysis tests ────────────────────────────────────────────────────────────

func TestPutAnalysis_ReturnsUUID(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	result := AnalysisResult{
		Type:       "equity_index",
		ScopeLevel: string(geo.County),
		Parameters: map[string]interface{}{"year": "2023"},
		Results:    map[string]interface{}{"status": "complete"},
		Vintage:    "2023",
	}

	id, err := s.PutAnalysis(ctx, result)
	if err != nil {
		t.Fatalf("PutAnalysis: %v", err)
	}
	if id == "" {
		t.Fatal("PutAnalysis returned empty ID")
	}
	// UUID format: 8-4-4-4-12 hex chars with dashes
	parts := strings.Split(id, "-")
	if len(parts) != 5 {
		t.Errorf("ID does not look like a UUID (expected 5 dash-separated parts): %q", id)
	}
}

func TestPutQueryAnalysisScores(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	// Analysis scores reference geographies — seed a few counties.
	geos := []geo.Geography{
		sampleCounty("55025", "Dane County"),
		sampleCounty("55079", "Milwaukee County"),
		sampleCounty("55001", "Adams County"),
	}
	if err := s.PutGeographies(ctx, geos); err != nil {
		t.Fatalf("PutGeographies: %v", err)
	}

	analysis := AnalysisResult{
		Type:       "equity_index",
		ScopeLevel: string(geo.County),
		Parameters: map[string]interface{}{"method": "z-score"},
		Results:    map[string]interface{}{"n": 3},
		Vintage:    "2023",
	}
	analysisID, err := s.PutAnalysis(ctx, analysis)
	if err != nil {
		t.Fatalf("PutAnalysis: %v", err)
	}

	scores := []AnalysisScore{
		{
			AnalysisID: analysisID,
			GEOID:      "55001",
			Score:      0.33,
			Rank:       3,
			Percentile: 33.0,
			Tier:       "low",
			Details:    map[string]interface{}{"note": "bottom third"},
		},
		{
			AnalysisID: analysisID,
			GEOID:      "55025",
			Score:      0.67,
			Rank:       2,
			Percentile: 67.0,
			Tier:       "mid",
			Details:    map[string]interface{}{"note": "middle"},
		},
		{
			AnalysisID: analysisID,
			GEOID:      "55079",
			Score:      1.00,
			Rank:       1,
			Percentile: 100.0,
			Tier:       "high",
			Details:    map[string]interface{}{"note": "top"},
		},
	}

	if err := s.PutAnalysisScores(ctx, scores); err != nil {
		t.Fatalf("PutAnalysisScores: %v", err)
	}

	got, err := s.QueryAnalysisScores(ctx, analysisID, "")
	if err != nil {
		t.Fatalf("QueryAnalysisScores: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 scores, got %d", len(got))
	}

	// Results are ordered by rank ASC; rank 1 should be first.
	if got[0].Rank != 1 {
		t.Errorf("first result rank: expected 1, got %d", got[0].Rank)
	}
	if got[0].GEOID != "55079" {
		t.Errorf("first result GEOID: expected 55079, got %q", got[0].GEOID)
	}
	if got[2].Rank != 3 {
		t.Errorf("last result rank: expected 3, got %d", got[2].Rank)
	}
}

// ── Edge-case and lifecycle tests ─────────────────────────────────────────────

func TestGetGeography_NotFound(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	_, err := s.GetGeography(ctx, "99999")
	if err == nil {
		t.Fatal("expected error for non-existent GEOID, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "not found") {
		t.Errorf("error should mention 'not found', got: %v", err)
	}
}

func TestMigrate_Idempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	// NewPostgresStore already called Migrate once; calling it again must not error.
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate call returned error: %v", err)
	}
}
