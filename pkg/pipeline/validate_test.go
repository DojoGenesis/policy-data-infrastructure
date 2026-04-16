package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/datasource"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// ---------------------------------------------------------------------------
// mockValidateStore — minimal store.Store for validate tests.
// All methods unused by ValidateStage are stubs that return nil/empty.
// ---------------------------------------------------------------------------

type mockValidateStore struct {
	indicators  []store.Indicator
	geographies []geo.Geography
	queryErr    error
}

func (m *mockValidateStore) PutGeographies(_ context.Context, _ []geo.Geography) error { return nil }
func (m *mockValidateStore) GetGeography(_ context.Context, _ string) (*geo.Geography, error) {
	return nil, nil
}
func (m *mockValidateStore) QueryGeographies(_ context.Context, _ store.GeoQuery) ([]geo.Geography, error) {
	return m.geographies, nil
}
func (m *mockValidateStore) PutIndicators(_ context.Context, _ []store.Indicator) error { return nil }
func (m *mockValidateStore) PutIndicatorsBatch(_ context.Context, _ []store.Indicator, _ int) error {
	return nil
}
func (m *mockValidateStore) QueryIndicators(_ context.Context, _ store.IndicatorQuery) ([]store.Indicator, error) {
	return m.indicators, m.queryErr
}
func (m *mockValidateStore) Aggregate(_ context.Context, _ store.AggregateQuery) (*store.AggregateResult, error) {
	return nil, nil
}
func (m *mockValidateStore) PutAnalysis(_ context.Context, _ store.AnalysisResult) (string, error) {
	return "", nil
}
func (m *mockValidateStore) PutAnalysisScores(_ context.Context, _ []store.AnalysisScore) error {
	return nil
}
func (m *mockValidateStore) QueryAnalysisScores(_ context.Context, _ string, _ string) ([]store.AnalysisScore, error) {
	return nil, nil
}
func (m *mockValidateStore) QueryVariables(_ context.Context) ([]store.VariableMeta, error) {
	return nil, nil
}
func (m *mockValidateStore) ListAnalyses(_ context.Context) ([]store.AnalysisSummary, error) {
	return nil, nil
}
func (m *mockValidateStore) Ping(_ context.Context) error        { return nil }
func (m *mockValidateStore) Migrate(_ context.Context) error    { return nil }
func (m *mockValidateStore) RefreshViews(_ context.Context) error { return nil }
func (m *mockValidateStore) Close() error                        { return nil }

// ---------------------------------------------------------------------------
// Config.Validate tests
// ---------------------------------------------------------------------------

func TestConfigValidate_Valid(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"national scope", Config{Year: 2023}},
		{"state only", Config{StateFIPS: "55", Year: 2023}},
		{"county + state", Config{StateFIPS: "55", CountyFIPS: "025", Year: 2023}},
		{"min year", Config{Year: 2000}},
		{"max year", Config{Year: 2099}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); err != nil {
				t.Errorf("expected nil, got: %v", err)
			}
		})
	}
}

func TestConfigValidate_BadStateFIPS(t *testing.T) {
	cases := []struct {
		name      string
		stateFIPS string
	}{
		{"1 digit", "5"},
		{"3 digits", "055"},
		{"non-numeric", "abc"},
		{"mixed", "5a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{StateFIPS: tc.stateFIPS, Year: 2023}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected error for StateFIPS=%q, got nil", tc.stateFIPS)
			}
			if !strings.Contains(err.Error(), "StateFIPS") {
				t.Errorf("expected error to mention StateFIPS, got: %v", err)
			}
		})
	}
}

func TestConfigValidate_BadCountyFIPS(t *testing.T) {
	cases := []struct {
		name       string
		countyFIPS string
	}{
		{"2 digits", "02"},
		{"4 digits", "0255"},
		{"non-numeric", "abcd"},
		{"mixed", "02a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{StateFIPS: "55", CountyFIPS: tc.countyFIPS, Year: 2023}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected error for CountyFIPS=%q, got nil", tc.countyFIPS)
			}
			if !strings.Contains(err.Error(), "CountyFIPS") {
				t.Errorf("expected error to mention CountyFIPS, got: %v", err)
			}
		})
	}
}

func TestConfigValidate_CountyWithoutState(t *testing.T) {
	cfg := Config{CountyFIPS: "025", Year: 2023} // StateFIPS intentionally empty
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for CountyFIPS without StateFIPS, got nil")
	}
	if !strings.Contains(err.Error(), "StateFIPS") {
		t.Errorf("expected error to mention StateFIPS, got: %v", err)
	}
}

func TestConfigValidate_BadYear(t *testing.T) {
	cases := []struct {
		name string
		year int
	}{
		{"zero", 0},
		{"too old", 1999},
		{"too new", 2100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Year: tc.year}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected error for Year=%d, got nil", tc.year)
			}
			if !strings.Contains(err.Error(), "Year") {
				t.Errorf("expected error to mention Year, got: %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ValidateStage.Run tests
// ---------------------------------------------------------------------------

func makeIndicators(count int, nullCount int, varID string) []store.Indicator {
	inds := make([]store.Indicator, count)
	for i := range inds {
		ind := store.Indicator{
			GEOID:      "55025000100",
			VariableID: varID,
			Vintage:    "ACS-2023-5yr",
		}
		if i < (count - nullCount) {
			v := float64(i + 1)
			ind.Value = &v
		}
		inds[i] = ind
	}
	return inds
}

func TestValidateStage_PassesWithData(t *testing.T) {
	inds := makeIndicators(10, 0, "poverty_rate")
	s := &mockValidateStore{indicators: inds}
	cfg := &Config{Vintage: "ACS-2023-5yr", Year: 2023}
	v := &ValidateStage{}
	if err := v.Run(context.Background(), s, cfg); err != nil {
		t.Errorf("expected pass, got: %v", err)
	}
}

func TestValidateStage_FailsOnZeroIndicators(t *testing.T) {
	s := &mockValidateStore{indicators: []store.Indicator{}}
	cfg := &Config{Vintage: "ACS-2023-5yr", Year: 2023}
	v := &ValidateStage{}
	err := v.Run(context.Background(), s, cfg)
	if err == nil {
		t.Fatal("expected error for zero indicators, got nil")
	}
	if !strings.Contains(err.Error(), "only 0 indicators") {
		t.Errorf("expected error to mention count, got: %v", err)
	}
}

func TestValidateStage_FailsOnHighNullRate(t *testing.T) {
	// 10 indicators, 5 nil = 50% null > 30% default threshold.
	inds := makeIndicators(10, 5, "poverty_rate")
	s := &mockValidateStore{indicators: inds}
	cfg := &Config{Vintage: "ACS-2023-5yr", Year: 2023}
	v := &ValidateStage{}
	err := v.Run(context.Background(), s, cfg)
	if err == nil {
		t.Fatal("expected error for high null rate, got nil")
	}
	if !strings.Contains(err.Error(), "null threshold") {
		t.Errorf("expected error to mention null threshold, got: %v", err)
	}
}

func TestValidateStage_PassesAtThreshold(t *testing.T) {
	// 10 indicators, 3 nil = 30% null = exactly at threshold (not above).
	inds := makeIndicators(10, 3, "poverty_rate")
	s := &mockValidateStore{indicators: inds}
	cfg := &Config{Vintage: "ACS-2023-5yr", Year: 2023}
	v := &ValidateStage{}
	if err := v.Run(context.Background(), s, cfg); err != nil {
		t.Errorf("expected pass at threshold, got: %v", err)
	}
}

func TestValidateStage_DryRun(t *testing.T) {
	// Even with zero indicators, dry-run must not return an error.
	s := &mockValidateStore{indicators: []store.Indicator{}}
	cfg := &Config{Vintage: "ACS-2023-5yr", Year: 2023, DryRun: true}
	v := &ValidateStage{}
	if err := v.Run(context.Background(), s, cfg); err != nil {
		t.Errorf("expected nil in dry-run mode, got: %v", err)
	}
}

func TestValidateStage_Dependencies(t *testing.T) {
	v := &ValidateStage{}
	deps := v.Dependencies()
	if len(deps) != 1 || deps[0] != "fetch" {
		t.Errorf("expected Dependencies()=[\"fetch\"], got: %v", deps)
	}
}

// ---------------------------------------------------------------------------
// Default pipeline stage order — updated to include "validate" (7 stages)
// ---------------------------------------------------------------------------

func TestDefaultPipeline_IncludesValidate(t *testing.T) {
	stages := []Stage{
		NewFetchStage(datasource.NewRegistry()),
		&ValidateStage{},
		&ProcessStage{},
		&EnrichStage{},
		&AnalyzeStage{},
		&SynthesizeStage{},
		&DeliverStage{},
	}

	order, err := topoSort(stages)
	if err != nil {
		t.Fatalf("topoSort: %v", err)
	}

	want := []string{"fetch", "validate", "process", "enrich", "analyze", "synthesize", "deliver"}
	if len(order) != len(want) {
		t.Fatalf("expected %d stages, got %d", len(want), len(order))
	}
	for i, st := range order {
		if st.Name() != want[i] {
			t.Errorf("stage[%d]: want %q got %q", i, want[i], st.Name())
		}
	}
}
