package pipeline

import (
	"context"
	"fmt"
	"log"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/datasource"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// FetchStage is Stage 01: it iterates all registered datasources, calls the
// appropriate fetch method based on scope (county, state, or national), and
// writes indicators to the store. Sources that return errors are logged and
// skipped so a single bad source does not abort the entire fetch.
//
// Scope detection:
//   - CountyFIPS != ""  → county scope (requires StateFIPS too)
//   - StateFIPS  != ""  → state scope
//   - Both empty        → national scope (calls datasource.FetchNational for each source)
type FetchStage struct {
	registry    *datasource.Registry
	maxParallel int // concurrency for national fetch; 0 → DefaultNationalParallelism
}

// NewFetchStage constructs a FetchStage backed by the given Registry.
// maxParallel controls state-level concurrency during national fetches (0 = default 5).
func NewFetchStage(registry *datasource.Registry, maxParallel ...int) *FetchStage {
	p := 0
	if len(maxParallel) > 0 {
		p = maxParallel[0]
	}
	return &FetchStage{registry: registry, maxParallel: p}
}

func (f *FetchStage) Name() string          { return "fetch" }
func (f *FetchStage) Dependencies() []string { return nil }

func (f *FetchStage) Run(ctx context.Context, s store.Store, cfg *Config) error {
	if cfg.DryRun {
		log.Printf("fetch: dry-run mode — skipping all source fetches")
		return nil
	}

	sources := f.registry.All()
	if len(sources) == 0 {
		log.Printf("fetch: no sources registered, nothing to fetch")
		return nil
	}

	// National scope: both StateFIPS and CountyFIPS are empty.
	if cfg.StateFIPS == "" && cfg.CountyFIPS == "" {
		return f.runNational(ctx, s, sources, cfg)
	}

	// County or state scope.
	var totalWritten int
	for _, src := range sources {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		indicators, err := f.fetchSource(ctx, src, cfg)
		if err != nil {
			log.Printf("fetch: source %q error (skipping): %v", src.Name(), err)
			continue
		}
		if len(indicators) == 0 {
			log.Printf("fetch: source %q returned 0 indicators", src.Name())
			continue
		}

		if err := s.PutIndicators(ctx, indicators); err != nil {
			log.Printf("fetch: source %q store write error (skipping): %v", src.Name(), err)
			continue
		}
		log.Printf("fetch: source %q wrote %d indicators", src.Name(), len(indicators))
		totalWritten += len(indicators)
	}

	log.Printf("fetch: total indicators written: %d", totalWritten)

	// Refresh materialized views once all sources have written.
	if err := s.RefreshViews(ctx); err != nil {
		log.Printf("fetch: RefreshViews error (non-fatal): %v", err)
	}
	return nil
}

// runNational runs a national-scope fetch across all 51 state FIPS codes for
// each registered source. Per-state errors are tracked inside FetchReport.
func (f *FetchStage) runNational(ctx context.Context, s store.Store, sources []datasource.DataSource, cfg *Config) error {
	log.Printf("fetch: national scope — fetching %d source(s) across %d states",
		len(sources), len(datasource.AllStateFIPS))

	var totalWritten int
	for _, src := range sources {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		report, err := datasource.FetchNational(ctx, src, s, f.maxParallel)
		if err != nil {
			// Context cancellation or catastrophic failure.
			return fmt.Errorf("fetch: national fetch for source %q: %w", src.Name(), err)
		}

		log.Printf("fetch: source %q national complete — states: %d ok / %d failed, records: %d, duration: %s",
			src.Name(), report.Completed, report.Failed, report.TotalRecords, report.Duration)

		for _, se := range report.Errors {
			log.Printf("fetch: source %q state %s (%s) error: %s",
				src.Name(), se.StateFIPS, datasource.StateName(se.StateFIPS), se.Error)
		}

		totalWritten += report.TotalRecords
	}

	log.Printf("fetch: national total indicators written: %d", totalWritten)

	// Refresh materialized views after all national fetches.
	if err := s.RefreshViews(ctx); err != nil {
		log.Printf("fetch: RefreshViews error (non-fatal): %v", err)
	}
	return nil
}

// fetchSource calls the correct DataSource method based on the pipeline scope.
// County-scoped runs use FetchCounty; state-scoped runs use FetchState.
func (f *FetchStage) fetchSource(ctx context.Context, src datasource.DataSource, cfg *Config) ([]store.Indicator, error) {
	if cfg.CountyFIPS != "" {
		if cfg.StateFIPS == "" {
			return nil, fmt.Errorf("CountyFIPS set but StateFIPS is empty")
		}
		return src.FetchCounty(ctx, cfg.StateFIPS, cfg.CountyFIPS)
	}
	return src.FetchState(ctx, cfg.StateFIPS)
}
