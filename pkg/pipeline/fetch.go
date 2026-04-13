package pipeline

import (
	"context"
	"fmt"
	"log"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/datasource"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// FetchStage is Stage 01: it iterates all registered datasources, calls the
// appropriate fetch method based on scope (county vs state), and writes
// indicators to the store. Sources that return errors are logged and skipped
// so a single bad source does not abort the entire fetch.
type FetchStage struct {
	registry *datasource.Registry
}

// NewFetchStage constructs a FetchStage backed by the given Registry.
func NewFetchStage(registry *datasource.Registry) *FetchStage {
	return &FetchStage{registry: registry}
}

func (f *FetchStage) Name() string         { return "fetch" }
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
	if cfg.StateFIPS == "" {
		return nil, fmt.Errorf("neither CountyFIPS nor StateFIPS is set in config")
	}
	return src.FetchState(ctx, cfg.StateFIPS)
}
