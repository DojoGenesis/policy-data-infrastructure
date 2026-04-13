package datasource

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// AllStateFIPS lists the FIPS codes for all 50 US states plus the District of
// Columbia, in numerical order. These are the valid values for the Census API
// "state" geography parameter.
var AllStateFIPS = []string{
	"01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
	"13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
	"24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
	"34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
	"45", "46", "47", "48", "49", "50", "51", "53", "54", "55", "56",
}

// DefaultNationalParallelism is the default concurrency used by FetchNational
// when maxParallel <= 0. Five concurrent state requests stays well within the
// Census API rate limit of 45 unauthenticated requests per minute.
const DefaultNationalParallelism = 5

// FetchReport summarises the result of a national-scale fetch across all states.
type FetchReport struct {
	Source       string
	TotalStates  int
	Completed    int
	Failed       int
	TotalRecords int
	Errors       []StateError
	Duration     time.Duration
}

// StateError captures a per-state fetch failure without aborting the overall
// national fetch.
type StateError struct {
	StateFIPS string
	Error     string
}

// FetchNational fetches tract-level indicators from ds for every state in
// AllStateFIPS (50 states + DC). Results are written to s via PutIndicators.
//
// maxParallel controls the number of concurrent state requests. Values <= 0
// default to DefaultNationalParallelism (5).
//
// Per-state errors are collected and returned inside the FetchReport rather
// than aborting the entire run. The returned error is non-nil only for
// infrastructure-level failures (e.g. context cancellation).
func FetchNational(ctx context.Context, ds DataSource, s store.Store, maxParallel int) (*FetchReport, error) {
	if maxParallel <= 0 {
		maxParallel = DefaultNationalParallelism
	}

	report := &FetchReport{
		Source:      ds.Name(),
		TotalStates: len(AllStateFIPS),
	}
	start := time.Now()

	// mu guards the mutable fields of report that goroutines update.
	var mu sync.Mutex

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxParallel)

	for _, fips := range AllStateFIPS {
		fips := fips // capture loop variable
		stateName := StateName(fips)
		if stateName == "" {
			stateName = fips
		}

		g.Go(func() error {
			indicators, err := ds.FetchState(gCtx, fips)
			if err != nil {
				mu.Lock()
				report.Failed++
				report.Errors = append(report.Errors, StateError{
					StateFIPS: fips,
					Error:     err.Error(),
				})
				mu.Unlock()
				log.Printf("national: source %q state %s (%s): fetch error (skipping): %v",
					ds.Name(), fips, stateName, err)
				return nil // don't abort — collect the error and continue
			}

			if len(indicators) == 0 {
				log.Printf("national: source %q state %s (%s): 0 indicators returned",
					ds.Name(), fips, stateName)
				mu.Lock()
				report.Completed++
				mu.Unlock()
				return nil
			}

			if err := s.PutIndicators(gCtx, indicators); err != nil {
				// Store writes are more critical; treat as a per-state failure but
				// still continue to collect results from other states.
				mu.Lock()
				report.Failed++
				report.Errors = append(report.Errors, StateError{
					StateFIPS: fips,
					Error:     fmt.Sprintf("store write: %v", err),
				})
				mu.Unlock()
				log.Printf("national: source %q state %s (%s): store write error (skipping): %v",
					ds.Name(), fips, stateName, err)
				return nil
			}

			mu.Lock()
			report.Completed++
			report.TotalRecords += len(indicators)
			mu.Unlock()

			log.Printf("national: source %q state %s (%s): wrote %d indicators",
				ds.Name(), fips, stateName, len(indicators))
			return nil
		})
	}

	// Wait for all goroutines. errgroup only propagates non-nil errors;
	// since our goroutines always return nil, this only fails on gCtx
	// cancellation or a panic-induced errgroup abort.
	if err := g.Wait(); err != nil {
		return report, fmt.Errorf("national fetch: %w", err)
	}

	report.Duration = time.Since(start)
	return report, nil
}
