package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/datasource"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// newFetchCmd returns the "pdi fetch" command.
//
// Usage:
//
//	pdi fetch --state 55 --county 025 --year 2023 --sources acs-5yr,cdc-places
//	pdi fetch --scope national --sources acs-5yr --parallel 5
func newFetchCmd() *cobra.Command {
	var (
		stateFIPS  string
		countyFIPS string
		scope      string
		year       int
		sources    string
		parallel   int
		dryRun     bool
	)

	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch source data from upstream APIs",
		Long: `fetch downloads indicators from registered data sources and writes them
to the PostgreSQL store. Use --sources to limit which sources are fetched
(comma-separated list of source names, e.g. "acs-5yr,cdc-places").

Scope options:
  --scope national    Fetch all 51 state FIPS codes in parallel (--state is ignored)
  --state 55          Fetch a single state (Wisconsin)
  --state 55 --county 025  Fetch a single county

When --dry-run is set, no data is written but the fetch plan is printed.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFetch(stateFIPS, countyFIPS, scope, year, sources, parallel, dryRun)
		},
	}

	cmd.Flags().StringVar(&stateFIPS, "state", "", "State FIPS code (e.g. 55 for Wisconsin)")
	cmd.Flags().StringVar(&countyFIPS, "county", "", "County FIPS code (e.g. 025 for Dane County; omit for state-wide fetch)")
	cmd.Flags().StringVar(&scope, "scope", "", `Fetch scope: "national" to fetch all 51 states in parallel`)
	cmd.Flags().IntVar(&year, "year", 2023, "ACS/TIGER vintage year")
	cmd.Flags().StringVar(&sources, "sources", "acs-5yr", "Comma-separated list of sources to fetch (e.g. acs-5yr,cdc-places,epa-ejscreen)")
	cmd.Flags().IntVar(&parallel, "parallel", datasource.DefaultNationalParallelism, "Max parallel state requests (national scope only)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print what would be fetched without writing to the store")

	return cmd
}

func runFetch(stateFIPS, countyFIPS, scope string, year int, sourcesFlag string, parallel int, dryRun bool) error {
	isNational := strings.EqualFold(scope, "national")

	if !isNational && stateFIPS == "" {
		return fmt.Errorf("fetch: --state is required unless --scope national is set")
	}

	// Parse requested source names.
	requestedSources := parseCSV(sourcesFlag)

	// Build the datasource registry.
	reg := datasource.NewRegistry()
	reg.Register(datasource.NewACSSource(datasource.ACSConfig{Year: year}))
	reg.Register(datasource.NewTIGERSource(year))
	reg.Register(datasource.NewCDCPlacesSource(datasource.CDCPlacesConfig{Year: year}))
	reg.Register(datasource.NewEPAEJScreenSource(datasource.EPAEJScreenConfig{Year: year}))

	// Filter to requested sources.
	var toFetch []datasource.DataSource
	for _, name := range requestedSources {
		ds, ok := reg.Get(name)
		if !ok {
			fmt.Fprintf(os.Stderr, "fetch: warning: unknown source %q (skipping)\n", name)
			continue
		}
		toFetch = append(toFetch, ds)
	}
	if len(toFetch) == 0 {
		return fmt.Errorf("fetch: no valid sources found in %q", sourcesFlag)
	}

	if dryRun {
		if isNational {
			fmt.Printf("fetch: dry-run — would fetch %d source(s) across %d states (parallel=%d)\n",
				len(toFetch), len(datasource.AllStateFIPS), parallel)
		} else {
			fmt.Printf("fetch: dry-run — would fetch %d source(s) for state=%s", len(toFetch), stateFIPS)
			if countyFIPS != "" {
				fmt.Printf(" county=%s", countyFIPS)
			}
			fmt.Println()
		}
		for _, ds := range toFetch {
			fmt.Printf("  source: %-16s category: %-14s vintage: %s\n", ds.Name(), ds.Category(), ds.Vintage())
		}
		return nil
	}

	// Connect to the store.
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Hour)
	defer cancel()

	s, err := store.NewPostgresStore(ctx, resolveConnString())
	if err != nil {
		return fmt.Errorf("fetch: connect to store: %w", err)
	}
	defer s.Close()

	if isNational {
		return runFetchNational(ctx, toFetch, s, parallel)
	}
	return runFetchScoped(ctx, toFetch, s, stateFIPS, countyFIPS)
}

// runFetchNational executes a national-scale fetch and prints a summary report.
func runFetchNational(ctx context.Context, toFetch []datasource.DataSource, s store.Store, parallel int) error {
	fmt.Printf("fetch: national scope — %d source(s), %d states, parallel=%d\n",
		len(toFetch), len(datasource.AllStateFIPS), parallel)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SOURCE\tSTATES_OK\tSTATES_FAILED\tRECORDS\tDURATION")

	for _, ds := range toFetch {
		report, err := datasource.FetchNational(ctx, ds, s, parallel)
		if err != nil {
			fmt.Fprintf(w, "%s\t-\t-\t0\tERROR: %v\n", ds.Name(), err)
			_ = w.Flush()
			return err
		}

		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%s\n",
			ds.Name(),
			report.Completed,
			report.Failed,
			report.TotalRecords,
			report.Duration.Round(time.Millisecond),
		)

		// Print per-state errors below the table row.
		for _, se := range report.Errors {
			name := datasource.StateName(se.StateFIPS)
			if name == "" {
				name = se.StateFIPS
			}
			fmt.Fprintf(os.Stderr, "  state %s (%s): %s\n", se.StateFIPS, name, se.Error)
		}
	}

	return w.Flush()
}

// runFetchScoped executes a county- or state-scoped fetch.
func runFetchScoped(ctx context.Context, toFetch []datasource.DataSource, s store.Store, stateFIPS, countyFIPS string) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SOURCE\tRECORDS\tDURATION\tSTATUS")

	for _, ds := range toFetch {
		start := time.Now()

		var indicators []store.Indicator
		var fetchErr error

		if countyFIPS != "" {
			indicators, fetchErr = ds.FetchCounty(ctx, stateFIPS, countyFIPS)
		} else {
			indicators, fetchErr = ds.FetchState(ctx, stateFIPS)
		}

		elapsed := time.Since(start).Round(time.Millisecond)

		if fetchErr != nil {
			fmt.Fprintf(w, "%s\t0\t%s\tERROR: %v\n", ds.Name(), elapsed, fetchErr)
			continue
		}

		if len(indicators) > 0 {
			if writeErr := s.PutIndicators(ctx, indicators); writeErr != nil {
				fmt.Fprintf(w, "%s\t%d\t%s\tERROR (write): %v\n", ds.Name(), len(indicators), elapsed, writeErr)
				continue
			}
		}

		fmt.Fprintf(w, "%s\t%d\t%s\tok\n", ds.Name(), len(indicators), elapsed)
	}

	return w.Flush()
}

// parseCSV splits a comma-separated string into trimmed, non-empty tokens.
func parseCSV(s string) []string {
	parts := strings.Split(s, ",")
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
