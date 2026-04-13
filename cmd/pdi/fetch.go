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
//	pdi fetch --state 55 --county 025 --year 2023 --sources acs,tiger
func newFetchCmd() *cobra.Command {
	var (
		stateFIPS  string
		countyFIPS string
		year       int
		sources    string
		dryRun     bool
	)

	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch source data from upstream APIs",
		Long: `fetch downloads indicators from registered data sources and writes them
to the PostgreSQL store. Use --sources to limit which sources are fetched
(comma-separated list of source names, e.g. "acs-5yr,tiger"). When --dry-run
is set, no data is written but the fetch plan is printed.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFetch(stateFIPS, countyFIPS, year, sources, dryRun)
		},
	}

	cmd.Flags().StringVar(&stateFIPS, "state", "", "State FIPS code (e.g. 55 for Wisconsin) [required]")
	cmd.Flags().StringVar(&countyFIPS, "county", "", "County FIPS code (e.g. 025 for Dane County; omit for state-wide fetch)")
	cmd.Flags().IntVar(&year, "year", 2023, "ACS/TIGER vintage year")
	cmd.Flags().StringVar(&sources, "sources", "acs-5yr", "Comma-separated list of sources to fetch (e.g. acs-5yr,tiger)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print what would be fetched without writing to the store")

	_ = cmd.MarkFlagRequired("state")

	return cmd
}

func runFetch(stateFIPS, countyFIPS string, year int, sourcesFlag string, dryRun bool) error {
	if stateFIPS == "" {
		return fmt.Errorf("fetch: --state is required")
	}

	// Parse requested source names.
	requestedSources := parseCSV(sourcesFlag)

	// Build the datasource registry.
	reg := datasource.NewRegistry()
	reg.Register(datasource.NewACSSource(datasource.ACSConfig{Year: year}))
	reg.Register(datasource.NewTIGERSource(year))

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
		fmt.Printf("fetch: dry-run — would fetch %d source(s) for state=%s", len(toFetch), stateFIPS)
		if countyFIPS != "" {
			fmt.Printf(" county=%s", countyFIPS)
		}
		fmt.Println()
		for _, ds := range toFetch {
			fmt.Printf("  source: %-16s category: %-14s vintage: %s\n", ds.Name(), ds.Category(), ds.Vintage())
		}
		return nil
	}

	// Connect to the store.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	s, err := store.NewPostgresStore(ctx, resolveConnString())
	if err != nil {
		return fmt.Errorf("fetch: connect to store: %w", err)
	}
	defer s.Close()

	// Fetch each source and write to store.
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
