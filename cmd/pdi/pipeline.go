package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/datasource"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/pipeline"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

func newPipelineCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pipeline",
		Short: "Orchestrate full ingest pipelines",
	}
	cmd.AddCommand(newPipelineRunCmd())
	return cmd
}

func newPipelineRunCmd() *cobra.Command {
	var (
		stateFIPS  string
		countyFIPS string
		scope      string
		year       int
		parallel   int
	)

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run a full ingest pipeline for a geography",
		Long: `Runs the 6-stage pipeline: fetch → process → enrich → analyze → synthesize → deliver.

Stages run in topological order, with independent stages executing in parallel.

Scope options:
  --scope national      Fetch all 51 states in parallel (--state is ignored)
  --state 55            Run for a single state (Wisconsin)
  --state 55 --county 025  Run for a single county`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPipeline(stateFIPS, countyFIPS, scope, year, parallel)
		},
	}

	cmd.Flags().StringVar(&stateFIPS, "state", "", "State FIPS code (e.g. 55)")
	cmd.Flags().StringVar(&countyFIPS, "county", "", "County FIPS code (e.g. 025)")
	cmd.Flags().StringVar(&scope, "scope", "", `Pipeline scope: "national" to run across all 51 states`)
	cmd.Flags().IntVar(&year, "year", 2023, "Data vintage year")
	cmd.Flags().IntVar(&parallel, "parallel", 4, "Maximum parallel stages (and state concurrency for national scope)")

	return cmd
}

func runPipeline(stateFIPS, countyFIPS, scope string, year, parallel int) error {
	isNational := strings.EqualFold(scope, "national")

	if !isNational && stateFIPS == "" {
		return fmt.Errorf("pipeline: --state is required unless --scope national is set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	s, err := store.NewPostgresStore(ctx, resolveConnString())
	if err != nil {
		return fmt.Errorf("pipeline: connect to store: %w", err)
	}
	defer s.Close()

	// For national scope, clear both state and county FIPS so that FetchStage
	// detects the empty-both condition and calls FetchNational.
	cfg := &pipeline.Config{
		Year:        year,
		Vintage:     fmt.Sprintf("ACS-%d-5yr", year),
		Parallelism: parallel,
	}
	if !isNational {
		cfg.StateFIPS = stateFIPS
		cfg.CountyFIPS = countyFIPS
	}
	// When isNational, StateFIPS and CountyFIPS remain "" — the FetchStage
	// detects this and routes to datasource.FetchNational.

	reg := datasource.NewRegistry()
	reg.Register(datasource.NewACSSource(datasource.ACSConfig{Year: year}))
	reg.Register(datasource.NewCDCPlacesSource(datasource.CDCPlacesConfig{Year: year}))
	reg.Register(datasource.NewEPAEJScreenSource(datasource.EPAEJScreenConfig{Year: year}))
	reg.Register(datasource.NewHRSASource(datasource.HRSAConfig{Year: year}))
	reg.Register(datasource.NewGTFSSource(datasource.GTFSConfig{Year: year}))
	reg.Register(datasource.NewWIDPISource(datasource.WIDPIConfig{Year: year}))
	reg.Register(datasource.NewHUDCHASSource(datasource.HUDCHASConfig{Year: year}))
	reg.Register(datasource.NewHMDASource(datasource.HMDAConfig{Year: year}))
	reg.Register(datasource.NewEPATRISource(datasource.EPATRIConfig{Year: year}))

	// Pass the national parallelism budget into the FetchStage.
	natParallel := parallel
	if natParallel <= 0 {
		natParallel = datasource.DefaultNationalParallelism
	}

	p := pipeline.New(
		pipeline.NewFetchStage(reg, natParallel),
		&pipeline.ProcessStage{},
		&pipeline.EnrichStage{},
		&pipeline.AnalyzeStage{},
		&pipeline.SynthesizeStage{},
		&pipeline.DeliverStage{},
	)

	if isNational {
		fmt.Printf("pipeline: national scope — %d states, year=%d, parallel=%d\n",
			len(datasource.AllStateFIPS), year, parallel)
	} else {
		fmt.Printf("pipeline: running for state=%s county=%s year=%d\n", stateFIPS, countyFIPS, year)
	}

	start := time.Now()
	if err := p.Run(ctx, s, cfg); err != nil {
		return fmt.Errorf("pipeline: %w", err)
	}

	fmt.Printf("pipeline: completed in %s\n", time.Since(start).Round(time.Millisecond))
	return nil
}
