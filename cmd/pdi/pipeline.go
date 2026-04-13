package main

import (
	"context"
	"fmt"
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
		year       int
		parallel   int
	)

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run a full ingest pipeline for a geography",
		Long: `Runs the 6-stage pipeline: fetch → process → enrich → analyze → synthesize → deliver.

Stages run in topological order, with independent stages executing in parallel.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPipeline(stateFIPS, countyFIPS, year, parallel)
		},
	}

	cmd.Flags().StringVar(&stateFIPS, "state", "", "State FIPS code (e.g. 55)")
	cmd.Flags().StringVar(&countyFIPS, "county", "", "County FIPS code (e.g. 025)")
	cmd.Flags().IntVar(&year, "year", 2023, "Data vintage year")
	cmd.Flags().IntVar(&parallel, "parallel", 4, "Maximum parallel stages")

	_ = cmd.MarkFlagRequired("state")

	return cmd
}

func runPipeline(stateFIPS, countyFIPS string, year, parallel int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	s, err := store.NewPostgresStore(ctx, resolveConnString())
	if err != nil {
		return fmt.Errorf("pipeline: connect to store: %w", err)
	}
	defer s.Close()

	cfg := &pipeline.Config{
		StateFIPS:   stateFIPS,
		CountyFIPS:  countyFIPS,
		Year:        year,
		Vintage:     fmt.Sprintf("ACS-%d-5yr", year),
		Parallelism: parallel,
	}

	reg := datasource.NewRegistry()
	// Register default data sources — extend here as more adapters are added.
	reg.Register(datasource.NewACSSource(datasource.ACSConfig{Year: year}))

	p := pipeline.New(
		pipeline.NewFetchStage(reg),
		&pipeline.ProcessStage{},
		&pipeline.EnrichStage{},
		&pipeline.AnalyzeStage{},
		&pipeline.SynthesizeStage{},
		&pipeline.DeliverStage{},
	)

	start := time.Now()
	fmt.Printf("pipeline: running for state=%s county=%s year=%d\n", stateFIPS, countyFIPS, year)

	if err := p.Run(ctx, s, cfg); err != nil {
		return fmt.Errorf("pipeline: %w", err)
	}

	fmt.Printf("pipeline: completed in %s\n", time.Since(start).Round(time.Millisecond))
	return nil
}
