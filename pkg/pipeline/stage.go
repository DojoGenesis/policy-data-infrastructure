// Package pipeline provides a DAG-based execution engine for the policy data
// ingest and analysis pipeline. Stages are connected by declared dependencies
// and executed in topological order with configurable parallelism.
package pipeline

import (
	"context"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// Stage is a discrete unit of pipeline work.
type Stage interface {
	// Name returns a unique identifier for this stage.
	Name() string
	// Dependencies returns the Names of stages that must complete before
	// this stage may begin.
	Dependencies() []string
	// Run executes the stage's logic. Implementations should respect ctx
	// cancellation and return a non-nil error on failure.
	Run(ctx context.Context, s store.Store, cfg *Config) error
}

// Config carries the parameters that are common across all pipeline stages.
type Config struct {
	StateFIPS   string
	CountyFIPS  string
	Year        int
	Vintage     string // e.g. "ACS-2023-5yr"
	Parallelism int
	DryRun      bool
}
