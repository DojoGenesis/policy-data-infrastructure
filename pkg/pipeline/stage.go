// Package pipeline provides a DAG-based execution engine for the policy data
// ingest and analysis pipeline. Stages are connected by declared dependencies
// and executed in topological order with configurable parallelism.
package pipeline

import (
	"context"
	"fmt"

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

// Validate checks that Config fields are internally consistent and within
// acceptable ranges. It must be called before Run to prevent silent failures
// from bad FIPS codes or out-of-range years.
func (c *Config) Validate() error {
	// StateFIPS must be "" (national) or exactly 2 digits.
	if c.StateFIPS != "" && !isValidFIPS(c.StateFIPS, 2) {
		return fmt.Errorf("pipeline: invalid StateFIPS %q (must be empty or 2 digits)", c.StateFIPS)
	}
	// CountyFIPS must be "" or exactly 3 digits.
	if c.CountyFIPS != "" && !isValidFIPS(c.CountyFIPS, 3) {
		return fmt.Errorf("pipeline: invalid CountyFIPS %q (must be empty or 3 digits)", c.CountyFIPS)
	}
	// CountyFIPS requires StateFIPS.
	if c.CountyFIPS != "" && c.StateFIPS == "" {
		return fmt.Errorf("pipeline: CountyFIPS set but StateFIPS is empty")
	}
	// Year must be reasonable.
	if c.Year < 2000 || c.Year > 2099 {
		return fmt.Errorf("pipeline: Year %d is out of range [2000, 2099]", c.Year)
	}
	return nil
}

// isValidFIPS returns true when s consists entirely of decimal digits and has
// exactly the required length.
func isValidFIPS(s string, length int) bool {
	if len(s) != length {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
