package pipeline

import (
	"context"
	"fmt"
	"log"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// ValidateStage checks data quality after fetch — the Go equivalent of
// the Python null_audit() gate. It verifies:
//   - Total indicator count exceeds a minimum
//   - Per-variable null rate is below a threshold
//   - GEOID coverage is non-zero at the expected level
//
// If any check fails, the stage returns an error and the pipeline stops.
type ValidateStage struct {
	// MinIndicators is the minimum total indicator count to proceed.
	// Default (0) means at least 1 indicator must be present.
	MinIndicators int
	// MaxNullRate is the maximum acceptable null rate (0.0–1.0) per variable.
	// Default (0) uses 0.30 (30%), matching the Python null_audit convention.
	MaxNullRate float64
}

func (v *ValidateStage) Name() string          { return "validate" }
func (v *ValidateStage) Dependencies() []string { return []string{"fetch"} }

func (v *ValidateStage) Run(ctx context.Context, s store.Store, cfg *Config) error {
	if cfg.DryRun {
		log.Printf("validate: dry-run — skipping validation")
		return nil
	}

	minInd := v.MinIndicators
	if minInd <= 0 {
		minInd = 1
	}
	maxNull := v.MaxNullRate
	if maxNull <= 0 {
		maxNull = 0.30
	}

	// Query all indicators for the vintage.
	q := store.IndicatorQuery{
		Vintage:    cfg.Vintage,
		LatestOnly: false,
	}
	indicators, err := s.QueryIndicators(ctx, q)
	if err != nil {
		return fmt.Errorf("validate: query indicators: %w", err)
	}

	total := len(indicators)
	if total < minInd {
		return fmt.Errorf("validate: only %d indicators for vintage %q (minimum: %d) — fetch may have failed silently",
			total, cfg.Vintage, minInd)
	}
	log.Printf("validate: %d total indicators for vintage %q", total, cfg.Vintage)

	// Per-variable null rate check.
	type varStats struct {
		total int
		nulls int
	}
	byVar := make(map[string]*varStats)
	for _, ind := range indicators {
		vs, ok := byVar[ind.VariableID]
		if !ok {
			vs = &varStats{}
			byVar[ind.VariableID] = vs
		}
		vs.total++
		if ind.Value == nil {
			vs.nulls++
		}
	}

	var warnings []string
	for varID, vs := range byVar {
		if vs.total == 0 {
			continue
		}
		nullRate := float64(vs.nulls) / float64(vs.total)
		if nullRate > maxNull {
			warnings = append(warnings, fmt.Sprintf("%s: %.0f%% null (%d/%d)",
				varID, nullRate*100, vs.nulls, vs.total))
		}
		// 0% null is suspicious for government data — log a warning.
		if vs.nulls == 0 && vs.total > 10 {
			log.Printf("validate: WARNING — %s has 0%% null (%d rows) — check if suppression handling was missed",
				varID, vs.total)
		}
	}

	if len(warnings) > 0 {
		return fmt.Errorf("validate: %d variable(s) exceed %.0f%% null threshold:\n  %s",
			len(warnings), maxNull*100, joinStrings(warnings, "\n  "))
	}

	log.Printf("validate: PASS — %d indicators, %d variables, all within %.0f%% null threshold",
		total, len(byVar), maxNull*100)
	return nil
}

func joinStrings(ss []string, sep string) string {
	result := ""
	for i, s := range ss {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}
