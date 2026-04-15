package pipeline

import (
	"context"
	"fmt"
	"log"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// ProcessStage is Stage 02: it queries all raw indicators, validates their
// GEOIDs against the geographies table, computes derived indicators, and
// writes the derived indicators back to the store.
type ProcessStage struct{}

func (p *ProcessStage) Name() string          { return "process" }
func (p *ProcessStage) Dependencies() []string { return []string{"validate"} }

func (p *ProcessStage) Run(ctx context.Context, s store.Store, cfg *Config) error {
	if cfg.DryRun {
		log.Printf("process: dry-run mode — skipping")
		return nil
	}

	// 1. Query all indicators in scope.
	indicators, err := s.QueryIndicators(ctx, store.IndicatorQuery{
		Vintage:    cfg.Vintage,
		LatestOnly: true,
	})
	if err != nil {
		return fmt.Errorf("process: query indicators: %w", err)
	}
	log.Printf("process: loaded %d raw indicators", len(indicators))

	// 2. Validate GEOIDs: build a lookup of all known tract GEOIDs for the scope.
	geoQuery := store.GeoQuery{Level: geo.Tract}
	if cfg.CountyFIPS != "" {
		geoQuery.CountyFIPS = cfg.CountyFIPS
		geoQuery.StateFIPS = cfg.StateFIPS
	} else {
		geoQuery.StateFIPS = cfg.StateFIPS
	}

	geographies, err := s.QueryGeographies(ctx, geoQuery)
	if err != nil {
		return fmt.Errorf("process: query geographies: %w", err)
	}

	knownGEOIDs := make(map[string]bool, len(geographies))
	for _, g := range geographies {
		knownGEOIDs[g.GEOID] = true
	}

	// Index indicators by GEOID+VariableID for derived computation.
	type derivedKey = struct{ geoid, variableID string }
	indicatorIdx := make(map[derivedKey]*float64, len(indicators))
	for _, ind := range indicators {
		if !knownGEOIDs[ind.GEOID] {
			log.Printf("process: warning: GEOID %q not in geographies table (skipping)", ind.GEOID)
			continue
		}
		k := derivedKey{ind.GEOID, ind.VariableID}
		indicatorIdx[k] = ind.Value
	}

	// 3. Compute derived indicators.
	derived := computeDerivedIndicators(indicatorIdx, cfg.Vintage, knownGEOIDs)
	if len(derived) == 0 {
		log.Printf("process: no derived indicators computed (missing source variables?)")
		return nil
	}

	// 4. Write derived indicators back.
	if err := s.PutIndicators(ctx, derived); err != nil {
		return fmt.Errorf("process: write derived indicators: %w", err)
	}
	log.Printf("process: wrote %d derived indicators", len(derived))
	return nil
}

// computeDerivedIndicators builds derived variables from the raw indicator index.
// Each entry in knownGEOIDs is a tract that should receive derived values where
// the required source variables are present.
//
// Derived variables:
//   - pct_poc = 1 - (white_alone / total_pop)  where both source vars are non-nil and total_pop > 0
func computeDerivedIndicators(
	idx map[struct{ geoid, variableID string }]*float64,
	vintage string,
	knownGEOIDs map[string]bool,
) []store.Indicator {
	type key = struct{ geoid, variableID string }

	var out []store.Indicator
	for geoid := range knownGEOIDs {
		totalPop := idx[key{geoid, "total_population_race"}]
		whiteAlone := idx[key{geoid, "pop_white_non_hispanic"}]

		if totalPop != nil && whiteAlone != nil && *totalPop > 0 {
			v := 1.0 - (*whiteAlone / *totalPop)
			out = append(out, store.Indicator{
				GEOID:      geoid,
				VariableID: "pct_poc",
				Vintage:    vintage,
				Value:      &v,
			})
		}
	}
	return out
}
