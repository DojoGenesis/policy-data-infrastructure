package gateway

// Launch-time analyses cache warm (2026-08-08 directive: "the spatial
// analyses cache updates at prod launch"). At serve boot, a registered set
// of statewide analyses is re-established through the ordinary run queue:
//
//   - each intent's vintage resolves to the CURRENT latest at boot, so a
//     data load that advanced a vintage changes the cache key, misses, and
//     recomputes — while unchanged data cache-hits and costs nothing. The
//     "refresh" is the D9 key doing its job, not a separate invalidation
//     scheme.
//   - runs go through the same queue, executors, and PutAnalysis upsert as
//     user requests — the warm set is configuration, not a second engine.
//
// LISA itself remains Python-only and off the queue (ADR-014 D11's last
// clause, tracked in TODO); the county page's LISA profile reads whatever
// analyses exist, which this warm keeps current for everything queue-borne.
//
// Disable with PDI_WARM_CACHE=off.

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

type warmIntent struct {
	runType    string
	scopeLevel string
	scopeGEOID string
	params     map[string]interface{}
}

// defaultWarmSet is the statewide layer the county pages read on arrival:
// the two segregation indices the Layer-2 panel renders, and the weighted
// county rollups for every tract-only source variable.
func defaultWarmSet() []warmIntent {
	set := []warmIntent{
		{"dissimilarity", "state", "55", map[string]interface{}{
			"group_variable": "pop_black", "reference_variable": "pop_white_non_hispanic",
			"group_pair": "black_white",
		}},
		{"dissimilarity", "state", "55", map[string]interface{}{
			"group_variable": "pop_hispanic_latino", "reference_variable": "pop_white_non_hispanic",
			"group_pair": "hispanic_white",
		}},
		// ADR-015: county factor profiles re-derive from the tract model.
		{"factor_rollup", "state", "55", map[string]interface{}{}},
	}
	rollupVars := []string{
		// CDC PLACES (tract-only source; county values exist only as rollups)
		"cdc_access2", "cdc_binge", "cdc_bphigh", "cdc_casthma",
		"cdc_csmoking", "cdc_diabetes", "cdc_mhlth", "cdc_obesity",
	}
	for _, v := range rollupVars {
		set = append(set, warmIntent{"tract_rollup", "state", "55",
			map[string]interface{}{"variable_id": v}})
	}
	return set
}

// WarmAnalysesCache resolves, cache-checks, and enqueues the warm set.
// Failures are logged and skipped — a cold cache is a degraded mode, never
// a boot failure.
func (p *PolicyPlugin) WarmAnalysesCache(ctx context.Context) {
	if strings.EqualFold(os.Getenv("PDI_WARM_CACHE"), "off") {
		log.Printf("analyses warm: disabled (PDI_WARM_CACHE=off)")
		return
	}
	hits, queued, failed := 0, 0, 0
	for _, wi := range defaultWarmSet() {
		exec, ok := runExecutors[wi.runType]
		if !ok {
			failed++
			continue
		}
		params, err := exec.canon(wi.params)
		if err != nil {
			log.Printf("analyses warm: %s canon: %v", wi.runType, err)
			failed++
			continue
		}
		vintage, err := p.store.LatestVintageForVariable(ctx, exec.primaryVariable(params))
		if err != nil || vintage == "" {
			// No data for the primary variable — nothing to warm.
			failed++
			continue
		}
		hit, err := p.store.FindAnalysisByKey(ctx, store.AnalysisKey{
			Type: wi.runType, ScopeGEOID: wi.scopeGEOID, ScopeLevel: wi.scopeLevel,
			Vintage: vintage, Parameters: params,
		})
		if err != nil {
			log.Printf("analyses warm: %s lookup: %v", wi.runType, err)
			failed++
			continue
		}
		if hit != nil {
			hits++
			continue
		}
		if _, err := p.store.CreateAnalysisRun(ctx, store.AnalysisRun{
			RunType: wi.runType, ScopeGEOID: wi.scopeGEOID, ScopeLevel: wi.scopeLevel,
			Vintage: vintage, Parameters: params, ClientKey: "boot-warm",
		}); err != nil {
			log.Printf("analyses warm: %s enqueue: %v", wi.runType, err)
			failed++
			continue
		}
		queued++
	}
	if p.runner != nil && queued > 0 {
		p.runner.Wake()
	}
	log.Printf("analyses warm: %d current, %d queued, %d skipped", hits, queued, failed)
}
