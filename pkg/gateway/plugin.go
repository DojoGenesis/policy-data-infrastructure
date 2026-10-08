// Package gateway exposes the policy data infrastructure as a Gin-compatible
// HTTP plugin. Register the plugin's routes under any router group; the plugin
// adds GET/POST endpoints for geographies, indicators, analysis scores,
// narrative generation, HTMLCraft deliverable export, pipeline control, and
// data-source listing.
package gateway

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/DojoGenesis/policy-data-infrastructure/internal/version"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// PolicyPlugin implements the gateway plugin interface for the policy data
// infrastructure. It is created with a Store and registered onto a Gin router
// group via RegisterRoutes.
type PolicyPlugin struct {
	store   store.Store
	varMeta map[string]store.VariableMeta // variable_id -> metadata, populated at startup

	// Queued-run surface (ADR-014 D3/D10). runToken empty = open access
	// with the budget as the only gate.
	runBudget *RunBudget
	runner    *AnalysisRunner
	runToken  string
}

// NewPlugin creates a PolicyPlugin backed by the given Store. It pre-loads
// variable metadata into an in-memory map so every indicator response can be
// enriched without hitting the database per-request.
func NewPlugin(s store.Store) *PolicyPlugin {
	p := &PolicyPlugin{store: s, varMeta: make(map[string]store.VariableMeta)}
	ctx := context.Background()
	if vars, err := s.QueryVariables(ctx); err == nil {
		for _, v := range vars {
			p.varMeta[v.VariableID] = v
		}
	}

	runCfg, warnings := RunBudgetConfigFromEnv()
	for _, w := range warnings {
		log.Printf("run budget config: %s", w)
	}
	p.runBudget = NewRunBudget(runCfg)
	p.runner = NewAnalysisRunner(s, 2*time.Second)
	p.runToken = os.Getenv(envRunToken)
	return p
}

// StartRunner launches the queued-run worker for the server's lifetime.
// Serve calls it once; tests may skip it and drive the runner directly.
func (p *PolicyPlugin) StartRunner(ctx context.Context) {
	go p.runner.Start(ctx)
}

// Name returns the plugin identifier.
func (p *PolicyPlugin) Name() string { return "policy-data-infrastructure" }

// Version returns the current binary version string.
func (p *PolicyPlugin) Version() string { return version.Version }

// Health performs a lightweight liveness check. A non-nil error means the
// plugin is unhealthy and should not serve traffic.
func (p *PolicyPlugin) Health() error { return nil }

// RegisterRoutes mounts all plugin endpoints under the given Gin router group.
// The caller is responsible for choosing a URL prefix (e.g. /api/policy).
func (p *PolicyPlugin) RegisterRoutes(group *gin.RouterGroup) {
	geoidMW := ValidateGEOID()

	// Geography endpoints.
	group.GET("/geographies", p.handleListGeographies)
	group.GET("/geographies/:geoid", geoidMW, p.handleGetGeography)
	group.GET("/geographies/:geoid/children", geoidMW, p.handleGetChildren)
	group.GET("/geographies/:geoid/indicators", geoidMW, p.handleGetIndicators)
	group.GET("/geographies/:geoid/ranks", geoidMW, p.handleGetRanks)

	// Query & comparison.
	group.POST("/query", p.handleQuery)
	group.POST("/compare", p.handleCompare)

	// Generation.
	group.POST("/generate/narrative", p.handleGenerateNarrative)
	group.GET("/generate/narrative/:analysis_id", p.handleServeNarrative)
	group.POST("/generate/deliverable", p.handleGenerateDeliverable)

	// Pipeline.
	group.POST("/pipeline/run", p.handlePipelineRun)
	group.GET("/pipeline/events", p.handlePipelineEvents)

	// Variable metadata catalog.
	group.GET("/variables", p.handleListVariables)

	// Analysis runs + detail + scores. The POST is the queued-run surface:
	// auth (when configured) and the run budget gate it BEFORE the endpoint
	// does any work (ADR-014 D10).
	group.GET("/analyses", p.handleListAnalyses)
	group.POST("/analyses", runAuthMiddleware(p.runToken), p.handleCreateAnalysisRun)
	group.GET("/analyses/runs/:id", p.handleGetAnalysisRun)
	group.GET("/analyses/:id", p.handleGetAnalysis)
	group.GET("/analyses/:id/scores", p.handleGetAnalysisScores)

	// Aggregation.
	group.POST("/aggregate", p.handleAggregate)

	// Composite computation (query-time only, never stored).
	group.POST("/composite", p.handleComposite)

	// Data source info.
	group.GET("/sources", p.handleListSources)

	// Policy positions.
	group.GET("/policies", p.handleListPolicies)
	group.GET("/policies/:id", p.handleGetPolicy)

	// Evidence cards.
	group.GET("/evidence-cards", p.handleListEvidenceCards)

	// Factor scores.
	group.GET("/geographies/:geoid/factors", geoidMW, p.handleGetFactors)

	// LISA county profile.
	group.GET("/geographies/:geoid/lisa-profile", geoidMW, p.handleGetLISACountyProfile)

	// Personalized video captions.
	group.GET("/videos/:name", p.handleGetVideoCaptions)

	// Policy experiment page (ADR-016, docs/contracts/experiment-contract.md).
	//
	// GET /briefs/:policy_id is read-only and cache-friendly (contract §3) —
	// no geoidMW, because its geoid is an OPTIONAL query parameter with its
	// own default chain (resolveBriefGeoid, contract S4), not the required
	// URL path param geoidMW validates; and no run token, for the same
	// reason POST /simulate has none (below).
	group.GET("/briefs/:policy_id", p.handleGetPolicyBrief)

	// POST /simulate is deliberately NOT behind runAuthMiddleware, unlike
	// POST /analyses. Contract §4/S20 is explicit and this integrator agrees
	// with the reasoning: it is a pure function over numbers the brief
	// already published — no DB write, no run row, no cache row, no budget
	// reservation — so gating it behind the run token would gate the page's
	// entire interactive layer behind a secret meant for the queued-analysis
	// surface. It is rate-limited by CF-Connecting-IP at the edge instead,
	// the same mechanism as the other read endpoints on this group. Every
	// other route registered above that touches the experiment package
	// (this one) is equally read-only/pure, so this is consistent with how
	// the rest of RegisterRoutes already splits GET/cheap-POST (no
	// middleware) from POST /analyses (runAuthMiddleware) by cost and
	// side-effect, not by HTTP verb alone.
	simHandler := NewSimulateHandler(
		p.store,
		newFileModelLoader(modelsDir),
		newStoreEvaluator(p.store, p.varMeta),
	)
	group.POST("/simulate", simHandler.Handle)
}
