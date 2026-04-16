// Package gateway exposes the policy data infrastructure as a Gin-compatible
// HTTP plugin. Register the plugin's routes under any router group; the plugin
// adds GET/POST endpoints for geographies, indicators, analysis scores,
// narrative generation, HTMLCraft deliverable export, pipeline control, and
// data-source listing.
package gateway

import (
	"context"

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
	return p
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

	// Analysis runs + detail + scores.
	group.GET("/analyses", p.handleListAnalyses)
	group.GET("/analyses/:id", p.handleGetAnalysis)
	group.GET("/analyses/:id/scores", p.handleGetAnalysisScores)

	// Aggregation.
	group.POST("/aggregate", p.handleAggregate)

	// Data source info.
	group.GET("/sources", p.handleListSources)

	// Policy positions.
	group.GET("/policies", p.handleListPolicies)
	group.GET("/policies/:id", p.handleGetPolicy)
}
