// Package gateway exposes the policy data infrastructure as a Gin-compatible
// HTTP plugin. Register the plugin's routes under any router group; the plugin
// adds GET/POST endpoints for geographies, indicators, analysis scores,
// narrative generation, HTMLCraft deliverable export, pipeline control, and
// data-source listing.
package gateway

import (
	"github.com/gin-gonic/gin"

	"github.com/DojoGenesis/policy-data-infrastructure/internal/version"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// PolicyPlugin implements the gateway plugin interface for the policy data
// infrastructure. It is created with a Store and registered onto a Gin router
// group via RegisterRoutes.
type PolicyPlugin struct {
	store store.Store
}

// NewPlugin creates a PolicyPlugin backed by the given Store.
func NewPlugin(s store.Store) *PolicyPlugin {
	return &PolicyPlugin{store: s}
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
	group.POST("/generate/deliverable", p.handleGenerateDeliverable)

	// Pipeline.
	group.POST("/pipeline/run", p.handlePipelineRun)
	group.GET("/pipeline/events", p.handlePipelineEvents)

	// Data source info.
	group.GET("/sources", p.handleListSources)
}
