package pipeline

import (
	"context"
	"log"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// EnrichStage is Stage 03: placeholder for transit scoring, school proximity
// enrichment, and other geospatial join operations. Currently a no-op stub.
type EnrichStage struct{}

func (e *EnrichStage) Name() string          { return "enrich" }
func (e *EnrichStage) Dependencies() []string { return []string{"process"} }

func (e *EnrichStage) Run(_ context.Context, _ store.Store, cfg *Config) error {
	log.Printf("enrich: stub stage — no enrichment operations implemented yet")
	return nil
}
