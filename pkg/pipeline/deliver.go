package pipeline

import (
	"context"
	"log"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// DeliverStage is Stage 06: placeholder for narrative generation and HTMLCraft
// deliverable export. Currently a no-op stub.
type DeliverStage struct{}

func (d *DeliverStage) Name() string          { return "deliver" }
func (d *DeliverStage) Dependencies() []string { return []string{"synthesize"} }

func (d *DeliverStage) Run(_ context.Context, _ store.Store, cfg *Config) error {
	log.Printf("deliver: stub stage — narrative and deliverable generation not yet implemented")
	return nil
}
