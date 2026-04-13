// Package datasource defines the DataSource interface and registry for all
// external data adapters used by the policy data infrastructure pipeline.
package datasource

import (
	"context"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// DataSource fetches indicator data from an external source.
type DataSource interface {
	// Name returns a unique machine-readable identifier for this source.
	Name() string

	// Category classifies the source (e.g. "demographic", "health",
	// "environment", "housing", "transit").
	Category() string

	// Vintage describes the data vintage, e.g. "ACS-2024-5yr".
	Vintage() string

	// Schema returns the variable definitions this source produces.
	Schema() []VariableDef

	// FetchCounty fetches all tract-level indicators for a single county.
	// stateFIPS is a 2-digit code; countyFIPS is a 3-digit code.
	FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error)

	// FetchState fetches all tract-level indicators for an entire state.
	// stateFIPS is a 2-digit code.
	FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error)
}

// VariableDef describes a single indicator variable produced by a DataSource.
type VariableDef struct {
	ID          string
	Name        string
	Description string
	// Unit is one of: "dollars", "percent", "count", "rate".
	Unit string
	// Direction is one of: "higher_better", "lower_better", "neutral".
	Direction string
	// ACSTable is the Census ACS variable code, e.g. "B19013_001E".
	ACSTable string
}

// Registry holds all registered DataSources, keyed by Name().
type Registry struct {
	sources map[string]DataSource
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{sources: make(map[string]DataSource)}
}

// Register adds ds to the registry. If a source with the same name already
// exists, it is replaced.
func (r *Registry) Register(ds DataSource) {
	r.sources[ds.Name()] = ds
}

// Get retrieves a DataSource by name. The boolean is false when not found.
func (r *Registry) Get(name string) (DataSource, bool) {
	ds, ok := r.sources[name]
	return ds, ok
}

// All returns every registered DataSource in an unspecified order.
func (r *Registry) All() []DataSource {
	out := make([]DataSource, 0, len(r.sources))
	for _, ds := range r.sources {
		out = append(out, ds)
	}
	return out
}
