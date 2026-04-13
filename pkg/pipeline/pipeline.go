package pipeline

import (
	"context"
	"fmt"
	"log"

	"golang.org/x/sync/errgroup"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// Pipeline runs a set of Stages in topological order.
// Stages whose dependencies are all satisfied can run concurrently, bounded by
// Config.Parallelism (0 or negative means unbounded).
type Pipeline struct {
	stages []Stage
}

// New creates a Pipeline from the provided stages.
func New(stages ...Stage) *Pipeline {
	return &Pipeline{stages: stages}
}

// Run executes all stages in topological order.
// It returns the first error encountered and cancels any in-flight stages.
func (p *Pipeline) Run(ctx context.Context, s store.Store, cfg *Config) error {
	order, err := topoSort(p.stages)
	if err != nil {
		return fmt.Errorf("pipeline: dependency resolution failed: %w", err)
	}

	parallelism := cfg.Parallelism
	if parallelism <= 0 {
		parallelism = len(order)
	}

	// completed tracks which stage names have finished successfully.
	completed := make(map[string]bool, len(order))

	// We process stages in waves: each iteration collects all stages whose
	// dependencies are satisfied, runs them in parallel (semaphore-bounded),
	// then marks them complete before the next wave.
	remaining := make([]Stage, len(order))
	copy(remaining, order)

	for len(remaining) > 0 {
		// Find all stages that are ready to run in this wave.
		var wave []Stage
		var deferred []Stage
		for _, st := range remaining {
			if depsmet(st, completed) {
				wave = append(wave, st)
			} else {
				deferred = append(deferred, st)
			}
		}
		if len(wave) == 0 {
			// No progress possible — graph has a cycle that topoSort missed,
			// or a dependency references a stage not in the pipeline.
			return fmt.Errorf("pipeline: no runnable stages remain but %d stages are pending", len(deferred))
		}

		// Run the wave with bounded concurrency.
		g, gCtx := errgroup.WithContext(ctx)
		sem := make(chan struct{}, parallelism)

		for _, st := range wave {
			st := st // capture
			g.Go(func() error {
				sem <- struct{}{}
				defer func() { <-sem }()

				log.Printf("pipeline: running stage %q", st.Name())
				if err := st.Run(gCtx, s, cfg); err != nil {
					return fmt.Errorf("stage %q: %w", st.Name(), err)
				}
				log.Printf("pipeline: stage %q complete", st.Name())
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}

		// Mark the whole wave as completed.
		for _, st := range wave {
			completed[st.Name()] = true
		}
		remaining = deferred
	}

	return nil
}

// depsmet returns true if every declared dependency of st appears in completed.
func depsmet(st Stage, completed map[string]bool) bool {
	for _, dep := range st.Dependencies() {
		if !completed[dep] {
			return false
		}
	}
	return true
}

// topoSort returns stages in a valid execution order using Kahn's algorithm.
// It detects cycles and returns an error if found.
func topoSort(stages []Stage) ([]Stage, error) {
	// Build adjacency and in-degree maps.
	index := make(map[string]Stage, len(stages))
	for _, st := range stages {
		if _, dup := index[st.Name()]; dup {
			return nil, fmt.Errorf("duplicate stage name %q", st.Name())
		}
		index[st.Name()] = st
	}

	// Validate all declared dependencies exist.
	for _, st := range stages {
		for _, dep := range st.Dependencies() {
			if _, ok := index[dep]; !ok {
				return nil, fmt.Errorf("stage %q declares unknown dependency %q", st.Name(), dep)
			}
		}
	}

	inDegree := make(map[string]int, len(stages))
	for _, st := range stages {
		if _, exists := inDegree[st.Name()]; !exists {
			inDegree[st.Name()] = 0
		}
		for _, dep := range st.Dependencies() {
			inDegree[st.Name()]++
			_ = dep
		}
	}

	// Re-compute in-degree correctly: inDegree[name] = number of dependencies.
	for k := range inDegree {
		delete(inDegree, k)
	}
	for _, st := range stages {
		inDegree[st.Name()] = len(st.Dependencies())
	}

	// Kahn's: start with all zero-in-degree nodes.
	queue := []string{}
	for _, st := range stages {
		if inDegree[st.Name()] == 0 {
			queue = append(queue, st.Name())
		}
	}

	result := make([]Stage, 0, len(stages))
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		result = append(result, index[name])

		// Reduce in-degree of every stage that depends on name.
		for _, st := range stages {
			for _, dep := range st.Dependencies() {
				if dep == name {
					inDegree[st.Name()]--
					if inDegree[st.Name()] == 0 {
						queue = append(queue, st.Name())
					}
				}
			}
		}
	}

	if len(result) != len(stages) {
		return nil, fmt.Errorf("cycle detected in stage dependency graph")
	}
	return result, nil
}
