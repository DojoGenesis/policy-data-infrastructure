package gateway

// The queued-run worker (ADR-014 D3): POST /analyses enqueues a row in
// analysis_runs; this runner claims rows one at a time, executes the
// registered run type, writes the analysis through the D9 cache key
// (PutAnalysis upsert), and closes the run. The queue is DB-backed, so a
// restart loses nothing — a row claimed by a process that died stays
// 'running' and is surfaced by operational queries rather than silently
// re-executed (re-running a half-written analysis would double-write scores;
// the operator re-queues deliberately).

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// runExecutor computes one run type. It returns the AnalysisResult to
// persist (whose Type/Scope/Vintage/Parameters MUST mirror the run row —
// they are the D9 cache key the enqueue-time lookup used) plus any
// per-geography scores.
type runExecutor struct {
	// canon validates request parameters and fills defaults. It runs at
	// ENQUEUE time so the canonical parameter map — not the raw request —
	// is what the cache lookup, the run row, and the executor all see.
	// One canonicalization site, or the cache key silently forks.
	canon func(params map[string]interface{}) (map[string]interface{}, error)
	// primaryVariable names the variable whose latest vintage resolves an
	// omitted request vintage.
	primaryVariable func(params map[string]interface{}) string
	// execute computes the analysis.
	execute func(ctx context.Context, s store.Store, run *store.AnalysisRun) (store.AnalysisResult, []store.AnalysisScore, error)
}

// AnalysisRunner drains the analysis_runs queue.
type AnalysisRunner struct {
	store    store.Store
	interval time.Duration
	wake     chan struct{}
}

// NewAnalysisRunner builds a runner polling at interval (poke Wake to skip
// the wait after an enqueue).
func NewAnalysisRunner(s store.Store, interval time.Duration) *AnalysisRunner {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &AnalysisRunner{
		store:    s,
		interval: interval,
		wake:     make(chan struct{}, 1),
	}
}

// Wake nudges the runner without blocking the caller.
func (r *AnalysisRunner) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Start runs the drain loop until ctx is cancelled. Call in a goroutine.
func (r *AnalysisRunner) Start(ctx context.Context) {
	for {
		claimed := r.runOne(ctx)
		if ctx.Err() != nil {
			return
		}
		if claimed {
			// Drain eagerly while work exists.
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
		case <-time.After(r.interval):
		}
	}
}

// runOne claims and executes at most one run. Returns whether a run was
// claimed (regardless of its success).
func (r *AnalysisRunner) runOne(ctx context.Context) bool {
	run, err := r.store.ClaimNextAnalysisRun(ctx)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("analysis runner: claim failed: %v", err)
		}
		return false
	}
	if run == nil {
		return false
	}

	execErr := r.executeClaimed(ctx, run)
	errMsg := ""
	if execErr != nil {
		errMsg = execErr.Error()
		log.Printf("analysis runner: run %s (%s) failed: %v", run.ID, run.RunType, execErr)
	}
	// run.AnalysisID is set by executeClaimed on success.
	if err := r.store.CompleteAnalysisRun(ctx, run.ID, run.AnalysisID, errMsg); err != nil {
		log.Printf("analysis runner: completing run %s: %v", run.ID, err)
	}
	return true
}

// executeClaimed dispatches to the registered executor and persists results.
// Panics are converted to failed runs — one malformed request must not take
// the worker down.
func (r *AnalysisRunner) executeClaimed(ctx context.Context, run *store.AnalysisRun) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("run executor panicked: %v", rec)
		}
	}()

	exec, ok := runExecutors[run.RunType]
	if !ok {
		return fmt.Errorf("unknown run type %q", run.RunType)
	}

	result, scores, err := exec.execute(ctx, r.store, run)
	if err != nil {
		return err
	}

	analysisID, err := r.store.PutAnalysis(ctx, result)
	if err != nil {
		return fmt.Errorf("persist analysis: %w", err)
	}
	for i := range scores {
		scores[i].AnalysisID = analysisID
	}
	if len(scores) > 0 {
		if err := r.store.PutAnalysisScores(ctx, scores); err != nil {
			return fmt.Errorf("persist scores: %w", err)
		}
	}
	run.AnalysisID = analysisID
	return nil
}
