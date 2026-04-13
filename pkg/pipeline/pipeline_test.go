package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/datasource"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// mockStage is a test double that records when it ran and can simulate errors.
type mockStage struct {
	name    string
	deps    []string
	runErr  error

	mu       sync.Mutex
	ranAt    time.Time
	runCount int
}

func newMock(name string, deps ...string) *mockStage {
	return &mockStage{name: name, deps: deps}
}

func (m *mockStage) Name() string           { return m.name }
func (m *mockStage) Dependencies() []string { return m.deps }

func (m *mockStage) Run(_ context.Context, _ store.Store, _ *Config) error {
	m.mu.Lock()
	m.ranAt = time.Now()
	m.runCount++
	m.mu.Unlock()
	if m.runErr != nil {
		return m.runErr
	}
	return nil
}

func (m *mockStage) RanAt() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ranAt
}

func (m *mockStage) RunCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runCount
}

// ---------------------------------------------------------------------------
// Topological sort tests
// ---------------------------------------------------------------------------

func TestTopoSort_Linear(t *testing.T) {
	a := newMock("a")
	b := newMock("b", "a")
	c := newMock("c", "b")

	order, err := topoSort([]Stage{c, b, a}) // intentionally out of order
	if err != nil {
		t.Fatalf("topoSort: unexpected error: %v", err)
	}
	if len(order) != 3 {
		t.Fatalf("expected 3 stages, got %d", len(order))
	}

	pos := make(map[string]int, 3)
	for i, st := range order {
		pos[st.Name()] = i
	}
	if pos["a"] > pos["b"] {
		t.Errorf("a should come before b: a=%d b=%d", pos["a"], pos["b"])
	}
	if pos["b"] > pos["c"] {
		t.Errorf("b should come before c: b=%d c=%d", pos["b"], pos["c"])
	}
}

func TestTopoSort_Diamond(t *testing.T) {
	// a → b, a → c, b → d, c → d
	a := newMock("a")
	b := newMock("b", "a")
	c := newMock("c", "a")
	d := newMock("d", "b", "c")

	order, err := topoSort([]Stage{d, c, b, a})
	if err != nil {
		t.Fatalf("topoSort: %v", err)
	}
	pos := make(map[string]int)
	for i, st := range order {
		pos[st.Name()] = i
	}
	if pos["a"] > pos["b"] || pos["a"] > pos["c"] {
		t.Error("a must precede b and c")
	}
	if pos["b"] > pos["d"] || pos["c"] > pos["d"] {
		t.Error("b and c must precede d")
	}
}

func TestTopoSort_Cycle(t *testing.T) {
	a := newMock("a", "b")
	b := newMock("b", "a")

	_, err := topoSort([]Stage{a, b})
	if err == nil {
		t.Fatal("expected cycle error, got nil")
	}
}

func TestTopoSort_UnknownDep(t *testing.T) {
	a := newMock("a", "nonexistent")
	_, err := topoSort([]Stage{a})
	if err == nil {
		t.Fatal("expected error for unknown dependency, got nil")
	}
}

func TestTopoSort_DuplicateName(t *testing.T) {
	a1 := newMock("a")
	a2 := newMock("a")
	_, err := topoSort([]Stage{a1, a2})
	if err == nil {
		t.Fatal("expected error for duplicate stage name, got nil")
	}
}

// ---------------------------------------------------------------------------
// Pipeline.Run correctness tests
// ---------------------------------------------------------------------------

func TestPipeline_RunOrder(t *testing.T) {
	a := newMock("a")
	b := newMock("b", "a")
	c := newMock("c", "b")

	p := New(c, b, a)
	cfg := &Config{Parallelism: 4}

	if err := p.Run(context.Background(), nil, cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, st := range []*mockStage{a, b, c} {
		if st.RunCount() != 1 {
			t.Errorf("stage %q: expected 1 run, got %d", st.name, st.RunCount())
		}
	}
}

func TestPipeline_ParallelStages(t *testing.T) {
	// a is the root; b and c both depend only on a — they should run concurrently.
	blockCh := make(chan struct{})
	var started int32

	a := newMock("a")
	b := &slowStage{name: "b", deps: []string{"a"}, started: &started, block: blockCh}
	c := &slowStage{name: "c", deps: []string{"a"}, started: &started, block: blockCh}

	p := New(a, b, c)
	cfg := &Config{Parallelism: 4}

	done := make(chan error, 1)
	go func() {
		done <- p.Run(context.Background(), nil, cfg)
	}()

	// Wait until both b and c have started concurrently.
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&started) < 2 {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for b and c to start concurrently")
		case <-time.After(5 * time.Millisecond):
		}
	}
	close(blockCh)

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// slowStage signals it has started then blocks until the channel is closed.
type slowStage struct {
	name    string
	deps    []string
	started *int32
	block   <-chan struct{}
}

func (s *slowStage) Name() string           { return s.name }
func (s *slowStage) Dependencies() []string { return s.deps }
func (s *slowStage) Run(_ context.Context, _ store.Store, _ *Config) error {
	atomic.AddInt32(s.started, 1)
	<-s.block
	return nil
}

// ---------------------------------------------------------------------------
// Error propagation
// ---------------------------------------------------------------------------

func TestPipeline_ErrorPropagation(t *testing.T) {
	sentinel := errors.New("stage b intentional failure")

	a := newMock("a")
	b := newMock("b", "a")
	b.runErr = sentinel
	c := newMock("c", "b")

	p := New(a, b, c)
	cfg := &Config{Parallelism: 4}

	err := p.Run(context.Background(), nil, cfg)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected wrapped sentinel error, got: %v", err)
	}
	// c depends on b; since b failed the wave containing b returns early,
	// so c never gets a chance to run.
	if c.RunCount() != 0 {
		t.Errorf("stage c should not have run after b's failure, got %d run(s)", c.RunCount())
	}
}

func TestPipeline_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	waiting := make(chan struct{})
	unblock := make(chan struct{})

	a := &contextStage{waiting: waiting, unblock: unblock}

	p := New(a)
	cfg := &Config{Parallelism: 2}

	done := make(chan error, 1)
	go func() {
		done <- p.Run(ctx, nil, cfg)
	}()

	select {
	case <-waiting:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for stage to start")
	}

	cancel()
	close(unblock)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error after context cancel, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not return after context cancel")
	}
}

// contextStage signals waiting when it starts, then blocks on either ctx or unblock.
type contextStage struct {
	waiting chan<- struct{}
	unblock <-chan struct{}
	once    sync.Once
}

func (cs *contextStage) Name() string           { return "ctx-stage" }
func (cs *contextStage) Dependencies() []string { return nil }
func (cs *contextStage) Run(ctx context.Context, _ store.Store, _ *Config) error {
	cs.once.Do(func() { close(cs.waiting) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-cs.unblock:
		return ctx.Err()
	}
}

// ---------------------------------------------------------------------------
// Default pipeline wiring (smoke test)
// ---------------------------------------------------------------------------

func TestDefaultPipeline_StageOrder(t *testing.T) {
	// Verify the canonical 6-stage pipeline produces the expected topo order.
	stages := []Stage{
		NewFetchStage(datasource.NewRegistry()),
		&ProcessStage{},
		&EnrichStage{},
		&AnalyzeStage{},
		&SynthesizeStage{},
		&DeliverStage{},
	}

	order, err := topoSort(stages)
	if err != nil {
		t.Fatalf("default pipeline topoSort: %v", err)
	}

	want := []string{"fetch", "process", "enrich", "analyze", "synthesize", "deliver"}
	if len(order) != len(want) {
		t.Fatalf("expected %d stages, got %d", len(want), len(order))
	}
	for i, st := range order {
		if st.Name() != want[i] {
			t.Errorf("stage[%d]: want %q got %q", i, want[i], st.Name())
		}
	}
}
