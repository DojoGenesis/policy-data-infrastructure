package grounding

import (
	"context"
	"strings"
	"testing"
)

// stubComposer stands in for a model so the fallback paths are testable without
// a network or a key — these are the behaviours that matter most and they must
// not be verified only in production.
type stubComposer struct {
	draft string
	err   error
}

func (s stubComposer) Name() string { return "stub" }
func (s stubComposer) Compose(_ context.Context, _ string, _ *Result) (string, error) {
	return s.draft, s.err
}

type stubPlanner struct {
	intents []*Intent
	calls   int
	hints   []string
}

func (s *stubPlanner) Name() string { return "stub-planner" }
func (s *stubPlanner) Plan(_ context.Context, _ string, _ *Dataset, hint string) (*Intent, error) {
	s.hints = append(s.hints, hint)
	i := s.calls
	s.calls++
	if i >= len(s.intents) {
		return &Intent{Operation: "unsupported"}, nil
	}
	return s.intents[i], nil
}

func engine(t *testing.T) *Engine {
	t.Helper()
	return &Engine{Dataset: load(t)}
}

func TestAnswerIntentDeterministicWithoutAModel(t *testing.T) {
	e := engine(t)
	ans, err := e.AnswerIntent(context.Background(), "poverty in Dane?",
		&Intent{Operation: OpLookup, Indicator: "poverty_rate", Places: []string{"Dane"}})
	if err != nil {
		t.Fatalf("AnswerIntent: %v", err)
	}
	if !ans.Answered || ans.Source != SourceDeterministic {
		t.Fatalf("got answered=%v source=%s", ans.Answered, ans.Source)
	}
	if !strings.Contains(ans.Text, "10.5") {
		t.Errorf("answer should carry the real value, got %q", ans.Text)
	}
}

// A model draft that invents a figure must never reach the caller.
func TestConfabulatedDraftIsRejectedAndFallsBack(t *testing.T) {
	e := engine(t)
	e.Composer = stubComposer{
		draft: "Dane County's poverty rate is 10.5 percent, down from 14.8 percent in 2019.",
	}
	ans, err := e.AnswerIntent(context.Background(), "poverty in Dane?",
		&Intent{Operation: OpLookup, Indicator: "poverty_rate", Places: []string{"Dane"}})
	if err != nil {
		t.Fatalf("AnswerIntent: %v", err)
	}
	if ans.Source != SourceModelRejected {
		t.Fatalf("source = %s, want %s", ans.Source, SourceModelRejected)
	}
	if strings.Contains(ans.Text, "14.8") {
		t.Errorf("the invented figure reached the caller: %q", ans.Text)
	}
	if len(ans.Violations) == 0 {
		t.Error("violations should be reported, not swallowed")
	}
	if !strings.Contains(ans.Text, "10.5") {
		t.Errorf("fallback should still answer the question, got %q", ans.Text)
	}
}

func TestCleanDraftIsUsed(t *testing.T) {
	e := engine(t)
	e.Composer = stubComposer{
		draft: "Dane County's poverty rate is 10.5 percent, from the 2020-2024 ACS.",
	}
	ans, _ := e.AnswerIntent(context.Background(), "poverty in Dane?",
		&Intent{Operation: OpLookup, Indicator: "poverty_rate", Places: []string{"Dane"}})
	if ans.Source != SourceModelVerified {
		t.Fatalf("source = %s, want %s", ans.Source, SourceModelVerified)
	}
	if !strings.Contains(ans.Text, "ACS") {
		t.Errorf("verified draft should be used verbatim, got %q", ans.Text)
	}
}

// A composer that errors must not take the answer down with it.
func TestComposerFailureFallsBackSilently(t *testing.T) {
	e := engine(t)
	e.Composer = stubComposer{err: context.DeadlineExceeded}
	ans, err := e.AnswerIntent(context.Background(), "poverty in Dane?",
		&Intent{Operation: OpLookup, Indicator: "poverty_rate", Places: []string{"Dane"}})
	if err != nil {
		t.Fatalf("a composer failure should not fail the answer: %v", err)
	}
	if !ans.Answered || !strings.Contains(ans.Text, "10.5") {
		t.Errorf("expected the deterministic answer, got %+v", ans)
	}
}

// An unanswerable question must be refused with the real vocabulary, not
// approximated with a different indicator.
func TestUnsupportedQuestionIsRefused(t *testing.T) {
	e := engine(t)
	e.Planner = &stubPlanner{intents: []*Intent{{Operation: "unsupported"}}}
	ans, err := e.Answer(context.Background(), "what will rents be in 2030?")
	if err != nil {
		t.Fatalf("Answer: %v", err)
	}
	if ans.Answered {
		t.Fatal("an unanswerable question was answered")
	}
	if !strings.Contains(ans.Text, "can't answer") {
		t.Errorf("refusal should say so plainly, got %q", ans.Text)
	}
	if !strings.Contains(ans.Text, "poverty_rate") {
		t.Errorf("refusal should list what IS available, got %q", ans.Text)
	}
}

// A rejected intent gets one corrective retry, and the hint is passed back.
func TestPlannerRetriesWithHint(t *testing.T) {
	e := engine(t)
	sp := &stubPlanner{intents: []*Intent{
		{Operation: OpLookup, Indicator: "vibes", Places: []string{"Dane"}}, // invalid
		{Operation: OpLookup, Indicator: "poverty_rate", Places: []string{"Dane"}},
	}}
	e.Planner = sp
	ans, err := e.Answer(context.Background(), "poverty in Dane?")
	if err != nil {
		t.Fatalf("Answer: %v", err)
	}
	if !ans.Answered {
		t.Fatalf("retry should have succeeded, got %+v", ans)
	}
	if sp.calls != 2 {
		t.Errorf("planner called %d times, want 2", sp.calls)
	}
	if len(sp.hints) < 2 || !strings.Contains(sp.hints[1], "vibes") {
		t.Errorf("second call should carry a specific hint, got %v", sp.hints)
	}
}

// Two bad plans in a row is a refusal, not an unbounded loop.
func TestPlannerGivesUpAfterMaxAttempts(t *testing.T) {
	e := engine(t)
	e.MaxPlanAttempts = 2
	e.Planner = &stubPlanner{intents: []*Intent{
		{Operation: OpLookup, Indicator: "vibes", Places: []string{"Dane"}},
		{Operation: OpLookup, Indicator: "auras", Places: []string{"Dane"}},
	}}
	ans, err := e.Answer(context.Background(), "unanswerable")
	if err != nil {
		t.Fatalf("Answer: %v", err)
	}
	if ans.Answered {
		t.Error("should have refused after exhausting attempts")
	}
}

func TestPlannerPromptCarriesRealVocabulary(t *testing.T) {
	ds := load(t)
	p := PlannerPrompt(ds)
	for _, want := range []string{"poverty_rate", "median_hh_income", "2020-2024", "unsupported"} {
		if !strings.Contains(p, want) {
			t.Errorf("planner prompt is missing %q", want)
		}
	}
	if !strings.Contains(p, "mean") {
		t.Error("planner prompt should rule out mean explicitly")
	}
}

func TestNoPlannerConfiguredIsHonest(t *testing.T) {
	e := engine(t)
	ans, err := e.Answer(context.Background(), "anything")
	if err != nil {
		t.Fatalf("Answer: %v", err)
	}
	if ans.Answered {
		t.Error("should not claim to have answered without a planner")
	}
	if !strings.Contains(ans.Text, "OPENROUTER_API_KEY") {
		t.Errorf("should say what is missing, got %q", ans.Text)
	}
}
