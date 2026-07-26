package grounding

import (
	"context"
	"errors"
	"strings"
)

// Engine runs the full pipeline. Planner and Composer are both optional: with
// neither configured the engine still answers anything expressible as an intent
// — the caller just has to supply the intent. That is deliberate. The
// deterministic path is the product; the model is a convenience on the front of
// it, not the thing that produces the answer.
type Engine struct {
	Dataset  *Dataset
	Planner  Planner
	Composer Composer
	// MaxPlanAttempts bounds the plan/validate/retry loop. Two is enough: the
	// first rejection carries a specific hint, and a model that cannot use it
	// is not going to succeed on a third.
	MaxPlanAttempts int
}

// AnswerSource records how the prose was produced, so a caller (and a reader)
// can tell a model-written sentence from a generated one.
type AnswerSource string

const (
	// SourceDeterministic — rendered by this package from the result. No model.
	SourceDeterministic AnswerSource = "deterministic"
	// SourceModelVerified — written by a model and passed numeric verification.
	SourceModelVerified AnswerSource = "model-verified"
	// SourceModelRejected — a model draft failed verification and was discarded;
	// the deterministic rendering was returned instead.
	SourceModelRejected AnswerSource = "model-rejected-fell-back"
)

// Answer is the full, auditable output.
type Answer struct {
	Question string       `json:"question"`
	Answered bool         `json:"answered"`
	Text     string       `json:"text"`
	Source   AnswerSource `json:"source"`
	Intent   *Intent      `json:"intent,omitempty"`
	Result   *Result      `json:"result,omitempty"`
	// Violations is non-empty when a model draft was rejected. It is kept in the
	// response on purpose: a silent fallback hides a misbehaving model.
	Violations []Violation `json:"violations,omitempty"`
	Refusal    string      `json:"refusal,omitempty"`
	PlannerUsed string     `json:"plannerUsed,omitempty"`
}

// ErrUnsupported is returned when the question cannot be expressed as an intent.
var ErrUnsupported = errors.New("question cannot be answered from this dataset")

// AnswerIntent runs execute → compose → verify for an already-built intent.
// This is the path with no planner in it, and it is fully testable.
func (e *Engine) AnswerIntent(ctx context.Context, question string, in *Intent) (*Answer, error) {
	if err := in.Validate(e.Dataset); err != nil {
		return &Answer{
			Question: question, Answered: false,
			Refusal: err.Error(),
			Text:    "I can't answer that from this dataset. " + err.Error(),
			Source:  SourceDeterministic,
		}, nil
	}

	res, err := Execute(in, e.Dataset)
	if err != nil {
		return nil, err
	}

	ans := &Answer{
		Question: question, Answered: true,
		Intent: in, Result: res,
		Text:   res.Facts,
		Source: SourceDeterministic,
	}

	if e.Composer == nil {
		return ans, nil
	}

	draft, err := e.Composer.Compose(ctx, question, res)
	if err != nil || strings.TrimSpace(draft) == "" {
		// A composer failure is not an answer failure. The deterministic
		// rendering is already correct and already cited.
		return ans, nil
	}

	if v := Verify(draft, res); len(v) > 0 {
		ans.Violations = v
		ans.Source = SourceModelRejected
		return ans, nil
	}

	ans.Text = strings.TrimSpace(draft)
	ans.Source = SourceModelVerified
	return ans, nil
}

// Answer runs the whole pipeline from a natural-language question.
func (e *Engine) Answer(ctx context.Context, question string) (*Answer, error) {
	if e.Planner == nil {
		return &Answer{
			Question: question, Answered: false,
			Refusal: "no planner configured",
			Text: "Natural-language questions need a model lane configured " +
				"(OPENROUTER_API_KEY or OLLAMA_BASE_URL). Structured queries still work.",
			Source: SourceDeterministic,
		}, nil
	}

	attempts := e.MaxPlanAttempts
	if attempts <= 0 {
		attempts = 2
	}

	var hint string
	for i := 0; i < attempts; i++ {
		in, err := e.Planner.Plan(ctx, question, e.Dataset, hint)
		if err != nil {
			return nil, err
		}
		// The planner's own refusal token. Honouring it is the point of having
		// one — a model that says "I can't" must not be argued with into a guess.
		if in.Operation == "unsupported" {
			return &Answer{
				Question: question, Answered: false,
				Refusal:     "not expressible as a query over this dataset",
				Text:        "I can't answer that from this dataset. It covers Wisconsin counties and census tracts on " + strings.Join(e.Dataset.IndicatorIDs(), ", ") + ".",
				Source:      SourceDeterministic,
				PlannerUsed: e.Planner.Name(),
			}, nil
		}

		if err := in.Validate(e.Dataset); err != nil {
			hint = err.Error()
			continue
		}

		ans, aerr := e.AnswerIntent(ctx, question, in)
		if aerr != nil {
			return nil, aerr
		}
		ans.PlannerUsed = e.Planner.Name()
		return ans, nil
	}

	return &Answer{
		Question: question, Answered: false,
		Refusal:     hint,
		Text:        "I can't answer that from this dataset. " + hint,
		Source:      SourceDeterministic,
		PlannerUsed: e.Planner.Name(),
	}, nil
}
