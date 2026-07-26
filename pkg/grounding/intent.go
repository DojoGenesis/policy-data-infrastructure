// Package grounding turns a natural-language question about the Atlas data into
// a checked answer whose every figure came out of the dataset.
//
// The contract, from ADR-006: a language model may plan a query and it may write
// prose, but it may never be the source of a number. Three stages —
//
//	Plan     model -> Intent, validated against a closed schema
//	Execute  Intent -> Result, deterministic Go over the real bundle
//	Verify   prose  -> every number in it must appear in the Result
//
// Stages 2 and 3 are pure functions over data and are covered by tests with no
// model in the loop, which is the point: the guarantee is a property of the
// pipeline, not a hope about model behaviour.
package grounding

import (
	"fmt"
	"sort"
	"strings"
)

// Operation is the set of things this dataset can actually be asked. Keeping it
// closed is what lets an unanswerable question be refused instead of guessed at.
type Operation string

const (
	// OpLookup — the value of one indicator for one or more named places.
	OpLookup Operation = "lookup"
	// OpRank — the highest or lowest N places by an indicator.
	OpRank Operation = "rank"
	// OpCompare — two or more named places side by side on one indicator.
	OpCompare Operation = "compare"
	// OpAggregate — a summary statistic across every place at a level.
	OpAggregate Operation = "aggregate"
	// OpThreshold — every place above or below a given value.
	OpThreshold Operation = "threshold"
	// OpRepresentation — which districts cover a place, and who holds the seats.
	OpRepresentation Operation = "representation"
)

// AllOperations is the enumeration handed to the planner in the system prompt.
var AllOperations = []Operation{
	OpLookup, OpRank, OpCompare, OpAggregate, OpThreshold, OpRepresentation,
}

// Level is a geography resolution present in the bundle.
type Level string

const (
	LevelCounty Level = "county"
	LevelTract  Level = "tract"
)

// Aggregate names a summary statistic. Deliberately excludes mean: averaging
// tract medians does not produce a county median, and offering the operation
// invites exactly that mistake.
type Aggregate string

const (
	AggMedian Aggregate = "median"
	AggMin    Aggregate = "min"
	AggMax    Aggregate = "max"
	AggCount  Aggregate = "count"
)

// Direction orders a rank. Note this is about the VALUE, not about whether the
// value is good — "highest poverty rate" is the worst place, and conflating the
// two in the schema is how a chat ends up congratulating a county.
type Direction string

const (
	DirHighest Direction = "highest"
	DirLowest  Direction = "lowest"
)

// Comparator selects a side of a threshold.
type Comparator string

const (
	CmpAbove Comparator = "above"
	CmpBelow Comparator = "below"
)

// Intent is the planner's entire output surface. A model that emits anything
// else, or anything that fails Validate, gets its answer refused rather than
// rendered.
type Intent struct {
	Operation Operation `json:"operation"`
	// Indicator is an indicator id from the bundle, e.g. "poverty_rate".
	// Not required for OpRepresentation.
	Indicator string `json:"indicator,omitempty"`
	// Places are names or GEOIDs; they are resolved against the bundle, and an
	// unresolvable place is an error, never a silent drop.
	Places []string `json:"places,omitempty"`
	// Level defaults to county — the resolution a question without a stated
	// geography almost always means.
	Level     Level     `json:"level,omitempty"`
	Limit     int       `json:"limit,omitempty"`
	Direction Direction `json:"direction,omitempty"`
	Aggregate Aggregate `json:"aggregate,omitempty"`
	Threshold *float64  `json:"threshold,omitempty"`
	Comparator Comparator `json:"comparator,omitempty"`
}

// ValidationError explains, in terms a planner can act on, why an intent was
// refused. The Hint is fed back to the model on a retry.
type ValidationError struct {
	Field   string
	Problem string
	Hint    string
}

func (e *ValidationError) Error() string {
	if e.Hint == "" {
		return fmt.Sprintf("%s: %s", e.Field, e.Problem)
	}
	return fmt.Sprintf("%s: %s (%s)", e.Field, e.Problem, e.Hint)
}

const defaultRankLimit = 5

// maxLimit caps how much a single answer can pull back. A "list every tract"
// request is a real question but not a chat answer; it is a link to the data.
const maxLimit = 50

// Validate checks an intent against the schema and the dataset it will run on.
// It normalises as it goes (default level, default limit) so Execute can assume
// a well-formed intent.
func (in *Intent) Validate(ds *Dataset) error {
	if in.Level == "" {
		in.Level = LevelCounty
	}
	if in.Level != LevelCounty && in.Level != LevelTract {
		return &ValidationError{"level", fmt.Sprintf("unknown level %q", in.Level),
			"use \"county\" or \"tract\""}
	}

	validOp := false
	for _, op := range AllOperations {
		if in.Operation == op {
			validOp = true
			break
		}
	}
	if !validOp {
		return &ValidationError{"operation", fmt.Sprintf("unknown operation %q", in.Operation),
			"one of: " + joinOps(AllOperations)}
	}

	// Representation is the one operation that is about seats, not values.
	if in.Operation == OpRepresentation {
		if ds.Representation == nil {
			return &ValidationError{"operation", "representation data is not loaded",
				"this deployment has no districts.geojson / representation.json"}
		}
		if len(in.Places) == 0 {
			return &ValidationError{"places", "representation needs a place",
				"name a county or tract"}
		}
		return in.resolvePlaces(ds)
	}

	if in.Indicator == "" {
		return &ValidationError{"indicator", "no indicator given",
			"one of: " + strings.Join(ds.IndicatorIDs(), ", ")}
	}
	if _, ok := ds.Indicator(in.Indicator); !ok {
		return &ValidationError{"indicator", fmt.Sprintf("unknown indicator %q", in.Indicator),
			"one of: " + strings.Join(ds.IndicatorIDs(), ", ")}
	}

	switch in.Operation {
	case OpLookup, OpCompare:
		if len(in.Places) == 0 {
			return &ValidationError{"places", "no place given",
				"name at least one county or tract"}
		}
		if in.Operation == OpCompare && len(in.Places) < 2 {
			return &ValidationError{"places", "compare needs at least two places",
				"use lookup for a single place"}
		}
	case OpRank:
		if in.Direction == "" {
			in.Direction = DirHighest
		}
		if in.Direction != DirHighest && in.Direction != DirLowest {
			return &ValidationError{"direction", fmt.Sprintf("unknown direction %q", in.Direction),
				"use \"highest\" or \"lowest\""}
		}
		if in.Limit <= 0 {
			in.Limit = defaultRankLimit
		}
		if in.Limit > maxLimit {
			in.Limit = maxLimit
		}
	case OpAggregate:
		if in.Aggregate == "" {
			in.Aggregate = AggMedian
		}
		switch in.Aggregate {
		case AggMedian, AggMin, AggMax, AggCount:
		default:
			return &ValidationError{"aggregate", fmt.Sprintf("unknown aggregate %q", in.Aggregate),
				"use median, min, max, or count. Mean is deliberately unavailable: " +
					"averaging medians does not produce a median."}
		}
	case OpThreshold:
		if in.Threshold == nil {
			return &ValidationError{"threshold", "no threshold value given",
				"give the number to compare against"}
		}
		if in.Comparator == "" {
			in.Comparator = CmpAbove
		}
		if in.Comparator != CmpAbove && in.Comparator != CmpBelow {
			return &ValidationError{"comparator", fmt.Sprintf("unknown comparator %q", in.Comparator),
				"use \"above\" or \"below\""}
		}
		if in.Limit <= 0 || in.Limit > maxLimit {
			in.Limit = maxLimit
		}
	}

	return in.resolvePlaces(ds)
}

// resolvePlaces turns names into GEOIDs in place, failing on anything the
// dataset does not contain. A silently dropped place would produce an answer
// that looks complete and is not.
func (in *Intent) resolvePlaces(ds *Dataset) error {
	if len(in.Places) == 0 {
		return nil
	}
	resolved := make([]string, 0, len(in.Places))
	var unknown []string
	for _, p := range in.Places {
		geoid, ok := ds.ResolvePlace(p, in.Level)
		if !ok {
			unknown = append(unknown, p)
			continue
		}
		resolved = append(resolved, geoid)
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return &ValidationError{"places",
			fmt.Sprintf("not found at %s level: %s", in.Level, strings.Join(unknown, ", ")),
			"use a Wisconsin county name, or a tract GEOID"}
	}
	in.Places = resolved
	return nil
}

func joinOps(ops []Operation) string {
	s := make([]string, len(ops))
	for i, o := range ops {
		s[i] = string(o)
	}
	return strings.Join(s, ", ")
}
