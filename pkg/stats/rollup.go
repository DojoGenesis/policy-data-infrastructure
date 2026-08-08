package stats

// Tract→county aggregation rules (ADR-014 D7) and the coverage-checked,
// uncertainty-carrying rollup computation (D8). The rules encode which
// variable classes may be aggregated at all; everything else is WITHHELD
// rather than approximated (D2: a caveat is read by roughly nobody, and the
// number, once rendered, travels without its footnote).
//
// The naive alternative — average whatever is present — shipped three times
// before this file existed (ADR-014 F1) while the correct argument sat in
// analysis/build_atlas_bundle.py. This is the enforced home the ADR asked
// for: gateways and scripts call this, not their own arithmetic.

import (
	"math"
	"strings"
)

// AggregationRule classifies how a variable class may roll up to a parent
// geography.
type AggregationRule int

const (
	// AggUnknown withholds: an unclassified variable has no defensible
	// aggregation until someone classifies it (D2).
	AggUnknown AggregationRule = iota
	// AggSum publishes the sum of child values (counts).
	AggSum
	// AggWeightedMean publishes the population-weighted mean (rates with a
	// same-source-or-vintage denominator supplied by the caller).
	AggWeightedMean
	// AggNeverPercentile refuses forever: the mean of percentile ranks is
	// not a percentile rank of anything. County SVI comes from CDC's own
	// county file or does not exist.
	AggNeverPercentile
	// AggNeverMedian refuses forever: a median of medians is not a median.
	// A county median comes from the source directly or not at all.
	AggNeverMedian
)

// String names the rule for parameters/provenance JSON.
func (r AggregationRule) String() string {
	switch r {
	case AggSum:
		return "sum"
	case AggWeightedMean:
		return "weighted_mean"
	case AggNeverPercentile:
		return "never_percentile_rank"
	case AggNeverMedian:
		return "never_median"
	default:
		return "unknown"
	}
}

// ClassifyVariable maps a variable to its D7 rule from its id and its
// registered unit. Classification is deliberately conservative: anything not
// positively identified as summable or weightable is AggUnknown → withheld.
func ClassifyVariable(variableID, unit string) AggregationRule {
	id := strings.ToLower(variableID)
	switch {
	case strings.HasPrefix(id, "cdc_svi_"):
		return AggNeverPercentile
	case strings.HasPrefix(id, "median_"),
		strings.Contains(id, "_median"):
		return AggNeverMedian
	}
	switch strings.ToLower(strings.TrimSpace(unit)) {
	case "count":
		return AggSum
	case "percent", "percentage", "rate":
		return AggWeightedMean
	}
	return AggUnknown
}

// RollupInput is one child geography's contribution: the indicator value and
// (for weighted means) its population weight. Either may be nil — nil values
// reduce coverage, they never become zeros.
type RollupInput struct {
	GEOID  string
	Value  *float64
	Weight *float64
}

// RollupResult is a published aggregate: a value, its bootstrap interval,
// and the coverage evidence that justified publishing it.
type RollupResult struct {
	Value    float64
	CI       ConfidenceInterval
	N        int     // children contributing (value present; weight too, when weighting)
	Total    int     // children in scope
	Coverage float64 // N / Total
}

// Withhold reasons, machine-readable so the API can distinguish "no data
// loaded" from "cannot be computed defensibly" (the D2 corollary).
const (
	WithheldRuleUnknown     = "unclassified_variable"
	WithheldRulePercentile  = "percentile_ranks_never_average"
	WithheldRuleMedian      = "median_of_medians_is_not_a_median"
	WithheldNoChildren      = "no_children_in_scope"
	WithheldNoData          = "no_child_values_loaded"
	WithheldCoverage        = "coverage_below_threshold"
	WithheldNoDenominator   = "no_denominator_weights"
	WithheldDegenerateBoot  = "bootstrap_interval_unavailable"
)

// RollupChildren computes one parent aggregate from child inputs under the
// given rule. It returns (nil, reason) when the aggregate is withheld; a
// non-nil result always carries a bootstrap interval (D8: an aggregate
// without an interval is not published).
//
// coverageThreshold is a required parameter, not a constant — its value is
// the caller's to choose and to record with the result (D7).
func RollupChildren(rule AggregationRule, inputs []RollupInput, coverageThreshold float64, nBoot int, alpha float64) (*RollupResult, string) {
	switch rule {
	case AggNeverPercentile:
		return nil, WithheldRulePercentile
	case AggNeverMedian:
		return nil, WithheldRuleMedian
	case AggSum, AggWeightedMean:
		// computable — continue below
	default:
		return nil, WithheldRuleUnknown
	}

	total := len(inputs)
	if total == 0 {
		return nil, WithheldNoChildren
	}

	var vals, weights []float64
	for _, in := range inputs {
		if in.Value == nil {
			continue
		}
		if rule == AggWeightedMean {
			if in.Weight == nil || *in.Weight <= 0 {
				continue
			}
			weights = append(weights, *in.Weight)
		}
		vals = append(vals, *in.Value)
	}

	n := len(vals)
	if n == 0 {
		if rule == AggWeightedMean {
			// Distinguish "values exist but no denominator" from "no values".
			anyValue := false
			for _, in := range inputs {
				if in.Value != nil {
					anyValue = true
					break
				}
			}
			if anyValue {
				return nil, WithheldNoDenominator
			}
		}
		return nil, WithheldNoData
	}

	coverage := float64(n) / float64(total)
	if coverage < coverageThreshold {
		return nil, WithheldCoverage
	}

	var ci ConfidenceInterval
	switch rule {
	case AggSum:
		ci = Bootstrap(func(xs []float64) float64 {
			var s float64
			for _, v := range xs {
				s += v
			}
			return s
		}, vals, nBoot, alpha)
	case AggWeightedMean:
		ci = BootstrapPairs(weightedMean, vals, weights, nBoot, alpha)
	}

	if math.IsNaN(ci.PointEstimate) || math.IsNaN(ci.Lower) || math.IsNaN(ci.Upper) {
		return nil, WithheldDegenerateBoot
	}

	return &RollupResult{
		Value:    ci.PointEstimate,
		CI:       ci,
		N:        n,
		Total:    total,
		Coverage: coverage,
	}, ""
}

// weightedMean is Σ(v·w)/Σ(w) over aligned slices. Callers guarantee
// positive weights and equal lengths.
func weightedMean(vals, weights []float64) float64 {
	var num, den float64
	for i, v := range vals {
		num += v * weights[i]
		den += weights[i]
	}
	if den == 0 {
		return math.NaN()
	}
	return num / den
}
