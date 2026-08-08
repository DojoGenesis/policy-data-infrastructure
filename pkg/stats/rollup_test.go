package stats

import (
	"math"
	"testing"
)

func TestClassifyVariable(t *testing.T) {
	cases := []struct {
		id, unit string
		want     AggregationRule
	}{
		{"cdc_svi_overall", "index", AggNeverPercentile},
		{"cdc_svi_socioeconomic", "percent", AggNeverPercentile}, // prefix wins over unit
		{"median_household_income", "dollars", AggNeverMedian},
		{"usda_population", "count", AggSum},
		{"total_population", "count", AggSum},
		{"cdc_obesity", "percent", AggWeightedMean},
		{"poverty_rate", "percent", AggWeightedMean},
		{"some_new_thing", "widgets", AggUnknown},
		{"some_new_thing", "", AggUnknown},
	}
	for _, c := range cases {
		if got := ClassifyVariable(c.id, c.unit); got != c.want {
			t.Errorf("ClassifyVariable(%q, %q) = %v, want %v", c.id, c.unit, got, c.want)
		}
	}
}

func TestRollupWithholdsForbiddenClasses(t *testing.T) {
	inputs := []RollupInput{{GEOID: "a", Value: fp(0.5), Weight: fp(100)}}

	if res, reason := RollupChildren(AggNeverPercentile, inputs, 0.8, 100, 0.05); res != nil || reason != WithheldRulePercentile {
		t.Errorf("percentile rollup: got (%v, %q), want withheld %q", res, reason, WithheldRulePercentile)
	}
	if res, reason := RollupChildren(AggNeverMedian, inputs, 0.8, 100, 0.05); res != nil || reason != WithheldRuleMedian {
		t.Errorf("median rollup: got (%v, %q), want withheld %q", res, reason, WithheldRuleMedian)
	}
	if res, reason := RollupChildren(AggUnknown, inputs, 0.8, 100, 0.05); res != nil || reason != WithheldRuleUnknown {
		t.Errorf("unknown rollup: got (%v, %q), want withheld %q", res, reason, WithheldRuleUnknown)
	}
}

func TestRollupWithholdsBelowCoverage(t *testing.T) {
	// 2 of 5 tracts report → 40% coverage, below an 80% threshold.
	inputs := []RollupInput{
		{GEOID: "a", Value: fp(10), Weight: fp(100)},
		{GEOID: "b", Value: fp(20), Weight: fp(300)},
		{GEOID: "c"},
		{GEOID: "d"},
		{GEOID: "e"},
	}
	res, reason := RollupChildren(AggWeightedMean, inputs, 0.8, 100, 0.05)
	if res != nil || reason != WithheldCoverage {
		t.Fatalf("got (%v, %q), want withheld %q", res, reason, WithheldCoverage)
	}
	// The same data clears a 40% threshold: the threshold is a parameter.
	res, reason = RollupChildren(AggWeightedMean, inputs, 0.4, 100, 0.05)
	if res == nil {
		t.Fatalf("40%% threshold should publish, got withheld %q", reason)
	}
	if res.N != 2 || res.Total != 5 || math.Abs(res.Coverage-0.4) > 1e-9 {
		t.Errorf("coverage bookkeeping wrong: %+v", res)
	}
}

func TestRollupWeightedMeanIsWeighted(t *testing.T) {
	// A county where the big tract is poor and the small tract is rich:
	// unweighted mean = 30; population-weighted = (10*900 + 50*100)/1000 = 14.
	inputs := []RollupInput{
		{GEOID: "big", Value: fp(10), Weight: fp(900)},
		{GEOID: "small", Value: fp(50), Weight: fp(100)},
	}
	res, reason := RollupChildren(AggWeightedMean, inputs, 0.8, 200, 0.05)
	if res == nil {
		t.Fatalf("withheld unexpectedly: %s", reason)
	}
	if math.Abs(res.Value-14.0) > 1e-9 {
		t.Errorf("weighted mean = %v, want 14.0 (the naive mean is 30 — the defect this file exists to prevent)", res.Value)
	}
	if res.CI.Lower > res.Value || res.CI.Upper < res.Value {
		t.Errorf("CI [%v, %v] does not bracket the point estimate %v", res.CI.Lower, res.CI.Upper, res.Value)
	}
}

func TestRollupSumAndMissingDenominator(t *testing.T) {
	inputs := []RollupInput{
		{GEOID: "a", Value: fp(100)},
		{GEOID: "b", Value: fp(250)},
	}
	res, reason := RollupChildren(AggSum, inputs, 0.8, 100, 0.05)
	if res == nil {
		t.Fatalf("sum withheld unexpectedly: %s", reason)
	}
	if math.Abs(res.Value-350) > 1e-9 {
		t.Errorf("sum = %v, want 350", res.Value)
	}

	// Same inputs as a weighted mean: values exist, weights don't →
	// the reason must say "no denominator", not "no data".
	res, reason = RollupChildren(AggWeightedMean, inputs, 0.8, 100, 0.05)
	if res != nil || reason != WithheldNoDenominator {
		t.Errorf("got (%v, %q), want withheld %q", res, reason, WithheldNoDenominator)
	}
}

func TestBootstrapPairsKeepsPairsAligned(t *testing.T) {
	// Perfectly correlated pairs: any index resample keeps fn constant at 1.
	xs := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	ys := []float64{2, 4, 6, 8, 10, 12, 14, 16}
	pearson := func(a, b []float64) float64 {
		var sa, sb, saa, sbb, sab float64
		n := float64(len(a))
		for i := range a {
			sa += a[i]
			sb += b[i]
			saa += a[i] * a[i]
			sbb += b[i] * b[i]
			sab += a[i] * b[i]
		}
		den := math.Sqrt(n*saa-sa*sa) * math.Sqrt(n*sbb-sb*sb)
		if den == 0 {
			return math.NaN()
		}
		return (n*sab - sa*sb) / den
	}
	ci := BootstrapPairs(pearson, xs, ys, 200, 0.05)
	// Degenerate resamples (all one point) make the correlation NaN; the
	// interval bounds may reflect that, but the point estimate must be 1
	// and non-NaN bounds must equal 1 as well.
	if math.Abs(ci.PointEstimate-1.0) > 1e-9 {
		t.Errorf("point estimate = %v, want 1.0", ci.PointEstimate)
	}
	if !math.IsNaN(ci.Lower) && math.Abs(ci.Lower-1.0) > 1e-9 {
		t.Errorf("lower = %v, want 1.0 for perfectly correlated pairs", ci.Lower)
	}

	// Length mismatch is a caller bug → NaN interval, no panic.
	bad := BootstrapPairs(pearson, xs, ys[:3], 50, 0.05)
	if !math.IsNaN(bad.PointEstimate) {
		t.Errorf("length mismatch should produce NaN point estimate, got %v", bad.PointEstimate)
	}
}
