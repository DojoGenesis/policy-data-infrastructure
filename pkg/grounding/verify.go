package grounding

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Verify is the stage ADR-005 does not have.
//
// Grounding a model's INPUT does not ground its OUTPUT. A model handed
// `poverty_rate: 8.8` can still write "just under 9 percent, up from 7.2 in the
// previous vintage" — inventing a comparison and a figure no query returned.
// So: pull every number out of the draft and require each one to be a number the
// result actually contains.
//
// This is deliberately lexical and deliberately cheap. It catches invented
// figures, which is the failure the requirement names. It does not catch a real
// number used in a wrong sentence — see the ADR's Consequences. Cheap and
// partial beats expensive and absent; a rejected draft costs one retry.
type Violation struct {
	Number  string
	Context string
}

func (v Violation) Error() string {
	return fmt.Sprintf("number %q is not in the result set (…%s…)", v.Number, v.Context)
}

// numberRe matches integers and decimals, with thousands separators only where
// they are actually separating thousands.
//
// The obvious `\d[\d,]*` is wrong: in "Census Tract 1, Dane County" it captures
// "1," — a number token that does not exist in the text — and then reports the
// non-existent token as unsupported. A comma must be followed by three digits
// to count as a separator.
var numberRe = regexp.MustCompile(`-?\d{1,3}(?:,\d{3})+(?:\.\d+)?|-?\d+(?:\.\d+)?`)

// Ordinals and small counts that describe the SHAPE of an answer rather than
// asserting a measurement — "the 5 highest", "1." in a numbered list. These are
// allowed only when they match a structural fact of the result (its length, a
// rank, the total), which allowedNumbers already covers; this set additionally
// permits the bare ordinals a list naturally produces.
func structuralAllowed(res *Result) map[float64]bool {
	out := map[float64]bool{}
	for i := 1; i <= len(res.Values); i++ {
		out[float64(i)] = true
	}
	return out
}

// AllowedNumbers is every figure a draft may legitimately contain: each value,
// each rank, the totals, the aggregate, and the vintage years. Values are
// registered at several roundings because prose legitimately says "8.8" for
// 8.7999 or "89,975" for 89975.
func AllowedNumbers(res *Result) map[float64]bool {
	allowed := map[float64]bool{}

	add := func(f float64) {
		allowed[f] = true
		allowed[round(f, 0)] = true
		allowed[round(f, 1)] = true
		allowed[round(f, 2)] = true
		// A model rounding 89,975 to "90,000" or "89 thousand" is paraphrase,
		// not fabrication — but it is also not a figure the data contains, so
		// it is NOT registered here. Rounding claims must be exact.
	}

	for _, v := range res.Values {
		if v.Value != nil {
			add(*v.Value)
		}
		if v.Rank > 0 {
			allowed[float64(v.Rank)] = true
		}
	}
	if res.Scalar != nil {
		add(*res.Scalar)
	}
	if res.TotalCount > 0 {
		allowed[float64(res.TotalCount)] = true
	}
	if res.Missing > 0 {
		allowed[float64(res.Missing)] = true
	}
	allowed[float64(len(res.Values))] = true

	// Differences between compared values are computed by this package for
	// compare answers, so a draft may legitimately restate them.
	if res.Operation == OpCompare {
		for i := range res.Values {
			for j := range res.Values {
				if i == j || res.Values[i].Value == nil || res.Values[j].Value == nil {
					continue
				}
				add(math.Abs(*res.Values[i].Value - *res.Values[j].Value))
			}
		}
	}

	// The asker's own threshold. "counties above 10 percent" restates the
	// question, not the data, and flagging it would reject every threshold
	// answer including this package's own rendering.
	if res.Threshold != nil {
		add(*res.Threshold)
	}

	// Numbers that are part of the dataset's own TEXT rather than claims about
	// it. Three sources, all grounded:
	//
	//   citation fields — the vintage years, the "5" in "5-Year Estimates",
	//     the ACS table number. A draft that names its source must not be
	//     accused of inventing figures for doing so.
	//   place names — "Census Tract 16.03" contains a number. Without this,
	//     EVERY tract-level answer is rejected for naming its own subject,
	//     which is a systematic false positive, not a caught fabrication.
	//   indicator definitions — "spending 30% or more of income on housing".
	//     The 30 is the federal threshold the indicator is defined by; a draft
	//     explaining what cost-burdened means is quoting the dataset.
	addTextNumbers := func(s string) {
		for _, tok := range numberRe.FindAllString(s, -1) {
			if f, err := strconv.ParseFloat(strings.ReplaceAll(tok, ",", ""), 64); err == nil {
				allowed[f] = true
			}
		}
	}
	for _, c := range res.Citations {
		for _, field := range []string{
			c.Vintage, c.Source, c.Table, c.IndicatorLabel, c.Unit, c.Level, c.Definition,
		} {
			addTextNumbers(field)
		}
	}
	for _, v := range res.Values {
		addTextNumbers(v.Name)
		addTextNumbers(v.GeoID)
	}
	for _, p := range res.Places {
		addTextNumbers(p)
	}
	for _, n := range res.PlaceNames {
		addTextNumbers(n)
	}
	for _, s := range res.Seats {
		addTextNumbers(s.Label)
		addTextNumbers(s.District)
	}

	for f := range structuralAllowed(res) {
		allowed[f] = true
	}
	return allowed
}

// Verify returns every number in prose that the result does not support.
// An empty slice means the draft is safe to ship.
func Verify(prose string, res *Result) []Violation {
	allowed := AllowedNumbers(res)
	var out []Violation

	for _, loc := range numberRe.FindAllStringIndex(prose, -1) {
		tok := prose[loc[0]:loc[1]]
		f, err := strconv.ParseFloat(strings.ReplaceAll(tok, ",", ""), 64)
		if err != nil {
			continue
		}
		if allowed[f] || allowed[round(f, 1)] || allowed[round(f, 2)] {
			continue
		}
		out = append(out, Violation{Number: tok, Context: excerpt(prose, loc[0], loc[1])})
	}
	return out
}

func round(f float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(f*p) / p
}

func excerpt(s string, start, end int) string {
	const pad = 24
	lo := start - pad
	if lo < 0 {
		lo = 0
	}
	hi := end + pad
	if hi > len(s) {
		hi = len(s)
	}
	return strings.ReplaceAll(s[lo:hi], "\n", " ")
}
