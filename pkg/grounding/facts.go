package grounding

import (
	"fmt"
	"strconv"
	"strings"
)

// renderFacts writes the answer deterministically from the Result.
//
// This exists so the pipeline has a correct answer BEFORE any model is asked to
// write prose. If the model is unavailable, or its draft fails verification,
// this is what ships — plainer, but never wrong. A grounded-chat feature whose
// only output path runs through a model has no floor.
func renderFacts(in *Intent, ds *Dataset, ind IndicatorMeta, res *Result) string {
	unit := unitSuffix(ind)
	label := strings.ToLower(ind.Label)
	var b strings.Builder

	switch in.Operation {
	case OpLookup:
		for _, v := range res.Values {
			if v.Missing {
				fmt.Fprintf(&b, "%s has no published %s for this vintage; the Census suppresses small-population estimates.\n",
					v.Name, label)
				continue
			}
			fmt.Fprintf(&b, "%s: %s%s (%s).\n", v.Name, formatValue(*v.Value, ind), unit, label)
		}

	case OpCompare:
		var present []Value
		for _, v := range res.Values {
			if v.Missing {
				fmt.Fprintf(&b, "%s has no published %s for this vintage.\n", v.Name, label)
				continue
			}
			present = append(present, v)
			fmt.Fprintf(&b, "%s: %s%s.\n", v.Name, formatValue(*v.Value, ind), unit)
		}
		if len(present) == 2 {
			d := *present[0].Value - *present[1].Value
			hi, lo := present[0], present[1]
			if d < 0 {
				d, hi, lo = -d, present[1], present[0]
			}
			fmt.Fprintf(&b, "%s is higher than %s by %s%s.\n",
				hi.Name, lo.Name, formatValue(d, ind), unit)
		}

	case OpRank:
		dir := "highest"
		if in.Direction == DirLowest {
			dir = "lowest"
		}
		fmt.Fprintf(&b, "The %d %s %s by %s, out of %d with a published value",
			len(res.Values), dir, plural(in.Level, len(res.Values)), label, res.TotalCount)
		if res.Missing > 0 {
			fmt.Fprintf(&b, " (%d %s none)", res.Missing, verb(res.Missing))
		}
		b.WriteString(":\n")
		for _, v := range res.Values {
			fmt.Fprintf(&b, "%d. %s: %s%s\n", v.Rank, v.Name, formatValue(*v.Value, ind), unit)
		}

	case OpAggregate:
		if res.Scalar == nil {
			break
		}
		if in.Aggregate == AggCount {
			n := int(*res.Scalar)
			fmt.Fprintf(&b, "%d %s %s a published %s.\n", n, plural(in.Level, n), verb(n), label)
			break
		}
		fmt.Fprintf(&b, "The %s %s across %d %s is %s%s",
			string(in.Aggregate), label, res.TotalCount, plural(in.Level, res.TotalCount),
			formatValue(*res.Scalar, ind), unit)
		if res.Missing > 0 {
			fmt.Fprintf(&b, "; %d %s %s no published value",
				res.Missing, plural(in.Level, res.Missing), verb(res.Missing))
		}
		b.WriteString(".\n")

	case OpThreshold:
		cmp := "above"
		if in.Comparator == CmpBelow {
			cmp = "below"
		}
		fmt.Fprintf(&b, "%d %s %s %s %s%s for %s",
			res.TotalCount, plural(in.Level, res.TotalCount), areIs(res.TotalCount),
			cmp, formatValue(*in.Threshold, ind), unit, label)
		if len(res.Values) < res.TotalCount {
			fmt.Fprintf(&b, "; the %d with the highest values", len(res.Values))
		}
		b.WriteString(":\n")
		for _, v := range res.Values {
			fmt.Fprintf(&b, "- %s: %s%s\n", v.Name, formatValue(*v.Value, ind), unit)
		}
	}

	if len(res.Citations) > 0 {
		c := res.Citations[0]
		fmt.Fprintf(&b, "\nSource: %s, %s estimates, %s level", c.Source, c.Vintage, c.Level)
		if c.Table != "" {
			fmt.Fprintf(&b, " (table %s)", c.Table)
		}
		b.WriteString(".")
	}
	return strings.TrimSpace(b.String())
}

func renderRepresentationFacts(in *Intent, ds *Dataset, res *Result) string {
	var b strings.Builder
	if len(in.Places) == 1 {
		fmt.Fprintf(&b, "%s is represented by:\n", ds.Name(in.Places[0], in.Level))
	} else {
		b.WriteString("Representation:\n")
	}
	for _, s := range res.Seats {
		if s.Official == "" {
			fmt.Fprintf(&b, "- %s: no officeholder on record\n", s.Label)
			continue
		}
		fmt.Fprintf(&b, "- %s: %s", s.Label, s.Official)
		if s.Party != "" {
			fmt.Fprintf(&b, " (%s)", s.Party)
		}
		b.WriteString("\n")
	}
	b.WriteString("\nDistrict boundaries from U.S. Census TIGERweb; officeholders from " +
		"unitedstates/congress-legislators and Open States. A tract is assigned by its " +
		"Census interior point, so a tract straddling a district line is reported under one district.")
	return strings.TrimSpace(b.String())
}

// plural handles the two geography words this package renders. "countys" is not
// a word, and an answer that says it reads as machine output regardless of how
// correct the number is.
func plural(level Level, n int) string {
	if n == 1 {
		return string(level)
	}
	if level == LevelCounty {
		return "counties"
	}
	return string(level) + "s"
}

func verb(n int) string {
	if n == 1 {
		return "has"
	}
	return "have"
}

func areIs(n int) string {
	if n == 1 {
		return "is"
	}
	return "are"
}

func unitSuffix(ind IndicatorMeta) string {
	switch ind.Format {
	case "percent":
		return "%"
	case "currency":
		return " dollars"
	default:
		return ""
	}
}

// formatValue renders a number at the precision the indicator actually has.
// Percentages carry one decimal because that is what the ACS publishes; counts
// and dollars carry none, because a fractional person or cent is a precision
// claim the source does not make.
func formatValue(v float64, ind IndicatorMeta) string {
	switch ind.Format {
	case "percent":
		return strconv.FormatFloat(v, 'f', 1, 64)
	default:
		return addThousands(strconv.FormatFloat(v, 'f', 0, 64))
	}
}

func addThousands(s string) string {
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}
