package grounding

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// Value is one figure the query actually returned, with the place it belongs to.
type Value struct {
	GeoID   string   `json:"geoid"`
	Name    string   `json:"name"`
	Value   *float64 `json:"value"` // nil means suppressed — never rendered as 0
	Rank    int      `json:"rank,omitempty"`
	Missing bool     `json:"missing,omitempty"`
}

// Citation is where a figure came from. Assembled from the intent and the
// dataset's own manifest, so it cannot disagree with the data.
type Citation struct {
	Indicator      string `json:"indicator"`
	IndicatorLabel string `json:"indicatorLabel"`
	Unit           string `json:"unit"`
	Level          string `json:"level"`
	Vintage        string `json:"vintage"`
	Source         string `json:"source"`
	Table          string `json:"table"`
	// Definition is the indicator's own wording. Sent to the composer so an
	// explanation is grounded rather than remembered.
	Definition string `json:"definition,omitempty"`
}

// Seat is one district and its officeholder, for representation answers.
type Seat struct {
	Chamber  string `json:"chamber"`
	Label    string `json:"label"`
	District string `json:"district"`
	Official string `json:"official,omitempty"`
	Party    string `json:"party,omitempty"`
	URL      string `json:"url,omitempty"`
}

// Result is everything the answer is allowed to contain.
type Result struct {
	Operation  Operation  `json:"operation"`
	Indicator  string     `json:"indicator,omitempty"`
	Level      string     `json:"level"`
	Values     []Value    `json:"values,omitempty"`
	Seats      []Seat     `json:"seats,omitempty"`
	Scalar     *float64   `json:"scalar,omitempty"` // aggregate result
	ScalarKind string     `json:"scalarKind,omitempty"`
	TotalCount int        `json:"totalCount,omitempty"`
	Missing    int        `json:"missing,omitempty"`
	Citations  []Citation `json:"citations"`
	// Threshold echoes back the value the question supplied. Verification has
	// to allow it: "counties above 10 percent" restates the asker's own number,
	// which is not a claim about the data and must not be flagged as invented.
	Threshold *float64 `json:"threshold,omitempty"`
	// Places are the resolved GEOIDs this query ran against. A GEOID is an
	// identifier, not a measurement, and an answer that names the tract it was
	// asked about must not be read as inventing a figure.
	Places []string `json:"places,omitempty"`
	// PlaceNames are those places' display names. Carried for the same reason:
	// "Census Tract 16.03" contains digits that belong to the subject, not to a
	// claim. Values[] carries names for value queries; representation queries
	// have no Values, so they would otherwise lose them.
	PlaceNames []string `json:"placeNames,omitempty"`
	// Facts is the deterministic English rendering of the result. It is written
	// by this package, not by a model, and is what a caller should show if it
	// wants zero model involvement in the answer at all.
	Facts string `json:"facts"`
}

// Execute runs a validated intent. It is a pure function of (intent, dataset):
// same inputs, same output, no model, no network.
func Execute(in *Intent, ds *Dataset) (*Result, error) {
	if in.Operation == OpRepresentation {
		return executeRepresentation(in, ds)
	}

	ind, ok := ds.Indicator(in.Indicator)
	if !ok {
		return nil, &ValidationError{"indicator", "unknown indicator " + in.Indicator, ""}
	}

	res := &Result{
		Operation: in.Operation,
		Indicator: in.Indicator,
		Level:     string(in.Level),
		Places:    in.Places,
		Citations: []Citation{ds.citation(ind, in.Level)},
	}
	for _, g := range in.Places {
		res.PlaceNames = append(res.PlaceNames, ds.Name(g, in.Level))
	}

	switch in.Operation {
	case OpLookup, OpCompare:
		for _, g := range in.Places {
			v, ok := ds.Value(g, in.Level, in.Indicator)
			if !ok {
				return nil, fmt.Errorf("place %s not present at %s level", g, in.Level)
			}
			res.Values = append(res.Values, Value{
				GeoID: g, Name: ds.Name(g, in.Level), Value: v, Missing: v == nil,
			})
		}

	case OpTimeSeries:
		// Static atlas only has one vintage. Return the current value with a note
		// about multi-vintage availability via the API.
		for _, g := range in.Places {
			v, ok := ds.Value(g, in.Level, in.Indicator)
			if !ok {
				return nil, fmt.Errorf("place %s not present at %s level", g, in.Level)
			}
			res.Values = append(res.Values, Value{
				GeoID: g, Name: ds.Name(g, in.Level), Value: v, Missing: v == nil,
			})
		}
		res.ScalarKind = "time_series_note"
		note := "The static atlas has one vintage (" + ds.Vintage + "). Multi-vintage time-series data is available via the API endpoint with ?vintage=YYYY,YYYY parameter. The current value shown is the latest available."
		res.Facts = renderFacts(in, ds, ind, res) + "\n\n" + note
		return res, nil

	case OpRank:
		vals := ds.collect(in.Level, in.Indicator)
		sort.SliceStable(vals, func(i, j int) bool {
			if in.Direction == DirHighest {
				return *vals[i].Value > *vals[j].Value
			}
			return *vals[i].Value < *vals[j].Value
		})
		res.TotalCount = len(vals)
		res.Missing = ds.Count(in.Level) - len(vals)
		if in.Limit < len(vals) {
			vals = vals[:in.Limit]
		}
		for i := range vals {
			vals[i].Rank = i + 1
		}
		res.Values = vals

	case OpAggregate:
		vals := ds.collect(in.Level, in.Indicator)
		res.TotalCount = len(vals)
		res.Missing = ds.Count(in.Level) - len(vals)
		res.ScalarKind = string(in.Aggregate)
		if in.Aggregate == AggCount {
			n := float64(len(vals))
			res.Scalar = &n
			break
		}
		if len(vals) == 0 {
			return nil, fmt.Errorf("no values for %s at %s level", in.Indicator, in.Level)
		}
		nums := make([]float64, len(vals))
		for i, v := range vals {
			nums[i] = *v.Value
		}
		sort.Float64s(nums)
		var s float64
		switch in.Aggregate {
		case AggMedian:
			s = median(nums)
		case AggMin:
			s = nums[0]
		case AggMax:
			s = nums[len(nums)-1]
		}
		res.Scalar = &s

	case OpThreshold:
		res.Threshold = in.Threshold
		vals := ds.collect(in.Level, in.Indicator)
		var kept []Value
		for _, v := range vals {
			if in.Comparator == CmpAbove && *v.Value > *in.Threshold {
				kept = append(kept, v)
			} else if in.Comparator == CmpBelow && *v.Value < *in.Threshold {
				kept = append(kept, v)
			}
		}
		sort.SliceStable(kept, func(i, j int) bool { return *kept[i].Value > *kept[j].Value })
		res.TotalCount = len(kept)
		res.Missing = ds.Count(in.Level) - len(vals)
		if in.Limit < len(kept) {
			kept = kept[:in.Limit]
		}
		res.Values = kept
	}

	res.Facts = renderFacts(in, ds, ind, res)
	return res, nil
}

func executeRepresentation(in *Intent, ds *Dataset) (*Result, error) {
	res := &Result{Operation: OpRepresentation, Level: string(in.Level), Places: in.Places}
	for _, g := range in.Places {
		res.PlaceNames = append(res.PlaceNames, ds.Name(g, in.Level))
		rep, ok := ds.Representation[g]
		if !ok {
			// A county-level question about representation is legitimate but
			// unanswerable: districts are joined at tract resolution, and a
			// county spans many. Say so rather than picking one.
			return nil, &ValidationError{"places",
				fmt.Sprintf("no representation record for %s", ds.Name(g, in.Level)),
				"representation is joined at tract level; ask about a tract GEOID"}
		}
		for _, chamber := range []string{"us_house", "state_upper", "state_lower"} {
			d, ok := rep[chamber]
			if !ok {
				continue
			}
			seat := Seat{Chamber: chamber, Label: d.DistrictLabel, District: d.District}
			if d.Official != nil {
				seat.Official = d.Official.Name
				seat.Party = d.Official.Party
				seat.URL = d.Official.URL
			}
			res.Seats = append(res.Seats, seat)
		}
	}
	res.Citations = []Citation{{
		Level:   string(in.Level),
		Source:  "U.S. Census Bureau TIGERweb; unitedstates/congress-legislators; Open States",
		Vintage: ds.Vintage,
	}}
	res.Facts = renderRepresentationFacts(in, ds, res)
	return res, nil
}

// collect returns every non-missing value at a level, as Values.
func (ds *Dataset) collect(level Level, indicator string) []Value {
	var out []Value
	for _, g := range ds.GeoIDs(level) {
		v, ok := ds.Value(g, level, indicator)
		if !ok || v == nil {
			continue
		}
		out = append(out, Value{GeoID: g, Name: ds.Name(g, level), Value: v})
	}
	return out
}

func (ds *Dataset) citation(ind IndicatorMeta, level Level) Citation {
	src := "U.S. Census Bureau, American Community Survey 5-Year Estimates"
	for _, s := range ds.Sources {
		if strings.Contains(strings.ToLower(s.UsedFor), "indicator") && s.Name != "" {
			src = s.Name
			if s.Publisher != "" {
				src = s.Publisher + ", " + s.Name
			}
			break
		}
	}
	return Citation{
		Indicator: ind.ID, IndicatorLabel: ind.Label, Unit: ind.Unit,
		Level: string(level), Vintage: ds.Vintage, Source: src, Table: ind.Table,
		Definition: ind.Description,
	}
}

func median(sorted []float64) float64 {
	n := len(sorted)
	if n == 0 {
		return math.NaN()
	}
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}
