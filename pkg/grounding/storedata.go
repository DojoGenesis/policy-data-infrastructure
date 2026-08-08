package grounding

// Store-backed Dataset construction — the "agent on disk" gap, closed
// (2026-08-08 operator directive: the chat agent must see everything the
// platform knows, not the 11-indicator atlas bundle frozen at build time).
//
// The original design loaded the shipped atlas bundle so chat could never
// answer from a different dataset than the map. That principle survives
// here inverted: instead of freezing a copy, both surfaces draw from the
// one source of truth. LoadFromStore builds the SAME *Dataset the engine
// already consumes — every indicator with data, at every level, each
// carrying its own vintage — so the engine, planner prompt, verifier and
// citations work unchanged over the full database.
//
// The snapshot is deliberately a point-in-time copy in memory (a few MB at
// WI scale): query execution stays allocation-free and deterministic, and
// freshness is the caller's explicit choice via re-load (gateway.ChatPlugin
// refreshes on a TTL). A snapshot that states its age honestly beats a
// live query path that couples every chat request to database latency.

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// StoreSource is the narrow slice of store.Store this loader needs. A
// dedicated seam keeps test fakes at three methods instead of the full
// Store surface.
type StoreSource interface {
	QueryVariables(ctx context.Context) ([]store.VariableMeta, error)
	QueryIndicators(ctx context.Context, q store.IndicatorQuery) ([]store.Indicator, error)
	QueryGeographies(ctx context.Context, q store.GeoQuery) ([]geo.Geography, error)
}

// Compile-time proof that the real store satisfies the seam.
var _ StoreSource = (store.Store)(nil)

// LoadFromStore builds a Dataset snapshot from the live database: the
// latest value of every indicator that actually has data, at county and
// tract level, with per-indicator vintages and per-source provenance.
// Variables with zero stored rows are excluded — offering the planner a
// vocabulary of empty columns invites plans that can only report missing.
func LoadFromStore(ctx context.Context, src StoreSource) (*Dataset, error) {
	vars, err := src.QueryVariables(ctx)
	if err != nil {
		return nil, fmt.Errorf("grounding: load variables: %w", err)
	}

	rows, err := src.QueryIndicators(ctx, store.IndicatorQuery{LatestOnly: true})
	if err != nil {
		return nil, fmt.Errorf("grounding: load indicators: %w", err)
	}

	ds := &Dataset{
		values: map[Level]map[string]map[string]*float64{
			LevelCounty: {}, LevelTract: {},
		},
		names:  map[Level]map[string]string{LevelCounty: {}, LevelTract: {}},
		byName: map[Level]map[string]string{LevelCounty: {}, LevelTract: {}},
	}

	// Geography names, both levels.
	for level, geoLevel := range map[Level]geo.Level{
		LevelCounty: geo.County,
		LevelTract:  geo.Tract,
	} {
		geos, err := src.QueryGeographies(ctx, store.GeoQuery{Level: geoLevel, Limit: 10000})
		if err != nil {
			return nil, fmt.Errorf("grounding: load %s geographies: %w", level, err)
		}
		for _, g := range geos {
			ds.names[level][g.GEOID] = g.Name
			ds.byName[level][strings.ToLower(g.Name)] = g.GEOID
			if level == LevelCounty {
				bare := strings.TrimSuffix(g.Name, " County")
				ds.byName[level][strings.ToLower(bare)] = g.GEOID
			}
		}
	}

	// Values by level, plus per-variable row/vintage tallies.
	type varStat struct {
		rows     int
		vintages map[string]int
	}
	stats := map[string]*varStat{}
	levelFor := func(geoid string) (Level, bool) {
		switch len(geoid) {
		case 5:
			return LevelCounty, true
		case 11:
			return LevelTract, true
		}
		return "", false
	}
	for _, r := range rows {
		level, ok := levelFor(r.GEOID)
		if !ok {
			continue
		}
		byGeo := ds.values[level]
		if byGeo[r.GEOID] == nil {
			byGeo[r.GEOID] = map[string]*float64{}
		}
		byGeo[r.GEOID][r.VariableID] = r.Value

		st := stats[r.VariableID]
		if st == nil {
			st = &varStat{vintages: map[string]int{}}
			stats[r.VariableID] = st
		}
		st.rows++
		if r.Vintage != "" {
			st.vintages[r.Vintage]++
		}
	}

	// Indicator metadata for variables that carry data, with the variable's
	// dominant vintage attached — provenance travels per indicator, not as
	// one dataset-wide claim.
	sourceVars := map[string][]string{}   // source name -> variable ids
	sourceVint := map[string]map[string]int{} // source name -> vintage tally
	for _, v := range vars {
		st, ok := stats[v.VariableID]
		if !ok || st.rows == 0 {
			continue
		}
		srcName := v.SourceName
		if srcName == "" {
			srcName = v.SourceID
		}
		ds.Indicators = append(ds.Indicators, IndicatorMeta{
			ID:          v.VariableID,
			Label:       v.Name,
			Unit:        v.Unit,
			Format:      formatForUnit(v.Unit),
			Direction:   v.Direction,
			Table:       v.VariableID,
			Description: v.Description,
			Vintage:     dominantKey(st.vintages),
			SourceName:  srcName,
		})
		sourceVars[srcName] = append(sourceVars[srcName], v.VariableID)
		if sourceVint[srcName] == nil {
			sourceVint[srcName] = map[string]int{}
		}
		for vint, n := range st.vintages {
			sourceVint[srcName][vint] += n
		}
	}
	sort.Slice(ds.Indicators, func(i, j int) bool { return ds.Indicators[i].ID < ds.Indicators[j].ID })
	if len(ds.Indicators) == 0 {
		return nil, fmt.Errorf("grounding: store holds no indicators with data")
	}

	// Every value in each geography map exists for each kept indicator, so
	// "place present, value nil" keeps meaning suppressed rather than
	// unknown-indicator (Dataset.Value's contract).
	for _, level := range []Level{LevelCounty, LevelTract} {
		for _, vals := range ds.values[level] {
			for _, ind := range ds.Indicators {
				if _, ok := vals[ind.ID]; !ok {
					vals[ind.ID] = nil
				}
			}
		}
	}

	// Source provenance, one entry per contributing source.
	var srcNames []string
	for name := range sourceVars {
		srcNames = append(srcNames, name)
	}
	sort.Strings(srcNames)
	for _, name := range srcNames {
		ids := sourceVars[name]
		sort.Strings(ids)
		ds.Sources = append(ds.Sources, SourceMeta{
			Name:    name,
			Vintage: dominantKey(sourceVint[name]),
			UsedFor: "indicators: " + strings.Join(ids, ", "),
		})
	}
	ds.Vintage = "per-indicator (see citations)"

	return ds, nil
}

// formatForUnit maps registered units onto facts.go's render formats.
// The distinction that matters: "percent" units are on a 0–100 scale;
// percentile ranks and population shares are 0–1 and must render as
// decimals — formatting a 0.87 rank as a thousands-separated integer
// displays "1", and as a percent displays "0.9%", both wrong.
func formatForUnit(unit string) string {
	switch strings.ToLower(strings.TrimSpace(unit)) {
	case "percent", "percentage", "rate":
		return "percent"
	case "percentile", "percent_share", "share", "index", "ratio":
		return "decimal"
	case "dollars", "usd", "currency":
		return "currency"
	default:
		return ""
	}
}

// dominantKey returns the most frequent key ("" for an empty tally); ties
// break lexicographically for determinism.
func dominantKey(tally map[string]int) string {
	best, bestN := "", -1
	keys := make([]string, 0, len(tally))
	for k := range tally {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if tally[k] > bestN {
			best, bestN = k, tally[k]
		}
	}
	return best
}
