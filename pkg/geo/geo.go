// Package geo defines the geographic hierarchy used throughout the policy data
// infrastructure. It covers US Census geographic levels from Nation down to
// Ward, along with supporting types and navigation helpers.
package geo

import (
	"fmt"
)

// Level represents a geographic level in the US Census hierarchy.
type Level string

const (
	Nation     Level = "nation"
	State      Level = "state"
	County     Level = "county"
	Tract      Level = "tract"
	BlockGroup Level = "block_group"
	Ward       Level = "ward"
)

// orderedLevels is the canonical top-down ordering of geographic levels.
var orderedLevels = []Level{
	Nation,
	State,
	County,
	Tract,
	BlockGroup,
	Ward,
}

// levelIndex maps each Level to its position in the hierarchy.
var levelIndex = func() map[Level]int {
	m := make(map[Level]int, len(orderedLevels))
	for i, l := range orderedLevels {
		m[l] = i
	}
	return m
}()

// Geography holds metadata for a single geographic unit.
type Geography struct {
	GEOID        string  `json:"geoid"`
	Level        Level   `json:"level"`
	ParentGEOID  string  `json:"parent_geoid,omitempty"`
	Name         string  `json:"name"`
	StateFIPS    string  `json:"state_fips,omitempty"`
	CountyFIPS   string  `json:"county_fips,omitempty"`
	Population   int     `json:"population"`
	LandAreaM2   float64 `json:"land_area_m2"`
	Lat          float64 `json:"lat"`
	Lon          float64 `json:"lon"`
}

// LevelFromString parses a level name (case-sensitive) and returns the
// corresponding Level constant. Returns an error if the name is not
// recognised.
func LevelFromString(s string) (Level, error) {
	l := Level(s)
	if _, ok := levelIndex[l]; ok {
		return l, nil
	}
	return "", fmt.Errorf("geo: unknown level %q", s)
}

// Levels returns all levels in top-down hierarchical order.
func Levels() []Level {
	out := make([]Level, len(orderedLevels))
	copy(out, orderedLevels)
	return out
}

// ParentLevel returns the level one step above l in the hierarchy. The second
// return value is false when l is already the top-most level (Nation).
func ParentLevel(l Level) (Level, bool) {
	idx, ok := levelIndex[l]
	if !ok || idx == 0 {
		return "", false
	}
	return orderedLevels[idx-1], true
}

// ChildLevel returns the level one step below l in the hierarchy. The second
// return value is false when l is already the bottom-most level (Ward).
func ChildLevel(l Level) (Level, bool) {
	idx, ok := levelIndex[l]
	if !ok || idx == len(orderedLevels)-1 {
		return "", false
	}
	return orderedLevels[idx+1], true
}
