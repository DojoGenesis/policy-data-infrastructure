package store

import (
	"strings"
	"testing"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
)

// These tests exercise geoWhereClause directly. It is the single point where
// the geography lifecycle predicate (migration 013: retired_at) is applied, and
// it is shared by QueryGeographies and CountGeographies so a listing and its
// pagination total can never disagree about which rows match. No database is
// required, so the coverage survives `go test -short`.

func TestGeoWhereClause_ExcludesRetiredByDefault(t *testing.T) {
	clause, args := geoWhereClause(GeoQuery{})

	if !strings.Contains(clause, "retired_at IS NULL") {
		t.Errorf("zero-value GeoQuery must filter retired rows; got %q", clause)
	}
	if strings.Contains(clause, "IS NOT NULL") {
		t.Errorf("zero-value GeoQuery must not select retired rows; got %q", clause)
	}
	if len(args) != 0 {
		t.Errorf("expected no positional args, got %v", args)
	}
}

func TestGeoWhereClause_IncludeRetired(t *testing.T) {
	clause, _ := geoWhereClause(GeoQuery{IncludeRetired: true})

	if strings.Contains(clause, "retired_at") {
		t.Errorf("IncludeRetired must drop the lifecycle predicate entirely; got %q", clause)
	}
}

func TestGeoWhereClause_RetiredOnly(t *testing.T) {
	clause, _ := geoWhereClause(GeoQuery{RetiredOnly: true})

	if !strings.Contains(clause, "retired_at IS NOT NULL") {
		t.Errorf("RetiredOnly must select only retired rows; got %q", clause)
	}
}

// RetiredOnly is documented to take precedence over IncludeRetired: asking for
// the historical set and also asking to include it is not a contradiction.
func TestGeoWhereClause_RetiredOnlyBeatsIncludeRetired(t *testing.T) {
	clause, _ := geoWhereClause(GeoQuery{RetiredOnly: true, IncludeRetired: true})

	if !strings.Contains(clause, "retired_at IS NOT NULL") {
		t.Errorf("RetiredOnly must win over IncludeRetired; got %q", clause)
	}
}

// The lifecycle predicate must compose with the other filters rather than
// replace them, and the positional placeholders must stay dense and in order —
// the predicate itself contributes no argument.
func TestGeoWhereClause_ComposesWithFilters(t *testing.T) {
	q := GeoQuery{
		Level:      geo.Tract,
		StateFIPS:  "55",
		CountyFIPS: "025",
		NameSearch: "Census Tract",
		Limit:      10,
		Offset:     20,
	}
	clause, args := geoWhereClause(q)

	for _, want := range []string{
		"level = $1::geo_level",
		"state_fips = $2",
		"county_fips = $3",
		"name ILIKE '%' || $4 || '%'",
		"retired_at IS NULL",
	} {
		if !strings.Contains(clause, want) {
			t.Errorf("clause missing %q; got %q", want, clause)
		}
	}
	if !strings.HasPrefix(clause, "WHERE ") {
		t.Errorf("clause must start with WHERE; got %q", clause)
	}
	if len(args) != 4 {
		t.Fatalf("expected 4 args (limit/offset are not args), got %d: %v", len(args), args)
	}
	if args[0] != string(geo.Tract) || args[1] != "55" || args[2] != "025" || args[3] != "Census Tract" {
		t.Errorf("args out of order: %v", args)
	}
}

// A query for the children of a county must not surface tracts that the 2020
// redistricting retired — this is the path the county profile uses.
func TestGeoWhereClause_ParentLookupExcludesRetired(t *testing.T) {
	clause, args := geoWhereClause(GeoQuery{Level: geo.Tract, ParentGEOID: "55025"})

	if !strings.Contains(clause, "parent_geoid = $2") {
		t.Errorf("clause missing parent filter; got %q", clause)
	}
	if !strings.Contains(clause, "retired_at IS NULL") {
		t.Errorf("child lookups must exclude retired tracts; got %q", clause)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %v", args)
	}
}
