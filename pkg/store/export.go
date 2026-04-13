package store

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/jackc/pgx/v5/pgxpool"
)

// geoJSONFeature is a minimal GeoJSON Feature object used during export.
type geoJSONFeature struct {
	Type       string                 `json:"type"`
	Geometry   json.RawMessage        `json:"geometry"`
	Properties map[string]interface{} `json:"properties"`
}

// geoJSONFeatureCollection is a minimal GeoJSON FeatureCollection used during
// export.
type geoJSONFeatureCollection struct {
	Type     string           `json:"type"`
	Features []geoJSONFeature `json:"features"`
}

// ExportGeoJSON returns a GeoJSON FeatureCollection containing all geographies
// at the given level (optionally filtered by stateFIPS). Each feature includes
// the geography metadata as properties and the PostGIS boundary geometry as the
// GeoJSON geometry field.
func ExportGeoJSON(ctx context.Context, pool *pgxpool.Pool, level geo.Level, stateFIPS string) ([]byte, error) {
	args := []interface{}{string(level)}
	stateFIPSClause := ""
	if stateFIPS != "" {
		stateFIPSClause = "AND state_fips = $2"
		args = append(args, stateFIPS)
	}

	sql := fmt.Sprintf(`
SELECT
    geoid,
    level,
    COALESCE(parent_geoid,''),
    name,
    COALESCE(state_fips,''),
    COALESCE(county_fips,''),
    COALESCE(population, 0),
    COALESCE(land_area_m2, 0),
    COALESCE(ST_AsGeoJSON(boundary), 'null')
FROM geographies
WHERE level = $1::geo_level
  %s
ORDER BY geoid`, stateFIPSClause)

	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("store: ExportGeoJSON query: %w", err)
	}
	defer rows.Close()

	fc := geoJSONFeatureCollection{Type: "FeatureCollection"}
	for rows.Next() {
		var geoid, lvl, parentGEOID, name, stateFIPSVal, countyFIPS string
		var population int
		var landArea float64
		var boundaryJSON string

		if err := rows.Scan(
			&geoid, &lvl, &parentGEOID, &name,
			&stateFIPSVal, &countyFIPS,
			&population, &landArea, &boundaryJSON,
		); err != nil {
			return nil, fmt.Errorf("store: ExportGeoJSON scan: %w", err)
		}

		props := map[string]interface{}{
			"geoid":        geoid,
			"level":        lvl,
			"parent_geoid": parentGEOID,
			"name":         name,
			"state_fips":   stateFIPSVal,
			"county_fips":  countyFIPS,
			"population":   population,
			"land_area_m2": landArea,
		}

		feature := geoJSONFeature{
			Type:       "Feature",
			Geometry:   json.RawMessage(boundaryJSON),
			Properties: props,
		}
		fc.Features = append(fc.Features, feature)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: ExportGeoJSON rows: %w", err)
	}

	if fc.Features == nil {
		fc.Features = []geoJSONFeature{}
	}

	out, err := json.Marshal(fc)
	if err != nil {
		return nil, fmt.Errorf("store: ExportGeoJSON marshal: %w", err)
	}
	return out, nil
}

// ExportCSV writes indicator data matching the given IndicatorQuery to w in
// CSV format. The header row is: geoid,variable_id,vintage,value,margin_of_error,raw_value.
// NULL values are written as empty strings.
func ExportCSV(ctx context.Context, pool *pgxpool.Pool, q IndicatorQuery, w io.Writer) error {
	args := []interface{}{}
	idx := 1
	var where []string

	table := "indicators"
	if q.LatestOnly {
		table = "indicators_latest"
	}

	if len(q.GEOIDs) > 0 {
		where = append(where, fmt.Sprintf("geoid = ANY($%d)", idx))
		args = append(args, q.GEOIDs)
		idx++
	}
	if len(q.VariableIDs) > 0 {
		where = append(where, fmt.Sprintf("variable_id = ANY($%d)", idx))
		args = append(args, q.VariableIDs)
		idx++
	}
	if q.Vintage != "" {
		where = append(where, fmt.Sprintf("vintage = $%d", idx))
		args = append(args, q.Vintage)
		idx++
	}
	_ = idx

	whereClause := ""
	if len(where) > 0 {
		whereClause = "WHERE " + joinStrings(where, " AND ")
	}

	rawValueExpr := "COALESCE(raw_value, '')"
	if q.LatestOnly {
		rawValueExpr = "''" // indicators_latest has no raw_value column
	}
	sql := fmt.Sprintf(`
SELECT geoid, variable_id, vintage, value, margin_of_error, %s
FROM %s
%s
ORDER BY geoid, variable_id, vintage`, rawValueExpr, table, whereClause)

	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("store: ExportCSV query: %w", err)
	}
	defer rows.Close()

	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"geoid", "variable_id", "vintage", "value", "margin_of_error", "raw_value"}); err != nil {
		return fmt.Errorf("store: ExportCSV write header: %w", err)
	}

	for rows.Next() {
		var geoid, variableID, vintage, rawValue string
		var value, moe *float64

		if err := rows.Scan(&geoid, &variableID, &vintage, &value, &moe, &rawValue); err != nil {
			return fmt.Errorf("store: ExportCSV scan: %w", err)
		}

		valStr := ""
		if value != nil {
			valStr = strconv.FormatFloat(*value, 'f', -1, 64)
		}
		moeStr := ""
		if moe != nil {
			moeStr = strconv.FormatFloat(*moe, 'f', -1, 64)
		}

		if err := cw.Write([]string{geoid, variableID, vintage, valStr, moeStr, rawValue}); err != nil {
			return fmt.Errorf("store: ExportCSV write row: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: ExportCSV rows: %w", err)
	}

	cw.Flush()
	return cw.Error()
}

// joinStrings concatenates a slice of strings with sep. It is used instead of
// strings.Join to avoid importing strings in a file where it is not otherwise
// needed.
func joinStrings(parts []string, sep string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += sep
		}
		result += p
	}
	return result
}
