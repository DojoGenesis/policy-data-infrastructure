package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// atlasRecord is the per-row JSON structure used in atlas-format data files.
// It always has a "geoid" key; all other keys are indicator variable IDs with
// their numeric values. For example:
//
//	{"geoid": "55025000100", "median_hh_income": 52000, "pct_poverty": 14.3}
type atlasRecord map[string]interface{}

// atlasFileSpec maps a filename pattern to the vintage string to use when
// inserting the indicators. Files not matching any pattern use "latest".
var atlasFileVintages = map[string]string{
	"acs5": "acs5_2022",
	"acs1": "acs1_2022",
}

// ImportAtlasJSON reads all JSON files in dataDir that follow the atlas format
// (a JSON array of objects, each with a "geoid" key and one or more indicator
// columns), converts them to Indicator records, and bulk-inserts them via
// PutIndicators.
//
// Each non-geoid key in the record becomes a VariableID. Numeric values (both
// JSON numbers and numeric strings) are stored as float64; non-numeric values
// are stored in RawValue with Value = nil.
//
// The function processes all *.json files in dataDir, skipping files that
// cannot be parsed. A slice of per-file errors is returned after all files
// have been attempted.
func ImportAtlasJSON(ctx context.Context, s *PostgresStore, dataDir string) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("store: ImportAtlasJSON: read dir %q: %w", dataDir, err)
	}

	var fileErrors []string

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		filePath := filepath.Join(dataDir, entry.Name())
		if err := importAtlasFile(ctx, s, filePath, entry.Name()); err != nil {
			fileErrors = append(fileErrors, fmt.Sprintf("%s: %v", entry.Name(), err))
		}
	}

	if len(fileErrors) > 0 {
		return fmt.Errorf("store: ImportAtlasJSON: %d file(s) failed:\n%s",
			len(fileErrors), strings.Join(fileErrors, "\n"))
	}
	return nil
}

// importAtlasFile reads a single atlas JSON file and upserts its contents.
func importAtlasFile(ctx context.Context, s *PostgresStore, filePath, fileName string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}

	var records []atlasRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return fmt.Errorf("parse: %w", err)
	}

	vintage := resolveVintage(fileName)

	var indicators []Indicator
	for _, rec := range records {
		geoidRaw, ok := rec["geoid"]
		if !ok {
			continue
		}
		geoid, ok := geoidRaw.(string)
		if !ok {
			continue
		}
		if geoid == "" {
			continue
		}

		for key, val := range rec {
			if key == "geoid" {
				continue
			}

			ind := Indicator{
				GEOID:      geoid,
				VariableID: key,
				Vintage:    vintage,
			}

			switch v := val.(type) {
			case float64:
				ind.Value = &v
			case string:
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					ind.Value = &f
				} else {
					ind.RawValue = v
				}
			default:
				// Non-numeric types are serialised as raw JSON strings.
				if b, err := json.Marshal(v); err == nil {
					ind.RawValue = string(b)
				}
			}

			indicators = append(indicators, ind)
		}
	}

	if len(indicators) == 0 {
		return nil
	}

	if err := s.PutIndicators(ctx, indicators); err != nil {
		return fmt.Errorf("upsert: %w", err)
	}
	return nil
}

// resolveVintage maps a filename to a vintage string by scanning for known
// atlas file vintage tokens. Falls back to "latest" when no pattern matches.
func resolveVintage(fileName string) string {
	lower := strings.ToLower(fileName)
	for token, vintage := range atlasFileVintages {
		if strings.Contains(lower, token) {
			return vintage
		}
	}
	return "latest"
}
