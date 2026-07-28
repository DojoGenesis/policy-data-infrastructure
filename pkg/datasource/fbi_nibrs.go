package datasource

// FBINIBRSSource fetches county-level crime statistics from the FBI
// National Incident-Based Reporting System (NIBRS) via the CDE API.
//
// API endpoint:
//
//	https://api.usa.gov/crime/fbi/cde/
//
// The API is behind the federal api.data.gov gateway and requires an
// API key. Sign up at https://api.data.gov/signup/ and set the
// FBI_CDE_API_KEY environment variable (or pass via config).
//
// Variables produced:
//
//	fbi_violent_crime_rate  — Violent crime rate per 100,000 population
//	fbi_property_crime_rate — Property crime rate per 100,000 population
//
// Geographic level: county (5-digit FIPS GEOID).
// Vintage: annual (most recent available year, typically the prior calendar year).
//
// Null handling: the FBI may suppress data for small populations (fewer than
// 10 reported incidents). Suppressed values appear as -1, null, or empty strings
// and are stored as nil *float64 values in store.Indicator records.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

const fbiNIBRSRateDelay = 2 * time.Second // ~30 req/min with margin

const (
	fbiNIBRSBaseURL = "https://api.usa.gov/crime/fbi/cde"
)

// fbiNIBRSMeasures maps FBI NIBRS variable names to our canonical variable IDs.
var fbiNIBRSMeasures = map[string]VariableDef{
	"violent_crime_rate": {
		ID:          "fbi_violent_crime_rate",
		Name:        "Violent Crime Rate",
		Description: "Estimated violent crime rate per 100,000 population (NIBRS estimation)",
		Unit:        "rate_per_100k",
		Direction:   "lower_better",
	},
	"property_crime_rate": {
		ID:          "fbi_property_crime_rate",
		Name:        "Property Crime Rate",
		Description: "Estimated property crime rate per 100,000 population (NIBRS estimation)",
		Unit:        "rate_per_100k",
		Direction:   "lower_better",
	},
}

// fbiNIBRSCountyRow is a single county-level NIBRS estimation record.
// Field names are derived from the CDE API response schema.
// TODO: Verify field names against actual API response once API key is available.
type fbiNIBRSCountyRow struct {
	CountyFIPS        string `json:"county_fips"`
	CountyName        string `json:"county_name"`
	StateAbbr         string `json:"state_abbr"`
	Year              int    `json:"data_year"`
	ViolentCrimeRate  string `json:"violent_crime_rate"`
	PropertyCrimeRate string `json:"property_crime_rate"`
	Population        string `json:"population"`
}

// FBINIBRSConfig configures a FBINIBRSSource.
type FBINIBRSConfig struct {
	// Year is the data year, e.g. 2023. When 0, the most recent year is used.
	Year int
	// APIKey is the api.data.gov API key. Falls back to FBI_CDE_API_KEY env var.
	APIKey string
	// HTTPClient is used for all requests. Defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// fbiNIBRSSource implements DataSource for FBI NIBRS county crime data.
type fbiNIBRSSource struct {
	cfg     FBINIBRSConfig
	vintage string
}

// NewFBINIBRSSource creates a FBINIBRSSource from cfg.
func NewFBINIBRSSource(cfg FBINIBRSConfig) *fbiNIBRSSource {
	if cfg.APIKey == "" {
		cfg.APIKey = os.Getenv("FBI_CDE_API_KEY")
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	vintage := "FBI-NIBRS"
	if cfg.Year > 0 {
		vintage = fmt.Sprintf("FBI-NIBRS-%d", cfg.Year)
	}
	return &fbiNIBRSSource{cfg: cfg, vintage: vintage}
}

func (s *fbiNIBRSSource) Name() string     { return "fbi-nibrs" }
func (s *fbiNIBRSSource) Category() string { return "crime" }
func (s *fbiNIBRSSource) Vintage() string  { return s.vintage }

func (s *fbiNIBRSSource) Schema() []VariableDef {
	out := make([]VariableDef, 0, len(fbiNIBRSMeasures))
	for _, def := range fbiNIBRSMeasures {
		out = append(out, def)
	}
	return out
}

// FetchCounty returns crime indicators for a single county.
// The GEOID is the 5-digit county FIPS code (stateFIPS + countyFIPS).
//
// TODO: Wire the actual HTTP fetch to the CDE API once the API key is available.
// The planned flow is:
//  1. Fetch all county data from /nibrs-estimations/counties/{year}
//  2. Filter to the requested county FIPS
//  3. Convert to store.Indicator records
func (s *fbiNIBRSSource) FetchCounty(
	ctx context.Context,
	stateFIPS, countyFIPS string,
) ([]store.Indicator, error) {
	if len(stateFIPS) != 2 {
		return nil, fmt.Errorf("fbi-nibrs: invalid state FIPS %q (want 2 digits)", stateFIPS)
	}
	if len(countyFIPS) != 3 {
		return nil, fmt.Errorf("fbi-nibrs: invalid county FIPS %q (want 3 digits)", countyFIPS)
	}

	geoid := sanitizeFIPS(stateFIPS + countyFIPS)
	if len(geoid) != 5 {
		return nil, fmt.Errorf("fbi-nibrs: invalid GEOID %q", stateFIPS+countyFIPS)
	}

	// TODO: Wire the actual HTTP fetch once API key is configured.
	// For now, return an error explaining that the API requires a key.
	if s.cfg.APIKey == "" {
		return nil, fmt.Errorf(
			"fbi-nibrs: FBI_CDE_API_KEY not set — the CDE API requires an api.data.gov key; "+
				"sign up at https://api.data.gov/signup/ and set the FBI_CDE_API_KEY env var or "+
				"pass it via FBINIBRSConfig.APIKey",
		)
	}

	// TODO: Implement the fetch:
	//   rows, err := s.fetchAllCounties(ctx)
	//   if err != nil { return nil, err }
	//   indicators := s.rowsToIndicators(rows, geoid)
	//   return indicators, nil

	return nil, fmt.Errorf(
		"fbi-nibrs: API fetch not yet wired — see TODO markers in fbi_nibrs.go",
	)
}

// FetchState returns crime indicators for all counties in the given state.
//
// TODO: Wire the actual HTTP fetch as described in FetchCounty.
func (s *fbiNIBRSSource) FetchState(
	ctx context.Context,
	stateFIPS string,
) ([]store.Indicator, error) {
	if len(stateFIPS) != 2 {
		return nil, fmt.Errorf("fbi-nibrs: invalid state FIPS %q (want 2 digits)", stateFIPS)
	}

	if s.cfg.APIKey == "" {
		return nil, fmt.Errorf(
			"fbi-nibrs: FBI_CDE_API_KEY not set — the CDE API requires an api.data.gov key; "+
				"sign up at https://api.data.gov/signup/ and set the FBI_CDE_API_KEY env var or "+
				"pass it via FBINIBRSConfig.APIKey",
		)
	}

	// TODO: Implement the fetch:
	//   rows, err := s.fetchAllCounties(ctx)
	//   if err != nil { return nil, err }
	//   var indicators []store.Indicator
	//   for _, row := range rows {
	//       if strings.HasPrefix(row.CountyFIPS, stateFIPS) {
	//           indicators = append(indicators, s.rowToIndicators(row)...)
	//       }
	//   }
	//   return indicators, nil

	return nil, fmt.Errorf(
		"fbi-nibrs: API fetch not yet wired — see TODO markers in fbi_nibrs.go",
	)
}

// fetchAllCounties retrieves all county-level NIBRS estimation data for the
// configured year.
//
// TODO: Wire the actual HTTP request to the CDE API.
// The planned endpoint is /nibrs-estimations/counties/{year}.
// The API may paginate results — handle pagination with $limit/$offset params.
func (s *fbiNIBRSSource) fetchAllCounties(ctx context.Context) ([]fbiNIBRSCountyRow, error) {
	year := s.cfg.Year
	if year == 0 {
		// Default to most recent year available.
		// TODO: Query the API for the most recent year or use a sensible default.
		year = time.Now().Year() - 1
	}

	endpoint := fmt.Sprintf("%s/nibrs-estimations/counties/%d", fbiNIBRSBaseURL, year)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("fbi-nibrs: build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if s.cfg.APIKey != "" {
		req.Header.Set("X-Api-Key", s.cfg.APIKey)
	}

	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fbi-nibrs: http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf(
			"fbi-nibrs: api returned status %d: %s",
			resp.StatusCode,
			strings.TrimSpace(string(body)),
		)
	}

	// TODO: Determine the actual response envelope structure.
	// The CDE API may wrap results in {"data": [...], "pagination": {...}}
	// or return a flat array. Adjust parsing accordingly.
	var rows []fbiNIBRSCountyRow
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		// Try envelope: { "data": [...] }
		resp.Body.Close()
		// Re-fetch is not possible here; for now, return the decode error.
		return nil, fmt.Errorf("fbi-nibrs: decode json: %w", err)
	}
	return rows, nil
}

// rowsToIndicators converts raw CDE API rows to store.Indicator records,
// filtering to a specific county GEOID (5-digit FIPS).
func (s *fbiNIBRSSource) rowsToIndicators(
	rows []fbiNIBRSCountyRow,
	geoid string,
) []store.Indicator {
	var out []store.Indicator
	for _, row := range rows {
		if row.CountyFIPS != geoid {
			continue
		}
		out = append(out, s.rowToIndicators(row)...)
	}
	return out
}

// rowToIndicators converts a single CDE API row to store.Indicator records.
func (s *fbiNIBRSSource) rowToIndicators(row fbiNIBRSCountyRow) []store.Indicator {
	geoid := strings.TrimSpace(row.CountyFIPS)
	if len(geoid) != 5 {
		return nil
	}

	parseVal := func(raw string) (*float64, string) {
		raw = strings.TrimSpace(raw)
		if raw == "" || raw == "-1" || raw == "null" {
			return nil, raw
		}
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil || f < 0 {
			return nil, raw
		}
		return &f, raw
	}

	violentVal, violentRaw := parseVal(row.ViolentCrimeRate)
	propertyVal, propertyRaw := parseVal(row.PropertyCrimeRate)

	return []store.Indicator{
		{
			GEOID:      geoid,
			VariableID: "fbi_violent_crime_rate",
			Vintage:    s.vintage,
			Value:      violentVal,
			RawValue:   violentRaw,
		},
		{
			GEOID:      geoid,
			VariableID: "fbi_property_crime_rate",
			Vintage:    s.vintage,
			Value:      propertyVal,
			RawValue:   propertyRaw,
		},
	}
}

// ensure we don't import url unused
var _ = url.Values{}
