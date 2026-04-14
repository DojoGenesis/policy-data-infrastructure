package datasource

// ACSSource implements DataSource for the Census Bureau's American Community
// Survey 5-Year Estimates.
//
// API endpoint pattern:
//
//	https://api.census.gov/data/{year}/acs/acs5?get={variables}&for=tract:*&in=state:{fips}+county:{fips}&key={key}
//
// The JSON response has the header row as the first element, e.g.:
//
//	[["B19013_001E","state","county","tract"],
//	 ["52000","55","025","000100"],
//	 ["38750","55","025","000201"],
//	 ...]
//
// The GEOID for a tract is constructed as state+county+tract
// (e.g. "55" + "025" + "000100" = "55025000100").
//
// Variables fetched:
//
//	B19013_001E — Median household income
//	B19013_001M — Margin of error: median household income
//	B03002_001E — Total population (race/ethnicity table)
//	B03002_003E — White alone, not Hispanic or Latino
//	B03002_004E — Black or African American alone
//	B03002_012E — Hispanic or Latino
//	S1701_C03_001E — Poverty rate (percent)
//	B01001_001E — Total population
//	S2701_C05_001E — Uninsured rate (percent)
//	B25106_001E — Total housing units (cost burden table)
//	B25106_006E — Owner-occupied, paying >30% of income (cost burden)
//	B25106_010E — Owner-occupied, paying >30% (alt bracket)
//	B25106_014E — Owner-occupied, paying >30% (alt bracket)
//	B25106_018E — Owner-occupied, paying >30% (alt bracket)
//	B25106_022E — Owner-occupied, paying >30% (alt bracket)
//	B25106_024E — Renter-occupied, paying >30% of income
//	B25106_028E — Renter-occupied, paying >30% (alt bracket)
//	B25106_032E — Renter-occupied, paying >30% (alt bracket)
//	B25106_036E — Renter-occupied, paying >30% (alt bracket)
//	B25106_040E — Renter-occupied, paying >30% (alt bracket)

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// acsVariable pairs a Census variable code with its human-readable metadata.
type acsVariable struct {
	code      string // e.g. "B19013_001E"
	moeCode   string // margin-of-error companion, e.g. "B19013_001M" (empty if none)
	varDef    VariableDef
}

// acsVariables is the ordered list of ACS variables fetched by ACSSource.
// MOE variables are requested alongside their estimate counterparts but
// stored as a separate Indicator with the same VariableID suffixed "_moe".
var acsVariables = []acsVariable{
	{
		code:    "B19013_001E",
		moeCode: "B19013_001M",
		varDef: VariableDef{
			ID:          "median_household_income",
			Name:        "Median Household Income",
			Description: "Median household income in the past 12 months (in inflation-adjusted dollars)",
			Unit:        "dollars",
			Direction:   "higher_better",
			ACSTable:    "B19013_001E",
		},
	},
	{
		code: "B03002_001E",
		varDef: VariableDef{
			ID:          "total_population_race",
			Name:        "Total Population (Race Table)",
			Description: "Total population from the race/ethnicity table B03002",
			Unit:        "count",
			Direction:   "neutral",
			ACSTable:    "B03002_001E",
		},
	},
	{
		code: "B03002_003E",
		varDef: VariableDef{
			ID:          "pop_white_non_hispanic",
			Name:        "White Alone, Not Hispanic or Latino",
			Description: "Population identifying as White alone and not Hispanic or Latino",
			Unit:        "count",
			Direction:   "neutral",
			ACSTable:    "B03002_003E",
		},
	},
	{
		code: "B03002_004E",
		varDef: VariableDef{
			ID:          "pop_black",
			Name:        "Black or African American Alone",
			Description: "Population identifying as Black or African American alone",
			Unit:        "count",
			Direction:   "neutral",
			ACSTable:    "B03002_004E",
		},
	},
	{
		code: "B03002_012E",
		varDef: VariableDef{
			ID:          "pop_hispanic_latino",
			Name:        "Hispanic or Latino",
			Description: "Population identifying as Hispanic or Latino of any race",
			Unit:        "count",
			Direction:   "neutral",
			ACSTable:    "B03002_012E",
		},
	},
	{
		code: "S1701_C03_001E",
		varDef: VariableDef{
			ID:          "poverty_rate",
			Name:        "Poverty Rate",
			Description: "Percent of population for whom poverty status is determined who are below the poverty level",
			Unit:        "percent",
			Direction:   "lower_better",
			ACSTable:    "S1701_C03_001E",
		},
	},
	{
		code: "B01001_001E",
		varDef: VariableDef{
			ID:          "total_population",
			Name:        "Total Population",
			Description: "Total population (sex by age table B01001)",
			Unit:        "count",
			Direction:   "neutral",
			ACSTable:    "B01001_001E",
		},
	},
	{
		code: "S2701_C05_001E",
		varDef: VariableDef{
			ID:          "uninsured_rate",
			Name:        "Uninsured Rate",
			Description: "Percent of civilian noninstitutionalized population without health insurance coverage",
			Unit:        "percent",
			Direction:   "lower_better",
			ACSTable:    "S2701_C05_001E",
		},
	},
	{
		code: "B25106_001E",
		varDef: VariableDef{
			ID:          "housing_units_cost_burden",
			Name:        "Total Housing Units (Cost Burden Table)",
			Description: "Total occupied housing units from the housing cost burden table B25106",
			Unit:        "count",
			Direction:   "neutral",
			ACSTable:    "B25106_001E",
		},
	},
	{
		code: "B25106_006E",
		varDef: VariableDef{
			ID:          "owner_cost_burden_30pct_1",
			Name:        "Owner Cost Burden >30% (Bracket 1)",
			Description: "Owner-occupied units paying more than 30% of income on housing costs (income bracket 1)",
			Unit:        "count",
			Direction:   "lower_better",
			ACSTable:    "B25106_006E",
		},
	},
	{
		code: "B25106_010E",
		varDef: VariableDef{
			ID:          "owner_cost_burden_30pct_2",
			Name:        "Owner Cost Burden >30% (Bracket 2)",
			Description: "Owner-occupied units paying more than 30% of income on housing costs (income bracket 2)",
			Unit:        "count",
			Direction:   "lower_better",
			ACSTable:    "B25106_010E",
		},
	},
	{
		code: "B25106_014E",
		varDef: VariableDef{
			ID:          "owner_cost_burden_30pct_3",
			Name:        "Owner Cost Burden >30% (Bracket 3)",
			Description: "Owner-occupied units paying more than 30% of income on housing costs (income bracket 3)",
			Unit:        "count",
			Direction:   "lower_better",
			ACSTable:    "B25106_014E",
		},
	},
	{
		code: "B25106_018E",
		varDef: VariableDef{
			ID:          "owner_cost_burden_30pct_4",
			Name:        "Owner Cost Burden >30% (Bracket 4)",
			Description: "Owner-occupied units paying more than 30% of income on housing costs (income bracket 4)",
			Unit:        "count",
			Direction:   "lower_better",
			ACSTable:    "B25106_018E",
		},
	},
	{
		code: "B25106_022E",
		varDef: VariableDef{
			ID:          "owner_cost_burden_30pct_5",
			Name:        "Owner Cost Burden >30% (Bracket 5)",
			Description: "Owner-occupied units paying more than 30% of income on housing costs (income bracket 5)",
			Unit:        "count",
			Direction:   "lower_better",
			ACSTable:    "B25106_022E",
		},
	},
	{
		code: "B25106_024E",
		varDef: VariableDef{
			ID:          "renter_cost_burden_30pct_1",
			Name:        "Renter Cost Burden >30% (Bracket 1)",
			Description: "Renter-occupied units paying more than 30% of income on housing costs (income bracket 1)",
			Unit:        "count",
			Direction:   "lower_better",
			ACSTable:    "B25106_024E",
		},
	},
	{
		code: "B25106_028E",
		varDef: VariableDef{
			ID:          "renter_cost_burden_30pct_2",
			Name:        "Renter Cost Burden >30% (Bracket 2)",
			Description: "Renter-occupied units paying more than 30% of income on housing costs (income bracket 2)",
			Unit:        "count",
			Direction:   "lower_better",
			ACSTable:    "B25106_028E",
		},
	},
	{
		code: "B25106_032E",
		varDef: VariableDef{
			ID:          "renter_cost_burden_30pct_3",
			Name:        "Renter Cost Burden >30% (Bracket 3)",
			Description: "Renter-occupied units paying more than 30% of income on housing costs (income bracket 3)",
			Unit:        "count",
			Direction:   "lower_better",
			ACSTable:    "B25106_032E",
		},
	},
	{
		code: "B25106_036E",
		varDef: VariableDef{
			ID:          "renter_cost_burden_30pct_4",
			Name:        "Renter Cost Burden >30% (Bracket 4)",
			Description: "Renter-occupied units paying more than 30% of income on housing costs (income bracket 4)",
			Unit:        "count",
			Direction:   "lower_better",
			ACSTable:    "B25106_036E",
		},
	},
	{
		code: "B25106_040E",
		varDef: VariableDef{
			ID:          "renter_cost_burden_30pct_5",
			Name:        "Renter Cost Burden >30% (Bracket 5)",
			Description: "Renter-occupied units paying more than 30% of income on housing costs (income bracket 5)",
			Unit:        "count",
			Direction:   "lower_better",
			ACSTable:    "B25106_040E",
		},
	},
}

// suppressedValues is the set of raw strings the Census API uses to indicate
// missing, suppressed, or unavailable data. Any of these becomes nil Value.
var suppressedValues = map[string]bool{
	"*":    true,
	"**":   true,
	"-":    true,
	"-1":   true,
	"-2":   true,
	"-3":   true,
	"-4":   true,
	"-5":   true,
	"-6":   true,
	"-7":   true,
	"-8":   true,
	"-9":   true,
	"null": true,
	"N":    true,
	"(X)":  true,
}

// ACSConfig configures an ACSSource.
type ACSConfig struct {
	// Year is the ACS vintage year (e.g. 2024 for the 2020–2024 5-year estimate).
	Year int
	// APIKey is the Census Bureau API key. If empty, the CENSUS_API_KEY
	// environment variable is used. Without a key the rate limit is 45 req/min.
	APIKey string
	// RateLimitPerMin controls outbound request pacing. 0 means use the
	// appropriate default: 45 without a key, 500 with a key.
	RateLimitPerMin int
	// HTTPClient is used for all outbound requests. If nil, http.DefaultClient
	// is used. Tests inject a mock transport here.
	HTTPClient *http.Client
}

// acsSource implements DataSource for ACS 5-Year data.
type acsSource struct {
	cfg     ACSConfig
	vintage string
	ticker  <-chan time.Time // rate-limit ticker; nil = no pacing
}

// NewACSSource creates a new ACSSource from cfg. The APIKey is resolved from
// the CENSUS_API_KEY environment variable when cfg.APIKey is empty.
func NewACSSource(cfg ACSConfig) *acsSource {
	if cfg.APIKey == "" {
		cfg.APIKey = os.Getenv("CENSUS_API_KEY")
	}
	if cfg.RateLimitPerMin == 0 {
		if cfg.APIKey != "" {
			cfg.RateLimitPerMin = 500
		} else {
			cfg.RateLimitPerMin = 45
		}
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	vintage := fmt.Sprintf("ACS-%d-5yr", cfg.Year)
	s := &acsSource{cfg: cfg, vintage: vintage}
	// Set up a rate-limit ticker: one token per interval.
	interval := time.Minute / time.Duration(cfg.RateLimitPerMin)
	ch := make(chan time.Time, 1)
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for tick := range t.C {
			ch <- tick
		}
	}()
	s.ticker = ch
	return s
}

func (s *acsSource) Name() string     { return "acs-5yr" }
func (s *acsSource) Category() string { return "demographic" }
func (s *acsSource) Vintage() string  { return s.vintage }

func (s *acsSource) Schema() []VariableDef {
	defs := make([]VariableDef, len(acsVariables))
	for i, v := range acsVariables {
		defs[i] = v.varDef
	}
	return defs
}

// FetchCounty fetches all tract-level ACS indicators for a single county.
// stateFIPS is a 2-digit string; countyFIPS is a 3-digit string.
// Splits variables into detail (B-prefix) and subject (S-prefix) groups,
// issuing separate API calls since they use different Census dataset paths.
func (s *acsSource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	var detailVars, subjectVars []acsVariable
	for _, v := range acsVariables {
		if strings.HasPrefix(v.code, "S") {
			subjectVars = append(subjectVars, v)
		} else {
			detailVars = append(detailVars, v)
		}
	}

	var all []store.Indicator

	// Detail variables via /acs/acs5
	if len(detailVars) > 0 {
		dCodes := varCodes(detailVars)
		url := s.buildCountyURLWithVars(stateFIPS, countyFIPS, dCodes, false)
		indicators, err := s.fetch(ctx, url)
		if err != nil {
			return nil, fmt.Errorf("acs detail fetch: %w", err)
		}
		all = append(all, indicators...)
	}

	// Subject variables via /acs/acs5/subject (one call per variable)
	for _, sv := range subjectVars {
		codes := sv.code
		if sv.moeCode != "" {
			codes += "," + sv.moeCode
		}
		url := s.buildCountyURLWithVars(stateFIPS, countyFIPS, codes, true)
		indicators, err := s.fetch(ctx, url)
		if err != nil {
			fmt.Printf("  warning: subject table %s failed: %v\n", sv.code, err)
			continue
		}
		all = append(all, indicators...)
	}

	return all, nil
}

// FetchState fetches all county-level ACS indicators for an entire state.
// It splits variables into detail (B-prefix) and subject (S-prefix) tables,
// issuing separate API calls for each since they use different dataset paths.
func (s *acsSource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	// Split variables into detail and subject groups.
	var detailVars, subjectVars []acsVariable
	for _, v := range acsVariables {
		if strings.HasPrefix(v.code, "S") {
			subjectVars = append(subjectVars, v)
		} else {
			detailVars = append(detailVars, v)
		}
	}

	// Fetch detail variables (acs5 dataset).
	var all []store.Indicator
	if len(detailVars) > 0 {
		dCodes := varCodes(detailVars)
		url := s.buildStateURLWithVars(stateFIPS, dCodes, false)
		indicators, err := s.fetch(ctx, url)
		if err != nil {
			return nil, fmt.Errorf("acs detail fetch: %w", err)
		}
		all = append(all, indicators...)
	}

	// Fetch subject variables (acs5/subject dataset).
	for _, sv := range subjectVars {
		codes := sv.code
		if sv.moeCode != "" {
			codes += "," + sv.moeCode
		}
		url := s.buildStateURLWithVars(stateFIPS, codes, true)
		indicators, err := s.fetch(ctx, url)
		if err != nil {
			// Subject tables may not support state-wide tract calls; log and continue.
			fmt.Printf("  warning: subject table %s failed: %v\n", sv.code, err)
			continue
		}
		all = append(all, indicators...)
	}

	return all, nil
}

// buildStateURLWithVars constructs a Census API URL with specific variables.
func (s *acsSource) buildStateURLWithVars(stateFIPS, vars string, subject bool) string {
	dataset := "acs5"
	if subject {
		dataset = "acs5/subject"
	}
	base := fmt.Sprintf(
		"https://api.census.gov/data/%d/acs/%s?get=NAME,%s&for=county:*&in=state:%s",
		s.cfg.Year, dataset, vars, stateFIPS,
	)
	if s.cfg.APIKey != "" {
		base += "&key=" + s.cfg.APIKey
	}
	return base
}

// varCodes extracts comma-separated variable codes from a slice of acsVariable.
func varCodes(vars []acsVariable) string {
	var codes []string
	for _, v := range vars {
		codes = append(codes, v.code)
		if v.moeCode != "" {
			codes = append(codes, v.moeCode)
		}
	}
	return strings.Join(codes, ",")
}

// buildCountyURLWithVars constructs a Census API URL for tracts in a county
// with specific variables. subject=true uses /acs5/subject.
func (s *acsSource) buildCountyURLWithVars(stateFIPS, countyFIPS, vars string, subject bool) string {
	dataset := "acs5"
	if subject {
		dataset = "acs5/subject"
	}
	base := fmt.Sprintf(
		"https://api.census.gov/data/%d/acs/%s?get=NAME,%s&for=tract:*&in=state:%s+county:%s",
		s.cfg.Year, dataset, vars, stateFIPS, countyFIPS,
	)
	if s.cfg.APIKey != "" {
		base += "&key=" + s.cfg.APIKey
	}
	return base
}

// variableList builds the comma-separated list of variable codes to request,
// including MOE companions where defined.
func (s *acsSource) variableList() string {
	var codes []string
	for _, v := range acsVariables {
		codes = append(codes, v.code)
		if v.moeCode != "" {
			codes = append(codes, v.moeCode)
		}
	}
	return strings.Join(codes, ",")
}

// fetch executes a single rate-limited GET request and parses the Census JSON.
func (s *acsSource) fetch(ctx context.Context, url string) ([]store.Indicator, error) {
	// Rate-limit: wait for our slot before issuing the request.
	if s.ticker != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.ticker:
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("acs: build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("acs: http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("acs: api returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return s.parseResponse(resp.Body)
}

// parseResponse reads the Census API JSON from r and converts every data row
// into a slice of store.Indicator records (one per variable per tract).
//
// Census API response format:
//
//	[["VAR1","VAR2","state","county","tract"],
//	 ["val1","val2","55","025","000100"],
//	 ...]
//
// The last three columns are always "state", "county", "tract" (geography).
// All other columns correspond to the requested variable codes in order.
func (s *acsSource) parseResponse(r io.Reader) ([]store.Indicator, error) {
	var raw [][]string
	if err := json.NewDecoder(r).Decode(&raw); err != nil {
		return nil, fmt.Errorf("acs: decode json: %w", err)
	}
	if len(raw) < 2 {
		// Only a header row — no data.
		return nil, nil
	}

	headers := raw[0]
	// Build index: column name → position.
	colIdx := make(map[string]int, len(headers))
	for i, h := range headers {
		colIdx[h] = i
	}

	// Verify geography columns are present. Tract is optional (county-level fetches omit it).
	for _, required := range []string{"state", "county"} {
		if _, ok := colIdx[required]; !ok {
			return nil, fmt.Errorf("acs: response missing column %q", required)
		}
	}

	// Build a map from variable code to its definition for fast lookup.
	// Also record which variable codes have MOE companions and their ID.
	type varMeta struct {
		variableID string
		isMOE      bool
		estimateID string // for MOE columns, the base variable ID
	}
	codeMeta := make(map[string]varMeta, len(acsVariables)*2)
	for _, v := range acsVariables {
		codeMeta[v.code] = varMeta{variableID: v.varDef.ID, isMOE: false}
		if v.moeCode != "" {
			codeMeta[v.moeCode] = varMeta{variableID: v.varDef.ID + "_moe", isMOE: true, estimateID: v.varDef.ID}
		}
	}

	var indicators []store.Indicator
	geoState := colIdx["state"]
	geoCounty := colIdx["county"]

	for rowNum, row := range raw[1:] {
		if len(row) != len(headers) {
			return nil, fmt.Errorf("acs: row %d has %d columns, header has %d", rowNum+1, len(row), len(headers))
		}

		// Construct GEOID: 5-digit county or 11-digit tract depending on what's available.
		geoid := row[geoState] + row[geoCounty]
		if tractIdx, ok := colIdx["tract"]; ok {
			geoid += row[tractIdx]
		}

		// Emit one Indicator per requested variable column.
		for code, meta := range codeMeta {
			colPos, ok := colIdx[code]
			if !ok {
				// Variable not present in this response (may not be supported
				// for this geography level).
				continue
			}
			raw := row[colPos]
			val, moe := parseValue(raw)

			ind := store.Indicator{
				GEOID:      geoid,
				VariableID: meta.variableID,
				Vintage:    s.vintage,
				Value:      val,
				RawValue:   raw,
			}
			// MOE columns become MarginOfError on the estimate Indicator rather
			// than separate Indicators. We handle this in a second pass below.
			// For now emit all as separate records; the store upsert handles
			// them by (GEOID, VariableID, Vintage).
			_ = moe
			indicators = append(indicators, ind)
		}
	}

	// Second pass: merge MOE values onto their estimate Indicators.
	// Build a key→index map for the estimate Indicators already in the slice.
	type indKey struct{ geoid, variableID string }
	idx := make(map[indKey]int, len(indicators))
	for i, ind := range indicators {
		idx[indKey{ind.GEOID, ind.VariableID}] = i
	}

	// Re-parse MOE columns and attach to estimate Indicators.
	for _, row := range raw[1:] {
		geoid := row[geoState] + row[geoCounty]
		if tractIdx, ok := colIdx["tract"]; ok {
			geoid += row[tractIdx]
		}
		for _, v := range acsVariables {
			if v.moeCode == "" {
				continue
			}
			moeCol, ok := colIdx[v.moeCode]
			if !ok {
				continue
			}
			rawMOE := row[moeCol]
			moeVal, _ := parseValue(rawMOE)
			if moeVal == nil {
				continue
			}
			// Find the estimate Indicator and attach the MOE.
			key := indKey{geoid, v.varDef.ID}
			if i, ok := idx[key]; ok {
				indicators[i].MarginOfError = moeVal
			}
		}
	}

	// Remove the separate "_moe" Indicator records we emitted in the first
	// pass — they have already been merged onto the estimate records.
	filtered := indicators[:0]
	for _, ind := range indicators {
		if !strings.HasSuffix(ind.VariableID, "_moe") {
			filtered = append(filtered, ind)
		}
	}

	return filtered, nil
}

// parseValue converts a raw Census API string into a *float64 value.
// Returns (nil, nil) for suppressed/missing values.
// The second return value is always nil here; MOE handling is in parseResponse.
func parseValue(raw string) (*float64, *float64) {
	trimmed := strings.TrimSpace(raw)
	if suppressedValues[trimmed] || trimmed == "" {
		return nil, nil
	}
	f, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return nil, nil
	}
	// Census API uses -666666666 as a sentinel for suppressed/missing data.
	if f == -666666666 {
		return nil, nil
	}
	return &f, nil
}
