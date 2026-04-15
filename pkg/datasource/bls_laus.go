package datasource

// BLSLAUSSource fetches county-level unemployment statistics from the Bureau
// of Labor Statistics Local Area Unemployment Statistics (LAUS) program.
//
// Source: https://api.bls.gov/publicAPI/v2/timeseries/data/
//
// Series ID format:
//
//	LAUST{SS}{CCC}000000000{MM}
//	  SS  = 2-digit state FIPS (e.g. 55 for Wisconsin)
//	  CCC = 3-digit county FIPS (e.g. 025 for Dane County)
//	  MM  = measure code:
//	        03 = unemployment rate (percent)
//	        04 = unemployed count
//	        05 = employed count
//	        06 = labor force
//
// CRITICAL GOTCHAS (documented from production use):
//
//  1. Fill zeros: series IDs use NINE fill zeros ("000000000"), not eight.
//     Correct: LAUST5502500000000003
//     Wrong:   LAUST550250000000003  (8 zeros — returns empty data)
//
//  2. Unregistered batch limit: BLS silently truncates at 25 series per request
//     (HTTP 200 returned even when data is dropped). Do not send more than 25
//     series per request without a registered API key.
//
//  3. annualaverage=true silently drops ALL data when combined with a year
//     range. Compute annual averages client-side from M01-M12 monthly values.
//
//  4. Rate-limit response: status=REQUEST_FAILED_OVER_LIMIT appears in the
//     JSON body (not as an HTTP error code) when the daily query limit is hit.
//     25 queries/day unregistered; 500/day with a registered key.
//
// Variables produced:
//
//	bls_unemployment_rate — Annual average unemployment rate (percent)
//	bls_unemployment_count — Annual average unemployed persons (count)
//	bls_employment_count  — Annual average employed persons (count)
//	bls_labor_force       — Annual average civilian labor force (count)
//
// Geographic level: county (5-digit GEOID). FetchCounty returns one county.
// FetchState fetches all counties in the state — for states with many counties
// (e.g. Wisconsin has 72 × 4 measures = 288 series), requests are batched into
// groups of blsBatchSizeUnregistered (25) or blsBatchSizeRegistered (50).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

const (
	blsAPIURL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

	// blsBatchSizeUnregistered is the max series per request without an API key.
	// BLS silently truncates at 25 — do not send more.
	blsBatchSizeUnregistered = 25

	// blsBatchSizeRegistered is the max series per request with a registered key.
	blsBatchSizeRegistered = 50

	// blsRateDelay is the inter-request pause to avoid hammering the API.
	blsRateDelay = 1200 * time.Millisecond

	// Measure codes for BLS LAUS series IDs.
	blsMeasureUnemploymentRate = "03"
	blsMeasureUnemployedCount  = "04"
	blsMeasureEmployedCount    = "05"
	blsMeasureLaborForce       = "06"
)

// blsVariables defines the schema produced by the BLS LAUS source.
var blsVariables = []VariableDef{
	{
		ID:          "bls_unemployment_rate",
		Name:        "Unemployment Rate",
		Description: "Annual average unemployment rate (percent of civilian labor force that is unemployed). Computed as the mean of the 12 monthly BLS LAUS values for the vintage year.",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	{
		ID:          "bls_unemployment_count",
		Name:        "Unemployed Persons",
		Description: "Annual average number of unemployed persons in the county. Computed as the mean of the 12 monthly BLS LAUS values for the vintage year.",
		Unit:        "count",
		Direction:   "lower_better",
	},
	{
		ID:          "bls_employment_count",
		Name:        "Employed Persons",
		Description: "Annual average number of employed persons in the county. Computed as the mean of the 12 monthly BLS LAUS values for the vintage year.",
		Unit:        "count",
		Direction:   "higher_better",
	},
	{
		ID:          "bls_labor_force",
		Name:        "Civilian Labor Force",
		Description: "Annual average civilian labor force size (employed + unemployed) in the county. Computed as the mean of the 12 monthly BLS LAUS values for the vintage year.",
		Unit:        "count",
		Direction:   "neutral",
	},
}

// BLSLAUSConfig configures a BLSLAUSSource.
type BLSLAUSConfig struct {
	// Year is the data vintage year (e.g. 2023). Required.
	Year int

	// APIKey is the BLS registered API key. When empty, unregistered limits
	// apply (25 series/request, 25 queries/day). Set via BLS_API_KEY env var
	// if not provided directly.
	APIKey string

	// HTTPClient is used for all outbound requests. Defaults to a client with
	// a 60-second timeout.
	HTTPClient *http.Client
}

// blsLAUSSource implements DataSource for BLS LAUS county unemployment data.
type blsLAUSSource struct {
	cfg     BLSLAUSConfig
	vintage string
	apiKey  string // resolved: cfg.APIKey or BLS_API_KEY env var
}

// NewBLSLAUSSource creates a BLSLAUSSource from cfg.
// If cfg.APIKey is empty, the BLS_API_KEY environment variable is checked.
func NewBLSLAUSSource(cfg BLSLAUSConfig) *blsLAUSSource {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 60 * time.Second}
	}
	apiKey := cfg.APIKey
	if apiKey == "" {
		apiKey = os.Getenv("BLS_API_KEY")
	}
	vintage := "BLS-LAUS"
	if cfg.Year > 0 {
		vintage = fmt.Sprintf("BLS-LAUS-%d", cfg.Year)
	}
	return &blsLAUSSource{
		cfg:     cfg,
		vintage: vintage,
		apiKey:  apiKey,
	}
}

func (s *blsLAUSSource) Name() string     { return "bls-laus" }
func (s *blsLAUSSource) Category() string { return "economic" }
func (s *blsLAUSSource) Vintage() string  { return s.vintage }

func (s *blsLAUSSource) Schema() []VariableDef {
	out := make([]VariableDef, len(blsVariables))
	copy(out, blsVariables)
	return out
}

// FetchCounty fetches BLS LAUS indicators for a single county.
// stateFIPS is a 2-digit code; countyFIPS is a 3-digit code.
// Returns 4 indicators (one per measure) as county-level GEOIDs.
func (s *blsLAUSSource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	sf := sanitizeFIPS(stateFIPS)
	cf := sanitizeFIPS(countyFIPS)
	if len(sf) != 2 {
		return nil, fmt.Errorf("bls-laus: invalid state FIPS %q (must be 2 digits)", stateFIPS)
	}
	if len(cf) != 3 {
		return nil, fmt.Errorf("bls-laus: invalid county FIPS %q (must be 3 digits)", countyFIPS)
	}

	seriesIDs := blsSeriesIDs(sf, cf)
	raw, err := s.fetchSeries(ctx, seriesIDs)
	if err != nil {
		return nil, fmt.Errorf("bls-laus: %w", err)
	}

	geoid := sf + cf
	return s.seriesToIndicators(geoid, sf, cf, raw), nil
}

// FetchState fetches BLS LAUS indicators for all counties in a state.
// stateFIPS is a 2-digit code. Results are returned as county-level GEOIDs.
//
// For states with many counties, series are batched into groups of
// blsBatchSizeUnregistered (25) or blsBatchSizeRegistered (50).
func (s *blsLAUSSource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	sf := sanitizeFIPS(stateFIPS)
	if len(sf) != 2 {
		return nil, fmt.Errorf("bls-laus: invalid state FIPS %q (must be 2 digits)", stateFIPS)
	}

	// Fetch the list of county FIPS codes for this state from the Census API.
	countyFIPSList, err := s.blsCountiesForState(ctx, sf)
	if err != nil {
		return nil, fmt.Errorf("bls-laus: get county list for state %s: %w", sf, err)
	}
	if len(countyFIPSList) == 0 {
		// Unknown or empty state — return empty (graceful no-op).
		return nil, nil
	}

	// Build all series IDs for every county × 4 measures.
	var allSeriesIDs []string
	for _, cf := range countyFIPSList {
		allSeriesIDs = append(allSeriesIDs, blsSeriesIDs(sf, cf)...)
	}

	// Fetch in batches.
	raw, err := s.fetchSeries(ctx, allSeriesIDs)
	if err != nil {
		return nil, fmt.Errorf("bls-laus: state %s: %w", sf, err)
	}

	// Assemble indicators for each county.
	var out []store.Indicator
	for _, cf := range countyFIPSList {
		geoid := sf + cf
		out = append(out, s.seriesToIndicators(geoid, sf, cf, raw)...)
	}
	return out, nil
}

// blsSeriesID constructs a single BLS LAUS series ID.
// Format: LAUST{SS}{CCC}000000000{MM}
//
//	SS  = 2-digit state FIPS
//	CCC = 3-digit county FIPS
//	9 fill zeros (not 8 — BLS is silent about truncating wrong-length IDs)
//	MM  = 2-digit measure code
func blsSeriesID(stateFIPS, countyFIPS, measure string) string {
	return "LAUST" + stateFIPS + countyFIPS + "000000000" + measure
}

// blsSeriesIDs returns the 4 measure series IDs for one county.
func blsSeriesIDs(stateFIPS, countyFIPS string) []string {
	return []string{
		blsSeriesID(stateFIPS, countyFIPS, blsMeasureUnemploymentRate),
		blsSeriesID(stateFIPS, countyFIPS, blsMeasureUnemployedCount),
		blsSeriesID(stateFIPS, countyFIPS, blsMeasureEmployedCount),
		blsSeriesID(stateFIPS, countyFIPS, blsMeasureLaborForce),
	}
}

// batchSize returns the effective batch size for this source.
func (s *blsLAUSSource) batchSize() int {
	if s.apiKey != "" {
		return blsBatchSizeRegistered
	}
	return blsBatchSizeUnregistered
}

// fetchSeries fetches a set of BLS series IDs in batches and returns a map
// from series ID to its computed annual average value for the configured year.
// A nil value means no data was returned for that series.
func (s *blsLAUSSource) fetchSeries(ctx context.Context, seriesIDs []string) (map[string]*float64, error) {
	results := make(map[string]*float64, len(seriesIDs))
	batch := s.batchSize()

	for i := 0; i < len(seriesIDs); i += batch {
		end := i + batch
		if end > len(seriesIDs) {
			end = len(seriesIDs)
		}
		chunk := seriesIDs[i:end]

		resp, err := s.postBLS(ctx, chunk)
		if err != nil {
			return nil, fmt.Errorf("batch %d: %w", i/batch+1, err)
		}

		// Check for over-limit status in the response body (HTTP 200 with error).
		if resp.Status == "REQUEST_FAILED_OVER_LIMIT" {
			return nil, fmt.Errorf("BLS daily query limit reached (status=REQUEST_FAILED_OVER_LIMIT); wait until next UTC midnight")
		}

		for _, sr := range resp.Results.Series {
			val := blsExtractAnnual(sr, s.cfg.Year)
			results[sr.SeriesID] = val
		}

		// Rate-limit delay between batches (skip after the last one).
		if end < len(seriesIDs) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(blsRateDelay):
			}
		}
	}

	return results, nil
}

// blsAPIRequest is the JSON body sent to the BLS v2 timeseries API.
type blsAPIRequest struct {
	SeriesIDs       []string `json:"seriesid"`
	StartYear       string   `json:"startyear"`
	EndYear         string   `json:"endyear"`
	RegistrationKey string   `json:"registrationkey,omitempty"`
}

// blsAPIResponse is the top-level JSON response from the BLS v2 API.
type blsAPIResponse struct {
	Status  string `json:"status"`
	Message []string `json:"message"`
	Results struct {
		Series []blsSeriesResult `json:"series"`
	} `json:"Results"`
}

// blsSeriesResult is one series entry in the BLS API response.
type blsSeriesResult struct {
	SeriesID string        `json:"seriesID"`
	Data     []blsDataEntry `json:"data"`
}

// blsDataEntry is one data point (monthly or annual) in a BLS series.
type blsDataEntry struct {
	Year   string `json:"year"`
	Period string `json:"period"`
	Value  string `json:"value"`
}

// postBLS sends a POST request to the BLS v2 API for the given series IDs
// and year range. Never sends annualaverage=true — we compute it client-side.
func (s *blsLAUSSource) postBLS(ctx context.Context, seriesIDs []string) (*blsAPIResponse, error) {
	yearStr := fmt.Sprintf("%d", s.cfg.Year)
	req := blsAPIRequest{
		SeriesIDs:       seriesIDs,
		StartYear:       yearStr,
		EndYear:         yearStr,
		RegistrationKey: s.apiKey,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, blsAPIURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", "policy-data-infrastructure/1.0")

	httpResp, err := s.cfg.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http post: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(httpResp.Body, 2048))
		return nil, fmt.Errorf("BLS API returned HTTP %d: %s", httpResp.StatusCode, strings.TrimSpace(string(b)))
	}

	var apiResp blsAPIResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &apiResp, nil
}

// blsExtractAnnual extracts the annual average value for the given year from a
// BLS series result. It first tries period M13 (official annual average from
// BLS). If M13 is absent, it computes the mean of available M01-M12 values.
// Returns nil when no data is available for the year.
func blsExtractAnnual(sr blsSeriesResult, year int) *float64 {
	yearStr := fmt.Sprintf("%d", year)
	var monthly []float64

	for _, d := range sr.Data {
		if d.Year != yearStr {
			continue
		}
		period := d.Period

		var val float64
		_, err := fmt.Sscanf(d.Value, "%f", &val)
		if err != nil {
			continue
		}

		if period == "M13" {
			// Official BLS annual average — use directly.
			v := val
			return &v
		}

		// Collect monthly values M01-M12.
		if len(period) == 3 && period[0] == 'M' {
			var month int
			_, err := fmt.Sscanf(period[1:], "%d", &month)
			if err == nil && month >= 1 && month <= 12 {
				monthly = append(monthly, val)
			}
		}
	}

	if len(monthly) == 0 {
		return nil
	}

	// Compute average from however many months are available.
	var sum float64
	for _, v := range monthly {
		sum += v
	}
	avg := sum / float64(len(monthly))
	// Round to 1 decimal place to match BLS reporting precision.
	avg = float64(int(avg*10+0.5)) / 10
	return &avg
}

// seriesToIndicators converts raw series map data to store.Indicator slice
// for one county. All four measures are always emitted; value is nil when
// no data was returned for a series.
func (s *blsLAUSSource) seriesToIndicators(geoid, stateFIPS, countyFIPS string, raw map[string]*float64) []store.Indicator {
	type measureDef struct {
		code  string
		varID string
	}
	measures := []measureDef{
		{blsMeasureUnemploymentRate, "bls_unemployment_rate"},
		{blsMeasureUnemployedCount, "bls_unemployment_count"},
		{blsMeasureEmployedCount, "bls_employment_count"},
		{blsMeasureLaborForce, "bls_labor_force"},
	}

	out := make([]store.Indicator, 0, len(measures))
	for _, m := range measures {
		sid := blsSeriesID(stateFIPS, countyFIPS, m.code)
		val := raw[sid] // nil if missing

		rawStr := ""
		if val != nil {
			rawStr = fmt.Sprintf("%g", *val)
		}

		out = append(out, store.Indicator{
			GEOID:      geoid,
			VariableID: m.varID,
			Vintage:    s.vintage,
			Value:      val,
			RawValue:   rawStr,
		})
	}
	return out
}

// blsCountiesForState fetches the 3-digit county FIPS codes for the given
// 2-digit state FIPS using the Census Geography API. Returns only the county
// portion (3 digits). The Census API is used because BLS LAUS covers all US
// counties and there is no embedded county list for all states.
//
// The Census call uses the ACS 5-year endpoint (2023 vintage by default) with
// NAME as the only requested variable so this is always a tiny request:
//
//	https://api.census.gov/data/{year}/acs/acs5?get=NAME&for=county:*&in=state:{fips}
//
// Returns nil (not an error) when the state is unknown or no counties are found.
func (s *blsLAUSSource) blsCountiesForState(ctx context.Context, stateFIPS string) ([]string, error) {
	year := s.cfg.Year
	if year == 0 {
		year = 2023
	}
	// Clamp to most recent ACS 5-year release (2022 covers through 2026 in practice).
	// If year > 2023, the endpoint may not exist yet; fall back to 2023.
	if year > 2023 {
		year = 2023
	}

	url := fmt.Sprintf(
		"https://api.census.gov/data/%d/acs/acs5?get=NAME&for=county:*&in=state:%s",
		year, stateFIPS,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build census county list request: %w", err)
	}
	req.Header.Set("User-Agent", "policy-data-infrastructure/1.0")

	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("census county list: http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("census county list: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	// Response is a JSON array of arrays. First row is header:
	// [["NAME","state","county"], ["Adams County, Wisconsin","55","001"], ...]
	var rows [][]string
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, fmt.Errorf("census county list: decode: %w", err)
	}

	// Find the "county" column index from the header row.
	if len(rows) < 2 {
		return nil, nil // state has no counties in census data
	}
	countyCol := -1
	for i, col := range rows[0] {
		if col == "county" {
			countyCol = i
			break
		}
	}
	if countyCol < 0 {
		return nil, fmt.Errorf("census county list: response missing 'county' column")
	}

	var counties []string
	for _, row := range rows[1:] {
		if countyCol >= len(row) {
			continue
		}
		cf := sanitizeFIPS(row[countyCol])
		if len(cf) == 3 {
			counties = append(counties, cf)
		}
	}
	return counties, nil
}

// chunkStrings splits a slice of strings into chunks of at most size n.
// Used in tests to verify batch-chunking logic independently.
func chunkStrings(ss []string, n int) [][]string {
	if n <= 0 {
		return nil
	}
	var chunks [][]string
	for i := 0; i < len(ss); i += n {
		end := i + n
		if end > len(ss) {
			end = len(ss)
		}
		chunks = append(chunks, ss[i:end])
	}
	return chunks
}
