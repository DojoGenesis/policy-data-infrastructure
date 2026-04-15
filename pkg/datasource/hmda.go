package datasource

// HMDASource fetches tract-level mortgage lending indicators from the CFPB
// FFIEC Data Browser API (https://ffiec.cfpb.gov/v2/data-browser-api/).
//
// The API returns loan-level CSV data. This adapter downloads records for a
// county (or state) and aggregates them to 11-digit census tract GEOIDs.
//
// Key API endpoint: https://ffiec.cfpb.gov/v2/data-browser-api/view/csv
// Parameters: states, counties, years, actions_taken (1=originated, 3=denied)
//
// Variables produced (tract-level GEOID = 11-digit census tract FIPS):
//
//	hmda_loan_count           — Total originated mortgage loans
//	hmda_denial_rate          — Mortgage denial rate (denied / (originated + denied))
//	hmda_median_loan_amount   — Median loan amount for originated loans (dollars)
//	hmda_minority_denial_rate — Denial rate for minority applicants
//	hmda_ltv_ratio            — Median combined loan-to-value ratio for originated loans
//
// Because HMDA files are large (national CSV ~20–40 GB/year), the adapter
// always fetches county-scoped or state-scoped slices, never the national file.
// FetchState issues one request per county in the state using all counties
// from AllStateFIPS mapping; this is acceptable for a 60 req/min rate limit.
//
// Implementation note: The CFPB API does not support tract-level filtering
// directly; the county-level download is the finest-grained option. Records
// are grouped in-memory by the census_tract column (11-digit FIPS).

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

const (
	hmdaBaseURL      = "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv"
	hmdaRateDelay    = time.Second // 60 req/min → 1s between requests
	hmdaActionOrigin = "1"        // action_taken=1: loan originated
	hmdaActionDenied = "3"        // action_taken=3: application denied
)

// hmdaVariables defines the schema produced by the HMDA source.
var hmdaVariables = []VariableDef{
	{
		ID:          "hmda_loan_count",
		Name:        "Originated Mortgage Loan Count",
		Description: "Total number of originated mortgage loans (action_taken=1) for the census tract in the given year.",
		Unit:        "count",
		Direction:   "neutral",
	},
	{
		ID:          "hmda_denial_rate",
		Name:        "Mortgage Denial Rate",
		Description: "Share of mortgage applications that were denied (action_taken=3 divided by originated + denied), expressed as a percent. Lower values indicate greater lending access.",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	{
		ID:          "hmda_median_loan_amount",
		Name:        "Median Loan Amount",
		Description: "Median loan amount (in dollars) for originated mortgage loans in the census tract.",
		Unit:        "dollars",
		Direction:   "neutral",
	},
	{
		ID:          "hmda_minority_denial_rate",
		Name:        "Minority Applicant Denial Rate",
		Description: "Denial rate for applicants identified as a racial or ethnic minority (non-White, non-Hispanic), expressed as a percent. Lower values indicate greater equity in lending.",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	{
		ID:          "hmda_ltv_ratio",
		Name:        "Median Loan-to-Value Ratio",
		Description: "Median combined loan-to-value ratio (CLTV, percent) for originated loans in the tract. Captures typical leverage at origination.",
		Unit:        "percent",
		Direction:   "neutral",
	},
}

// HMDAConfig configures an HMDASource.
type HMDAConfig struct {
	// Year is the HMDA data vintage year (e.g. 2023).
	Year int
	// HTTPClient is used for all outbound requests. Defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// hmdaSource implements DataSource for HMDA mortgage lending data.
type hmdaSource struct {
	cfg     HMDAConfig
	vintage string
}

// NewHMDASource creates an HMDASource from cfg.
func NewHMDASource(cfg HMDAConfig) *hmdaSource {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Year == 0 {
		cfg.Year = 2023
	}
	vintage := fmt.Sprintf("HMDA-%d", cfg.Year)
	return &hmdaSource{cfg: cfg, vintage: vintage}
}

func (s *hmdaSource) Name() string     { return "hmda" }
func (s *hmdaSource) Category() string { return "housing" }
func (s *hmdaSource) Vintage() string  { return s.vintage }

func (s *hmdaSource) Schema() []VariableDef {
	out := make([]VariableDef, len(hmdaVariables))
	copy(out, hmdaVariables)
	return out
}

// FetchCounty fetches all HMDA tract-level indicators for a single county.
// stateFIPS is a 2-digit code; countyFIPS is a 3-digit code.
func (s *hmdaSource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	sf := sanitizeFIPS(stateFIPS)
	cf := sanitizeFIPS(countyFIPS)
	if len(sf) != 2 {
		return nil, fmt.Errorf("hmda: invalid state FIPS %q (must be 2 digits)", stateFIPS)
	}
	if len(cf) != 3 {
		return nil, fmt.Errorf("hmda: invalid county FIPS %q (must be 3 digits)", countyFIPS)
	}

	// CFPB API uses full 5-digit county FIPS (state+county concatenated).
	county5 := sf + cf
	rows, err := s.fetchCSV(ctx, county5, "")
	if err != nil {
		return nil, fmt.Errorf("hmda FetchCounty(%s,%s): %w", stateFIPS, countyFIPS, err)
	}

	tracts := aggregateHMDARows(rows)
	return s.tractDataToIndicators(tracts, county5, ""), nil
}

// FetchState fetches all HMDA tract-level indicators for every county in a state.
// stateFIPS is a 2-digit code.
func (s *hmdaSource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	sf := sanitizeFIPS(stateFIPS)
	if len(sf) != 2 {
		return nil, fmt.Errorf("hmda: invalid state FIPS %q (must be 2 digits)", stateFIPS)
	}

	// Fetch by state directly — the API supports states= parameter.
	rows, err := s.fetchCSV(ctx, "", sf)
	if err != nil {
		return nil, fmt.Errorf("hmda FetchState(%s): %w", stateFIPS, err)
	}

	tracts := aggregateHMDARows(rows)
	return s.tractDataToIndicators(tracts, "", sf), nil
}

// fetchCSV downloads a loan-level HMDA CSV from the CFPB API.
// county5 is the 5-digit county FIPS (non-empty = county scope).
// stateFIPS is the 2-digit state FIPS (non-empty = state scope, used when county5 is empty).
// The API always returns originated AND denied records in a single request
// (actions_taken=1,3), which reduces round-trips within the rate limit.
func (s *hmdaSource) fetchCSV(ctx context.Context, county5, stateFIPS string) ([][]string, error) {
	params := url.Values{}
	params.Set("years", strconv.Itoa(s.cfg.Year))
	params.Set("actions_taken", hmdaActionOrigin+","+hmdaActionDenied)

	if county5 != "" {
		params.Set("counties", county5)
	} else if stateFIPS != "" {
		params.Set("states", stateFIPS)
	} else {
		return nil, fmt.Errorf("hmda: fetchCSV requires either county5 or stateFIPS")
	}

	reqURL := hmdaBaseURL + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "text/csv,application/octet-stream")

	// Respect rate limit before sending.
	time.Sleep(hmdaRateDelay)

	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("api returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	r := csv.NewReader(resp.Body)
	r.LazyQuotes = true
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1

	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse csv: %w", err)
	}
	return rows, nil
}

// hmdaTractData accumulates per-tract aggregates from loan-level rows.
type hmdaTractData struct {
	// originated loan amounts (for median computation)
	loanAmounts []float64
	// CLTV ratios for originated loans (for median computation)
	cltvRatios []float64
	// originations + denials (for overall denial rate)
	totalApplications int
	denials           int
	// minority originations + denials (for minority denial rate)
	minorityApplications int
	minorityDenials      int
}

// hmdaColIdx holds column indices for HMDA CSV rows.
type hmdaColIdx struct {
	censusTract  int // "census_tract" — 11-digit FIPS
	actionTaken  int // "action_taken"
	loanAmount   int // "loan_amount"
	cltv         int // "combined_loan_to_value_ratio"
	derivedRace  int // "derived_race"
	derivedEthn  int // "derived_ethnicity"
}

// findHMDACols locates required column indices from the HMDA CSV header.
// Returns an error if any required column is missing.
func findHMDACols(header []string) (hmdaColIdx, error) {
	idx := hmdaColIdx{
		censusTract: -1,
		actionTaken: -1,
		loanAmount:  -1,
		cltv:        -1,
		derivedRace: -1,
		derivedEthn: -1,
	}
	for i, col := range header {
		switch strings.ToLower(strings.TrimSpace(col)) {
		case "census_tract":
			idx.censusTract = i
		case "action_taken":
			idx.actionTaken = i
		case "loan_amount":
			idx.loanAmount = i
		case "combined_loan_to_value_ratio":
			idx.cltv = i
		case "derived_race":
			idx.derivedRace = i
		case "derived_ethnicity":
			idx.derivedEthn = i
		}
	}
	// census_tract and action_taken are required; others are optional but
	// their absence will produce nil indicators for the dependent variables.
	if idx.censusTract < 0 {
		return idx, fmt.Errorf("hmda: CSV missing column 'census_tract'")
	}
	if idx.actionTaken < 0 {
		return idx, fmt.Errorf("hmda: CSV missing column 'action_taken'")
	}
	return idx, nil
}

// isMinorityApplicant returns true when the row's race/ethnicity indicates a
// minority applicant (non-White, non-Hispanic).
func isMinorityApplicant(idx hmdaColIdx, row []string) bool {
	race := ""
	if idx.derivedRace >= 0 && idx.derivedRace < len(row) {
		race = strings.TrimSpace(row[idx.derivedRace])
	}
	ethn := ""
	if idx.derivedEthn >= 0 && idx.derivedEthn < len(row) {
		ethn = strings.TrimSpace(row[idx.derivedEthn])
	}

	// Treat as minority if derived_race is not "White" and not "Race Not Available"
	// OR if derived_ethnicity is "Hispanic or Latino".
	if strings.EqualFold(ethn, "Hispanic or Latino") {
		return true
	}
	whiteRace := strings.EqualFold(race, "White")
	notAvail := strings.Contains(strings.ToLower(race), "not available") ||
		strings.Contains(strings.ToLower(race), "not applicable") ||
		race == "" ||
		strings.EqualFold(race, "Free Form Text Only") ||
		strings.EqualFold(race, "2 or more minority races")
	return !whiteRace && !notAvail
}

// aggregateHMDARows parses loan-level CSV rows and groups them by census tract.
// rows[0] must be the header row.
func aggregateHMDARows(rows [][]string) map[string]*hmdaTractData {
	byTract := make(map[string]*hmdaTractData)
	if len(rows) < 2 {
		return byTract
	}

	idx, err := findHMDACols(rows[0])
	if err != nil {
		// Column mismatch — return empty map; callers get nil indicators.
		return byTract
	}

	for _, row := range rows[1:] {
		if len(row) <= idx.censusTract || len(row) <= idx.actionTaken {
			continue
		}

		tractRaw := strings.TrimSpace(row[idx.censusTract])
		// Census tract must be an 11-digit numeric FIPS; skip "NA" / empty / short.
		tractFIPS := sanitizeFIPS(tractRaw)
		if len(tractFIPS) != 11 {
			continue
		}

		action := strings.TrimSpace(row[idx.actionTaken])

		td := getOrCreateTract(byTract, tractFIPS)

		switch action {
		case hmdaActionOrigin:
			td.totalApplications++

			// Loan amount.
			if idx.loanAmount >= 0 && idx.loanAmount < len(row) {
				if amt, err2 := parseHMDAFloat(row[idx.loanAmount]); err2 == nil && amt > 0 {
					td.loanAmounts = append(td.loanAmounts, amt)
				}
			}
			// CLTV ratio.
			if idx.cltv >= 0 && idx.cltv < len(row) {
				if cltv, err2 := parseHMDAFloat(row[idx.cltv]); err2 == nil && cltv > 0 {
					td.cltvRatios = append(td.cltvRatios, cltv)
				}
			}
			// Minority check for minority denial rate denominator.
			if isMinorityApplicant(idx, row) {
				td.minorityApplications++
			}

		case hmdaActionDenied:
			td.totalApplications++
			td.denials++
			if isMinorityApplicant(idx, row) {
				td.minorityApplications++
				td.minorityDenials++
			}
		}
	}

	return byTract
}

// getOrCreateTract returns the hmdaTractData for tractFIPS, creating it if absent.
func getOrCreateTract(m map[string]*hmdaTractData, tractFIPS string) *hmdaTractData {
	if td, ok := m[tractFIPS]; ok {
		return td
	}
	td := &hmdaTractData{}
	m[tractFIPS] = td
	return td
}

// parseHMDAFloat parses a HMDA numeric field value. HMDA uses "NA", "Exempt",
// or empty string for missing values; these return an error.
func parseHMDAFloat(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "NA") || strings.EqualFold(s, "Exempt") {
		return 0, fmt.Errorf("missing value: %q", s)
	}
	return strconv.ParseFloat(s, 64)
}

// medianFloat64 returns the median of a sorted copy of vals. Returns nil for
// empty or nil slices.
func medianFloat64(vals []float64) *float64 {
	if len(vals) == 0 {
		return nil
	}
	cp := make([]float64, len(vals))
	copy(cp, vals)
	sort.Float64s(cp)
	n := len(cp)
	var med float64
	if n%2 == 1 {
		med = cp[n/2]
	} else {
		med = (cp[n/2-1] + cp[n/2]) / 2.0
	}
	v := math.Round(med*100) / 100
	return &v
}

// tractDataToIndicators converts the aggregated tract map to store.Indicator
// slice. countyFilter (5-digit) or stateFilter (2-digit) optionally restricts
// output to GEOIDs that start with the filter prefix.
func (s *hmdaSource) tractDataToIndicators(byTract map[string]*hmdaTractData, countyFilter, stateFilter string) []store.Indicator {
	var out []store.Indicator

	for tractFIPS, td := range byTract {
		// Optional geography filter.
		if countyFilter != "" && !strings.HasPrefix(tractFIPS, countyFilter) {
			continue
		}
		if stateFilter != "" && !strings.HasPrefix(tractFIPS, stateFilter) {
			continue
		}

		out = append(out, s.tractToIndicators(tractFIPS, td)...)
	}
	return out
}

// tractToIndicators converts a single hmdaTractData to store.Indicators.
func (s *hmdaSource) tractToIndicators(tractFIPS string, td *hmdaTractData) []store.Indicator {
	out := make([]store.Indicator, 0, 5)

	makeInd := func(varID string, val *float64, raw string) store.Indicator {
		return store.Indicator{
			GEOID:      tractFIPS,
			VariableID: varID,
			Vintage:    s.vintage,
			Value:      val,
			RawValue:   raw,
		}
	}

	// 1. Loan count (originated only).
	// originations = totalApplications - denials
	originatedCount := td.totalApplications - td.denials
	loanCountVal := float64(originatedCount)
	out = append(out, makeInd("hmda_loan_count", &loanCountVal, strconv.Itoa(originatedCount)))

	// 2. Denial rate: denials / (originated + denied), percent.
	var denialRateVal *float64
	denialRateRaw := ""
	if td.totalApplications > 0 {
		rate := (float64(td.denials) / float64(td.totalApplications)) * 100.0
		rate = math.Round(rate*100) / 100
		denialRateVal = &rate
		denialRateRaw = strconv.FormatFloat(rate, 'f', 2, 64)
	}
	out = append(out, makeInd("hmda_denial_rate", denialRateVal, denialRateRaw))

	// 3. Median loan amount.
	medLoan := medianFloat64(td.loanAmounts)
	medLoanRaw := ""
	if medLoan != nil {
		medLoanRaw = strconv.FormatFloat(*medLoan, 'f', 2, 64)
	}
	out = append(out, makeInd("hmda_median_loan_amount", medLoan, medLoanRaw))

	// 4. Minority denial rate.
	var minorityDenialVal *float64
	minorityDenialRaw := ""
	if td.minorityApplications > 0 {
		rate := (float64(td.minorityDenials) / float64(td.minorityApplications)) * 100.0
		rate = math.Round(rate*100) / 100
		minorityDenialVal = &rate
		minorityDenialRaw = strconv.FormatFloat(rate, 'f', 2, 64)
	}
	out = append(out, makeInd("hmda_minority_denial_rate", minorityDenialVal, minorityDenialRaw))

	// 5. Median CLTV ratio.
	medCLTV := medianFloat64(td.cltvRatios)
	medCLTVRaw := ""
	if medCLTV != nil {
		medCLTVRaw = strconv.FormatFloat(*medCLTV, 'f', 2, 64)
	}
	out = append(out, makeInd("hmda_ltv_ratio", medCLTV, medCLTVRaw))

	return out
}
