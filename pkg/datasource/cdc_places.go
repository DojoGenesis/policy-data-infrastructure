package datasource

// CDCPlacesSource fetches tract-level health measures from the CDC PLACES
// dataset via the Socrata Open Data API (SODA).
//
// API endpoint:
//
//	https://data.cdc.gov/resource/cwsq-ngmh.json
//
// Variables fetched (MeasureId values):
//
//	BPHIGH  — High blood pressure prevalence (%)
//	DIABETES — Diagnosed diabetes prevalence (%)
//	OBESITY  — Obesity prevalence (%)
//	MHLTH    — Mental health not good for >=14 days (%)
//	CASTHMA  — Current asthma prevalence (%)
//	CSMOKING — Current smoking prevalence (%)
//	ACCESS2  — No health insurance coverage (%)
//	BINGE    — Binge drinking prevalence (%)
//
// Geographic level: census tract (locationname is the 11-digit tract GEOID).
// Pagination: the SODA API paginates results in batches of up to 50,000 rows.
// The API is public and does not require an API key for small queries.

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

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

const (
	cdcPlacesBaseURL   = "https://data.cdc.gov/resource/cwsq-ngmh.json"
	cdcPlacesPageLimit = 50000
)

// cdcMeasures maps CDC PLACES MeasureId values to our canonical variable IDs.
var cdcMeasures = map[string]VariableDef{
	"BPHIGH": {
		ID:          "cdc_bphigh",
		Name:        "High Blood Pressure Prevalence",
		Description: "Crude prevalence of adults aged >=18 years who have been told they have high blood pressure (%)",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	"DIABETES": {
		ID:          "cdc_diabetes",
		Name:        "Diagnosed Diabetes Prevalence",
		Description: "Crude prevalence of adults aged >=18 years who have been told they have diabetes (%)",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	"OBESITY": {
		ID:          "cdc_obesity",
		Name:        "Obesity Prevalence",
		Description: "Crude prevalence of adults aged >=18 years with BMI >= 30 kg/m2 (%)",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	"MHLTH": {
		ID:          "cdc_mhlth",
		Name:        "Poor Mental Health Days",
		Description: "Crude prevalence of adults aged >=18 years with mental health not good for >=14 days (%)",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	"CASTHMA": {
		ID:          "cdc_casthma",
		Name:        "Current Asthma Prevalence",
		Description: "Crude prevalence of adults aged >=18 years who currently have asthma (%)",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	"CSMOKING": {
		ID:          "cdc_csmoking",
		Name:        "Current Smoking Prevalence",
		Description: "Crude prevalence of adults aged >=18 years who are current smokers (%)",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	"ACCESS2": {
		ID:          "cdc_access2",
		Name:        "No Health Insurance Coverage",
		Description: "Crude prevalence of adults aged 18-64 years with no health insurance coverage (%)",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	"BINGE": {
		ID:          "cdc_binge",
		Name:        "Binge Drinking Prevalence",
		Description: "Crude prevalence of adults aged >=18 years who report binge drinking (%)",
		Unit:        "percent",
		Direction:   "lower_better",
	},
}

// cdcPlacesRow is a single row returned by the SODA API.
type cdcPlacesRow struct {
	StateAbbr    string `json:"stateabbr"`
	StateName    string `json:"statename"`
	LocationName string `json:"locationname"` // 11-digit tract GEOID
	MeasureID    string `json:"measureid"`
	Data_Value   string `json:"data_value"`   // crude prevalence
	Year         string `json:"year"`
	Datasource   string `json:"datasource"`
}

// CDCPlacesConfig configures a CDCPlacesSource.
type CDCPlacesConfig struct {
	// Year is the data year, e.g. 2022. When 0, all years are fetched.
	Year int
	// AppToken is the Socrata app token for higher rate limits.
	// Falls back to CDC_PLACES_APP_TOKEN env var. Optional.
	AppToken string
	// HTTPClient is used for all requests. Defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// cdcPlacesSource implements DataSource for CDC PLACES tract data.
type cdcPlacesSource struct {
	cfg     CDCPlacesConfig
	vintage string
}

// NewCDCPlacesSource creates a CDCPlacesSource from cfg.
func NewCDCPlacesSource(cfg CDCPlacesConfig) *cdcPlacesSource {
	if cfg.AppToken == "" {
		cfg.AppToken = os.Getenv("CDC_PLACES_APP_TOKEN")
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	vintage := "CDC-PLACES"
	if cfg.Year > 0 {
		vintage = fmt.Sprintf("CDC-PLACES-%d", cfg.Year)
	}
	return &cdcPlacesSource{cfg: cfg, vintage: vintage}
}

func (s *cdcPlacesSource) Name() string     { return "cdc-places" }
func (s *cdcPlacesSource) Category() string { return "health" }
func (s *cdcPlacesSource) Vintage() string  { return s.vintage }

func (s *cdcPlacesSource) Schema() []VariableDef {
	out := make([]VariableDef, 0, len(cdcMeasures))
	for _, def := range cdcMeasures {
		out = append(out, def)
	}
	return out
}

// FetchCounty fetches CDC PLACES tract-level data for a single county.
// It filters the SODA API by a GEOID prefix match (state+county = first 5 chars).
func (s *cdcPlacesSource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	prefix := stateFIPS + countyFIPS // 5-digit prefix
	return s.fetch(ctx, fmt.Sprintf("starts_with(locationname,'%s')", prefix))
}

// FetchState fetches CDC PLACES tract-level data for an entire state.
// It filters by state abbreviation using the stateabbr column.
func (s *cdcPlacesSource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	abbr := s.fipsToAbbr(stateFIPS)
	if abbr == "" {
		return nil, fmt.Errorf("cdc-places: unknown state FIPS %q", stateFIPS)
	}
	return s.fetch(ctx, fmt.Sprintf("stateabbr='%s'", abbr))
}

// fetch issues paginated SODA requests using whereClause and assembles all rows.
func (s *cdcPlacesSource) fetch(ctx context.Context, whereClause string) ([]store.Indicator, error) {
	var allRows []cdcPlacesRow
	offset := 0

	for {
		batch, err := s.fetchPage(ctx, whereClause, offset, cdcPlacesPageLimit)
		if err != nil {
			return nil, err
		}
		allRows = append(allRows, batch...)
		if len(batch) < cdcPlacesPageLimit {
			break // last page
		}
		offset += cdcPlacesPageLimit
	}

	return s.rowsToIndicators(allRows), nil
}

// fetchPage issues a single paginated SODA API request.
func (s *cdcPlacesSource) fetchPage(ctx context.Context, whereClause string, offset, limit int) ([]cdcPlacesRow, error) {
	params := url.Values{}
	params.Set("$where", whereClause)
	params.Set("$limit", strconv.Itoa(limit))
	params.Set("$offset", strconv.Itoa(offset))
	if s.cfg.Year > 0 {
		params.Set("$where", fmt.Sprintf("%s AND year='%d'", whereClause, s.cfg.Year))
	}

	endpoint := cdcPlacesBaseURL + "?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("cdc-places: build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if s.cfg.AppToken != "" {
		req.Header.Set("X-App-Token", s.cfg.AppToken)
	}

	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cdc-places: http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("cdc-places: api returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var rows []cdcPlacesRow
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, fmt.Errorf("cdc-places: decode json: %w", err)
	}
	return rows, nil
}

// rowsToIndicators converts raw SODA rows to store.Indicator records.
// Rows with unrecognised MeasureId or non-numeric data values are silently
// skipped — the dataset mixes different measure types in a single table.
func (s *cdcPlacesSource) rowsToIndicators(rows []cdcPlacesRow) []store.Indicator {
	var out []store.Indicator
	for _, row := range rows {
		def, ok := cdcMeasures[strings.ToUpper(row.MeasureID)]
		if !ok {
			continue
		}
		// LocationName in PLACES is the 11-digit census tract GEOID.
		geoid := strings.TrimSpace(row.LocationName)
		if len(geoid) != 11 {
			continue
		}
		rawVal := strings.TrimSpace(row.Data_Value)
		var valPtr *float64
		if f, err := strconv.ParseFloat(rawVal, 64); err == nil {
			valPtr = &f
		}
		out = append(out, store.Indicator{
			GEOID:      geoid,
			VariableID: def.ID,
			Vintage:    s.vintage,
			Value:      valPtr,
			RawValue:   rawVal,
		})
	}
	return out
}

// fipsToAbbr returns the 2-letter postal abbreviation for a state FIPS code.
// It uses the reverse of stateAbbrToFIPS from fips.go.
func (s *cdcPlacesSource) fipsToAbbr(fips string) string {
	for abbr, f := range stateAbbrToFIPS {
		if f == fips {
			return abbr
		}
	}
	return ""
}
