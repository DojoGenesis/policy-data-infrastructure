package datasource

// EPATRISource fetches facility-level toxic release data from the EPA Toxics
// Release Inventory (TRI) via the Envirofacts REST API and aggregates
// indicators to the county geographic level.
//
// The Envirofacts API endpoint used is:
//
//	https://data.epa.gov/efservice/tri_facility/state_abbr/{STATE}/rows/{start}:{end}/json
//
// Each page returns up to 1 000 facility records. The adapter paginates until
// an empty response is received.
//
// Variables produced (county-level GEOID = 5-digit county FIPS):
//
//	epa_tri_facility_count       — Number of TRI-reporting facilities in the county
//	epa_tri_total_releases_lbs   — Total on-site releases (lbs) summed across facilities
//	epa_tri_air_releases_lbs     — Total fugitive + stack air emissions (lbs)
//	epa_tri_carcinogen_releases  — Total carcinogen releases (lbs)
//
// Facility-to-county mapping: the TRI facility record contains a COUNTY_FIPS
// field (3-digit county code) combined with the state's 2-digit FIPS code to
// form a 5-digit county GEOID.  If COUNTY_FIPS is absent the facility's
// FIPS_CD field (sometimes a 5-digit code) is used as a fallback.
//
// Implementation note: all facilities within a county are summed.  Callers
// that need tract-level attribution should use the PostGIS ST_Within path via
// the Python ingest layer (ingest/fetch_epa_tri.py, not yet implemented).

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

const (
	epaTriBaseURL    = "https://data.epa.gov/efservice/tri_facility"
	epaTriPageSize   = 1000
	epaTriRateDelay  = 2100 * time.Millisecond
)

// epaTriVariables defines the schema produced by the EPA TRI source.
var epaTriVariables = []VariableDef{
	{
		ID:          "epa_tri_facility_count",
		Name:        "TRI Facility Count",
		Description: "Number of EPA Toxics Release Inventory (TRI) reporting facilities located in the county. Each facility handles or releases threshold quantities of listed toxic chemicals.",
		Unit:        "count",
		Direction:   "lower_better",
	},
	{
		ID:          "epa_tri_total_releases_lbs",
		Name:        "Total On-Site Releases (lbs)",
		Description: "Total on-site chemical releases in pounds from all TRI facilities in the county, summed across all chemicals and release media (air, water, land).",
		Unit:        "count",
		Direction:   "lower_better",
	},
	{
		ID:          "epa_tri_air_releases_lbs",
		Name:        "Total Air Releases (lbs)",
		Description: "Total fugitive air emissions plus stack air emissions in pounds from all TRI facilities in the county.",
		Unit:        "count",
		Direction:   "lower_better",
	},
	{
		ID:          "epa_tri_carcinogen_facility_count",
		Name:        "Carcinogen-Handling Facility Count",
		Description: "Number of TRI-reporting facilities in the county that handle or release any quantity of chemicals classified as known or probable carcinogens (CARCINOGEN=YES in TRI records). The TRI facility endpoint does not report per-chemical carcinogen pounds; use the tri_release endpoint for pound-level carcinogen attribution.",
		Unit:        "count",
		Direction:   "lower_better",
	},
}

// EPATRIConfig configures an EPATRISource.
type EPATRIConfig struct {
	// Year is the TRI reporting year (e.g. 2022). When 0 the vintage is "EPA-TRI".
	Year int
	// HTTPClient is used for all outbound requests. Defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// triRecord is one JSON object returned by the Envirofacts tri_facility endpoint.
// Field names match the Envirofacts column names (uppercase).
type triRecord struct {
	// FIPS_CD is typically the 5-digit county FIPS concatenation, but may be
	// absent in some records — always fall back to COUNTY_FIPS + state FIPS.
	FIPSD          string `json:"FIPS_CD"`
	StateFIPS      string `json:"STATE_FIPS_CODE"`
	CountyFIPS     string `json:"COUNTY_FIPS"`
	FugitiveAir    string `json:"FUGITIVE_AIR"`
	StackAir       string `json:"STACK_AIR"`
	TotalReleases  string `json:"TOTAL_RELEASES"`
	Carcinogen     string `json:"CARCINOGEN"`
	// YEAR field lets us filter when the API does not support year-based filtering.
	ReportingYear  string `json:"REPORTING_YEAR"`
}

// countyTRIRecord holds accumulated TRI data for one county.
type countyTRIRecord struct {
	facilityCount           int
	totalReleasesLbs        float64
	airReleasesLbs          float64
	carcinogenFacilityCount int // facilities where CARCINOGEN=YES (not pound-level — use tri_release for that)
}

// epaTRISource implements DataSource for EPA TRI data.
type epaTRISource struct {
	cfg     EPATRIConfig
	vintage string
}

// NewEPATRISource creates an EPATRISource from cfg.
func NewEPATRISource(cfg EPATRIConfig) *epaTRISource {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	vintage := "EPA-TRI"
	if cfg.Year > 0 {
		vintage = fmt.Sprintf("EPA-TRI-%d", cfg.Year)
	}
	return &epaTRISource{cfg: cfg, vintage: vintage}
}

func (s *epaTRISource) Name() string     { return "epa-tri" }
func (s *epaTRISource) Category() string { return "environment" }
func (s *epaTRISource) Vintage() string  { return s.vintage }

func (s *epaTRISource) Schema() []VariableDef {
	out := make([]VariableDef, len(epaTriVariables))
	copy(out, epaTriVariables)
	return out
}

// FetchCounty fetches TRI indicators for a single county.
// stateFIPS is a 2-digit code; countyFIPS is a 3-digit code.
func (s *epaTRISource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	fips2 := sanitizeFIPS(stateFIPS)
	if len(fips2) != 2 {
		return nil, fmt.Errorf("epa-tri: invalid state FIPS %q (must be 2 digits)", stateFIPS)
	}
	fips3 := sanitizeFIPS(countyFIPS)
	if len(fips3) != 3 {
		return nil, fmt.Errorf("epa-tri: invalid county FIPS %q (must be 3 digits)", countyFIPS)
	}

	byCounty, err := s.fetchState(ctx, fips2)
	if err != nil {
		return nil, err
	}

	fips5 := fips2 + fips3
	rec, ok := byCounty[fips5]
	if !ok {
		return nil, nil
	}
	return s.countyRecordToIndicators(fips5, rec), nil
}

// FetchState fetches TRI indicators for every county in a state.
// stateFIPS is a 2-digit code. Returns county-level GEOIDs (5-digit FIPS).
func (s *epaTRISource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	fips2 := sanitizeFIPS(stateFIPS)
	if len(fips2) != 2 {
		return nil, fmt.Errorf("epa-tri: invalid state FIPS %q (must be 2 digits)", stateFIPS)
	}

	byCounty, err := s.fetchState(ctx, fips2)
	if err != nil {
		return nil, err
	}

	var out []store.Indicator
	for fips5, rec := range byCounty {
		out = append(out, s.countyRecordToIndicators(fips5, rec)...)
	}
	return out, nil
}

// fetchState downloads all TRI facility records for a state, paginates until
// exhausted, and returns a map keyed by 5-digit county FIPS.
func (s *epaTRISource) fetchState(ctx context.Context, stateFIPS string) (map[string]*countyTRIRecord, error) {
	abbr := stateFIPSToAbbr(stateFIPS)
	if abbr == "" {
		return nil, fmt.Errorf("epa-tri: unrecognised state FIPS %q", stateFIPS)
	}

	byCounty := make(map[string]*countyTRIRecord)
	start := 0

	for {
		end := start + epaTriPageSize - 1
		url := fmt.Sprintf("%s/state_abbr/%s/rows/%d:%d/json", epaTriBaseURL, abbr, start, end)

		records, err := s.fetchPage(ctx, url)
		if err != nil {
			return nil, fmt.Errorf("epa-tri: page %d–%d: %w", start, end, err)
		}
		if len(records) == 0 {
			break
		}

		for _, rec := range records {
			// Year filter — only include records matching the configured year when
			// a year is set. Some API responses include multiple reporting years.
			if s.cfg.Year > 0 && rec.ReportingYear != "" {
				if rec.ReportingYear != strconv.Itoa(s.cfg.Year) {
					continue
				}
			}

			fips5 := resolveCountyFIPS(rec, stateFIPS)
			if fips5 == "" {
				continue
			}

			county := getOrCreateCountyTRI(byCounty, fips5)
			county.facilityCount++
			county.totalReleasesLbs += parseFloatOrZero(rec.TotalReleases)
			county.airReleasesLbs += parseFloatOrZero(rec.FugitiveAir) + parseFloatOrZero(rec.StackAir)
			if strings.EqualFold(strings.TrimSpace(rec.Carcinogen), "YES") {
				county.carcinogenFacilityCount++
			}
		}

		// The API may return fewer than epaTriPageSize on the last page; any
		// count < page size means we've hit the end.
		if len(records) < epaTriPageSize {
			break
		}

		start += epaTriPageSize
		time.Sleep(epaTriRateDelay)
	}

	return byCounty, nil
}

// fetchPage fetches a single paginated JSON response from the Envirofacts API.
func (s *epaTRISource) fetchPage(ctx context.Context, url string) ([]triRecord, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("api returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var records []triRecord
	if err := json.NewDecoder(resp.Body).Decode(&records); err != nil {
		return nil, fmt.Errorf("decode json: %w", err)
	}
	return records, nil
}

// resolveCountyFIPS derives the 5-digit county FIPS from a triRecord.
// Priority order: FIPS_CD (if 5 digits), then STATE_FIPS_CODE + COUNTY_FIPS.
// Returns "" if a valid FIPS cannot be determined.
func resolveCountyFIPS(rec triRecord, stateFIPS string) string {
	// Try FIPS_CD first (may already be the full 5-digit county code).
	fipsCD := sanitizeFIPS(rec.FIPSD)
	if len(fipsCD) == 5 {
		return fipsCD
	}

	// Build from state + county components.
	sf := sanitizeFIPS(rec.StateFIPS)
	if sf == "" {
		sf = stateFIPS
	}
	cf := sanitizeFIPS(rec.CountyFIPS)

	// Pad county component to 3 digits.
	for len(cf) < 3 && len(cf) > 0 {
		cf = "0" + cf
	}

	if len(sf) == 2 && len(cf) == 3 {
		return sf + cf
	}
	return ""
}

// getOrCreateCountyTRI returns the countyTRIRecord for fips5, creating it if absent.
func getOrCreateCountyTRI(m map[string]*countyTRIRecord, fips5 string) *countyTRIRecord {
	if rec, ok := m[fips5]; ok {
		return rec
	}
	rec := &countyTRIRecord{}
	m[fips5] = rec
	return rec
}

// countyRecordToIndicators converts a countyTRIRecord to store.Indicator slice.
func (s *epaTRISource) countyRecordToIndicators(fips5 string, rec *countyTRIRecord) []store.Indicator {
	out := make([]store.Indicator, 0, 4)

	makeInd := func(varID string, val float64) store.Indicator {
		v := val
		return store.Indicator{
			GEOID:      fips5,
			VariableID: varID,
			Vintage:    s.vintage,
			Value:      &v,
			RawValue:   strconv.FormatFloat(val, 'f', -1, 64),
		}
	}

	out = append(out, makeInd("epa_tri_facility_count", float64(rec.facilityCount)))
	out = append(out, makeInd("epa_tri_total_releases_lbs", rec.totalReleasesLbs))
	out = append(out, makeInd("epa_tri_air_releases_lbs", rec.airReleasesLbs))
	out = append(out, makeInd("epa_tri_carcinogen_facility_count", float64(rec.carcinogenFacilityCount)))

	return out
}

// parseFloatOrZero parses s as a float64, returning 0 on any error.
func parseFloatOrZero(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}

// stateFIPSToAbbr returns the 2-letter postal abbreviation for a state FIPS code.
// Returns "" when the code is not recognised.
func stateFIPSToAbbr(fips string) string {
	abbr, _ := stateFIPSToAbbrMap[fips]
	return abbr
}

// stateFIPSToAbbrMap is the reverse of stateAbbrToFIPS.
var stateFIPSToAbbrMap = func() map[string]string {
	m := make(map[string]string, len(stateAbbrToFIPS))
	for abbr, fips := range stateAbbrToFIPS {
		m[fips] = abbr
	}
	return m
}()
