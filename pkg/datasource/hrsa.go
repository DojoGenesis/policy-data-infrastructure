package datasource

// HRSASource fetches county-level shortage area and health center indicators
// from the HRSA (Health Resources and Services Administration) Data Warehouse.
//
// Data is sourced from three publicly available CSV bulk downloads:
//
//	Primary Care HPSA:
//	  https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_PC.csv
//	Dental Health HPSA:
//	  https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_DH.csv
//	Mental Health HPSA:
//	  https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_MH.csv
//	Health Center (FQHC) Service Delivery Sites:
//	  https://data.hrsa.gov/DataDownload/DD_Files/Health_Center_Service_Delivery_and_LookAlike_Sites.csv
//
// Geographic level: county. HPSA records map to county via the
// "State and County Federal Information Processing Standard Code" column
// (5-digit county FIPS). FQHCs are similarly aggregated by county.
//
// For tract-level output, the county-level value is propagated to every
// census tract whose GEOID begins with the 5-digit county FIPS code.
// Callers that require true tract-level disaggregation should use the
// PostGIS spatial join path in the Python ingest layer.
//
// Variables produced:
//
//	hrsa_hpsa_primary_care    — Primary care HPSA score (0–25; higher = more shortage)
//	hrsa_hpsa_dental          — Dental HPSA score (0–25)
//	hrsa_hpsa_mental_health   — Mental health HPSA score (0–25)
//	hrsa_hpsa_designation     — Whether the county has ANY active HPSA designation (1=yes, 0=no)
//	hrsa_fqhc_count           — Count of active FQHC/look-alike sites serving the county
//
// Implementation note: because the CSV files are delivered as full national
// snapshots (no county-filter API), FetchCounty downloads the full file and
// filters in-memory. FetchState does the same and returns results for all
// tracts in the state. A per-source in-process cache keyed by discipline
// prevents redundant downloads within a single binary invocation.

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

const hrsaRateDelay = 1000 * time.Millisecond

const (
	hrsaHPSAPCURL  = "https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_PC.csv"
	hrsaHPSADHURL  = "https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_DH.csv"
	hrsaHPSAMHURL  = "https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_MH.csv"
	hrsaFQHCURL    = "https://data.hrsa.gov/DataDownload/DD_Files/Health_Center_Service_Delivery_and_LookAlike_Sites.csv"

	// hpsaStatusDesignated is the HPSA Status Code value for active designations.
	hpsaStatusDesignated = "D"
)

// hrsaVariables defines the schema produced by the HRSA source.
var hrsaVariables = []VariableDef{
	{
		ID:          "hrsa_hpsa_primary_care",
		Name:        "Primary Care HPSA Score",
		Description: "Health Professional Shortage Area score for primary care (0–25 scale; higher = more severe shortage). Reflects the most severe active HPSA designation within the county.",
		Unit:        "rate",
		Direction:   "lower_better",
	},
	{
		ID:          "hrsa_hpsa_dental",
		Name:        "Dental HPSA Score",
		Description: "Health Professional Shortage Area score for dental health (0–25 scale; higher = more severe shortage). Reflects the most severe active HPSA designation within the county.",
		Unit:        "rate",
		Direction:   "lower_better",
	},
	{
		ID:          "hrsa_hpsa_mental_health",
		Name:        "Mental Health HPSA Score",
		Description: "Health Professional Shortage Area score for mental health (0–25 scale; higher = more severe shortage). Reflects the most severe active HPSA designation within the county.",
		Unit:        "rate",
		Direction:   "lower_better",
	},
	{
		ID:          "hrsa_hpsa_designation",
		Name:        "HPSA Designation Flag",
		Description: "Whether the county contains at least one active Health Professional Shortage Area designation of any type (1 = designated, 0 = not designated).",
		Unit:        "count",
		Direction:   "lower_better",
	},
	{
		ID:          "hrsa_fqhc_count",
		Name:        "FQHC Site Count",
		Description: "Count of active Federally Qualified Health Center (FQHC) and FQHC Look-Alike service delivery sites located within the county.",
		Unit:        "count",
		Direction:   "higher_better",
	},
}

// HRSAConfig configures an HRSASource.
type HRSAConfig struct {
	// Year is the data vintage year label used in the vintage string (e.g. 2024).
	// When 0, the vintage is "HRSA" without a year suffix.
	Year int
	// HTTPClient is used for all outbound requests. Defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// hrsaSource implements DataSource for HRSA HPSA and FQHC data.
type hrsaSource struct {
	cfg     HRSAConfig
	vintage string

	// cache stores downloaded CSV rows per discipline to avoid re-downloading
	// within a single process invocation. The key is the CSV URL.
	cacheMu sync.Mutex
	cache   map[string][][]string // url → rows (including header)
}

// NewHRSASource creates an HRSASource from cfg.
func NewHRSASource(cfg HRSAConfig) *hrsaSource {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	vintage := "HRSA"
	if cfg.Year > 0 {
		vintage = fmt.Sprintf("HRSA-%d", cfg.Year)
	}
	return &hrsaSource{
		cfg:     cfg,
		vintage: vintage,
		cache:   make(map[string][][]string),
	}
}

func (s *hrsaSource) Name() string     { return "hrsa" }
func (s *hrsaSource) Category() string { return "health" }
func (s *hrsaSource) Vintage() string  { return s.vintage }

func (s *hrsaSource) Schema() []VariableDef {
	out := make([]VariableDef, len(hrsaVariables))
	copy(out, hrsaVariables)
	return out
}

// FetchCounty fetches all HRSA indicators for a single county.
// stateFIPS is a 2-digit code; countyFIPS is a 3-digit code.
// Downloads the full national CSV for each discipline and filters in-memory.
func (s *hrsaSource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	fips5 := sanitizeFIPS(stateFIPS + countyFIPS) // 5-digit county FIPS
	if len(fips5) != 5 {
		return nil, fmt.Errorf("hrsa: invalid county FIPS %q+%q (must produce 5 digits)", stateFIPS, countyFIPS)
	}

	byCounty, err := s.fetchAll(ctx)
	if err != nil {
		return nil, err
	}

	data, ok := byCounty[fips5]
	if !ok {
		// No HRSA designations for this county — return zero indicators
		// (all variables will be nil/missing for this geography).
		return nil, nil
	}

	return s.countyDataToIndicators(fips5, data), nil
}

// FetchState fetches all HRSA indicators for every county in a state.
// The results are returned as county-level GEOIDs (5-digit FIPS).
// Callers that need tract-level assignment should propagate these by prefix.
func (s *hrsaSource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	fips2 := sanitizeFIPS(stateFIPS)
	if len(fips2) != 2 {
		return nil, fmt.Errorf("hrsa: invalid state FIPS %q (must be 2 digits)", stateFIPS)
	}

	byCounty, err := s.fetchAll(ctx)
	if err != nil {
		return nil, err
	}

	var out []store.Indicator
	for countyFIPS, data := range byCounty {
		if !strings.HasPrefix(countyFIPS, fips2) {
			continue
		}
		out = append(out, s.countyDataToIndicators(countyFIPS, data)...)
	}
	return out, nil
}

// countyRecord holds the aggregated HRSA data for one county.
type countyRecord struct {
	// Max active HPSA scores per discipline (nil if no active designation).
	primaryCareScore  *float64
	dentalScore       *float64
	mentalHealthScore *float64
	// hasAnyDesignation is true if any of the three disciplines has an active designation.
	hasAnyDesignation bool
	// fqhcCount is the number of active FQHC sites in the county.
	fqhcCount int
}

// fetchAll downloads all four CSVs and aggregates the data into a map
// keyed by 5-digit county FIPS.
func (s *hrsaSource) fetchAll(ctx context.Context) (map[string]*countyRecord, error) {
	// Fetch all three HPSA disciplines.
	pcRows, err := s.csvRows(ctx, hrsaHPSAPCURL)
	if err != nil {
		return nil, fmt.Errorf("hrsa: primary care HPSA: %w", err)
	}
	time.Sleep(hrsaRateDelay)

	dhRows, err := s.csvRows(ctx, hrsaHPSADHURL)
	if err != nil {
		return nil, fmt.Errorf("hrsa: dental HPSA: %w", err)
	}
	time.Sleep(hrsaRateDelay)

	mhRows, err := s.csvRows(ctx, hrsaHPSAMHURL)
	if err != nil {
		return nil, fmt.Errorf("hrsa: mental health HPSA: %w", err)
	}
	time.Sleep(hrsaRateDelay)

	fqhcRows, err := s.csvRows(ctx, hrsaFQHCURL)
	if err != nil {
		return nil, fmt.Errorf("hrsa: FQHC sites: %w", err)
	}

	byCounty := make(map[string]*countyRecord)

	s.aggregateHPSA(byCounty, pcRows, "primary_care")
	s.aggregateHPSA(byCounty, dhRows, "dental")
	s.aggregateHPSA(byCounty, mhRows, "mental_health")
	s.aggregateFQHC(byCounty, fqhcRows)

	return byCounty, nil
}

// csvRows downloads and parses a CSV from url, returning all rows including
// the header. Results are cached in-process.
func (s *hrsaSource) csvRows(ctx context.Context, url string) ([][]string, error) {
	s.cacheMu.Lock()
	if rows, ok := s.cache[url]; ok {
		s.cacheMu.Unlock()
		return rows, nil
	}
	s.cacheMu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "text/csv,application/octet-stream")

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
	// Allow variable field counts per row — HRSA CSVs have a trailing comma on
	// the header that adds a phantom empty field, making the header count differ
	// from data rows. -1 disables the strict per-record field-count check.
	r.FieldsPerRecord = -1

	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse csv: %w", err)
	}

	s.cacheMu.Lock()
	s.cache[url] = rows
	s.cacheMu.Unlock()

	return rows, nil
}

// hpsaColIdx holds the column indices for HPSA CSVs.
type hpsaColIdx struct {
	countyFIPS  int // "State and County Federal Information Processing Standard Code"
	score       int // "HPSA Score"
	statusCode  int // "HPSA Status Code"
}

// findHPSACols locates required column indices from the HPSA CSV header row.
// Returns an error if any required column is missing.
func findHPSACols(header []string) (hpsaColIdx, error) {
	idx := hpsaColIdx{countyFIPS: -1, score: -1, statusCode: -1}
	for i, col := range header {
		switch strings.TrimSpace(col) {
		case "State and County Federal Information Processing Standard Code":
			idx.countyFIPS = i
		case "HPSA Score":
			idx.score = i
		case "HPSA Status Code":
			idx.statusCode = i
		}
	}
	if idx.countyFIPS < 0 {
		return idx, fmt.Errorf("hrsa: HPSA CSV missing column 'State and County Federal Information Processing Standard Code'")
	}
	if idx.score < 0 {
		return idx, fmt.Errorf("hrsa: HPSA CSV missing column 'HPSA Score'")
	}
	if idx.statusCode < 0 {
		return idx, fmt.Errorf("hrsa: HPSA CSV missing column 'HPSA Status Code'")
	}
	return idx, nil
}

// aggregateHPSA processes HPSA rows and updates byCounty.
// discipline must be one of "primary_care", "dental", "mental_health".
func (s *hrsaSource) aggregateHPSA(byCounty map[string]*countyRecord, rows [][]string, discipline string) {
	if len(rows) < 2 {
		return
	}
	idx, err := findHPSACols(rows[0])
	if err != nil {
		// Silently skip if the schema changed — callers will get nil indicators.
		return
	}

	for _, row := range rows[1:] {
		if len(row) <= idx.countyFIPS || len(row) <= idx.score || len(row) <= idx.statusCode {
			continue
		}
		countyFIPS := strings.TrimSpace(row[idx.countyFIPS])
		countyFIPS = sanitizeFIPS(countyFIPS)
		if len(countyFIPS) != 5 {
			continue
		}

		statusCode := strings.TrimSpace(row[idx.statusCode])
		if statusCode != hpsaStatusDesignated {
			// Only count currently active (Designated) HPSAs.
			continue
		}

		scoreStr := strings.TrimSpace(row[idx.score])
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			continue
		}

		rec := s.getOrCreate(byCounty, countyFIPS)
		rec.hasAnyDesignation = true

		switch discipline {
		case "primary_care":
			if rec.primaryCareScore == nil || score > *rec.primaryCareScore {
				v := score
				rec.primaryCareScore = &v
			}
		case "dental":
			if rec.dentalScore == nil || score > *rec.dentalScore {
				v := score
				rec.dentalScore = &v
			}
		case "mental_health":
			if rec.mentalHealthScore == nil || score > *rec.mentalHealthScore {
				v := score
				rec.mentalHealthScore = &v
			}
		}
	}
}

// fqhcColIdx holds column indices for the FQHC CSV.
type fqhcColIdx struct {
	countyFIPS int // "State and County Federal Information Processing Standard Code"
	status     int // "Site Status Description"
}

// findFQHCCols locates required column indices from the FQHC CSV header.
func findFQHCCols(header []string) (fqhcColIdx, error) {
	idx := fqhcColIdx{countyFIPS: -1, status: -1}
	for i, col := range header {
		switch strings.TrimSpace(col) {
		case "State and County Federal Information Processing Standard Code":
			idx.countyFIPS = i
		case "Site Status Description":
			idx.status = i
		}
	}
	if idx.countyFIPS < 0 {
		return idx, fmt.Errorf("hrsa: FQHC CSV missing column 'State and County Federal Information Processing Standard Code'")
	}
	if idx.status < 0 {
		return idx, fmt.Errorf("hrsa: FQHC CSV missing column 'Site Status Description'")
	}
	return idx, nil
}

// aggregateFQHC processes FQHC site rows and counts active sites per county.
func (s *hrsaSource) aggregateFQHC(byCounty map[string]*countyRecord, rows [][]string) {
	if len(rows) < 2 {
		return
	}
	idx, err := findFQHCCols(rows[0])
	if err != nil {
		return
	}

	for _, row := range rows[1:] {
		if len(row) <= idx.countyFIPS || len(row) <= idx.status {
			continue
		}
		countyFIPS := strings.TrimSpace(row[idx.countyFIPS])
		countyFIPS = sanitizeFIPS(countyFIPS)
		if len(countyFIPS) != 5 {
			continue
		}

		status := strings.TrimSpace(row[idx.status])
		if !strings.EqualFold(status, "Active") {
			continue
		}

		rec := s.getOrCreate(byCounty, countyFIPS)
		rec.fqhcCount++
	}
}

// getOrCreate returns the countyRecord for fips5, creating it if absent.
func (s *hrsaSource) getOrCreate(m map[string]*countyRecord, fips5 string) *countyRecord {
	if rec, ok := m[fips5]; ok {
		return rec
	}
	rec := &countyRecord{}
	m[fips5] = rec
	return rec
}

// countyDataToIndicators converts a countyRecord to store.Indicator slice.
// The GEOID is the 5-digit county FIPS code (county-level output).
func (s *hrsaSource) countyDataToIndicators(fips5 string, rec *countyRecord) []store.Indicator {
	out := make([]store.Indicator, 0, 5)

	makeIndicator := func(varID string, val *float64, raw string) store.Indicator {
		return store.Indicator{
			GEOID:      fips5,
			VariableID: varID,
			Vintage:    s.vintage,
			Value:      val,
			RawValue:   raw,
		}
	}

	// Primary care HPSA score.
	pcRaw := ""
	if rec.primaryCareScore != nil {
		pcRaw = strconv.FormatFloat(*rec.primaryCareScore, 'f', -1, 64)
	}
	out = append(out, makeIndicator("hrsa_hpsa_primary_care", rec.primaryCareScore, pcRaw))

	// Dental HPSA score.
	dhRaw := ""
	if rec.dentalScore != nil {
		dhRaw = strconv.FormatFloat(*rec.dentalScore, 'f', -1, 64)
	}
	out = append(out, makeIndicator("hrsa_hpsa_dental", rec.dentalScore, dhRaw))

	// Mental health HPSA score.
	mhRaw := ""
	if rec.mentalHealthScore != nil {
		mhRaw = strconv.FormatFloat(*rec.mentalHealthScore, 'f', -1, 64)
	}
	out = append(out, makeIndicator("hrsa_hpsa_mental_health", rec.mentalHealthScore, mhRaw))

	// Designation flag (1 if any active HPSA, 0 otherwise).
	var desigVal float64
	desigRaw := "0"
	if rec.hasAnyDesignation {
		desigVal = 1
		desigRaw = "1"
	}
	out = append(out, makeIndicator("hrsa_hpsa_designation", &desigVal, desigRaw))

	// FQHC count.
	fqhcVal := float64(rec.fqhcCount)
	fqhcRaw := strconv.Itoa(rec.fqhcCount)
	out = append(out, makeIndicator("hrsa_fqhc_count", &fqhcVal, fqhcRaw))

	return out
}
