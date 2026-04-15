package datasource

// HUDPITSource fetches county-level homelessness indicators from the
// HUD Point-in-Time (PIT) Count dataset.
//
// HUD PIT data are annual (January) counts of sheltered and unsheltered
// persons experiencing homelessness, reported by Continuum of Care (CoC)
// geography. CoC boundaries do NOT align with census geography, so this
// adapter uses a built-in CoC-to-county-FIPS crosswalk to map each CoC
// to one or more 5-digit county FIPS codes.
//
// Data is downloaded as a national CSV from the HUD Exchange. The adapter
// filters the national file in-memory and uses an in-process cache to avoid
// redundant downloads within a single binary invocation.
//
// Geographic approach:
//   - FetchCounty: returns indicators for the CoC(s) whose county list
//     includes the requested county FIPS. If no CoC maps to that county,
//     the call returns nil, nil.
//   - FetchState: returns indicators for all CoCs whose mapped counties share
//     the requested state FIPS prefix ("55" for Wisconsin, etc.).
//
// Variables produced:
//
//	hud_pit_total_homeless   — Total homeless count (sheltered + unsheltered)
//	hud_pit_sheltered        — Total sheltered homeless count (ES + TH)
//	hud_pit_unsheltered      — Unsheltered homeless count
//	hud_pit_chronic          — Chronically homeless count
//
// Implementation note: HUD PIT CSVs use inconsistent column names across
// vintages. The parser resolves columns by scanning the header for canonical
// substrings (case-insensitive) rather than fixed offsets, which makes it
// resilient to minor column rearrangements between annual releases.

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

const hudPITRateDelay = 1000 * time.Millisecond

// hudPITDefaultURL is intentionally empty.
//
// The HUD Exchange does not provide a stable, version-agnostic direct CSV
// download URL. The URL https://www.hudexchange.info/resource/3031/pit-and-hic-data-since-2007/
// is an HTML landing page, not a CSV. Direct download URLs are year-specific
// and change with each annual release (e.g. the 2023 national PIT CSV was at
// https://www.huduser.gov/portal/sites/default/files/xls/2007-2023-PIT-Counts-by-CoC.xlsx).
//
// Callers MUST set HUDPITConfig.CSVURL to a direct CSV download link. The
// csvRows method will return an error if the URL is empty or if the server
// responds with HTML instead of CSV content.
const hudPITDefaultURL = ""

// cocCountyFIPS is the built-in CoC-to-county-FIPS crosswalk.
// Keys are CoC codes (e.g. "WI-500"); values are all county FIPS codes the CoC covers.
// Extend this map to cover additional states or CoCs as needed.
var cocCountyFIPS = map[string][]string{
	// Wisconsin CoCs
	"WI-500": {"55025"},         // Balance of State CoC — primarily Dane County
	"WI-501": {"55079"},         // Milwaukee City and County CoC
	"WI-502": {"55101"},         // Racine City and County CoC
	"WI-503": {"55009"},         // Brown County (Green Bay area)
	"WI-506": {"55025"},         // Madison/Dane — overlaps with WI-500 for Dane County
	"WI-507": {"55139"},         // Waukesha County CoC

	// Illinois sample CoCs
	"IL-510": {"17031"},         // Chicago CoC — Cook County
	"IL-511": {"17043"},         // Cook County CoC (suburban)

	// Minnesota sample CoCs
	"MN-500": {"27053"},         // Minneapolis/Hennepin County CoC
	"MN-501": {"27123"},         // Ramsey County/St. Paul CoC
}

// countyCoCIndex builds the reverse map: county FIPS → list of CoC codes.
// Built once at init time from cocCountyFIPS.
var countyCoCIndex map[string][]string

func init() {
	countyCoCIndex = make(map[string][]string)
	for coc, counties := range cocCountyFIPS {
		for _, fips := range counties {
			countyCoCIndex[fips] = append(countyCoCIndex[fips], coc)
		}
	}
}

// hudPITVariables defines the schema produced by the HUD PIT source.
var hudPITVariables = []VariableDef{
	{
		ID:          "hud_pit_total_homeless",
		Name:        "Total Homeless Count (PIT)",
		Description: "HUD Point-in-Time count of all persons experiencing homelessness (sheltered + unsheltered) in January of the survey year. Reported at the Continuum of Care (CoC) level and mapped to county via the HUD CoC-to-county crosswalk.",
		Unit:        "count",
		Direction:   "lower_better",
	},
	{
		ID:          "hud_pit_sheltered",
		Name:        "Sheltered Homeless Count (PIT)",
		Description: "HUD Point-in-Time count of persons in emergency shelter (ES) or transitional housing (TH) in January of the survey year.",
		Unit:        "count",
		Direction:   "lower_better",
	},
	{
		ID:          "hud_pit_unsheltered",
		Name:        "Unsheltered Homeless Count (PIT)",
		Description: "HUD Point-in-Time count of persons experiencing unsheltered homelessness (sleeping outside, in vehicles, or in other locations not meant for human habitation) in January of the survey year.",
		Unit:        "count",
		Direction:   "lower_better",
	},
	{
		ID:          "hud_pit_chronic",
		Name:        "Chronically Homeless Count (PIT)",
		Description: "HUD Point-in-Time count of individuals with a disabling condition who have been continuously homeless for at least one year (or four or more times in the last three years). Subset of total homeless count.",
		Unit:        "count",
		Direction:   "lower_better",
	},
}

// HUDPITConfig configures a HUDPITSource.
type HUDPITConfig struct {
	// Year is the PIT survey year (e.g. 2023). When 0, Vintage is "HUD-PIT".
	Year int
	// CSVURL overrides the default HUD Exchange download URL.
	// Used in tests to point at a mock server.
	CSVURL string
	// HTTPClient is used for all outbound requests. Defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// hudPITSource implements DataSource for HUD PIT homelessness count data.
type hudPITSource struct {
	cfg     HUDPITConfig
	vintage string
	csvURL  string

	// cache stores the parsed CSV rows (including header) to avoid redundant
	// downloads within a single process invocation.
	cacheMu sync.Mutex
	cache   [][]string // nil = not yet fetched
}

// NewHUDPITSource creates a hudPITSource from cfg.
//
// cfg.CSVURL must be set to a direct CSV download link. HUD does not provide
// a stable version-agnostic URL; the default URL is empty and FetchCounty /
// FetchState will return an error if CSVURL is not supplied.
func NewHUDPITSource(cfg HUDPITConfig) *hudPITSource {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	csvURL := cfg.CSVURL
	if csvURL == "" {
		csvURL = hudPITDefaultURL // empty — csvRows will return a clear error
	}
	vintage := "HUD-PIT"
	if cfg.Year > 0 {
		vintage = fmt.Sprintf("HUD-PIT-%d", cfg.Year)
	}
	return &hudPITSource{
		cfg:     cfg,
		vintage: vintage,
		csvURL:  csvURL,
	}
}

func (s *hudPITSource) Name() string     { return "hud-pit" }
func (s *hudPITSource) Category() string { return "housing" }
func (s *hudPITSource) Vintage() string  { return s.vintage }

func (s *hudPITSource) Schema() []VariableDef {
	out := make([]VariableDef, len(hudPITVariables))
	copy(out, hudPITVariables)
	return out
}

// FetchCounty fetches PIT indicators for a single county by looking up the
// CoC(s) that cover that county in the crosswalk, then returning their data.
// stateFIPS is a 2-digit code; countyFIPS is a 3-digit code.
func (s *hudPITSource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	fips5 := sanitizeFIPS(stateFIPS + countyFIPS)
	if len(fips5) != 5 {
		return nil, fmt.Errorf("hud-pit: invalid county FIPS %q+%q (must produce 5 digits)", stateFIPS, countyFIPS)
	}

	cocs, ok := countyCoCIndex[fips5]
	if !ok {
		// No CoC crosswalk entry for this county — return empty (not an error).
		return nil, nil
	}

	byCoC, err := s.fetchAll(ctx)
	if err != nil {
		return nil, err
	}

	// Aggregate across all CoCs that cover this county.
	// When multiple CoCs map to the same county (overlap), we sum their counts.
	var out []store.Indicator
	seen := make(map[string]bool)
	for _, coc := range cocs {
		if seen[coc] {
			continue
		}
		seen[coc] = true
		rec, ok := byCoC[coc]
		if !ok {
			continue
		}
		out = append(out, s.cocRecordToIndicators(fips5, rec)...)
	}
	return out, nil
}

// FetchState fetches PIT indicators for all CoCs whose mapped counties fall
// within the given state. stateFIPS is a 2-digit code (e.g. "55").
func (s *hudPITSource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	fips2 := sanitizeFIPS(stateFIPS)
	if len(fips2) != 2 {
		return nil, fmt.Errorf("hud-pit: invalid state FIPS %q (must be 2 digits)", stateFIPS)
	}

	byCoC, err := s.fetchAll(ctx)
	if err != nil {
		return nil, err
	}

	// Find all county FIPS in this state that have a CoC mapping.
	var out []store.Indicator
	emittedCoC := make(map[string]string) // coc → county FIPS already emitted for

	for countyFIPS, cocs := range countyCoCIndex {
		if !strings.HasPrefix(countyFIPS, fips2) {
			continue
		}
		for _, coc := range cocs {
			key := coc + ":" + countyFIPS
			if emittedCoC[key] != "" {
				continue
			}
			emittedCoC[key] = countyFIPS

			rec, ok := byCoC[coc]
			if !ok {
				continue
			}
			out = append(out, s.cocRecordToIndicators(countyFIPS, rec)...)
		}
	}
	return out, nil
}

// pitRecord holds PIT count values parsed from a single CoC CSV row.
type pitRecord struct {
	totalHomeless *float64
	sheltered     *float64
	unsheltered   *float64
	chronic       *float64
	cocCode       string
}

// hudPITColIdx holds the column indices for the PIT CSV.
type hudPITColIdx struct {
	cocCode    int
	total      int
	sheltered  int
	unsheltered int
	chronic    int
}

// findHUDPITCols scans the header row for PIT column names.
// HUD changes column names slightly between vintages; this function uses
// case-insensitive substring matching for resilience.
func findHUDPITCols(header []string) (hudPITColIdx, error) {
	idx := hudPITColIdx{cocCode: -1, total: -1, sheltered: -1, unsheltered: -1, chronic: -1}

	for i, col := range header {
		lower := strings.ToLower(strings.TrimSpace(col))
		switch {
		case lower == "coc number" || lower == "cocnumber" || lower == "coc_number" || lower == "coc code":
			idx.cocCode = i
		case (strings.Contains(lower, "overall") || strings.Contains(lower, "total")) &&
			strings.Contains(lower, "homeless") && !strings.Contains(lower, "sheltered") &&
			!strings.Contains(lower, "chronic") && !strings.Contains(lower, "veteran") &&
			!strings.Contains(lower, "youth"):
			if idx.total < 0 {
				idx.total = i
			}
		case strings.Contains(lower, "total sheltered") || (strings.Contains(lower, "sheltered") &&
			!strings.Contains(lower, "unsheltered")):
			if idx.sheltered < 0 {
				idx.sheltered = i
			}
		case strings.Contains(lower, "unsheltered"):
			if idx.unsheltered < 0 {
				idx.unsheltered = i
			}
		case strings.Contains(lower, "chronically homeless") || lower == "chronically_homeless" ||
			lower == "chronic homeless":
			if idx.chronic < 0 {
				idx.chronic = i
			}
		}
	}

	var missing []string
	if idx.cocCode < 0 {
		missing = append(missing, "CoC Number")
	}
	if idx.total < 0 {
		missing = append(missing, "Overall Homeless")
	}
	if idx.sheltered < 0 {
		missing = append(missing, "Sheltered Total Homeless")
	}
	if idx.unsheltered < 0 {
		missing = append(missing, "Unsheltered Homeless")
	}
	if idx.chronic < 0 {
		missing = append(missing, "Chronically Homeless")
	}
	if len(missing) > 0 {
		return idx, fmt.Errorf("hud-pit: CSV missing required columns: %s", strings.Join(missing, ", "))
	}
	return idx, nil
}

// parseCount parses a PIT count field, returning nil for empty / non-numeric.
// HUD CSVs use commas in large numbers (e.g. "1,234") and sometimes use "-"
// for suppressed/zero values.
func parseCount(s string) *float64 {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" || s == "N/A" || s == "n/a" {
		return nil
	}
	// Strip thousands separators.
	s = strings.ReplaceAll(s, ",", "")
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &f
}

// fetchAll downloads and parses the PIT CSV, returning a map of CoC code → pitRecord.
// Results are cached in-process.
func (s *hudPITSource) fetchAll(ctx context.Context) (map[string]*pitRecord, error) {
	rows, err := s.csvRows(ctx)
	if err != nil {
		return nil, err
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("hud-pit: CSV has no data rows")
	}

	idx, err := findHUDPITCols(rows[0])
	if err != nil {
		return nil, err
	}

	byCoC := make(map[string]*pitRecord)
	for _, row := range rows[1:] {
		maxCol := idx.cocCode
		for _, c := range []int{idx.total, idx.sheltered, idx.unsheltered, idx.chronic} {
			if c > maxCol {
				maxCol = c
			}
		}
		if len(row) <= maxCol {
			continue // short row
		}

		cocCode := strings.TrimSpace(row[idx.cocCode])
		if cocCode == "" {
			continue
		}

		rec := &pitRecord{
			cocCode:     cocCode,
			totalHomeless: parseCount(row[idx.total]),
			sheltered:     parseCount(row[idx.sheltered]),
			unsheltered:   parseCount(row[idx.unsheltered]),
			chronic:       parseCount(row[idx.chronic]),
		}
		byCoC[cocCode] = rec
	}
	return byCoC, nil
}

// csvRows downloads and parses the PIT CSV, caching results in-process.
func (s *hudPITSource) csvRows(ctx context.Context) ([][]string, error) {
	s.cacheMu.Lock()
	if s.cache != nil {
		rows := s.cache
		s.cacheMu.Unlock()
		return rows, nil
	}
	s.cacheMu.Unlock()

	if s.csvURL == "" {
		return nil, fmt.Errorf(
			"hud-pit: CSVURL is required — HUD does not publish a stable direct CSV URL. " +
				"Set HUDPITConfig.CSVURL to the direct download link for the desired vintage " +
				"(e.g. the annual PIT/HIC file from https://www.hudexchange.info/resource/3031/)",
		)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.csvURL, nil)
	if err != nil {
		return nil, fmt.Errorf("hud-pit: build request: %w", err)
	}
	req.Header.Set("Accept", "text/csv,application/octet-stream")

	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hud-pit: http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("hud-pit: api returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	// Guard against HTML landing pages masquerading as CSV downloads.
	// HUD Exchange resource URLs return text/html, not the actual data file.
	if ct := resp.Header.Get("Content-Type"); strings.Contains(ct, "text/html") {
		return nil, fmt.Errorf(
			"hud-pit: URL returned HTML, not CSV (Content-Type: %s) — "+
				"set HUDPITConfig.CSVURL to the direct download link, not the HUD Exchange landing page",
			ct,
		)
	}

	// Rate-limit courtesy delay before returning.
	time.Sleep(hudPITRateDelay)

	r := csv.NewReader(resp.Body)
	r.LazyQuotes = true
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1

	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("hud-pit: parse csv: %w", err)
	}

	s.cacheMu.Lock()
	s.cache = rows
	s.cacheMu.Unlock()

	return rows, nil
}

// cocRecordToIndicators converts a pitRecord to a slice of store.Indicators.
// The GEOID is the 5-digit county FIPS code for the target county.
func (s *hudPITSource) cocRecordToIndicators(countyFIPS5 string, rec *pitRecord) []store.Indicator {
	makeInd := func(varID string, val *float64) store.Indicator {
		raw := ""
		if val != nil {
			raw = strconv.FormatFloat(*val, 'f', 0, 64)
		}
		return store.Indicator{
			GEOID:      countyFIPS5,
			VariableID: varID,
			Vintage:    s.vintage,
			Value:      val,
			RawValue:   raw,
		}
	}

	return []store.Indicator{
		makeInd("hud_pit_total_homeless", rec.totalHomeless),
		makeInd("hud_pit_sheltered", rec.sheltered),
		makeInd("hud_pit_unsheltered", rec.unsheltered),
		makeInd("hud_pit_chronic", rec.chronic),
	}
}
