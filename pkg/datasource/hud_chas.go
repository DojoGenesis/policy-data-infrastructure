package datasource

// HUDCHASSource fetches tract-level housing affordability indicators from
// HUD's Comprehensive Housing Affordability Strategy (CHAS) dataset.
//
// CHAS data are custom tabulations of American Community Survey microdata
// produced by HUD at a 2–3 year lag relative to the ACS vintage. The data
// are distributed as a ZIP archive containing a single tract-level CSV at:
//
//	https://www.huduser.gov/portal/datasets/cp/{year}thru{year+4}-140-csv.zip
//
// (where "140" is the Census summary level code for census tracts)
//
// The adapter downloads the ZIP once per process invocation, extracts the
// tract CSV in-memory, and derives five housing affordability indicators:
//
//	hud_cost_burden_30pct   — % households paying >30% income on housing
//	hud_cost_burden_50pct   — % households paying >50% income on housing
//	hud_housing_problems    — % households with 1+ housing problems
//	hud_eli_renters         — % renters who are extremely low income (≤30% AMI)
//	hud_overcrowded         — % households that are overcrowded (>1 person/room)
//
// Geographic level: census tract (11-digit FIPS GEOID). FetchCounty filters by
// the 5-digit county FIPS prefix. FetchState filters by the 2-digit state FIPS
// prefix. Both methods download the full national file and filter in-memory;
// the file is cached for the lifetime of the process.
//
// Column mapping (CHAS tract CSV):
//
//	geoid             — 11-digit tract FIPS (e.g. "55025000100")
//	T1_est1           — Total households (denominator for cost burden / problems)
//	T8_est10          — Renter households, cost burden >30% but ≤50% (income ≤80% AMI)
//	T8_est11          — Renter households, cost burden >50% (income ≤80% AMI)
//	T9_est10          — Owner households, cost burden >30% but ≤50% (income ≤80% AMI)
//	T9_est11          — Owner households, cost burden >50% (income ≤80% AMI)
//	T1_est4           — Households with 1 or more housing problems
//	T4_est3           — Renter households, income ≤30% AMI (ELI renters)
//	T4_est2           — Total renter households (denominator for ELI renter %)
//	T1_est11          — Overcrowded households (>1 person per room)
//
// Note: The column names in the CSV are stable across recent CHAS vintages
// (2012–2020). If HUD changes the schema in a future release, the adapter
// will fail at the column-lookup phase and return an explicit error listing
// the missing column name.

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// chasBaseURL is the HUD User bulk download base URL pattern.
// The archive contains a single file named "Table{N}.csv" at the 140 level.
// For the 2016–2020 5-year vintage the tract file is at:
//
//	https://www.huduser.gov/portal/datasets/cp/2016thru2020-140-csv.zip
const chasBaseURL = "https://www.huduser.gov/portal/datasets/cp/%dthru%d-140-csv.zip"

// chasVariables defines the schema produced by the CHAS source.
var chasVariables = []VariableDef{
	{
		ID:          "hud_cost_burden_30pct",
		Name:        "Cost Burden >30% (HUD CHAS)",
		Description: "Percentage of households paying more than 30 percent of household income on housing costs (rent or mortgage plus utilities). Derived from HUD CHAS Tables 8 and 9.",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	{
		ID:          "hud_cost_burden_50pct",
		Name:        "Severe Cost Burden >50% (HUD CHAS)",
		Description: "Percentage of households paying more than 50 percent of household income on housing costs (severe cost burden). Derived from HUD CHAS Tables 8 and 9.",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	{
		ID:          "hud_housing_problems",
		Name:        "1+ Housing Problems (HUD CHAS)",
		Description: "Percentage of households experiencing at least one of the following housing problems: cost burden >30%, overcrowding, incomplete kitchen facilities, or incomplete plumbing. Derived from HUD CHAS Table 1.",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	{
		ID:          "hud_eli_renters",
		Name:        "Extremely Low-Income Renters (HUD CHAS)",
		Description: "Percentage of renter households with income at or below 30 percent of Area Median Income (extremely low income, ELI). Derived from HUD CHAS Table 4.",
		Unit:        "percent",
		Direction:   "neutral",
	},
	{
		ID:          "hud_overcrowded",
		Name:        "Overcrowded Households (HUD CHAS)",
		Description: "Percentage of households that are overcrowded, defined as having more than 1.0 person per room. Derived from HUD CHAS Table 1.",
		Unit:        "percent",
		Direction:   "lower_better",
	},
}

// HUDCHASConfig configures a HUDCHASSource.
type HUDCHASConfig struct {
	// Year is the ending year of the 5-year ACS vintage used by CHAS.
	// For example, Year=2020 downloads the 2016–2020 CHAS data.
	// When 0, defaults to 2020.
	Year int
	// HTTPClient is used for all outbound requests. Defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// hudCHASSource implements DataSource for HUD CHAS tract-level data.
type hudCHASSource struct {
	cfg     HUDCHASConfig
	vintage string
	zipURL  string

	// cache holds the parsed CSV rows from the national tract file.
	// Populated on first call to fetchRows; subsequent calls return the
	// cached result without re-downloading.
	cacheMu sync.Mutex
	cache   [][]string // rows including header; nil = not yet downloaded
}

// NewHUDCHASSource creates a HUDCHASSource from cfg.
func NewHUDCHASSource(cfg HUDCHASConfig) *hudCHASSource {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Year == 0 {
		cfg.Year = 2020
	}
	startYear := cfg.Year - 4
	vintage := fmt.Sprintf("CHAS-%d", cfg.Year)
	url := fmt.Sprintf(chasBaseURL, startYear, cfg.Year)
	return &hudCHASSource{
		cfg:     cfg,
		vintage: vintage,
		zipURL:  url,
	}
}

func (s *hudCHASSource) Name() string     { return "hud-chas" }
func (s *hudCHASSource) Category() string { return "housing" }
func (s *hudCHASSource) Vintage() string  { return s.vintage }

func (s *hudCHASSource) Schema() []VariableDef {
	out := make([]VariableDef, len(chasVariables))
	copy(out, chasVariables)
	return out
}

// FetchCounty fetches all CHAS indicators for a single county.
// stateFIPS is a 2-digit code; countyFIPS is a 3-digit code.
// Returns tract-level indicators whose GEOID begins with the 5-digit county FIPS.
func (s *hudCHASSource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	fips5 := sanitizeFIPS(stateFIPS + countyFIPS)
	if len(fips5) != 5 {
		return nil, fmt.Errorf("hud-chas: invalid county FIPS %q+%q (must produce 5 digits)", stateFIPS, countyFIPS)
	}

	rows, err := s.fetchRows(ctx)
	if err != nil {
		return nil, err
	}

	return s.parseRows(rows, fips5, true)
}

// FetchState fetches all CHAS indicators for every tract in a state.
// stateFIPS is a 2-digit code.
// Returns tract-level indicators whose GEOID begins with the 2-digit state FIPS.
func (s *hudCHASSource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	fips2 := sanitizeFIPS(stateFIPS)
	if len(fips2) != 2 {
		return nil, fmt.Errorf("hud-chas: invalid state FIPS %q (must be 2 digits)", stateFIPS)
	}

	rows, err := s.fetchRows(ctx)
	if err != nil {
		return nil, err
	}

	return s.parseRows(rows, fips2, false)
}

// fetchRows downloads and caches the tract-level CHAS CSV rows.
// The ZIP is downloaded once per process; subsequent calls return the cached rows.
func (s *hudCHASSource) fetchRows(ctx context.Context) ([][]string, error) {
	s.cacheMu.Lock()
	if s.cache != nil {
		rows := s.cache
		s.cacheMu.Unlock()
		return rows, nil
	}
	s.cacheMu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.zipURL, nil)
	if err != nil {
		return nil, fmt.Errorf("hud-chas: build request: %w", err)
	}

	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hud-chas: http get %s: %w", s.zipURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("hud-chas: api returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	// Read the full body so we can pass it to zip.NewReader (which needs io.ReaderAt).
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("hud-chas: read zip body: %w", err)
	}

	rows, err := s.extractCSVFromZip(body)
	if err != nil {
		return nil, err
	}

	s.cacheMu.Lock()
	s.cache = rows
	s.cacheMu.Unlock()

	return rows, nil
}

// extractCSVFromZip extracts the first CSV file found in the ZIP body and
// parses it into a slice of rows (including the header).
func (s *hudCHASSource) extractCSVFromZip(zipBody []byte) ([][]string, error) {
	zr, err := zip.NewReader(bytes.NewReader(zipBody), int64(len(zipBody)))
	if err != nil {
		return nil, fmt.Errorf("hud-chas: open zip: %w", err)
	}

	// Find the first .csv file — the CHAS ZIP typically contains exactly one.
	var csvFile *zip.File
	for _, f := range zr.File {
		if strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			csvFile = f
			break
		}
	}
	if csvFile == nil {
		return nil, fmt.Errorf("hud-chas: no CSV file found in ZIP archive")
	}

	rc, err := csvFile.Open()
	if err != nil {
		return nil, fmt.Errorf("hud-chas: open %s in ZIP: %w", csvFile.Name, err)
	}
	defer rc.Close()

	r := csv.NewReader(rc)
	r.LazyQuotes = true
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1

	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("hud-chas: parse CSV %s: %w", csvFile.Name, err)
	}
	return rows, nil
}

// chasColIdx holds the column indices required to compute CHAS indicators.
type chasColIdx struct {
	geoid         int // "geoid" — 11-digit tract FIPS
	totalHH       int // T1_est1 — total households
	problemHH     int // T1_est4 — households with 1+ housing problems
	overcrowdedHH int // T1_est11 — overcrowded households
	totalRenters  int // T4_est2 — total renter households
	eliRenters    int // T4_est3 — ELI renter households (income ≤30% AMI)
	// Cost burden columns: renter >30–50%, renter >50%, owner >30–50%, owner >50%.
	renterCB30 int // T8_est10
	renterCB50 int // T8_est11
	ownerCB30  int // T9_est10
	ownerCB50  int // T9_est11
}

// findCHASCols locates required column indices in the CHAS CSV header.
// Returns an error if any required column is absent.
func findCHASCols(header []string) (chasColIdx, error) {
	idx := chasColIdx{
		geoid:         -1,
		totalHH:       -1,
		problemHH:     -1,
		overcrowdedHH: -1,
		totalRenters:  -1,
		eliRenters:    -1,
		renterCB30:    -1,
		renterCB50:    -1,
		ownerCB30:     -1,
		ownerCB50:     -1,
	}

	for i, col := range header {
		switch strings.TrimSpace(strings.ToLower(col)) {
		case "geoid":
			idx.geoid = i
		case "t1_est1":
			idx.totalHH = i
		case "t1_est4":
			idx.problemHH = i
		case "t1_est11":
			idx.overcrowdedHH = i
		case "t4_est2":
			idx.totalRenters = i
		case "t4_est3":
			idx.eliRenters = i
		case "t8_est10":
			idx.renterCB30 = i
		case "t8_est11":
			idx.renterCB50 = i
		case "t9_est10":
			idx.ownerCB30 = i
		case "t9_est11":
			idx.ownerCB50 = i
		}
	}

	required := []struct {
		field string
		val   int
	}{
		{"geoid", idx.geoid},
		{"T1_est1", idx.totalHH},
		{"T1_est4", idx.problemHH},
		{"T1_est11", idx.overcrowdedHH},
		{"T4_est2", idx.totalRenters},
		{"T4_est3", idx.eliRenters},
		{"T8_est10", idx.renterCB30},
		{"T8_est11", idx.renterCB50},
		{"T9_est10", idx.ownerCB30},
		{"T9_est11", idx.ownerCB50},
	}
	for _, req := range required {
		if req.val < 0 {
			return idx, fmt.Errorf("hud-chas: CSV missing required column %q", req.field)
		}
	}
	return idx, nil
}

// parseRows converts raw CSV rows (header + data) into store.Indicator slices,
// filtering by GEOID prefix. When byCounty is true, prefix is a 5-digit county
// FIPS; when false, prefix is a 2-digit state FIPS.
func (s *hudCHASSource) parseRows(rows [][]string, geoidPrefix string, byCounty bool) ([]store.Indicator, error) {
	if len(rows) < 2 {
		return nil, nil
	}

	idx, err := findCHASCols(rows[0])
	if err != nil {
		return nil, err
	}

	var out []store.Indicator

	for _, row := range rows[1:] {
		maxIdx := idx.geoid
		for _, v := range []int{idx.totalHH, idx.problemHH, idx.overcrowdedHH,
			idx.totalRenters, idx.eliRenters,
			idx.renterCB30, idx.renterCB50, idx.ownerCB30, idx.ownerCB50} {
			if v > maxIdx {
				maxIdx = v
			}
		}
		if len(row) <= maxIdx {
			continue
		}

		geoid := strings.TrimSpace(row[idx.geoid])
		// Normalize: CHAS sometimes prefixes with state FIPS digit padding.
		geoid = sanitizeFIPS(geoid)
		if len(geoid) != 11 {
			continue
		}

		if !strings.HasPrefix(geoid, geoidPrefix) {
			continue
		}

		totalHH := parseFloat(row[idx.totalHH])
		problemHH := parseFloat(row[idx.problemHH])
		overcrowdedHH := parseFloat(row[idx.overcrowdedHH])
		totalRenters := parseFloat(row[idx.totalRenters])
		eliRenters := parseFloat(row[idx.eliRenters])
		renterCB30 := parseFloat(row[idx.renterCB30])
		renterCB50 := parseFloat(row[idx.renterCB50])
		ownerCB30 := parseFloat(row[idx.ownerCB30])
		ownerCB50 := parseFloat(row[idx.ownerCB50])

		// Derive indicators as percentages.

		// hud_cost_burden_30pct: (renter CB30 + renter CB50 + owner CB30 + owner CB50) / totalHH
		var cb30Pct *float64
		if totalHH != nil && *totalHH > 0 &&
			renterCB30 != nil && renterCB50 != nil && ownerCB30 != nil && ownerCB50 != nil {
			v := 100.0 * (*renterCB30 + *renterCB50 + *ownerCB30 + *ownerCB50) / *totalHH
			cb30Pct = &v
		}

		// hud_cost_burden_50pct: (renter CB50 + owner CB50) / totalHH
		var cb50Pct *float64
		if totalHH != nil && *totalHH > 0 && renterCB50 != nil && ownerCB50 != nil {
			v := 100.0 * (*renterCB50 + *ownerCB50) / *totalHH
			cb50Pct = &v
		}

		// hud_housing_problems: problemHH / totalHH
		var problemPct *float64
		if totalHH != nil && *totalHH > 0 && problemHH != nil {
			v := 100.0 * *problemHH / *totalHH
			problemPct = &v
		}

		// hud_eli_renters: eliRenters / totalRenters
		var eliPct *float64
		if totalRenters != nil && *totalRenters > 0 && eliRenters != nil {
			v := 100.0 * *eliRenters / *totalRenters
			eliPct = &v
		}

		// hud_overcrowded: overcrowdedHH / totalHH
		var overcrowdedPct *float64
		if totalHH != nil && *totalHH > 0 && overcrowdedHH != nil {
			v := 100.0 * *overcrowdedHH / *totalHH
			overcrowdedPct = &v
		}

		makeInd := func(varID string, val *float64) store.Indicator {
			raw := ""
			if val != nil {
				raw = strconv.FormatFloat(*val, 'f', 4, 64)
			}
			return store.Indicator{
				GEOID:      geoid,
				VariableID: varID,
				Vintage:    s.vintage,
				Value:      val,
				RawValue:   raw,
			}
		}

		out = append(out,
			makeInd("hud_cost_burden_30pct", cb30Pct),
			makeInd("hud_cost_burden_50pct", cb50Pct),
			makeInd("hud_housing_problems", problemPct),
			makeInd("hud_eli_renters", eliPct),
			makeInd("hud_overcrowded", overcrowdedPct),
		)
	}

	return out, nil
}

// parseFloat parses a CSV cell as float64. Returns nil on empty or invalid input.
func parseFloat(s string) *float64 {
	s = strings.TrimSpace(s)
	if s == "" || s == "." || s == "N/A" {
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &v
}
