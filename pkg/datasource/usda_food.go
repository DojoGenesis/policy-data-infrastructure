package datasource

// USDAFoodSource fetches tract-level food access indicators from the USDA
// Economic Research Service (ERS) Food Access Research Atlas.
//
// The ERS publishes a national bulk download (ZIP archive containing a CSV)
// updated with each decennial Census / ACS revision cycle. No API key is
// required. The download URL changes with each vintage release; update
// usdaFoodDataURL when the ERS publishes a new vintage.
//
// Current vintage: 2019 (most recent as of 2025).
// Download page:
//
//	https://www.ers.usda.gov/data-products/food-access-research-atlas/
//
// Variables produced:
//
//	usda_food_desert     — Food desert flag: low-income + low-access tract (1=yes, 0=no)
//	usda_low_access_1mi  — Population with low access at 1-mile urban threshold
//	usda_low_access_10mi — Population with low access at 10-mile rural threshold
//	usda_snap_count      — SNAP-recipient population (or share, depending on vintage)
//	usda_grocery_count   — Grocery store count within the tract
//
// Geographic level: census tract (11-digit GEOID). FetchCounty filters by the
// 5-digit county FIPS prefix; FetchState filters by the 2-digit state prefix.
//
// Implementation notes:
//   - The ZIP archive is downloaded once per process invocation and cached in
//     memory; subsequent FetchCounty / FetchState calls reuse the parsed rows.
//   - Column names vary across ERS vintages; columnAliases maps known variants
//     to canonical names. Extend the map when the ERS changes the schema.
//   - The ERS distributes both a "share" form (fraction 0–1) and an absolute
//     count form for some variables. The adapter stores the raw string in
//     RawValue and parses to float64 for Value; callers should check units.
//   - GEOIDs are zero-padded to 11 digits per PDI convention.

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
	"time"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// usdaFoodDataURL is the current ERS Food Access Research Atlas bulk download.
// If this returns a 404, visit:
//
//	https://www.ers.usda.gov/data-products/food-access-research-atlas/
//
// to find the updated URL and update this constant.
const usdaFoodDataURL = "https://www.ers.usda.gov/media/5627/food-access-research-atlas-data-download-2019.zip"

// usdaRateDelay is the inter-request courtesy delay for ERS (~10 req/min).
const usdaRateDelay = 2000 * time.Millisecond

// usdaFoodVariables defines the schema produced by the USDA Food Access source.
var usdaFoodVariables = []VariableDef{
	{
		ID:          "usda_food_desert",
		Name:        "Food Desert Flag",
		Description: "Whether the census tract is classified as a food desert: simultaneously low-income and low-access (USDA FARA LILATracts definition). 1 = food desert, 0 = not a food desert.",
		Unit:        "count",
		Direction:   "lower_better",
	},
	{
		ID:          "usda_low_access_1mi",
		Name:        "Low-Access Population (1-Mile Urban)",
		Description: "Population living more than 1 mile from a supermarket or large grocery store (urban threshold). Source: USDA ERS LAPOP1 / LAPOP1_10 column.",
		Unit:        "count",
		Direction:   "lower_better",
	},
	{
		ID:          "usda_low_access_10mi",
		Name:        "Low-Access Population (10-Mile Rural)",
		Description: "Population living more than 10 miles from a supermarket or large grocery store (rural threshold). Source: USDA ERS LAPOP10 / LAPOP10_10 column.",
		Unit:        "count",
		Direction:   "lower_better",
	},
	{
		ID:          "usda_snap_count",
		Name:        "SNAP Recipients",
		Description: "Count or share of SNAP-benefit recipients residing in the tract. Source: USDA ERS TractSNAP / SNAP_1 / lasnap1share column (varies by vintage).",
		Unit:        "count",
		Direction:   "neutral",
	},
	{
		ID:          "usda_grocery_count",
		Name:        "Grocery Store Count",
		Description: "Number of supermarkets and large grocery stores located within the tract. Source: USDA ERS TractSuper / SuperCount column.",
		Unit:        "count",
		Direction:   "higher_better",
	},
}

// usdaCanonicalCol enumerates the canonical column names used internally.
type usdaCanonicalCol int

const (
	usdaColGEOID      usdaCanonicalCol = iota
	usdaColFoodDesert                  // LILATracts_1And10 / lilatracts_1and10
	usdaColLowAcc1Mi                   // LAPOP1_10 / LAPOP1 / lapop1_10
	usdaColLowAcc10Mi                  // LAPOP10_10 / LAPOP10 / lapop10
	usdaColSNAP                        // TractSNAP / SNAP_1 / lasnap1share
	usdaColGrocery                     // TractSuper / SuperCount / NumSupermarkets
)

// columnAliases maps known ERS source column names to canonical column IDs.
// The ERS renames columns between vintages; extend this map as needed.
var columnAliases = map[string]usdaCanonicalCol{
	// GEOID (tract)
	"CensusTract": usdaColGEOID,
	"census_tract": usdaColGEOID,
	"GEOID":        usdaColGEOID,
	"geoid":        usdaColGEOID,

	// Food desert flag
	"LILATracts_1And10":      usdaColFoodDesert,
	"lilatracts_1and10":      usdaColFoodDesert,
	"LILATracts_halfAnd10":   usdaColFoodDesert,
	"lilatracts_halfand10":   usdaColFoodDesert,
	"LowIncomeLowAccess":     usdaColFoodDesert,
	"low_income_low_access":  usdaColFoodDesert,

	// Low access 1 mile (population count or share)
	"LAPOP1_10":    usdaColLowAcc1Mi,
	"lapop1_10":    usdaColLowAcc1Mi,
	"LAPOP1":       usdaColLowAcc1Mi,
	"lapop1":       usdaColLowAcc1Mi,
	"LowAccess1Mi": usdaColLowAcc1Mi,

	// Low access 10 mile (population count or share)
	"LAPOP10_10":   usdaColLowAcc10Mi,
	"lapop10_10":   usdaColLowAcc10Mi,
	"LAPOP10":      usdaColLowAcc10Mi,
	"lapop10":      usdaColLowAcc10Mi,
	"lapop10share": usdaColLowAcc10Mi,
	"LowAccess10Mi": usdaColLowAcc10Mi,

	// SNAP recipients / share
	"TractSNAP":   usdaColSNAP,
	"tractsnap":   usdaColSNAP,
	"SNAP_1":      usdaColSNAP,
	"snap_1":      usdaColSNAP,
	"SNAPFlag":    usdaColSNAP,
	"SNAP":        usdaColSNAP,
	"lasnap1share": usdaColSNAP,

	// Grocery store count
	"TractSuper":      usdaColGrocery,
	"tractsuper":      usdaColGrocery,
	"SuperCount":      usdaColGrocery,
	"supercount":      usdaColGrocery,
	"NumSupermarkets": usdaColGrocery,
	"GROCPTH":         usdaColGrocery,
	"grocpth":         usdaColGrocery,
}

// variableIDForCol maps canonical column IDs to output variable IDs.
var variableIDForCol = map[usdaCanonicalCol]string{
	usdaColFoodDesert: "usda_food_desert",
	usdaColLowAcc1Mi:  "usda_low_access_1mi",
	usdaColLowAcc10Mi: "usda_low_access_10mi",
	usdaColSNAP:       "usda_snap_count",
	usdaColGrocery:    "usda_grocery_count",
}

// tractRecord holds the raw string values for a single census tract row.
type tractRecord struct {
	geoid  string
	values map[usdaCanonicalCol]string // canonical col → raw string value
}

// USDAFoodConfig configures a USDAFoodSource.
type USDAFoodConfig struct {
	// Year is the data vintage label used in the vintage string (e.g. 2019).
	// When 0, the vintage is "USDA-FARA" without a year suffix.
	Year int
	// DataURL overrides the default ERS download URL (useful for testing).
	DataURL string
	// HTTPClient is used for all outbound requests. Defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// usdaFoodSource implements DataSource for USDA Food Access Research Atlas data.
type usdaFoodSource struct {
	cfg     USDAFoodConfig
	vintage string

	// parsedMu guards parsed.
	parsedMu sync.Mutex
	// parsed caches the downloaded and parsed tract rows keyed by 11-digit GEOID.
	parsed map[string]*tractRecord
}

// NewUSDAFoodSource creates a USDAFoodSource from cfg.
func NewUSDAFoodSource(cfg USDAFoodConfig) *usdaFoodSource {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.DataURL == "" {
		cfg.DataURL = usdaFoodDataURL
	}
	vintage := "USDA-FARA"
	if cfg.Year > 0 {
		vintage = fmt.Sprintf("USDA-FARA-%d", cfg.Year)
	}
	return &usdaFoodSource{
		cfg:     cfg,
		vintage: vintage,
	}
}

func (s *usdaFoodSource) Name() string     { return "usda-foodaccess" }
func (s *usdaFoodSource) Category() string { return "food" }
func (s *usdaFoodSource) Vintage() string  { return s.vintage }

func (s *usdaFoodSource) Schema() []VariableDef {
	out := make([]VariableDef, len(usdaFoodVariables))
	copy(out, usdaFoodVariables)
	return out
}

// FetchCounty fetches food access indicators for all tracts in a single county.
// stateFIPS is a 2-digit code; countyFIPS is a 3-digit code.
func (s *usdaFoodSource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	sf := sanitizeFIPS(stateFIPS)
	cf := sanitizeFIPS(countyFIPS)
	if len(sf) != 2 {
		return nil, fmt.Errorf("usda-foodaccess: invalid state FIPS %q (must be 2 digits)", stateFIPS)
	}
	if len(cf) != 3 {
		return nil, fmt.Errorf("usda-foodaccess: invalid county FIPS %q (must be 3 digits)", countyFIPS)
	}

	prefix := sf + cf // 5-digit county prefix
	byGEOID, err := s.loadAll(ctx)
	if err != nil {
		return nil, err
	}

	var out []store.Indicator
	for geoid, rec := range byGEOID {
		if !strings.HasPrefix(geoid, prefix) {
			continue
		}
		out = append(out, s.tractToIndicators(rec)...)
	}
	return out, nil
}

// FetchState fetches food access indicators for all tracts in a state.
// stateFIPS is a 2-digit code.
func (s *usdaFoodSource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	sf := sanitizeFIPS(stateFIPS)
	if len(sf) != 2 {
		return nil, fmt.Errorf("usda-foodaccess: invalid state FIPS %q (must be 2 digits)", stateFIPS)
	}

	byGEOID, err := s.loadAll(ctx)
	if err != nil {
		return nil, err
	}

	var out []store.Indicator
	for geoid, rec := range byGEOID {
		if !strings.HasPrefix(geoid, sf) {
			continue
		}
		out = append(out, s.tractToIndicators(rec)...)
	}
	return out, nil
}

// loadAll downloads (or returns cached) parsed tract records keyed by GEOID.
func (s *usdaFoodSource) loadAll(ctx context.Context) (map[string]*tractRecord, error) {
	s.parsedMu.Lock()
	if s.parsed != nil {
		m := s.parsed
		s.parsedMu.Unlock()
		return m, nil
	}
	s.parsedMu.Unlock()

	rows, err := s.downloadAndParse(ctx)
	if err != nil {
		return nil, err
	}

	s.parsedMu.Lock()
	s.parsed = rows
	s.parsedMu.Unlock()

	return rows, nil
}

// downloadAndParse fetches the ERS ZIP/CSV and returns tract records by GEOID.
func (s *usdaFoodSource) downloadAndParse(ctx context.Context) (map[string]*tractRecord, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.DataURL, nil)
	if err != nil {
		return nil, fmt.Errorf("usda-foodaccess: build request: %w", err)
	}
	req.Header.Set("User-Agent", "policy-data-infrastructure/1.0")
	req.Header.Set("Accept", "application/zip,text/csv,application/octet-stream")

	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("usda-foodaccess: http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("usda-foodaccess: api returned status %d: %s\n"+
			"  Visit https://www.ers.usda.gov/data-products/food-access-research-atlas/ to find the current URL",
			resp.StatusCode, strings.TrimSpace(string(body)))
	}

	rawBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("usda-foodaccess: read body: %w", err)
	}

	// Courtesy delay after the download.
	time.Sleep(usdaRateDelay)

	// Detect ZIP vs raw CSV by URL suffix.
	isZIP := strings.HasSuffix(strings.ToLower(s.cfg.DataURL), ".zip")
	var csvBytes []byte

	if isZIP {
		csvBytes, err = extractLargestCSVFromZIP(rawBytes)
		if err != nil {
			return nil, fmt.Errorf("usda-foodaccess: extract ZIP: %w", err)
		}
	} else {
		csvBytes = rawBytes
	}

	// Decode: try UTF-8 with BOM, fall back to latin-1.
	text, err := decodeCSVBytes(csvBytes)
	if err != nil {
		return nil, fmt.Errorf("usda-foodaccess: decode CSV: %w", err)
	}

	return parseUSDACSV(text)
}

// extractLargestCSVFromZIP opens a ZIP archive and returns the bytes of the
// largest CSV file it contains (the main data table).
func extractLargestCSVFromZIP(data []byte) ([]byte, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("bad ZIP: %w", err)
	}

	var best *zip.File
	for _, f := range zr.File {
		if !strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			continue
		}
		if best == nil || f.UncompressedSize64 > best.UncompressedSize64 {
			best = f
		}
	}
	if best == nil {
		names := make([]string, 0, len(zr.File))
		for _, f := range zr.File {
			names = append(names, f.Name)
		}
		return nil, fmt.Errorf("ZIP contains no CSV files; members: %v", names)
	}

	rc, err := best.Open()
	if err != nil {
		return nil, fmt.Errorf("open %q from ZIP: %w", best.Name, err)
	}
	defer rc.Close()

	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read %q from ZIP: %w", best.Name, err)
	}
	return b, nil
}

// decodeCSVBytes decodes raw bytes as UTF-8-with-BOM, falling back to latin-1.
func decodeCSVBytes(b []byte) (string, error) {
	// Strip UTF-8 BOM if present.
	if len(b) >= 3 && b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF {
		b = b[3:]
	}

	// Try UTF-8 first.
	s := string(b)
	if isValidUTF8(s) {
		return s, nil
	}

	// Latin-1 fallback: each byte maps to the same Unicode code point.
	runes := make([]rune, len(b))
	for i, c := range b {
		runes[i] = rune(c)
	}
	return string(runes), nil
}

// isValidUTF8 reports whether s is valid UTF-8.
func isValidUTF8(s string) bool {
	for _, r := range s {
		if r == '\uFFFD' {
			return false
		}
	}
	return true
}

// parseUSDACSV parses the ERS CSV text and returns tract records keyed by GEOID.
func parseUSDACSV(text string) (map[string]*tractRecord, error) {
	r := csv.NewReader(strings.NewReader(text))
	r.LazyQuotes = true
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1 // tolerate variable field counts

	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	// Build index: source column position → canonical column ID.
	colIndex := make(map[int]usdaCanonicalCol)
	geoidCol := -1
	for i, h := range header {
		h = strings.TrimSpace(h)
		if canon, ok := columnAliases[h]; ok {
			colIndex[i] = canon
			if canon == usdaColGEOID {
				geoidCol = i
			}
		}
	}
	if geoidCol < 0 {
		return nil, fmt.Errorf("usda-foodaccess: CSV missing GEOID column (expected CensusTract, GEOID, or similar); header: %v", header[:min(10, len(header))])
	}

	byGEOID := make(map[string]*tractRecord)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Skip malformed rows rather than aborting.
			continue
		}
		if geoidCol >= len(row) {
			continue
		}

		rawGEOID := strings.TrimSpace(row[geoidCol])
		if rawGEOID == "" {
			continue
		}
		// Zero-pad to 11 digits.
		geoid := strings.TrimLeft(rawGEOID, " ")
		if len(geoid) < 11 {
			geoid = fmt.Sprintf("%011s", geoid)
			// Replace leading spaces from %s with zeroes.
			b := []byte(geoid)
			for i := range b {
				if b[i] == ' ' {
					b[i] = '0'
				}
			}
			geoid = string(b)
		}

		rec := &tractRecord{
			geoid:  geoid,
			values: make(map[usdaCanonicalCol]string),
		}

		for colPos, canon := range colIndex {
			if canon == usdaColGEOID {
				continue
			}
			if colPos < len(row) {
				raw := strings.TrimSpace(row[colPos])
				if raw != "" && raw != "N/A" && raw != "-" && raw != "." {
					rec.values[canon] = raw
				}
			}
		}

		byGEOID[geoid] = rec
	}

	return byGEOID, nil
}

// tractToIndicators converts a tractRecord to store.Indicator slice.
func (s *usdaFoodSource) tractToIndicators(rec *tractRecord) []store.Indicator {
	out := make([]store.Indicator, 0, 5)

	for _, col := range []usdaCanonicalCol{
		usdaColFoodDesert,
		usdaColLowAcc1Mi,
		usdaColLowAcc10Mi,
		usdaColSNAP,
		usdaColGrocery,
	} {
		varID := variableIDForCol[col]
		rawStr, ok := rec.values[col]

		var val *float64
		if ok && rawStr != "" {
			f, err := strconv.ParseFloat(rawStr, 64)
			if err == nil {
				v := f
				val = &v
			}
		}

		out = append(out, store.Indicator{
			GEOID:      rec.geoid,
			VariableID: varID,
			Vintage:    s.vintage,
			Value:      val,
			RawValue:   rawStr,
		})
	}

	return out
}

// min returns the smaller of a and b.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
