package datasource

// WIDPISource fetches school-level education indicators from the Wisconsin
// Department of Public Instruction (WI DPI) WISEdash certified data downloads.
//
// This adapter is inherently WI-specific (FIPS "55"). For non-WI states it
// returns an empty slice without error. The same pattern could be generalized
// to other state education departments that publish certified enrollment and
// attendance CSVs (e.g. MN MDE, MI CEPI) by parameterizing the URL template
// and district-code format.
//
// Data distribution:
//
//	Enrollment ZIP: https://dpi.wi.gov/sites/default/files/wise/downloads/enrollment_certified_{school_year}.zip
//	Attendance ZIP: https://dpi.wi.gov/sites/default/files/wise/downloads/attendance_dropouts_certified_{school_year}.zip
//
// The ZIP archives contain a single CSV in a long (group-by) format where each
// row is one school × GROUP_BY × GROUP_BY_VALUE combination. This adapter
// pivots the long format to wide indicators at the district level and uses the
// 4-digit district code as the GEOID.
//
// Note on geography: WI DPI data is published at the school and district level,
// not at the census-tract level. Indicators are stored with GEOID set to the
// 4-digit WI district code prefixed with "wi-dist-" (e.g. "wi-dist-0378" for
// Madison Metropolitan). Downstream tract-level mapping requires a
// district-to-tract crosswalk (not implemented in this adapter).
//
// Variables produced:
//
//	dpi_enrollment               — Total student enrollment (count)
//	dpi_chronic_absence_rate     — Chronic absence rate: 100 - attendance_rate (%)
//	dpi_attendance_rate          — District-level attendance rate (%)
//	dpi_chronic_absence_black    — Chronic absence rate, Black students (%)
//	dpi_chronic_absence_hispanic — Chronic absence rate, Hispanic students (%)
//	dpi_chronic_absence_white    — Chronic absence rate, White students (%)
//	dpi_chronic_absence_econ_disadv — Chronic absence rate, economically disadvantaged (%)

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
	"time"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

const (
	wiDPIStateFIPS = "55"

	wiDPIEnrollmentURLTemplate  = "https://dpi.wi.gov/sites/default/files/wise/downloads/enrollment_certified_%s.zip"
	wiDPIAttendanceURLTemplate  = "https://dpi.wi.gov/sites/default/files/wise/downloads/attendance_dropouts_certified_%s.zip"
	wiDPIAttendanceFallbackURL  = "https://dpi.wi.gov/sites/default/files/imce/wisedash/downloads/attendance_dropouts_certified_%s.zip"

	wiDPIRateDelay  = 2 * time.Second
	wiDPIUserAgent  = "policy-data-infrastructure/1.0"
)

// wiDPIVariables is the complete set of indicator variables this adapter produces.
var wiDPIVariables = []VariableDef{
	{
		ID:          "dpi_enrollment",
		Name:        "Total Student Enrollment",
		Description: "Total student enrollment across all grade levels for the school district",
		Unit:        "count",
		Direction:   "neutral",
	},
	{
		ID:          "dpi_chronic_absence_rate",
		Name:        "Chronic Absence Rate",
		Description: "Estimated chronic absence rate (100 - district attendance rate) for all students (%)",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	{
		ID:          "dpi_attendance_rate",
		Name:        "Attendance Rate",
		Description: "District-level mean attendance rate for all students across all grade levels (%)",
		Unit:        "percent",
		Direction:   "higher_better",
	},
	{
		ID:          "dpi_chronic_absence_black",
		Name:        "Chronic Absence Rate — Black Students",
		Description: "Estimated chronic absence rate for Black or African American students (%)",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	{
		ID:          "dpi_chronic_absence_hispanic",
		Name:        "Chronic Absence Rate — Hispanic Students",
		Description: "Estimated chronic absence rate for Hispanic or Latino students (%)",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	{
		ID:          "dpi_chronic_absence_white",
		Name:        "Chronic Absence Rate — White Students",
		Description: "Estimated chronic absence rate for White students (%)",
		Unit:        "percent",
		Direction:   "lower_better",
	},
	{
		ID:          "dpi_chronic_absence_econ_disadv",
		Name:        "Chronic Absence Rate — Economically Disadvantaged Students",
		Description: "Estimated chronic absence rate for economically disadvantaged students (%)",
		Unit:        "percent",
		Direction:   "lower_better",
	},
}

// WIDPIConfig configures a WIDPISource.
type WIDPIConfig struct {
	// SchoolYear is the WI DPI school year string, e.g. "2024-25".
	// When empty, defaults to "2022-23" (the most recent certified release).
	SchoolYear string
	// Year is the calendar year for the vintage label (e.g. 2024 for "2024-25").
	// When 0, derived from SchoolYear by parsing the first component.
	Year int
	// HTTPClient is used for all HTTP requests. Defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// wiDPISource implements DataSource for WI DPI WISEdash data.
type wiDPISource struct {
	cfg        WIDPIConfig
	vintage    string
	schoolYear string
}

// NewWIDPISource creates a WIDPISource from cfg.
//
// Defaults:
//   - SchoolYear: "2022-23"
//   - Year: 2022
//   - HTTPClient: http.DefaultClient
func NewWIDPISource(cfg WIDPIConfig) *wiDPISource {
	if cfg.SchoolYear == "" {
		cfg.SchoolYear = "2022-23"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	year := cfg.Year
	if year == 0 {
		// Parse year from the first component of the school year string (e.g. "2022" from "2022-23").
		parts := strings.SplitN(cfg.SchoolYear, "-", 2)
		if len(parts) > 0 {
			if y, err := strconv.Atoi(parts[0]); err == nil {
				year = y
			}
		}
	}
	vintage := fmt.Sprintf("WI-DPI-%d-%s", year, strings.ReplaceAll(cfg.SchoolYear, "-", ""))
	return &wiDPISource{
		cfg:        cfg,
		vintage:    vintage,
		schoolYear: cfg.SchoolYear,
	}
}

func (s *wiDPISource) Name() string     { return "wi-dpi" }
func (s *wiDPISource) Category() string { return "education" }
func (s *wiDPISource) Vintage() string  { return s.vintage }

func (s *wiDPISource) Schema() []VariableDef {
	out := make([]VariableDef, len(wiDPIVariables))
	copy(out, wiDPIVariables)
	return out
}

// FetchState fetches WI DPI indicators for Wisconsin (stateFIPS "55").
// Returns an empty slice without error for any non-WI state — this source is
// WI-specific.
func (s *wiDPISource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	if stateFIPS != wiDPIStateFIPS {
		// Gracefully return empty for non-WI states.
		return nil, nil
	}
	return s.fetchAll(ctx, "")
}

// FetchCounty fetches WI DPI district-level indicators for schools whose
// district code is within the given county. Because WI district codes do not
// embed county FIPS directly, this fetches all districts and then filters to
// the requested county using a static district-to-county mapping stub.
//
// For now, when stateFIPS != "55" this returns an empty slice without error.
// County-level filtering on WI districts requires a crosswalk table that maps
// 4-digit district codes to county FIPS; that crosswalk is not embedded in this
// adapter. The full-state fetch provides complete data.
func (s *wiDPISource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	if stateFIPS != wiDPIStateFIPS {
		return nil, nil
	}
	// Fetch all districts; filtering by countyFIPS requires a crosswalk not
	// currently available in this adapter. Return full state for WI.
	return s.fetchAll(ctx, countyFIPS)
}

// fetchAll downloads and parses both the enrollment and attendance ZIPs,
// then merges results into indicators keyed by district code.
// countyFIPS is informational only (not yet used for filtering).
func (s *wiDPISource) fetchAll(ctx context.Context, _ string) ([]store.Indicator, error) {
	// 1. Fetch enrollment data.
	enrollURL := fmt.Sprintf(wiDPIEnrollmentURLTemplate, s.schoolYear)
	enrollBytes, err := s.downloadZIP(ctx, enrollURL, "")
	if err != nil {
		return nil, fmt.Errorf("wi-dpi: download enrollment: %w", err)
	}
	time.Sleep(wiDPIRateDelay)

	// 2. Parse enrollment CSV from ZIP.
	enrollRecords, err := parseWIDPIEnrollmentZIP(enrollBytes)
	if err != nil {
		return nil, fmt.Errorf("wi-dpi: parse enrollment: %w", err)
	}

	// 3. Fetch attendance data (with fallback URL).
	attURL := fmt.Sprintf(wiDPIAttendanceURLTemplate, s.schoolYear)
	attFallback := fmt.Sprintf(wiDPIAttendanceFallbackURL, s.schoolYear)
	attBytes, err := s.downloadZIP(ctx, attURL, attFallback)
	if err != nil {
		return nil, fmt.Errorf("wi-dpi: download attendance: %w", err)
	}
	time.Sleep(wiDPIRateDelay)

	// 4. Parse attendance CSV from ZIP.
	attRecords, err := parseWIDPIAttendanceZIP(attBytes)
	if err != nil {
		return nil, fmt.Errorf("wi-dpi: parse attendance: %w", err)
	}

	// 5. Merge and emit indicators.
	return s.buildIndicators(enrollRecords, attRecords), nil
}

// downloadZIP fetches url (with fallbackURL on non-200) and returns raw bytes.
func (s *wiDPISource) downloadZIP(ctx context.Context, primaryURL, fallbackURL string) ([]byte, error) {
	for _, u := range []string{primaryURL, fallbackURL} {
		if u == "" {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, fmt.Errorf("build request %s: %w", u, err)
		}
		req.Header.Set("User-Agent", wiDPIUserAgent)

		resp, err := s.cfg.HTTPClient.Do(req)
		if err != nil {
			if fallbackURL != "" {
				continue // try fallback
			}
			return nil, fmt.Errorf("http get %s: %w", u, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			if fallbackURL != "" && u != fallbackURL {
				continue // try fallback
			}
			return nil, fmt.Errorf("http %d from %s", resp.StatusCode, u)
		}

		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read body from %s: %w", u, err)
		}
		return data, nil
	}
	return nil, fmt.Errorf("all URLs failed (primary=%s fallback=%s)", primaryURL, fallbackURL)
}

// wiDPIEnrollRecord holds pivoted enrollment data for a single district.
type wiDPIEnrollRecord struct {
	DistrictCode string
	Enrollment   *float64 // total enrollment, all students
	PctWhite     *float64
	PctBlack     *float64
	PctHispanic  *float64
	PctEconDisadv *float64
}

// wiDPIAttRecord holds attendance data for a single district.
type wiDPIAttRecord struct {
	DistrictCode   string
	AttendanceRate *float64 // mean attendance rate for All Students, district level
}

// parseWIDPIEnrollmentZIP extracts and parses the largest CSV from the enrollment ZIP.
func parseWIDPIEnrollmentZIP(data []byte) (map[string]*wiDPIEnrollRecord, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}
	csvFile := largestCSV(zr)
	if csvFile == nil {
		return nil, fmt.Errorf("no CSV found in enrollment ZIP")
	}
	rc, err := csvFile.Open()
	if err != nil {
		return nil, fmt.Errorf("open csv %s: %w", csvFile.Name, err)
	}
	defer rc.Close()

	return parseWIDPIEnrollCSV(rc)
}

// parseWIDPIAttendanceZIP extracts and parses the attendance CSV from the ZIP.
func parseWIDPIAttendanceZIP(data []byte) (map[string]*wiDPIAttRecord, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}
	// Prefer a file whose name starts with "attendance"; fall back to largest CSV.
	var chosen *zip.File
	for _, f := range zr.File {
		name := strings.ToLower(f.Name)
		if strings.HasPrefix(name, "attendance") && strings.HasSuffix(name, ".csv") && !strings.Contains(name, "layout") {
			if chosen == nil || f.UncompressedSize64 > chosen.UncompressedSize64 {
				chosen = f
			}
		}
	}
	if chosen == nil {
		chosen = largestCSV(zr)
	}
	if chosen == nil {
		return nil, fmt.Errorf("no CSV found in attendance ZIP")
	}
	rc, err := chosen.Open()
	if err != nil {
		return nil, fmt.Errorf("open csv %s: %w", chosen.Name, err)
	}
	defer rc.Close()

	return parseWIDPIAttendanceCSV(rc)
}

// largestCSV returns the largest (by uncompressed size) CSV file in the ZIP
// that is not a layout/readme file.
func largestCSV(zr *zip.Reader) *zip.File {
	var best *zip.File
	for _, f := range zr.File {
		name := strings.ToLower(f.Name)
		if !strings.HasSuffix(name, ".csv") || strings.Contains(name, "layout") {
			continue
		}
		if best == nil || f.UncompressedSize64 > best.UncompressedSize64 {
			best = f
		}
	}
	return best
}

// parseWIDPIEnrollCSV reads the long-format enrollment CSV and pivots to wide
// records keyed by district code. Only district-level (empty SCHOOL_CODE) rows
// for grade group [All] are used.
func parseWIDPIEnrollCSV(r io.Reader) (map[string]*wiDPIEnrollRecord, error) {
	// Decode with UTF-8-sig awareness (strip BOM if present).
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	text := decodeWIDPIBytes(raw)

	cr := csv.NewReader(strings.NewReader(text))
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true

	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	idx := columnIndex(header)

	records := make(map[string]*wiDPIEnrollRecord)

	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // skip malformed lines
		}

		dc := colVal(row, idx, "DISTRICT_CODE")
		sc := colVal(row, idx, "SCHOOL_CODE")
		if dc == "" || dc == "0000" {
			continue // skip statewide rollup
		}
		if sc != "" {
			continue // district-level only (empty SCHOOL_CODE)
		}

		gradeGroup := colVal(row, idx, "GRADE_GROUP")
		if gradeGroup != "[All]" && gradeGroup != "All Grades" {
			continue
		}

		groupBy := colVal(row, idx, "GROUP_BY")
		groupVal := colVal(row, idx, "GROUP_BY_VALUE")

		pct := parseDPIFloat(colVal(row, idx, "PERCENT_OF_GROUP"))
		count := parseDPIFloat(colVal(row, idx, "STUDENT_COUNT"))

		rec, exists := records[dc]
		if !exists {
			rec = &wiDPIEnrollRecord{DistrictCode: dc}
			records[dc] = rec
		}

		groupValLower := strings.ToLower(groupVal)
		switch groupBy {
		case "All Students":
			if groupVal == "All Students" {
				rec.Enrollment = count
			}
		case "Race/Ethnicity":
			// Use case-insensitive contains matching to handle WISEdash label
			// variations across releases (e.g. "Black or African American",
			// "Hispanic or Latino") and any trimming/encoding differences.
			switch {
			case groupValLower == "white":
				rec.PctWhite = pct
			case strings.Contains(groupValLower, "black"):
				rec.PctBlack = pct
			case strings.Contains(groupValLower, "hispanic"):
				rec.PctHispanic = pct
			}
		case "Economic Status":
			if groupVal == "Econ Disadv" {
				rec.PctEconDisadv = pct
			}
		}
	}

	return records, nil
}

// parseWIDPIAttendanceCSV reads the attendance CSV and extracts district-level
// attendance rates for All Students / [All] grade group.
func parseWIDPIAttendanceCSV(r io.Reader) (map[string]*wiDPIAttRecord, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	text := decodeWIDPIBytes(raw)

	cr := csv.NewReader(strings.NewReader(text))
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true

	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	idx := columnIndex(header)

	// Primary: district-level rows (empty SCHOOL_CODE).
	// Fallback: average school-level rows per district.
	districtRates := make(map[string]float64)
	schoolRates := make(map[string][]float64)

	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		dc := colVal(row, idx, "DISTRICT_CODE")
		sc := colVal(row, idx, "SCHOOL_CODE")
		if dc == "" || dc == "0000" {
			continue
		}

		gradeGroup := colVal(row, idx, "GRADE_GROUP")
		if gradeGroup != "[All]" && gradeGroup != "All Grades" {
			continue
		}

		groupBy := colVal(row, idx, "GROUP_BY")
		groupVal := colVal(row, idx, "GROUP_BY_VALUE")
		if groupBy != "All Students" || groupVal != "All Students" {
			continue
		}

		attStr := colVal(row, idx, "ATTENDANCE_RATE")
		att, err2 := strconv.ParseFloat(strings.TrimSpace(attStr), 64)
		if err2 != nil {
			continue
		}

		if sc == "" {
			// District-level: preferred.
			districtRates[dc] = att
		} else {
			// School-level: fallback.
			schoolRates[dc] = append(schoolRates[dc], att)
		}
	}

	// Merge into final map.
	allDC := make(map[string]struct{})
	for dc := range districtRates {
		allDC[dc] = struct{}{}
	}
	for dc := range schoolRates {
		allDC[dc] = struct{}{}
	}

	records := make(map[string]*wiDPIAttRecord, len(allDC))
	for dc := range allDC {
		var att float64
		if r, ok := districtRates[dc]; ok {
			att = r
		} else {
			rates := schoolRates[dc]
			if len(rates) == 0 {
				continue
			}
			sum := 0.0
			for _, r := range rates {
				sum += r
			}
			att = sum / float64(len(rates))
		}
		v := att
		records[dc] = &wiDPIAttRecord{
			DistrictCode:   dc,
			AttendanceRate: &v,
		}
	}
	return records, nil
}

// buildIndicators merges enrollment and attendance records into store.Indicators.
// GEOID is formatted as "wi-dist-XXXX" where XXXX is the 4-digit district code.
func (s *wiDPISource) buildIndicators(
	enroll map[string]*wiDPIEnrollRecord,
	att map[string]*wiDPIAttRecord,
) []store.Indicator {
	// Collect all district codes.
	dcSet := make(map[string]struct{})
	for dc := range enroll {
		dcSet[dc] = struct{}{}
	}
	for dc := range att {
		dcSet[dc] = struct{}{}
	}

	var out []store.Indicator
	for dc := range dcSet {
		geoid := "wi-dist-" + dc

		if e, ok := enroll[dc]; ok {
			if e.Enrollment != nil {
				v := *e.Enrollment
				out = append(out, store.Indicator{
					GEOID:      geoid,
					VariableID: "dpi_enrollment",
					Vintage:    s.vintage,
					Value:      &v,
					RawValue:   fmt.Sprintf("%.0f", v),
				})
			}
			// Race/ethnicity absence proxies are derived from enrollment %s;
			// for now we store them directly as they are enrollment-derived
			// sub-group percentages (not absence rates). These would require
			// attendance sub-group data for true absence rates.
			// We emit them only when the attendance record is also present.
		}

		if a, ok := att[dc]; ok && a.AttendanceRate != nil {
			attVal := *a.AttendanceRate

			// Attendance rate indicator.
			attCopy := attVal
			out = append(out, store.Indicator{
				GEOID:      geoid,
				VariableID: "dpi_attendance_rate",
				Vintage:    s.vintage,
				Value:      &attCopy,
				RawValue:   fmt.Sprintf("%.2f", attCopy),
			})

			// Chronic absence = 100 - attendance rate.
			chronic := roundFloat(100.0-attVal, 2)
			out = append(out, store.Indicator{
				GEOID:      geoid,
				VariableID: "dpi_chronic_absence_rate",
				Vintage:    s.vintage,
				Value:      &chronic,
				RawValue:   fmt.Sprintf("%.2f", chronic),
			})
		}

		// Race-stratified and economic-status chronic absence variables.
		// WISEdash attendance downloads provide only aggregate district-level
		// attendance rates (All Students). Race-stratified attendance rates are
		// NOT available in the certified attendance download, so these indicators
		// are emitted with nil values to make the data gap visible in queries
		// rather than silently omitting the variables from the output.
		//
		// If WI DPI publishes race-stratified attendance data in a future
		// release, populate the values here from those records.
		raceAbsenceVars := []string{
			"dpi_chronic_absence_black",
			"dpi_chronic_absence_hispanic",
			"dpi_chronic_absence_white",
			"dpi_chronic_absence_econ_disadv",
		}
		for _, varID := range raceAbsenceVars {
			out = append(out, store.Indicator{
				GEOID:      geoid,
				VariableID: varID,
				Vintage:    s.vintage,
				Value:      nil, // not available in certified attendance download
				RawValue:   "",
			})
		}
	}
	return out
}

// decodeWIDPIBytes returns the string content of raw bytes, stripping a UTF-8
// BOM if present and falling back to latin-1 if not valid UTF-8.
func decodeWIDPIBytes(b []byte) string {
	// Strip UTF-8 BOM (EF BB BF).
	if len(b) >= 3 && b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF {
		b = b[3:]
	}
	// Try UTF-8 first.
	s := string(b)
	// Validate by attempting to range over runes; invalid UTF-8 shows as U+FFFD.
	// For production correctness we use a simple heuristic: if the string contains
	// the replacement character and len differs, try latin-1.
	if strings.ContainsRune(s, '\uFFFD') {
		// Fallback: treat as latin-1 (ISO-8859-1).
		runes := make([]rune, len(b))
		for i, bb := range b {
			runes[i] = rune(bb)
		}
		return string(runes)
	}
	return s
}

// columnIndex builds a map from column name (uppercased) to column index.
func columnIndex(header []string) map[string]int {
	m := make(map[string]int, len(header))
	for i, h := range header {
		m[strings.ToUpper(strings.TrimSpace(h))] = i
	}
	return m
}

// colVal returns the value at column name (case-insensitive) or "".
func colVal(row []string, idx map[string]int, col string) string {
	i, ok := idx[strings.ToUpper(col)]
	if !ok || i >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[i])
}

// parseDPIFloat parses a string from a DPI CSV as a float64 pointer.
// Returns nil for suppressed / missing values ("", "N/A", "*", "-", "null").
func parseDPIFloat(s string) *float64 {
	s = strings.TrimSpace(s)
	switch s {
	case "", "N/A", "*", "-", "null", "n/a":
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &f
}

// roundFloat rounds f to decimals places.
func roundFloat(f float64, decimals int) float64 {
	pow := 1.0
	for i := 0; i < decimals; i++ {
		pow *= 10
	}
	return float64(int(f*pow+0.5)) / pow
}
