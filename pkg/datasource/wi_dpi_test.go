package datasource

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// wiDPIEnrollFixture is a minimal long-format enrollment CSV with two districts.
// District 0378: all-student enrollment = 25000, White 42%, Black or African American 22%, Hispanic or Latino 18%, EconDisadv 55%.
// District 0441: all-student enrollment = 5200.
// Race/ethnicity labels use the full WISEdash encoding ("Black or African American",
// "Hispanic or Latino") to match real WISEdash certified download format.
const wiDPIEnrollFixture = `DISTRICT_CODE,SCHOOL_CODE,SCHOOL_NAME,DISTRICT_NAME,GRADE_GROUP,GROUP_BY,GROUP_BY_VALUE,STUDENT_COUNT,PERCENT_OF_GROUP
0378,,,,  [All],All Students,All Students,25000,
0378,,,,[All],Race/Ethnicity,White,,42.1
0378,,,,[All],Race/Ethnicity,Black or African American,,22.3
0378,,,,[All],Race/Ethnicity,Hispanic or Latino,,18.5
0378,,,,[All],Economic Status,Econ Disadv,,55.2
0441,,,,[All],All Students,All Students,5200,
`

// wiDPIAttFixture is a minimal attendance CSV with district-level rows.
// District 0378: attendance rate 88.5 → chronic_absence = 11.5
// District 0441: attendance rate 94.0 → chronic_absence = 6.0
const wiDPIAttFixture = `DISTRICT_CODE,SCHOOL_CODE,DISTRICT_NAME,SCHOOL_NAME,GRADE_GROUP,GROUP_BY,GROUP_BY_VALUE,ATTENDANCE_RATE
0378,,,,[All],All Students,All Students,88.5
0441,,,,[All],All Students,All Students,94.0
`

// makeZIP wraps content bytes into a ZIP archive with the given filename.
func makeZIP(filename string, content []byte) []byte {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, err := w.Create(filename)
	if err != nil {
		panic(err)
	}
	_, err = f.Write(content)
	if err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// enrollZIP is a ZIP containing the enrollment fixture CSV.
var enrollZIP = makeZIP("enrollment_certified_2022-23.csv", []byte(wiDPIEnrollFixture))

// attZIP is a ZIP containing the attendance fixture CSV.
var attZIP = makeZIP("attendance_dropouts_certified_2022-23.csv", []byte(wiDPIAttFixture))

// ---------------------------------------------------------------------------
// Test: defaults and interface
// ---------------------------------------------------------------------------

func TestNewWIDPISource_Defaults(t *testing.T) {
	s := NewWIDPISource(WIDPIConfig{})
	if s.Name() != "wi-dpi" {
		t.Errorf("Name(): want wi-dpi, got %q", s.Name())
	}
	if s.Category() != "education" {
		t.Errorf("Category(): want education, got %q", s.Category())
	}
	// Default school year is 2022-23; vintage should encode that.
	v := s.Vintage()
	if !strings.Contains(v, "WI-DPI") {
		t.Errorf("Vintage(): want WI-DPI prefix, got %q", v)
	}
	if s.cfg.SchoolYear != "2022-23" {
		t.Errorf("default SchoolYear: want 2022-23, got %q", s.cfg.SchoolYear)
	}
}

func TestNewWIDPISource_ExplicitConfig(t *testing.T) {
	s := NewWIDPISource(WIDPIConfig{SchoolYear: "2024-25", Year: 2024})
	if s.Vintage() != "WI-DPI-2024-202425" {
		t.Errorf("Vintage(): want WI-DPI-2024-202425, got %q", s.Vintage())
	}
}

func TestWIDPISource_ImplementsInterface(t *testing.T) {
	var _ DataSource = NewWIDPISource(WIDPIConfig{})
}

// ---------------------------------------------------------------------------
// Test: Schema
// ---------------------------------------------------------------------------

func TestWIDPISchema(t *testing.T) {
	s := NewWIDPISource(WIDPIConfig{})
	schema := s.Schema()

	if len(schema) == 0 {
		t.Fatal("Schema() returned empty slice")
	}

	// All required variable IDs must be present.
	wantIDs := []string{
		"dpi_enrollment",
		"dpi_chronic_absence_rate",
		"dpi_attendance_rate",
		"dpi_chronic_absence_black",
		"dpi_chronic_absence_hispanic",
		"dpi_chronic_absence_white",
		"dpi_chronic_absence_econ_disadv",
	}

	found := make(map[string]bool, len(schema))
	for _, def := range schema {
		found[def.ID] = true
	}

	for _, id := range wantIDs {
		if !found[id] {
			t.Errorf("Schema() missing variable %q", id)
		}
	}

	// All defs must have ID, Name, Unit, Direction.
	for _, def := range schema {
		if def.ID == "" {
			t.Errorf("VariableDef missing ID: %+v", def)
		}
		if def.Name == "" {
			t.Errorf("VariableDef %q missing Name", def.ID)
		}
		if def.Unit == "" {
			t.Errorf("VariableDef %q missing Unit", def.ID)
		}
		if def.Direction == "" {
			t.Errorf("VariableDef %q missing Direction", def.ID)
		}
	}

	// Schema() must return a copy — mutations must not affect the source.
	schema[0].ID = "mutated"
	schema2 := s.Schema()
	if schema2[0].ID == "mutated" {
		t.Error("Schema() returned shared slice — mutations propagate")
	}
}

// ---------------------------------------------------------------------------
// Test: non-WI state returns empty
// ---------------------------------------------------------------------------

func TestWIDPINonWIState_FetchState(t *testing.T) {
	s := NewWIDPISource(WIDPIConfig{HTTPClient: http.DefaultClient})

	ctx := context.Background()
	result, err := s.FetchState(ctx, "17") // Illinois
	if err != nil {
		t.Errorf("FetchState non-WI: unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("FetchState non-WI: expected empty slice, got %d indicators", len(result))
	}
}

func TestWIDPINonWIState_FetchCounty(t *testing.T) {
	s := NewWIDPISource(WIDPIConfig{HTTPClient: http.DefaultClient})

	ctx := context.Background()
	result, err := s.FetchCounty(ctx, "17", "031") // Illinois, Cook County
	if err != nil {
		t.Errorf("FetchCounty non-WI: unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("FetchCounty non-WI: expected empty slice, got %d indicators", len(result))
	}
}

// ---------------------------------------------------------------------------
// Test: CSV parsing helpers
// ---------------------------------------------------------------------------

func TestWIDPIParseEnrollCSV_Pivot(t *testing.T) {
	records, err := parseWIDPIEnrollCSV(strings.NewReader(wiDPIEnrollFixture))
	if err != nil {
		t.Fatalf("parseWIDPIEnrollCSV: %v", err)
	}

	if len(records) == 0 {
		t.Fatal("expected enrollment records, got none")
	}

	rec, ok := records["0378"]
	if !ok {
		t.Fatal("expected record for district 0378")
	}

	if rec.Enrollment == nil {
		t.Fatal("district 0378: Enrollment is nil")
	}
	if *rec.Enrollment != 25000 {
		t.Errorf("district 0378 enrollment: want 25000, got %v", *rec.Enrollment)
	}

	if rec.PctWhite == nil {
		t.Error("district 0378: PctWhite is nil")
	} else if *rec.PctWhite != 42.1 {
		t.Errorf("district 0378 PctWhite: want 42.1, got %v", *rec.PctWhite)
	}

	if rec.PctBlack == nil {
		t.Error("district 0378: PctBlack is nil")
	} else if *rec.PctBlack != 22.3 {
		t.Errorf("district 0378 PctBlack: want 22.3, got %v", *rec.PctBlack)
	}
}

func TestWIDPIParseAttendanceCSV_ChronicAbsence(t *testing.T) {
	records, err := parseWIDPIAttendanceCSV(strings.NewReader(wiDPIAttFixture))
	if err != nil {
		t.Fatalf("parseWIDPIAttendanceCSV: %v", err)
	}

	rec, ok := records["0378"]
	if !ok {
		t.Fatal("expected record for district 0378")
	}
	if rec.AttendanceRate == nil {
		t.Fatal("district 0378: AttendanceRate is nil")
	}
	if *rec.AttendanceRate != 88.5 {
		t.Errorf("district 0378 AttendanceRate: want 88.5, got %v", *rec.AttendanceRate)
	}

	rec2, ok := records["0441"]
	if !ok {
		t.Fatal("expected record for district 0441")
	}
	if rec2.AttendanceRate == nil {
		t.Fatal("district 0441: AttendanceRate is nil")
	}
	if *rec2.AttendanceRate != 94.0 {
		t.Errorf("district 0441 AttendanceRate: want 94.0, got %v", *rec2.AttendanceRate)
	}
}

// ---------------------------------------------------------------------------
// Test: buildIndicators produces correct indicators
// ---------------------------------------------------------------------------

func TestWIDPIBuildIndicators(t *testing.T) {
	enrollRecords, err := parseWIDPIEnrollCSV(strings.NewReader(wiDPIEnrollFixture))
	if err != nil {
		t.Fatalf("parseWIDPIEnrollCSV: %v", err)
	}
	attRecords, err := parseWIDPIAttendanceCSV(strings.NewReader(wiDPIAttFixture))
	if err != nil {
		t.Fatalf("parseWIDPIAttendanceCSV: %v", err)
	}

	s := NewWIDPISource(WIDPIConfig{SchoolYear: "2022-23"})
	indicators := s.buildIndicators(enrollRecords, attRecords)

	if len(indicators) == 0 {
		t.Fatal("buildIndicators: expected indicators, got none")
	}

	// Index by (GEOID, VariableID) for easy lookup — value indicators.
	idx := make(map[string]float64)
	// Track which (GEOID, VariableID) pairs were emitted at all (including nil-value ones).
	emitted := make(map[string]bool)
	for _, ind := range indicators {
		key := ind.GEOID + "|" + ind.VariableID
		emitted[key] = true
		if ind.Value != nil {
			idx[key] = *ind.Value
		}
	}

	// Check GEOID format.
	for _, ind := range indicators {
		if !strings.HasPrefix(ind.GEOID, "wi-dist-") {
			t.Errorf("GEOID %q should have wi-dist- prefix", ind.GEOID)
		}
	}

	// District 0378: enrollment = 25000.
	key := "wi-dist-0378|dpi_enrollment"
	if v, ok := idx[key]; !ok || v != 25000 {
		t.Errorf("enrollment for 0378: want 25000, got %v (ok=%v)", v, ok)
	}

	// District 0378: attendance = 88.5.
	key = "wi-dist-0378|dpi_attendance_rate"
	if v, ok := idx[key]; !ok || v != 88.5 {
		t.Errorf("attendance_rate for 0378: want 88.5, got %v (ok=%v)", v, ok)
	}

	// District 0378: chronic_absence = 100 - 88.5 = 11.5.
	key = "wi-dist-0378|dpi_chronic_absence_rate"
	if v, ok := idx[key]; !ok || v != 11.5 {
		t.Errorf("chronic_absence_rate for 0378: want 11.5, got %v (ok=%v)", v, ok)
	}

	// District 0441: chronic_absence = 100 - 94.0 = 6.0.
	key = "wi-dist-0441|dpi_chronic_absence_rate"
	if v, ok := idx[key]; !ok || v != 6.0 {
		t.Errorf("chronic_absence_rate for 0441: want 6.0, got %v (ok=%v)", v, ok)
	}

	// Race-stratified chronic absence variables must be emitted for every district,
	// even though they have nil values (no race-stratified attendance data in the
	// certified attendance download). Emitting nil makes the gap visible in queries.
	raceAbsenceVars := []string{
		"dpi_chronic_absence_black",
		"dpi_chronic_absence_hispanic",
		"dpi_chronic_absence_white",
		"dpi_chronic_absence_econ_disadv",
	}
	for _, dc := range []string{"0378", "0441"} {
		geoid := "wi-dist-" + dc
		for _, varID := range raceAbsenceVars {
			k := geoid + "|" + varID
			if !emitted[k] {
				t.Errorf("race-stratified var %q not emitted for district %s", varID, dc)
			}
			// Value must be nil — these are data-gap indicators.
			if _, hasValue := idx[k]; hasValue {
				t.Errorf("race-stratified var %q for %s: expected nil value, got non-nil", varID, dc)
			}
		}
	}

	// All indicators must have Vintage set.
	for _, ind := range indicators {
		if ind.Vintage == "" {
			t.Errorf("indicator %s|%s: missing Vintage", ind.GEOID, ind.VariableID)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: HTTP round-trip with mock server (parseCSV via httptest)
// ---------------------------------------------------------------------------

func TestWIDPIParseCSV_HTTPMock(t *testing.T) {
	// Build ZIP responses for enrollment and attendance.
	enrollZIPData := makeZIP("enrollment_certified_2022-23.csv", []byte(wiDPIEnrollFixture))
	attZIPData := makeZIP("attendance_dropouts_certified_2022-23.csv", []byte(wiDPIAttFixture))

	var enrollHits, attHits int

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/zip")
		path := r.URL.Path
		switch {
		case strings.Contains(path, "enrollment"):
			enrollHits++
			_, _ = w.Write(enrollZIPData)
		case strings.Contains(path, "attendance"):
			attHits++
			_, _ = w.Write(attZIPData)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	// Override the source URLs to point at the test server.
	s := NewWIDPISource(WIDPIConfig{
		SchoolYear: "2022-23",
		HTTPClient: ts.Client(),
	})

	// Directly test downloadZIP + parse pipeline by substituting the URLs.
	ctx := context.Background()

	enrollBytes, err := s.downloadZIP(ctx,
		fmt.Sprintf("%s/enrollment_certified_2022-23.zip", ts.URL),
		"")
	if err != nil {
		t.Fatalf("downloadZIP enrollment: %v", err)
	}
	if enrollHits != 1 {
		t.Errorf("enrollment handler hits: want 1, got %d", enrollHits)
	}

	enrollRecords, err := parseWIDPIEnrollmentZIP(enrollBytes)
	if err != nil {
		t.Fatalf("parseWIDPIEnrollmentZIP: %v", err)
	}
	if len(enrollRecords) == 0 {
		t.Fatal("expected enrollment records from HTTP mock")
	}

	attBytes, err := s.downloadZIP(ctx,
		fmt.Sprintf("%s/attendance_dropouts_certified_2022-23.zip", ts.URL),
		"")
	if err != nil {
		t.Fatalf("downloadZIP attendance: %v", err)
	}
	if attHits != 1 {
		t.Errorf("attendance handler hits: want 1, got %d", attHits)
	}

	attRecords, err := parseWIDPIAttendanceZIP(attBytes)
	if err != nil {
		t.Fatalf("parseWIDPIAttendanceZIP: %v", err)
	}
	if len(attRecords) == 0 {
		t.Fatal("expected attendance records from HTTP mock")
	}

	// Validate a specific indicator value.
	indicators := s.buildIndicators(enrollRecords, attRecords)
	var found bool
	for _, ind := range indicators {
		if ind.GEOID == "wi-dist-0378" && ind.VariableID == "dpi_chronic_absence_rate" {
			found = true
			if ind.Value == nil {
				t.Fatal("chronic_absence_rate for 0378: nil value")
			}
			if *ind.Value != 11.5 {
				t.Errorf("chronic_absence_rate for 0378: want 11.5, got %v", *ind.Value)
			}
			break
		}
	}
	if !found {
		t.Error("did not find dpi_chronic_absence_rate for wi-dist-0378 in HTTP mock test")
	}
}

// ---------------------------------------------------------------------------
// Test: helper functions
// ---------------------------------------------------------------------------

func TestParseDPIFloat_SuppressedValues(t *testing.T) {
	suppressed := []string{"", "N/A", "*", "-", "null", "n/a"}
	for _, s := range suppressed {
		v := parseDPIFloat(s)
		if v != nil {
			t.Errorf("parseDPIFloat(%q): want nil, got %v", s, *v)
		}
	}
}

func TestParseDPIFloat_ValidNumbers(t *testing.T) {
	cases := []struct {
		in  string
		out float64
	}{
		{"0", 0},
		{"42.1", 42.1},
		{"100", 100},
		{"  18.5  ", 18.5},
	}
	for _, c := range cases {
		v := parseDPIFloat(c.in)
		if v == nil {
			t.Errorf("parseDPIFloat(%q): want %v, got nil", c.in, c.out)
			continue
		}
		if *v != c.out {
			t.Errorf("parseDPIFloat(%q): want %v, got %v", c.in, c.out, *v)
		}
	}
}

func TestDecodeWIDPIBytes_BOMStripped(t *testing.T) {
	// UTF-8 BOM: EF BB BF
	withBOM := append([]byte{0xEF, 0xBB, 0xBF}, []byte("hello")...)
	result := decodeWIDPIBytes(withBOM)
	if result != "hello" {
		t.Errorf("BOM not stripped: got %q", result)
	}
}

func TestRoundFloat(t *testing.T) {
	cases := []struct {
		f    float64
		d    int
		want float64
	}{
		{11.5000001, 2, 11.5},
		{6.04999, 2, 6.05},
		{100.0 - 88.5, 2, 11.5},
	}
	for _, c := range cases {
		got := roundFloat(c.f, c.d)
		if got != c.want {
			t.Errorf("roundFloat(%v, %d): want %v, got %v", c.f, c.d, c.want, got)
		}
	}
}
