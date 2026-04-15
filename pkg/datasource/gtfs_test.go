package datasource

import (
	"strings"
	"testing"
)

// ---- TestNewGTFSSource -------------------------------------------------------

func TestNewGTFSSource_Defaults(t *testing.T) {
	s := NewGTFSSource(GTFSConfig{Year: 2026})

	if s.Name() != "gtfs" {
		t.Errorf("Name(): want gtfs, got %q", s.Name())
	}
	if s.Category() != "transit" {
		t.Errorf("Category(): want transit, got %q", s.Category())
	}
	if s.Vintage() != "GTFS-2026" {
		t.Errorf("Vintage(): want GTFS-2026, got %q", s.Vintage())
	}
	if s.cfg.HTTPClient == nil {
		t.Error("HTTPClient must not be nil after NewGTFSSource")
	}
	if len(s.cfg.FeedsByState) == 0 {
		t.Error("FeedsByState must be populated by default (DefaultFeeds)")
	}
	// DefaultFeeds must include Wisconsin (FIPS 55).
	if _, ok := s.cfg.FeedsByState["55"]; !ok {
		t.Error("DefaultFeeds must contain entry for state FIPS 55 (Wisconsin)")
	}
}

func TestNewGTFSSource_ZeroYear(t *testing.T) {
	s := NewGTFSSource(GTFSConfig{})
	if s.Vintage() != "GTFS" {
		t.Errorf("Vintage() with zero Year: want GTFS, got %q", s.Vintage())
	}
}

func TestGTFSSource_InterfaceSatisfied(t *testing.T) {
	// Compile-time check: gtfsSource must satisfy DataSource.
	var _ DataSource = NewGTFSSource(GTFSConfig{Year: 2026})
}

// ---- TestGTFSSchema ----------------------------------------------------------

func TestGTFSSchema_VariableCount(t *testing.T) {
	s := NewGTFSSource(GTFSConfig{Year: 2026})
	schema := s.Schema()
	if len(schema) != 3 {
		t.Errorf("Schema() length: want 3, got %d", len(schema))
	}
}

func TestGTFSSchema_RequiredFields(t *testing.T) {
	s := NewGTFSSource(GTFSConfig{Year: 2026})
	for _, def := range s.Schema() {
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
}

func TestGTFSSchema_ExpectedVariableIDs(t *testing.T) {
	s := NewGTFSSource(GTFSConfig{Year: 2026})
	wantIDs := map[string]bool{
		"gtfs_stop_count":    false,
		"gtfs_daily_trips":   false,
		"gtfs_trips_per_hour": false,
	}
	for _, def := range s.Schema() {
		if _, ok := wantIDs[def.ID]; ok {
			wantIDs[def.ID] = true
		}
	}
	for id, found := range wantIDs {
		if !found {
			t.Errorf("Schema() missing expected variable ID %q", id)
		}
	}
}

// ---- TestParseStops ----------------------------------------------------------

const stopsCSV = `stop_id,stop_code,stop_name,stop_lat,stop_lon
S1,1001,State Street & Park St,43.0731,-89.4012
S2,1002,University Ave & Charter St,43.0755,-89.4082
S3,1003,Capitol Square,43.0744,-89.3838
`

func TestParseStops_Count(t *testing.T) {
	stops, err := parseStops(strings.NewReader(stopsCSV))
	if err != nil {
		t.Fatalf("parseStops error: %v", err)
	}
	if len(stops) != 3 {
		t.Errorf("want 3 stops, got %d", len(stops))
	}
}

func TestParseStops_Fields(t *testing.T) {
	stops, err := parseStops(strings.NewReader(stopsCSV))
	if err != nil {
		t.Fatalf("parseStops error: %v", err)
	}
	s := stops[0]
	if s.ID != "S1" {
		t.Errorf("stop_id: want S1, got %q", s.ID)
	}
	if s.Lat != 43.0731 {
		t.Errorf("stop_lat: want 43.0731, got %v", s.Lat)
	}
	if s.Lon != -89.4012 {
		t.Errorf("stop_lon: want -89.4012, got %v", s.Lon)
	}
}

func TestParseStops_MissingColumn(t *testing.T) {
	// stop_lon column is absent — should return an error.
	bad := "stop_id,stop_lat\nS1,43.07\n"
	_, err := parseStops(strings.NewReader(bad))
	if err == nil {
		t.Error("expected error for missing stop_lon column, got nil")
	}
}

func TestParseStops_InvalidLatSkipped(t *testing.T) {
	// Row with non-numeric lat should be skipped silently.
	csv := "stop_id,stop_lat,stop_lon\nS1,bad,-89.0\nS2,43.07,-89.4\n"
	stops, err := parseStops(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("parseStops error: %v", err)
	}
	// Only S2 should be present (S1 skipped due to bad lat).
	if len(stops) != 1 {
		t.Errorf("want 1 stop (invalid row skipped), got %d", len(stops))
	}
}

// ---- TestParseStopTimes ------------------------------------------------------

const stopTimesCSV = `trip_id,arrival_time,departure_time,stop_id,stop_sequence
T1,08:00:00,08:00:00,S1,1
T1,08:05:00,08:05:00,S2,2
T2,08:10:00,08:10:00,S1,1
T2,08:15:00,08:15:00,S2,2
T3,09:00:00,09:00:00,S2,1
`

func TestParseStopTimes_TripCounts(t *testing.T) {
	counts, err := parseStopTimes(strings.NewReader(stopTimesCSV))
	if err != nil {
		t.Fatalf("parseStopTimes error: %v", err)
	}
	// S1 is visited by T1 and T2 → 2 distinct trips.
	if counts["S1"] != 2 {
		t.Errorf("S1 trip count: want 2, got %d", counts["S1"])
	}
	// S2 is visited by T1, T2, T3 → 3 distinct trips.
	if counts["S2"] != 3 {
		t.Errorf("S2 trip count: want 3, got %d", counts["S2"])
	}
}

func TestParseStopTimes_DuplicateTripNotDoubleCounted(t *testing.T) {
	// Same trip_id + stop_id twice (should count as 1).
	csv := "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,08:00:00,08:00:00,S1,1\nT1,08:00:00,08:00:00,S1,2\n"
	counts, err := parseStopTimes(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("parseStopTimes error: %v", err)
	}
	if counts["S1"] != 1 {
		t.Errorf("duplicate trip: want count 1 for S1, got %d", counts["S1"])
	}
}

func TestParseStopTimes_MissingColumn(t *testing.T) {
	bad := "trip_id,arrival_time\nT1,08:00:00\n"
	_, err := parseStopTimes(strings.NewReader(bad))
	if err == nil {
		t.Error("expected error for missing stop_id column, got nil")
	}
}

// ---- TestAggregateToTracts --------------------------------------------------

func TestAggregateToTracts_Basic(t *testing.T) {
	stops := []gtfsStop{
		{ID: "S1", Lat: 43.07, Lon: -89.40},
		{ID: "S2", Lat: 43.07, Lon: -89.41},
		{ID: "S3", Lat: 43.08, Lon: -89.42},
	}
	// S1 and S2 → tract A; S3 → tract B.
	stopGEOID := map[string]string{
		"S1": "55025000100",
		"S2": "55025000100",
		"S3": "55025000200",
	}
	tripsPerStop := map[string]int{
		"S1": 10,
		"S2": 5,
		"S3": 8,
	}

	result := aggregateToTracts(stops, stopGEOID, tripsPerStop)

	tractA := result["55025000100"]
	if tractA == nil {
		t.Fatal("expected tract 55025000100 in result")
	}
	if tractA.StopCount != 2 {
		t.Errorf("tract A StopCount: want 2, got %d", tractA.StopCount)
	}
	if tractA.DailyTrips != 15 {
		t.Errorf("tract A DailyTrips: want 15 (10+5), got %d", tractA.DailyTrips)
	}

	tractB := result["55025000200"]
	if tractB == nil {
		t.Fatal("expected tract 55025000200 in result")
	}
	if tractB.StopCount != 1 {
		t.Errorf("tract B StopCount: want 1, got %d", tractB.StopCount)
	}
	if tractB.DailyTrips != 8 {
		t.Errorf("tract B DailyTrips: want 8, got %d", tractB.DailyTrips)
	}
}

func TestAggregateToTracts_UnmappedStopSkipped(t *testing.T) {
	stops := []gtfsStop{
		{ID: "S1"},
		{ID: "S2"}, // no GEOID in stopGEOID
	}
	stopGEOID := map[string]string{"S1": "55025000100"}
	tripsPerStop := map[string]int{"S1": 4, "S2": 99}

	result := aggregateToTracts(stops, stopGEOID, tripsPerStop)
	if len(result) != 1 {
		t.Errorf("want 1 tract (S2 skipped), got %d", len(result))
	}
	if result["55025000100"].DailyTrips != 4 {
		t.Errorf("DailyTrips: want 4, got %d", result["55025000100"].DailyTrips)
	}
}

// ---- TestBuildIndicators (via gtfsSource) ------------------------------------

func TestBuildIndicators_TripsPerHourComputed(t *testing.T) {
	s := NewGTFSSource(GTFSConfig{Year: 2026})
	tractData := map[string]*gtfsTractData{
		"55025000100": {StopCount: 2, DailyTrips: 36},
	}

	indicators := s.buildIndicators(tractData, "")
	if len(indicators) != 3 {
		t.Fatalf("want 3 indicators, got %d", len(indicators))
	}

	for _, ind := range indicators {
		if ind.GEOID != "55025000100" {
			t.Errorf("GEOID: want 55025000100, got %q", ind.GEOID)
		}
		if ind.Vintage != "GTFS-2026" {
			t.Errorf("Vintage: want GTFS-2026, got %q", ind.Vintage)
		}
		if ind.Value == nil {
			t.Errorf("VariableID %q: Value must not be nil", ind.VariableID)
		}
		switch ind.VariableID {
		case "gtfs_stop_count":
			if *ind.Value != 2 {
				t.Errorf("stop_count: want 2, got %v", *ind.Value)
			}
		case "gtfs_daily_trips":
			if *ind.Value != 36 {
				t.Errorf("daily_trips: want 36, got %v", *ind.Value)
			}
		case "gtfs_trips_per_hour":
			// 36 trips / 18 hours = 2.0
			want := 36.0 / gtfsServiceHours
			if *ind.Value != want {
				t.Errorf("trips_per_hour: want %v, got %v", want, *ind.Value)
			}
		default:
			t.Errorf("unexpected VariableID %q", ind.VariableID)
		}
	}
}

func TestBuildIndicators_CountyFilter(t *testing.T) {
	s := NewGTFSSource(GTFSConfig{Year: 2026})
	tractData := map[string]*gtfsTractData{
		"55025000100": {StopCount: 1, DailyTrips: 10},
		"55079000200": {StopCount: 2, DailyTrips: 20}, // Milwaukee county
	}

	// Filter to Dane County (55025).
	indicators := s.buildIndicators(tractData, "55025")
	for _, ind := range indicators {
		if !strings.HasPrefix(ind.GEOID, "55025") {
			t.Errorf("county filter failed: unexpected GEOID %q", ind.GEOID)
		}
	}
	if len(indicators) != 3 {
		t.Errorf("want 3 indicators for one tract, got %d", len(indicators))
	}
}

// ---- TestDefaultFeeds --------------------------------------------------------

func TestDefaultFeeds_Wisconsin(t *testing.T) {
	urls, ok := DefaultFeeds["55"]
	if !ok {
		t.Fatal("DefaultFeeds missing Wisconsin (FIPS 55)")
	}
	if len(urls) == 0 {
		t.Error("DefaultFeeds[55] must have at least one feed URL")
	}
	for _, u := range urls {
		if !strings.HasPrefix(u, "https://") {
			t.Errorf("feed URL %q should start with https://", u)
		}
	}
}
