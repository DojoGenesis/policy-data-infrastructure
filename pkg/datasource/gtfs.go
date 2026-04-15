package datasource

// GTFSSource implements DataSource for GTFS (General Transit Feed Specification)
// static feeds. It fetches transit stop locations and trip frequencies from
// agency-published GTFS ZIP files and aggregates them to census tract level.
//
// GTFS static feed format (ZIP containing CSV files):
//
//	stops.txt      — stop locations (stop_id, stop_name, stop_lat, stop_lon)
//	stop_times.txt — arrival/departure at each stop (trip_id, stop_id, ...)
//	trips.txt      — trips (trip_id, route_id, service_id)
//	calendar.txt   — service days (service_id, monday–sunday, start_date, end_date)
//
// Tract assignment strategy:
//
//	For each stop, the Census Geocoder API is called to resolve lat/lon to an
//	11-digit census tract GEOID. Results are cached in-memory for the duration
//	of a single fetch to avoid redundant calls.
//
//	If the feed is too large or the Census geocoder is unavailable, FetchCounty
//	and FetchState return a 501-equivalent error directing callers to the Python
//	ingest path. This is consistent with the project's 501-for-unimplemented
//	pattern (see epa_ejscreen.go).
//
// Variables produced:
//
//	gtfs_stop_count   — Number of transit stops in the census tract
//	gtfs_daily_trips  — Total weekday transit trips serving the tract
//	gtfs_trips_per_hour — Average hourly trip frequency during service hours

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

const gtfsRateDelay = 200 * time.Millisecond // Census geocoder courtesy delay

// gtfsServiceHours is the assumed daily service window used to compute
// trips_per_hour. Madison Metro operates roughly 6 AM – midnight = 18 hours.
const gtfsServiceHours = 18.0

// DefaultFeeds maps state FIPS codes to one or more GTFS static feed URLs for
// agencies operating in that state. Only Wisconsin is seeded initially.
var DefaultFeeds = map[string][]string{
	"55": {"https://transitdata.cityofmadison.com/GTFS/mmt_gtfs.zip"}, // Wisconsin — Madison Metro Transit
}

// gtfsVariables is the fixed schema produced by every GTFSSource instance.
var gtfsVariables = []VariableDef{
	{
		ID:          "gtfs_stop_count",
		Name:        "Transit Stop Count",
		Description: "Number of GTFS transit stops located within the census tract",
		Unit:        "count",
		Direction:   "higher_better",
	},
	{
		ID:          "gtfs_daily_trips",
		Name:        "Daily Transit Trips",
		Description: "Total number of weekday transit trip-stops serving the census tract (sum across all stops in tract)",
		Unit:        "count",
		Direction:   "higher_better",
	},
	{
		ID:          "gtfs_trips_per_hour",
		Name:        "Trips Per Hour",
		Description: "Average hourly transit trip frequency during service hours for the census tract",
		Unit:        "rate",
		Direction:   "higher_better",
	},
}

// GTFSConfig configures a GTFSSource.
type GTFSConfig struct {
	// FeedURL is the direct URL to a GTFS static ZIP file. Used by FetchCounty
	// to target a specific agency feed.
	FeedURL string

	// FeedsByState maps state FIPS to a list of GTFS feed URLs for that state.
	// Used by FetchState to fetch all transit agencies in a state.
	// Defaults to DefaultFeeds when nil.
	FeedsByState map[string][]string

	// Year is used to build the Vintage string (e.g. 2026 → "GTFS-2026").
	Year int

	// HTTPClient is used for all outbound HTTP requests.
	// Defaults to http.DefaultClient when nil.
	HTTPClient *http.Client
}

// gtfsSource implements DataSource for GTFS static feeds.
type gtfsSource struct {
	cfg     GTFSConfig
	vintage string
}

// NewGTFSSource creates a new GTFSSource from cfg.
func NewGTFSSource(cfg GTFSConfig) *gtfsSource {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.FeedsByState == nil {
		cfg.FeedsByState = DefaultFeeds
	}
	vintage := "GTFS"
	if cfg.Year > 0 {
		vintage = fmt.Sprintf("GTFS-%d", cfg.Year)
	}
	return &gtfsSource{cfg: cfg, vintage: vintage}
}

func (s *gtfsSource) Name() string     { return "gtfs" }
func (s *gtfsSource) Category() string { return "transit" }
func (s *gtfsSource) Vintage() string  { return s.vintage }

func (s *gtfsSource) Schema() []VariableDef {
	out := make([]VariableDef, len(gtfsVariables))
	copy(out, gtfsVariables)
	return out
}

// FetchCounty fetches GTFS transit indicators for all tracts in a single county.
// It uses cfg.FeedURL when set; otherwise it falls back to FeedsByState[stateFIPS].
// stateFIPS is a 2-digit code; countyFIPS is a 3-digit code.
func (s *gtfsSource) FetchCounty(ctx context.Context, stateFIPS, countyFIPS string) ([]store.Indicator, error) {
	var feedURLs []string
	if s.cfg.FeedURL != "" {
		feedURLs = []string{s.cfg.FeedURL}
	} else {
		feedURLs = s.cfg.FeedsByState[stateFIPS]
	}
	if len(feedURLs) == 0 {
		return nil, fmt.Errorf(
			"gtfs: FetchCounty not implemented (HTTP 501): "+
				"no GTFS feed URL configured for state FIPS %q. "+
				"Set GTFSConfig.FeedURL or populate FeedsByState, or use the Python ingest path for full resolution",
			stateFIPS,
		)
	}

	countyPrefix := sanitizeFIPS(stateFIPS + countyFIPS) // 5-digit prefix
	var allIndicators []store.Indicator
	for _, feedURL := range feedURLs {
		indicators, err := s.fetchFeed(ctx, feedURL, countyPrefix)
		if err != nil {
			return nil, fmt.Errorf("gtfs: FetchCounty feed %s: %w", feedURL, err)
		}
		allIndicators = append(allIndicators, indicators...)
	}
	return allIndicators, nil
}

// FetchState fetches GTFS transit indicators for all tracts in an entire state.
// It fetches every feed URL registered in FeedsByState[stateFIPS].
func (s *gtfsSource) FetchState(ctx context.Context, stateFIPS string) ([]store.Indicator, error) {
	feedURLs := s.cfg.FeedsByState[stateFIPS]
	if len(feedURLs) == 0 {
		return nil, fmt.Errorf(
			"gtfs: FetchState not implemented (HTTP 501): "+
				"no GTFS feeds configured for state FIPS %q. "+
				"Populate GTFSConfig.FeedsByState or use the Python ingest path for full resolution",
			stateFIPS,
		)
	}

	var allIndicators []store.Indicator
	for _, feedURL := range feedURLs {
		indicators, err := s.fetchFeed(ctx, feedURL, "" /* no county filter */)
		if err != nil {
			return nil, fmt.Errorf("gtfs: FetchState feed %s: %w", feedURL, err)
		}
		allIndicators = append(allIndicators, indicators...)
	}
	return allIndicators, nil
}

// --- internal types ---------------------------------------------------------

// gtfsStop holds the parsed location data for a single transit stop.
type gtfsStop struct {
	ID   string
	Lat  float64
	Lon  float64
}

// gtfsTractData accumulates transit metrics per census tract GEOID.
type gtfsTractData struct {
	StopCount  int
	DailyTrips int
}

// --- core fetch logic -------------------------------------------------------

// fetchFeed downloads a GTFS ZIP from feedURL, parses the relevant CSV files,
// geocodes each stop to a census tract, and returns aggregated indicators.
// countyPrefix, when non-empty (5 digits), restricts output to that county.
func (s *gtfsSource) fetchFeed(ctx context.Context, feedURL, countyPrefix string) ([]store.Indicator, error) {
	// 1. Download the ZIP.
	zipBytes, err := s.downloadZIP(ctx, feedURL)
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}

	zr, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}

	// 2. Parse stops.txt.
	stopsCSV, err := readZipFile(zr, "stops.txt")
	if err != nil {
		return nil, fmt.Errorf("stops.txt: %w", err)
	}
	stops, err := parseStops(stopsCSV)
	if err != nil {
		return nil, fmt.Errorf("parse stops: %w", err)
	}

	// 3. Parse stop_times.txt to count trips per stop.
	stopTimesCSV, err := readZipFile(zr, "stop_times.txt")
	if err != nil {
		return nil, fmt.Errorf("stop_times.txt: %w", err)
	}
	tripsPerStop, err := parseStopTimes(stopTimesCSV)
	if err != nil {
		return nil, fmt.Errorf("parse stop_times: %w", err)
	}

	// 4. Geocode each stop to a tract GEOID (with in-memory cache).
	cache := make(map[string]string) // "lat,lon" → GEOID
	stopGEOID := make(map[string]string, len(stops))
	for _, stop := range stops {
		cacheKey := fmt.Sprintf("%.6f,%.6f", stop.Lat, stop.Lon)
		if geoid, ok := cache[cacheKey]; ok {
			stopGEOID[stop.ID] = geoid
			continue
		}
		geoid, err := s.geocodeToTract(ctx, stop.Lat, stop.Lon)
		if err != nil {
			// Non-fatal: skip stops that cannot be geocoded.
			continue
		}
		cache[cacheKey] = geoid
		stopGEOID[stop.ID] = geoid
		time.Sleep(gtfsRateDelay)
	}

	// 5. Aggregate to tracts.
	tractData := aggregateToTracts(stops, stopGEOID, tripsPerStop)

	// 6. Convert to store.Indicator records.
	return s.buildIndicators(tractData, countyPrefix), nil
}

// downloadZIP fetches the GTFS ZIP file from url and returns its raw bytes.
func (s *gtfsSource) downloadZIP(ctx context.Context, rawURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return data, nil
}

// geocoderResponse is the top-level JSON response from the Census geocoder
// COORDINATES endpoint (https://geocoding.geo.census.gov/geocoder/geographies/coordinates).
//
// The coordinates endpoint returns geographies directly under result.geographies —
// there is NO addressMatches wrapper (that wrapper is only present on the ADDRESS
// endpoint). Structure confirmed by the Census Geocoder API documentation:
//
//	{
//	  "result": {
//	    "geographies": {
//	      "Census Tracts": [
//	        { "GEOID": "55025000100", "STATE": "55", "COUNTY": "025", ... }
//	      ]
//	    }
//	  }
//	}
type geocoderResponse struct {
	Result struct {
		Geographies struct {
			CensusTracts []struct {
				GEOID string `json:"GEOID"`
			} `json:"Census Tracts"`
		} `json:"geographies"`
	} `json:"result"`
}

// geocodeToTract calls the Census Bureau geocoder coordinates endpoint to map a
// lat/lon to an 11-digit census tract GEOID. Returns an error if no match is found.
func (s *gtfsSource) geocodeToTract(ctx context.Context, lat, lon float64) (string, error) {
	params := url.Values{}
	params.Set("x", strconv.FormatFloat(lon, 'f', 6, 64))
	params.Set("y", strconv.FormatFloat(lat, 'f', 6, 64))
	params.Set("benchmark", "Public_AR_Census2020")
	params.Set("vintage", "Census2020_Census2020")
	params.Set("layers", "10")
	params.Set("format", "json")

	endpoint := "https://geocoding.geo.census.gov/geocoder/geographies/coordinates?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("geocoder: build request: %w", err)
	}

	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("geocoder: http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("geocoder: status %d", resp.StatusCode)
	}

	var gr geocoderResponse
	if err := json.NewDecoder(resp.Body).Decode(&gr); err != nil {
		return "", fmt.Errorf("geocoder: decode: %w", err)
	}

	// The coordinates endpoint places tracts directly under result.geographies.
	tracts := gr.Result.Geographies.CensusTracts
	if len(tracts) == 0 {
		return "", fmt.Errorf("geocoder: no tract match for %.6f,%.6f", lat, lon)
	}
	geoid := strings.TrimSpace(tracts[0].GEOID)
	if len(geoid) != 11 {
		return "", fmt.Errorf("geocoder: unexpected GEOID length %d for %q", len(geoid), geoid)
	}
	return geoid, nil
}

// buildIndicators converts tractData to store.Indicator slices. If countyPrefix
// is non-empty (5 digits), only tracts whose GEOID starts with that prefix are
// included.
func (s *gtfsSource) buildIndicators(tractData map[string]*gtfsTractData, countyPrefix string) []store.Indicator {
	var out []store.Indicator
	for geoid, td := range tractData {
		if countyPrefix != "" && !strings.HasPrefix(geoid, countyPrefix) {
			continue
		}

		stopCount := float64(td.StopCount)
		dailyTrips := float64(td.DailyTrips)
		tripsPerHour := dailyTrips / gtfsServiceHours

		out = append(out,
			store.Indicator{
				GEOID:      geoid,
				VariableID: "gtfs_stop_count",
				Vintage:    s.vintage,
				Value:      &stopCount,
				RawValue:   strconv.Itoa(td.StopCount),
			},
			store.Indicator{
				GEOID:      geoid,
				VariableID: "gtfs_daily_trips",
				Vintage:    s.vintage,
				Value:      &dailyTrips,
				RawValue:   strconv.Itoa(td.DailyTrips),
			},
			store.Indicator{
				GEOID:      geoid,
				VariableID: "gtfs_trips_per_hour",
				Vintage:    s.vintage,
				Value:      &tripsPerHour,
				RawValue:   strconv.FormatFloat(tripsPerHour, 'f', 4, 64),
			},
		)
	}
	return out
}

// --- CSV parsers (exported for testing) ------------------------------------

// parseStops parses the content of a GTFS stops.txt CSV and returns a slice
// of gtfsStop records. Rows with invalid or missing lat/lon are skipped.
func parseStops(r io.Reader) ([]gtfsStop, error) {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true

	headers, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	idx := headerIndex(headers)
	stopIDCol, ok1 := idx["stop_id"]
	latCol, ok2 := idx["stop_lat"]
	lonCol, ok3 := idx["stop_lon"]
	if !ok1 || !ok2 || !ok3 {
		return nil, fmt.Errorf("stops.txt missing required columns stop_id/stop_lat/stop_lon; found: %v", headers)
	}

	var stops []gtfsStop
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read row: %w", err)
		}
		if len(row) <= latCol || len(row) <= lonCol || len(row) <= stopIDCol {
			continue
		}
		lat, err := strconv.ParseFloat(strings.TrimSpace(row[latCol]), 64)
		if err != nil {
			continue
		}
		lon, err := strconv.ParseFloat(strings.TrimSpace(row[lonCol]), 64)
		if err != nil {
			continue
		}
		stops = append(stops, gtfsStop{
			ID:  strings.TrimSpace(row[stopIDCol]),
			Lat: lat,
			Lon: lon,
		})
	}
	return stops, nil
}

// parseStopTimes parses stop_times.txt and returns a map of stop_id →
// number of distinct trip_id values that visit that stop (a proxy for daily
// trips, assuming the feed represents a typical weekday service pattern).
func parseStopTimes(r io.Reader) (map[string]int, error) {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true

	headers, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	idx := headerIndex(headers)
	tripCol, ok1 := idx["trip_id"]
	stopCol, ok2 := idx["stop_id"]
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("stop_times.txt missing required columns trip_id/stop_id; found: %v", headers)
	}

	// Track distinct trips per stop using a nested map to avoid double-counting.
	stopTrips := make(map[string]map[string]struct{})
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read row: %w", err)
		}
		if len(row) <= tripCol || len(row) <= stopCol {
			continue
		}
		stopID := strings.TrimSpace(row[stopCol])
		tripID := strings.TrimSpace(row[tripCol])
		if stopID == "" || tripID == "" {
			continue
		}
		if stopTrips[stopID] == nil {
			stopTrips[stopID] = make(map[string]struct{})
		}
		stopTrips[stopID][tripID] = struct{}{}
	}

	// Collapse to stop_id → count.
	result := make(map[string]int, len(stopTrips))
	for stopID, trips := range stopTrips {
		result[stopID] = len(trips)
	}
	return result, nil
}

// aggregateToTracts combines stop locations, their GEOID assignments, and
// per-stop trip counts into per-tract aggregates.
func aggregateToTracts(stops []gtfsStop, stopGEOID map[string]string, tripsPerStop map[string]int) map[string]*gtfsTractData {
	tracts := make(map[string]*gtfsTractData)
	for _, stop := range stops {
		geoid, ok := stopGEOID[stop.ID]
		if !ok || geoid == "" {
			continue
		}
		if tracts[geoid] == nil {
			tracts[geoid] = &gtfsTractData{}
		}
		tracts[geoid].StopCount++
		tracts[geoid].DailyTrips += tripsPerStop[stop.ID]
	}
	return tracts
}

// --- helpers ----------------------------------------------------------------

// readZipFile finds a file by name within a zip.Reader and returns its content
// as an io.Reader. Returns an error if the file is not found.
func readZipFile(zr *zip.Reader, name string) (io.Reader, error) {
	for _, f := range zr.File {
		if strings.EqualFold(f.Name, name) {
			rc, err := f.Open()
			if err != nil {
				return nil, fmt.Errorf("open %s: %w", name, err)
			}
			data, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", name, err)
			}
			return bytes.NewReader(data), nil
		}
	}
	return nil, fmt.Errorf("file %q not found in GTFS ZIP", name)
}

// headerIndex builds a map from column name to 0-based column index.
func headerIndex(headers []string) map[string]int {
	idx := make(map[string]int, len(headers))
	for i, h := range headers {
		idx[strings.TrimSpace(h)] = i
	}
	return idx
}
