package gateway

import "github.com/DojoGenesis/policy-data-infrastructure/pkg/store"

// ── Request types ───────────────────────────────────────────────────────────

// QueryRequest is the body for POST /query.
type QueryRequest struct {
	Level       string   `json:"level"`        // geo level filter (optional)
	ParentGEOID string   `json:"parent_geoid"` // restrict to children of this GEOID
	StateFIPS   string   `json:"state_fips"`   // restrict to a state
	NameSearch  string   `json:"name_search"`  // fuzzy name search
	Limit       int      `json:"limit"`
	Offset      int      `json:"offset"`
	VariableIDs []string `json:"variable_ids"` // inline indicators to fetch
	Vintage     string   `json:"vintage"`
}

// CompareRequest is the body for POST /compare.
type CompareRequest struct {
	GEOID1      string   `json:"geoid1" binding:"required"`
	GEOID2      string   `json:"geoid2" binding:"required"`
	VariableIDs []string `json:"variable_ids"` // if empty, fetch all available
	Vintage     string   `json:"vintage"`
}

// NarrativeRequest is the body for POST /generate/narrative.
type NarrativeRequest struct {
	GEOID   string `json:"geoid" binding:"required"`
	Vintage string `json:"vintage"`
	// TemplateID selects a narrative template; defaults to "summary".
	TemplateID string `json:"template_id"`
}

// DeliverableRequest is the body for POST /generate/deliverable.
type DeliverableRequest struct {
	GEOID         string     `json:"geoid" binding:"required"`
	Title         string     `json:"title"`
	Vintage       string     `json:"vintage"`
	TemplateID    string     `json:"template_id"`
	IncludeMap    bool       `json:"include_map"`
	IncludeCharts bool       `json:"include_charts"`
	MapCenter     [2]float64 `json:"map_center"`
	MapZoom       int        `json:"map_zoom"`
	TileLayer     string     `json:"tile_layer"`
	Components    []string   `json:"components"`
}

// PipelineRunRequest is the body for POST /pipeline/run.
type PipelineRunRequest struct {
	Source  string                 `json:"source" binding:"required"` // e.g. "census", "tiger"
	Level   string                 `json:"level"`
	Vintage string                 `json:"vintage"`
	Params  map[string]interface{} `json:"params"`
}

// ── Response types ──────────────────────────────────────────────────────────

// ErrorResponse is returned on any HTTP error.
type ErrorResponse struct {
	Error  string `json:"error"`
	Detail string `json:"detail,omitempty"`
}

// GeographyResponse wraps a geography with optional embedded indicator data.
type GeographyResponse struct {
	GEOID       string              `json:"geoid"`
	Level       string              `json:"level"`
	ParentGEOID string              `json:"parent_geoid,omitempty"`
	Name        string              `json:"name"`
	StateFIPS   string              `json:"state_fips,omitempty"`
	CountyFIPS  string              `json:"county_fips,omitempty"`
	Population  int                 `json:"population"`
	LandAreaM2  float64             `json:"land_area_m2"`
	Lat         float64             `json:"lat"`
	Lon         float64             `json:"lon"`
	Indicators  []IndicatorResponse `json:"indicators,omitempty"`
	Scores      []ScoreResponse     `json:"scores,omitempty"`
}

// IndicatorResponse is a single indicator value in a response.
type IndicatorResponse struct {
	VariableID    string   `json:"variable_id"`
	Vintage       string   `json:"vintage"`
	Value         *float64 `json:"value"`
	MarginOfError *float64 `json:"margin_of_error,omitempty"`
	RawValue      string   `json:"raw_value,omitempty"`
}

// ScoreResponse is a single analysis score in a response.
type ScoreResponse struct {
	AnalysisID string                 `json:"analysis_id"`
	Score      float64                `json:"score"`
	Rank       int                    `json:"rank"`
	Percentile float64                `json:"percentile"`
	Tier       string                 `json:"tier,omitempty"`
	Details    map[string]interface{} `json:"details,omitempty"`
}

// GeographyListResponse wraps a paginated list of geographies.
type GeographyListResponse struct {
	Items  []GeographyResponse `json:"items"`
	Total  int                 `json:"total"`
	Limit  int                 `json:"limit"`
	Offset int                 `json:"offset"`
}

// CompareResponse holds a side-by-side comparison of two geographies.
type CompareResponse struct {
	Geography1  GeographyResponse            `json:"geography1"`
	Geography2  GeographyResponse            `json:"geography2"`
	Differences []IndicatorDiffResponse      `json:"differences,omitempty"`
}

// IndicatorDiffResponse compares one indicator across two geographies.
type IndicatorDiffResponse struct {
	VariableID string   `json:"variable_id"`
	Vintage    string   `json:"vintage"`
	Value1     *float64 `json:"value1"`
	Value2     *float64 `json:"value2"`
	Diff       *float64 `json:"diff,omitempty"`
	PctDiff    *float64 `json:"pct_diff,omitempty"`
}

// NarrativeResponse wraps generated narrative HTML.
type NarrativeResponse struct {
	GEOID   string `json:"geoid"`
	HTML    string `json:"html"`
	Vintage string `json:"vintage"`
}

// SourceResponse describes a registered data source.
type SourceResponse struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Levels      []string `json:"levels"`
	Description string   `json:"description,omitempty"`
}

// PipelineRunResponse is returned immediately when a pipeline run is accepted.
type PipelineRunResponse struct {
	RunID   string `json:"run_id"`
	Source  string `json:"source"`
	Message string `json:"message"`
}

// PipelineEvent is one event emitted on the SSE stream.
type PipelineEvent struct {
	RunID   string `json:"run_id"`
	Stage   string `json:"stage"`
	Status  string `json:"status"` // "running", "done", "error"
	Message string `json:"message,omitempty"`
	Count   int    `json:"count,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ── Conversion helpers ──────────────────────────────────────────────────────

// geoToResponse converts a store Geography and optional indicators/scores into
// the HTTP response shape.
func geoToResponse(g interface{ getFields() geoFields }, inds []store.Indicator, scores []store.AnalysisScore) GeographyResponse {
	f := g.getFields()
	resp := GeographyResponse{
		GEOID:       f.GEOID,
		Level:       f.Level,
		ParentGEOID: f.ParentGEOID,
		Name:        f.Name,
		StateFIPS:   f.StateFIPS,
		CountyFIPS:  f.CountyFIPS,
		Population:  f.Population,
		LandAreaM2:  f.LandAreaM2,
		Lat:         f.Lat,
		Lon:         f.Lon,
	}
	for _, ind := range inds {
		resp.Indicators = append(resp.Indicators, IndicatorResponse{
			VariableID:    ind.VariableID,
			Vintage:       ind.Vintage,
			Value:         ind.Value,
			MarginOfError: ind.MarginOfError,
			RawValue:      ind.RawValue,
		})
	}
	for _, s := range scores {
		resp.Scores = append(resp.Scores, ScoreResponse{
			AnalysisID: s.AnalysisID,
			Score:      s.Score,
			Rank:       s.Rank,
			Percentile: s.Percentile,
			Tier:       s.Tier,
			Details:    s.Details,
		})
	}
	return resp
}

// geoFields is a flat struct used by geoToResponse to avoid importing geo
// package types directly in this file.
type geoFields struct {
	GEOID, Level, ParentGEOID, Name, StateFIPS, CountyFIPS string
	Population                                               int
	LandAreaM2, Lat, Lon                                     float64
}
