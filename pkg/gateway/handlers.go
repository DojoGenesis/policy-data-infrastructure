package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/htmlcraft"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// ── GET /geographies ────────────────────────────────────────────────────────

// handleListGeographies lists geographies with optional query-parameter filters.
//
// Query params: level, parent_geoid, state_fips, name, limit (default 50), offset.
func (p *PolicyPlugin) handleListGeographies(c *gin.Context) {
	q := store.GeoQuery{
		Limit:  50,
		Offset: 0,
	}

	if lvl := c.Query("level"); lvl != "" {
		l, err := geo.LevelFromString(lvl)
		if err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid level", Detail: err.Error()})
			return
		}
		q.Level = l
	}
	q.ParentGEOID = c.Query("parent_geoid")
	q.StateFIPS = c.Query("state_fips")
	q.NameSearch = c.Query("name")

	if lim := c.Query("limit"); lim != "" {
		n, err := strconv.Atoi(lim)
		if err != nil || n < 1 {
			c.JSON(http.StatusBadRequest, ErrorResponse{Error: "limit must be a positive integer"})
			return
		}
		if n > 1000 {
			n = 1000
		}
		q.Limit = n
	}
	if off := c.Query("offset"); off != "" {
		n, err := strconv.Atoi(off)
		if err != nil || n < 0 {
			c.JSON(http.StatusBadRequest, ErrorResponse{Error: "offset must be a non-negative integer"})
			return
		}
		q.Offset = n
	}

	geos, err := p.store.QueryGeographies(c.Request.Context(), q)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "query failed", Detail: err.Error()})
		return
	}

	items := make([]GeographyResponse, 0, len(geos))
	for _, g := range geos {
		items = append(items, geoFromStore(g, nil, nil))
	}

	c.JSON(http.StatusOK, GeographyListResponse{
		Items:  items,
		Total:  len(items),
		Limit:  q.Limit,
		Offset: q.Offset,
	})
}

// ── GET /geographies/:geoid ─────────────────────────────────────────────────

// handleGetGeography returns the full geography profile for a single GEOID,
// optionally embedding indicators and analysis scores.
//
// Query params: vintage, analysis_id (for scores).
func (p *PolicyPlugin) handleGetGeography(c *gin.Context) {
	geoid := c.Param("geoid")

	g, err := p.store.GetGeography(c.Request.Context(), geoid)
	if err != nil {
		status := http.StatusInternalServerError
		if isNotFound(err) {
			status = http.StatusNotFound
		}
		c.JSON(status, ErrorResponse{Error: "geography not found", Detail: err.Error()})
		return
	}

	// Optionally embed indicator data.
	var inds []store.Indicator
	vintage := c.Query("vintage")
	indQ := store.IndicatorQuery{
		GEOIDs:     []string{geoid},
		LatestOnly: vintage == "",
	}
	if vintage != "" {
		indQ.Vintage = vintage
	}
	inds, err = p.store.QueryIndicators(c.Request.Context(), indQ)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "indicator query failed", Detail: err.Error()})
		return
	}

	// Optionally embed analysis scores.
	var scores []store.AnalysisScore
	if analysisID := c.Query("analysis_id"); analysisID != "" {
		scores, err = p.store.QueryAnalysisScores(c.Request.Context(), analysisID, "")
		if err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "score query failed", Detail: err.Error()})
			return
		}
		// Filter to this GEOID only.
		filtered := scores[:0]
		for _, s := range scores {
			if s.GEOID == geoid {
				filtered = append(filtered, s)
			}
		}
		scores = filtered
	}

	c.JSON(http.StatusOK, geoFromStore(*g, inds, scores))
}

// ── GET /geographies/:geoid/children ───────────────────────────────────────

// handleGetChildren returns the immediate children of a geography in the
// hierarchy (e.g. tracts within a county).
//
// Query params: limit (default 200), offset.
func (p *PolicyPlugin) handleGetChildren(c *gin.Context) {
	geoid := c.Param("geoid")

	parent, err := p.store.GetGeography(c.Request.Context(), geoid)
	if err != nil {
		status := http.StatusInternalServerError
		if isNotFound(err) {
			status = http.StatusNotFound
		}
		c.JSON(status, ErrorResponse{Error: "geography not found", Detail: err.Error()})
		return
	}

	childLevel, ok := geo.ChildLevel(parent.Level)
	if !ok {
		c.JSON(http.StatusOK, GeographyListResponse{Items: []GeographyResponse{}, Total: 0, Limit: 200, Offset: 0})
		return
	}

	limit := 200
	offset := 0
	if lim := c.Query("limit"); lim != "" {
		if n, err := strconv.Atoi(lim); err == nil && n > 0 {
			if n > 2000 {
				n = 2000
			}
			limit = n
		}
	}
	if off := c.Query("offset"); off != "" {
		if n, err := strconv.Atoi(off); err == nil && n >= 0 {
			offset = n
		}
	}

	geos, err := p.store.QueryGeographies(c.Request.Context(), store.GeoQuery{
		Level:       childLevel,
		ParentGEOID: geoid,
		Limit:       limit,
		Offset:      offset,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "query failed", Detail: err.Error()})
		return
	}

	items := make([]GeographyResponse, 0, len(geos))
	for _, g := range geos {
		items = append(items, geoFromStore(g, nil, nil))
	}

	c.JSON(http.StatusOK, GeographyListResponse{
		Items:  items,
		Total:  len(items),
		Limit:  limit,
		Offset: offset,
	})
}

// ── GET /geographies/:geoid/indicators ─────────────────────────────────────

// handleGetIndicators returns all indicators for a geography.
//
// Query params: variable_id (repeatable), vintage, latest (bool, default true).
func (p *PolicyPlugin) handleGetIndicators(c *gin.Context) {
	geoid := c.Param("geoid")

	q := store.IndicatorQuery{
		GEOIDs:     []string{geoid},
		LatestOnly: true,
	}
	q.VariableIDs = c.QueryArray("variable_id")
	if v := c.Query("vintage"); v != "" {
		q.Vintage = v
		q.LatestOnly = false
	}
	if latest := c.Query("latest"); latest == "false" {
		q.LatestOnly = false
	}

	inds, err := p.store.QueryIndicators(c.Request.Context(), q)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "indicator query failed", Detail: err.Error()})
		return
	}

	items := make([]IndicatorResponse, 0, len(inds))
	for _, ind := range inds {
		items = append(items, IndicatorResponse{
			VariableID:    ind.VariableID,
			Vintage:       ind.Vintage,
			Value:         ind.Value,
			MarginOfError: ind.MarginOfError,
			RawValue:      ind.RawValue,
		})
	}

	c.JSON(http.StatusOK, gin.H{"geoid": geoid, "indicators": items, "total": len(items)})
}

// ── POST /query ─────────────────────────────────────────────────────────────

// handleQuery runs a flexible geography query with optional inline indicator
// embedding.
func (p *PolicyPlugin) handleQuery(c *gin.Context) {
	var req QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid request body", Detail: err.Error()})
		return
	}

	q := store.GeoQuery{
		ParentGEOID: req.ParentGEOID,
		StateFIPS:   req.StateFIPS,
		NameSearch:  req.NameSearch,
		Limit:       req.Limit,
		Offset:      req.Offset,
	}
	if q.Limit == 0 {
		q.Limit = 50
	}
	if req.Level != "" {
		l, err := geo.LevelFromString(req.Level)
		if err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid level", Detail: err.Error()})
			return
		}
		q.Level = l
	}

	geos, err := p.store.QueryGeographies(c.Request.Context(), q)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "query failed", Detail: err.Error()})
		return
	}

	items := make([]GeographyResponse, 0, len(geos))

	if len(req.VariableIDs) > 0 {
		// Fetch indicators for all matched GEOIDs in one query.
		geoids := make([]string, len(geos))
		for i, g := range geos {
			geoids[i] = g.GEOID
		}
		indQ := store.IndicatorQuery{
			GEOIDs:      geoids,
			VariableIDs: req.VariableIDs,
			Vintage:     req.Vintage,
			LatestOnly:  req.Vintage == "",
		}
		allInds, err := p.store.QueryIndicators(c.Request.Context(), indQ)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "indicator query failed", Detail: err.Error()})
			return
		}
		// Index by GEOID.
		indsByGEO := make(map[string][]store.Indicator, len(geoids))
		for _, ind := range allInds {
			indsByGEO[ind.GEOID] = append(indsByGEO[ind.GEOID], ind)
		}
		for _, g := range geos {
			items = append(items, geoFromStore(g, indsByGEO[g.GEOID], nil))
		}
	} else {
		for _, g := range geos {
			items = append(items, geoFromStore(g, nil, nil))
		}
	}

	c.JSON(http.StatusOK, GeographyListResponse{
		Items:  items,
		Total:  len(items),
		Limit:  q.Limit,
		Offset: q.Offset,
	})
}

// ── POST /compare ───────────────────────────────────────────────────────────

// handleCompare returns a side-by-side comparison of two geographies with
// per-indicator difference calculations.
func (p *PolicyPlugin) handleCompare(c *gin.Context) {
	var req CompareRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid request body", Detail: err.Error()})
		return
	}

	ctx := c.Request.Context()

	g1, err := p.store.GetGeography(ctx, req.GEOID1)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "geography 1 not found", Detail: err.Error()})
		return
	}
	g2, err := p.store.GetGeography(ctx, req.GEOID2)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "geography 2 not found", Detail: err.Error()})
		return
	}

	indQ := store.IndicatorQuery{
		GEOIDs:      []string{req.GEOID1, req.GEOID2},
		VariableIDs: req.VariableIDs,
		Vintage:     req.Vintage,
		LatestOnly:  req.Vintage == "",
	}
	allInds, err := p.store.QueryIndicators(ctx, indQ)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "indicator query failed", Detail: err.Error()})
		return
	}

	inds1 := make([]store.Indicator, 0)
	inds2 := make([]store.Indicator, 0)
	idx1 := make(map[string]store.Indicator)
	idx2 := make(map[string]store.Indicator)

	for _, ind := range allInds {
		key := ind.VariableID + "|" + ind.Vintage
		switch ind.GEOID {
		case req.GEOID1:
			inds1 = append(inds1, ind)
			idx1[key] = ind
		case req.GEOID2:
			inds2 = append(inds2, ind)
			idx2[key] = ind
		}
	}

	// Build difference list.
	seen := make(map[string]bool)
	var diffs []IndicatorDiffResponse
	for key, i1 := range idx1 {
		seen[key] = true
		i2, ok := idx2[key]
		d := IndicatorDiffResponse{
			VariableID: i1.VariableID,
			Vintage:    i1.Vintage,
			Value1:     i1.Value,
		}
		if ok {
			d.Value2 = i2.Value
		}
		if i1.Value != nil && ok && i2.Value != nil {
			diff := *i2.Value - *i1.Value
			d.Diff = &diff
			if *i1.Value != 0 {
				pct := diff / *i1.Value * 100
				d.PctDiff = &pct
			}
		}
		diffs = append(diffs, d)
	}
	for key, i2 := range idx2 {
		if seen[key] {
			continue
		}
		diffs = append(diffs, IndicatorDiffResponse{
			VariableID: i2.VariableID,
			Vintage:    i2.Vintage,
			Value2:     i2.Value,
		})
	}

	c.JSON(http.StatusOK, CompareResponse{
		Geography1:  geoFromStore(*g1, inds1, nil),
		Geography2:  geoFromStore(*g2, inds2, nil),
		Differences: diffs,
	})
}

// ── POST /generate/narrative ────────────────────────────────────────────────

// handleGenerateNarrative generates an HTML narrative summary for a geography.
// The narrative engine is a stub; production callers should replace the inline
// template with the pkg/narrative engine.
func (p *PolicyPlugin) handleGenerateNarrative(c *gin.Context) {
	var req NarrativeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid request body", Detail: err.Error()})
		return
	}

	ctx := c.Request.Context()

	g, err := p.store.GetGeography(ctx, req.GEOID)
	if err != nil {
		status := http.StatusInternalServerError
		if isNotFound(err) {
			status = http.StatusNotFound
		}
		c.JSON(status, ErrorResponse{Error: "geography not found", Detail: err.Error()})
		return
	}

	indQ := store.IndicatorQuery{
		GEOIDs:     []string{req.GEOID},
		Vintage:    req.Vintage,
		LatestOnly: req.Vintage == "",
	}
	inds, err := p.store.QueryIndicators(ctx, indQ)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "indicator query failed", Detail: err.Error()})
		return
	}

	html := generateNarrativeHTML(g, inds, req.Vintage)

	c.JSON(http.StatusOK, NarrativeResponse{
		GEOID:   req.GEOID,
		HTML:    html,
		Vintage: req.Vintage,
	})
}

// ── POST /generate/deliverable ──────────────────────────────────────────────

// handleGenerateDeliverable generates a complete single-file HTML deliverable
// and returns it as text/html so callers can save directly to disk.
func (p *PolicyPlugin) handleGenerateDeliverable(c *gin.Context) {
	var req DeliverableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid request body", Detail: err.Error()})
		return
	}

	ctx := c.Request.Context()

	g, err := p.store.GetGeography(ctx, req.GEOID)
	if err != nil {
		status := http.StatusInternalServerError
		if isNotFound(err) {
			status = http.StatusNotFound
		}
		c.JSON(status, ErrorResponse{Error: "geography not found", Detail: err.Error()})
		return
	}

	indQ := store.IndicatorQuery{
		GEOIDs:     []string{req.GEOID},
		Vintage:    req.Vintage,
		LatestOnly: req.Vintage == "",
	}
	inds, err := p.store.QueryIndicators(ctx, indQ)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "indicator query failed", Detail: err.Error()})
		return
	}

	narrativeHTML := generateNarrativeHTML(g, inds, req.Vintage)

	title := req.Title
	if title == "" {
		title = fmt.Sprintf("Policy Brief — %s", g.Name)
	}

	components := req.Components
	if len(components) == 0 {
		components = []string{"data-table", "metric-card", "stat-callout"}
		if req.IncludeCharts {
			components = append(components, "chart-bar")
		}
	}

	bridge := htmlcraft.NewBridge(p.store)
	fullHTML, err := bridge.BuildDeliverable(ctx, narrativeHTML, req.GEOID, htmlcraft.DeliverableOpts{
		Title:         title,
		IncludeMap:    req.IncludeMap,
		IncludeCharts: req.IncludeCharts,
		MapCenter:     req.MapCenter,
		MapZoom:       req.MapZoom,
		TileLayer:     req.TileLayer,
		Components:    components,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "deliverable generation failed", Detail: err.Error()})
		return
	}

	c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.html"`, sanitiseFilename(g.Name)))
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(fullHTML))
}

// ── POST /pipeline/run ──────────────────────────────────────────────────────

// handlePipelineRun accepts a pipeline run request and returns a run ID.
// Actual pipeline execution is out of scope for the HTTP layer; this handler
// returns 501 so the gap is visible, not silent.
func (p *PolicyPlugin) handlePipelineRun(c *gin.Context) {
	var req PipelineRunRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid request body", Detail: err.Error()})
		return
	}
	// 501 makes the gap visible — replace with real pipeline dispatch when ready.
	c.JSON(http.StatusNotImplemented, ErrorResponse{
		Error:  "pipeline execution not implemented",
		Detail: "implement handlePipelineRun by wiring pkg/pipeline once it is available",
	})
}

// ── GET /pipeline/events ────────────────────────────────────────────────────

// handlePipelineEvents streams pipeline progress as Server-Sent Events.
// Clients should pass run_id as a query parameter and read until the stream
// closes. Returns 501 until pipeline dispatch is wired.
func (p *PolicyPlugin) handlePipelineEvents(c *gin.Context) {
	runID := c.Query("run_id")
	if runID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "run_id query parameter required"})
		return
	}

	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	// Emit a single "not implemented" event so the client knows the stream
	// opened and closed cleanly. Replace with real pipeline events later.
	ev := PipelineEvent{
		RunID:   runID,
		Stage:   "init",
		Status:  "error",
		Message: "pipeline SSE not yet implemented",
	}
	b, _ := json.Marshal(ev)
	c.SSEvent("pipeline", string(b))

	// Flush and close.
	c.Writer.Flush()
}

// ── GET /sources ─────────────────────────────────────────────────────────────

// handleListSources returns the registered data sources supported by this
// deployment.
func (p *PolicyPlugin) handleListSources(c *gin.Context) {
	sources := []SourceResponse{
		{
			ID:          "census",
			Name:        "US Census Bureau ACS",
			Type:        "api",
			Levels:      []string{"state", "county", "tract", "block_group"},
			Description: "American Community Survey 5-year estimates via Census API",
		},
		{
			ID:          "tiger",
			Name:        "TIGER/Line Shapefiles",
			Type:        "file",
			Levels:      []string{"state", "county", "tract", "block_group", "ward"},
			Description: "Census TIGER/Line boundary files — provides PostGIS geometries",
		},
		{
			ID:          "hud",
			Name:        "HUD CHAS",
			Type:        "api",
			Levels:      []string{"state", "county", "tract"},
			Description: "HUD Comprehensive Housing Affordability Strategy data",
		},
		{
			ID:          "epa",
			Name:        "EPA EJScreen",
			Type:        "api",
			Levels:      []string{"tract", "block_group"},
			Description: "EPA Environmental Justice screening tool data",
		},
		{
			ID:          "hmda",
			Name:        "HMDA Loan Data",
			Type:        "file",
			Levels:      []string{"county", "tract"},
			Description: "Home Mortgage Disclosure Act loan-level data",
		},
		{
			ID:          "bls",
			Name:        "Bureau of Labor Statistics",
			Type:        "api",
			Levels:      []string{"state", "county"},
			Description: "BLS Local Area Unemployment Statistics",
		},
		{
			ID:          "fema",
			Name:        "FEMA NFHL",
			Type:        "file",
			Levels:      []string{"county"},
			Description: "FEMA National Flood Hazard Layer",
		},
		{
			ID:          "cdc",
			Name:        "CDC Places",
			Type:        "api",
			Levels:      []string{"county", "tract"},
			Description: "CDC PLACES local health measures",
		},
		{
			ID:          "usda",
			Name:        "USDA Food Access",
			Type:        "api",
			Levels:      []string{"tract"},
			Description: "USDA Food Access Research Atlas",
		},
	}
	c.JSON(http.StatusOK, gin.H{"sources": sources, "total": len(sources)})
}

// ── Narrative stub ──────────────────────────────────────────────────────────

// generateNarrativeHTML produces a minimal HTML narrative fragment from raw
// geography + indicator data. Replace this stub with pkg/narrative once the
// full template engine is wired.
func generateNarrativeHTML(g *geo.Geography, inds []store.Indicator, vintage string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf(`<h2>%s</h2>`, g.Name))
	sb.WriteString(fmt.Sprintf(`<p><strong>Level:</strong> %s &nbsp;|&nbsp; <strong>GEOID:</strong> %s</p>`, string(g.Level), g.GEOID))
	if g.Population > 0 {
		sb.WriteString(fmt.Sprintf(`<p><strong>Population:</strong> %s</p>`, formatInt(g.Population)))
	}

	if len(inds) == 0 {
		sb.WriteString(`<p class="pdi-empty">No indicator data available for this geography.</p>`)
		return sb.String()
	}

	v := vintage
	if v == "" {
		v = "latest"
	}
	sb.WriteString(fmt.Sprintf(`<h3>Indicators (%s)</h3>`, v))
	sb.WriteString(`<data-table data-src="indicators"></data-table>`)

	return sb.String()
}

// ── Internal helpers ────────────────────────────────────────────────────────

// geoFromStore converts a geo.Geography and optional store slices to the HTTP
// response shape, avoiding the interface{} indirection in types.go.
func geoFromStore(g geo.Geography, inds []store.Indicator, scores []store.AnalysisScore) GeographyResponse {
	resp := GeographyResponse{
		GEOID:       g.GEOID,
		Level:       string(g.Level),
		ParentGEOID: g.ParentGEOID,
		Name:        g.Name,
		StateFIPS:   g.StateFIPS,
		CountyFIPS:  g.CountyFIPS,
		Population:  g.Population,
		LandAreaM2:  g.LandAreaM2,
		Lat:         g.Lat,
		Lon:         g.Lon,
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

// isNotFound returns true when the error indicates a missing row. pgx does not
// export a specific not-found error type; we test the error string.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	// pgx/v5 returns pgx.ErrNoRows for single-row scans.
	return errors.Is(err, io.EOF) || strings.Contains(err.Error(), "no rows") || strings.Contains(err.Error(), "not found")
}

// sanitiseFilename replaces characters that are unsafe in filenames with
// underscores so that the Content-Disposition header is valid.
func sanitiseFilename(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

// formatInt formats an integer with thousands separators.
func formatInt(n int) string {
	s := strconv.Itoa(n)
	if len(s) <= 3 {
		return s
	}
	var parts []string
	for len(s) > 3 {
		parts = append([]string{s[len(s)-3:]}, parts...)
		s = s[:len(s)-3]
	}
	parts = append([]string{s}, parts...)
	return strings.Join(parts, ",")
}
