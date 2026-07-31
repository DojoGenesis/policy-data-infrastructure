package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/htmlcraft"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/narrative"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/stats"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// ── GET /geographies ────────────────────────────────────────────────────────

// handleListGeographies lists geographies with optional query-parameter filters.
//
// Query params: level, parent_geoid, state_fips, name, limit (default 50),
// offset, include_retired, retired_only.
//
// Retired geographies — rows superseded by a later census vintage but retained
// for their historical indicator data — are excluded by default, so the counts
// this endpoint reports describe the current world (e.g. level=tract yields the
// 1,542 tracts the map draws, not the 1,669 rows the table holds). Temporal
// callers opt in with include_retired=true, or isolate the historical set with
// retired_only=true. See ADR-012 §Integration 5.
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
	q.IncludeRetired = queryBool(c, "include_retired")
	q.RetiredOnly = queryBool(c, "retired_only")

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

	// Total is the count of ALL rows matching the filters, not the size of this
	// page. Reporting len(items) capped pagination at the page size and made
	// clients that loop while offset < total stop early.
	total, err := p.store.CountGeographies(c.Request.Context(), q)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "count failed", Detail: err.Error()})
		return
	}

	items := make([]GeographyResponse, 0, len(geos))
	for _, g := range geos {
		items = append(items, p.geoFromStore(g, nil, nil))
	}

	c.JSON(http.StatusOK, GeographyListResponse{
		Items:  items,
		Total:  total,
		Count:  len(items),
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
		indQ.Vintages = parseCSV(vintage)
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

	c.JSON(http.StatusOK, p.geoFromStore(*g, inds, scores))
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
		c.JSON(http.StatusOK, GeographyListResponse{Items: []GeographyResponse{}, Total: 0, Count: 0, Limit: 200, Offset: 0})
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

	childQ := store.GeoQuery{
		Level:          childLevel,
		ParentGEOID:    geoid,
		IncludeRetired: queryBool(c, "include_retired"),
		RetiredOnly:    queryBool(c, "retired_only"),
		Limit:          limit,
		Offset:         offset,
	}

	geos, err := p.store.QueryGeographies(c.Request.Context(), childQ)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "query failed", Detail: err.Error()})
		return
	}

	// Same pagination contract as GET /geographies: Total counts every matching
	// child, not just this page.
	total, err := p.store.CountGeographies(c.Request.Context(), childQ)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "count failed", Detail: err.Error()})
		return
	}

	items := make([]GeographyResponse, 0, len(geos))
	for _, g := range geos {
		items = append(items, p.geoFromStore(g, nil, nil))
	}

	c.JSON(http.StatusOK, GeographyListResponse{
		Items:  items,
		Total:  total,
		Count:  len(items),
		Limit:  limit,
		Offset: offset,
	})
}

// ── GET /geographies/:geoid/indicators ─────────────────────────────────────

// handleGetIndicators returns all indicators for a geography.
//
// Query params: variable_id (repeatable), vintage (comma-separated for multi-vintage), latest (bool, default true).
func (p *PolicyPlugin) handleGetIndicators(c *gin.Context) {
	geoid := c.Param("geoid")

	q := store.IndicatorQuery{
		GEOIDs:     []string{geoid},
		LatestOnly: true,
	}
	q.VariableIDs = c.QueryArray("variable_id")
	if v := c.Query("vintage"); v != "" {
		q.Vintage = v
		q.Vintages = parseCSV(v)
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
		items = append(items, p.indicatorToResponse(ind))
	}

	c.JSON(http.StatusOK, gin.H{"geoid": geoid, "indicators": items, "total": len(items)})
}

// ── GET /geographies/:geoid/factors ──────────────────────────────────────

// handleGetFactors returns factor scores for a single geography.
func (p *PolicyPlugin) handleGetFactors(c *gin.Context) {
	geoid := c.Param("geoid")

	scores, err := p.store.QueryFactorScores(c.Request.Context(), geoid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:  "factor score query failed",
			Detail: err.Error(),
		})
		return
	}

	items := make([]FactorScoreResponse, 0, len(scores))
	for _, fs := range scores {
		items = append(items, FactorScoreResponse{
			GEOID:            fs.GEOID,
			FactorName:       fs.FactorName,
			FactorScore:      fs.FactorScore,
			FactorPercentile: fs.FactorPercentile,
			LoadingsJSON:     fs.LoadingsJSON,
			AnalysisVintage:  fs.AnalysisVintage,
		})
	}

	c.JSON(http.StatusOK, gin.H{"geoid": geoid, "factors": items, "total": len(items)})
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
		ParentGEOID:    req.ParentGEOID,
		StateFIPS:      req.StateFIPS,
		NameSearch:     req.NameSearch,
		IncludeRetired: req.IncludeRetired,
		RetiredOnly:    req.RetiredOnly,
		Limit:          req.Limit,
		Offset:         req.Offset,
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

	total, err := p.store.CountGeographies(c.Request.Context(), q)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "count failed", Detail: err.Error()})
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
			Vintages:    parseCSV(req.Vintage),
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
			items = append(items, p.geoFromStore(g, indsByGEO[g.GEOID], nil))
		}
	} else {
		for _, g := range geos {
			items = append(items, p.geoFromStore(g, nil, nil))
		}
	}

	c.JSON(http.StatusOK, GeographyListResponse{
		Items:  items,
		Total:  total,
		Count:  len(items),
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
		Vintages:    parseCSV(req.Vintage),
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
		Geography1:  p.geoFromStore(*g1, inds1, nil),
		Geography2:  p.geoFromStore(*g2, inds2, nil),
		Differences: diffs,
	})
}

// resolveScopeName returns a human-readable name for geoid, mirroring the
// lookup cmd/pdi/generate.go performs via store.GetGeography so that a
// narrative generated through the HTTP API gets the same title as one
// generated through the CLI for the same scope. This is the fix for the P0
// where every API-generated narrative rendered "Five Mornings in " with the
// county name missing — the handlers below built narrative.GenerateRequest
// without ever populating ScopeName.
//
// When geoid is empty (a caller supplied only an analysis_id), it first
// tries to recover a GEOID from the analysis record's ScopeGEOID.
//
// This never returns an error and never panics: a lookup failure (not
// found, a DB error, or an analysis/geoid that resolves to nothing at all)
// degrades to a fallback rather than blocking narrative generation — a
// narrative should still generate even when the scope name can't be
// resolved. When a GEOID is known but its name isn't, the GEOID itself is
// returned (e.g. "55025") since it's still more useful to a reader than a
// blank title and mirrors the CLI's own fallback-to-GEOID behavior. When
// nothing is known at all, "" is returned and pkg/narrative's
// Engine.Generate / defaultTitle substitute their own generic fallback so
// the title can never again render with a dangling "... in ".
func resolveScopeName(ctx context.Context, s store.Store, geoid, analysisID string) string {
	if geoid == "" && analysisID != "" {
		a, err := s.GetAnalysis(ctx, analysisID)
		if err != nil {
			log.Printf("gateway: resolveScopeName: GetAnalysis(%s): %v", analysisID, err)
		} else if a != nil {
			geoid = a.ScopeGEOID
		}
	}
	if geoid == "" {
		return ""
	}
	g, err := s.GetGeography(ctx, geoid)
	if err != nil {
		log.Printf("gateway: resolveScopeName: GetGeography(%s): %v", geoid, err)
		return geoid
	}
	if g == nil || g.Name == "" {
		return geoid
	}
	return g.Name
}

// ── POST /generate/narrative ────────────────────────────────────────────────

// handleGenerateNarrative generates a full HTML narrative using pkg/narrative
// engine with analysis scores for tier-based profile selection.
func (p *PolicyPlugin) handleGenerateNarrative(c *gin.Context) {
	var req NarrativeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid request body", Detail: err.Error()})
		return
	}

	if req.AnalysisID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "analysis_id is required"})
		return
	}

	ctx := c.Request.Context()
	tmplName := req.Template
	if tmplName == "" {
		tmplName = "five_mornings"
	}
	count := req.Count
	if count <= 0 {
		count = 5
	}

	eng := narrative.NewEngine(p.store)
	if err := eng.LoadEmbeddedTemplates(); err != nil {
		log.Printf("gateway: LoadEmbeddedTemplates: %v", err)
	}

	doc, err := eng.Generate(ctx, narrative.GenerateRequest{
		Template:     tmplName,
		ScopeGEOID:   req.GEOID,
		ScopeName:    resolveScopeName(ctx, p.store, req.GEOID, req.AnalysisID),
		AnalysisID:   req.AnalysisID,
		ChapterCount: count,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "narrative generation failed", Detail: err.Error()})
		return
	}

	html, err := eng.RenderHTML(doc)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "narrative render failed", Detail: err.Error()})
		return
	}

	// If Accept header wants HTML, return it directly.
	if c.GetHeader("Accept") == "text/html" {
		c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(html))
		return
	}

	c.JSON(http.StatusOK, NarrativeResponse{
		GEOID:   req.GEOID,
		HTML:    html,
		Vintage: req.Vintage,
	})
}

// ── GET /generate/narrative/:analysis_id ────────────────────────────────────

// handleServeNarrative serves a Five Mornings narrative as a standalone HTML
// page. Any browser can view it at /v1/policy/generate/narrative/:analysis_id.
func (p *PolicyPlugin) handleServeNarrative(c *gin.Context) {
	analysisID := c.Param("analysis_id")

	scope := c.DefaultQuery("scope", "")
	tmpl := c.DefaultQuery("template", "five_mornings")
	count := 5

	ctx := c.Request.Context()
	eng := narrative.NewEngine(p.store)
	if err := eng.LoadEmbeddedTemplates(); err != nil {
		log.Printf("gateway: LoadEmbeddedTemplates: %v", err)
	}

	doc, err := eng.Generate(ctx, narrative.GenerateRequest{
		Template:     tmpl,
		ScopeGEOID:   scope,
		ScopeName:    resolveScopeName(ctx, p.store, scope, analysisID),
		AnalysisID:   analysisID,
		ChapterCount: count,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "narrative generation failed", Detail: err.Error()})
		return
	}

	html, err := eng.RenderHTML(doc)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "narrative render failed", Detail: err.Error()})
		return
	}

	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(html))
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

// ── GET /variables ───────────────────────────────────────────────────────────

// handleListVariables returns the full indicator_meta catalog with source info.
func (p *PolicyPlugin) handleListVariables(c *gin.Context) {
	vars, err := p.store.QueryVariables(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "variable query failed", Detail: err.Error()})
		return
	}

	items := make([]VariableResponse, 0, len(vars))
	for _, v := range vars {
		items = append(items, VariableResponse{
			ID:          v.VariableID,
			Name:        v.Name,
			Description: v.Description,
			Unit:        v.Unit,
			Direction:   normalizeDirection(v.Direction),
			SourceID:    v.SourceID,
			SourceName:  v.SourceName,
		})
	}

	c.JSON(http.StatusOK, VariableListResponse{
		Variables: items,
		Total:     len(items),
	})
}

// ── GET /analyses ─────────────────────────────────────────────────────────────

// handleListAnalyses returns a summary of all analysis runs stored in the
// database, ordered most-recent first. Frontends use this to discover
// analysis_ids for use in score queries and narrative generation.
func (p *PolicyPlugin) handleListAnalyses(c *gin.Context) {
	summaries, err := p.store.ListAnalyses(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "analyses query failed", Detail: err.Error()})
		return
	}

	items := make([]AnalysisSummaryResponse, 0, len(summaries))
	for _, as := range summaries {
		items = append(items, AnalysisSummaryResponse{
			ID:         as.ID,
			Type:       as.Type,
			ScopeGEOID: as.ScopeGEOID,
			ScopeLevel: as.ScopeLevel,
			Vintage:    as.Vintage,
			ComputedAt: as.ComputedAt,
			ScoreCount: as.ScoreCount,
		})
	}

	c.JSON(http.StatusOK, AnalysisListResponse{
		Analyses: items,
		Total:    len(items),
	})
}

// ── GET /sources ─────────────────────────────────────────────────────────────

// handleListSources returns the registered data sources supported by this
// deployment.
//
// "sources" and "total" describe the static catalog of source ADAPTERS this
// binary ships — code that exists to fetch from census/tiger/hud/epa/hmda/
// bls/fema/cdc/usda. That catalog does not depend on the database and is
// non-zero even against a freshly migrated, empty DB; treat it as "adapters
// supported", not "data available".
//
// "sources_loaded" is the DB-derived complement: the number of distinct,
// non-empty source_id values among rows returned by a fresh
// store.QueryVariables call this request (QueryVariables joins
// indicator_meta to indicator_sources — see pkg/store/postgres.go — so this
// counts distinct sources with at least one registered variable, which is
// bounded by, and may be smaller than, the row count of indicator_sources
// itself). It is always present and always a plain JSON number — 0 when
// indicator_meta is legitimately empty, never null, never omitted — so a
// caller can hydrate an honest "sources loaded" figure instead of the
// static catalog size.
//
// "sources_loaded_ok" disambiguates that 0 from a failed lookup: true when
// the QueryVariables call succeeded this request (0 may be a real,
// confirmed answer), false when it errored (0 is only a fallback and the
// real count is unknown). A failed loaded-count lookup deliberately does
// NOT fail this endpoint — "sources"/"total" have no DB dependency and
// must keep serving the three existing consumers of this payload
// (explorer.html, about.html, index.html) that were reading it before
// this field existed and do not read it.
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

	// sources_loaded / sources_loaded_ok: a fresh per-request lookup, not
	// p.varMeta — that map is populated once at boot (see NewPlugin) and
	// stays empty forever if the DB was empty at startup, which would
	// silently freeze this figure at 0 regardless of later loads.
	sourcesLoaded := 0
	sourcesLoadedOK := true
	if vars, err := p.store.QueryVariables(c.Request.Context()); err != nil {
		sourcesLoadedOK = false
	} else {
		distinct := make(map[string]struct{}, len(vars))
		for _, v := range vars {
			if v.SourceID == "" {
				continue
			}
			distinct[v.SourceID] = struct{}{}
		}
		sourcesLoaded = len(distinct)
	}

	c.JSON(http.StatusOK, gin.H{
		"sources":           sources,
		"total":             len(sources),
		"sources_loaded":    sourcesLoaded,
		"sources_loaded_ok": sourcesLoadedOK,
	})
}

// ── Narrative stub ──────────────────────────────────────────────────────────

// generateNarrativeHTML produces a minimal HTML narrative fragment from raw
// geography + indicator data. Replace this stub with pkg/narrative once the
// full template engine is wired.
func generateNarrativeHTML(g *geo.Geography, inds []store.Indicator, vintage string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf(`<h2>%s</h2>`, html.EscapeString(g.Name)))
	sb.WriteString(fmt.Sprintf(`<p><strong>Level:</strong> %s &nbsp;|&nbsp; <strong>GEOID:</strong> %s</p>`, html.EscapeString(string(g.Level)), html.EscapeString(g.GEOID)))
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

// ── GET /analyses/:id ─────────────────────────────────────────────────────

// handleGetAnalysis returns the full analysis result including computed Results map.
func (p *PolicyPlugin) handleGetAnalysis(c *gin.Context) {
	id := c.Param("id")
	result, err := p.store.GetAnalysis(c.Request.Context(), id)
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "not found") {
			status = http.StatusNotFound
		}
		c.JSON(status, ErrorResponse{Error: "analysis not found", Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"id":          result.ID,
		"type":        result.Type,
		"scope_geoid": result.ScopeGEOID,
		"scope_level": result.ScopeLevel,
		"parameters":  result.Parameters,
		"results":     result.Results,
		"vintage":     result.Vintage,
	})
}

// ── GET /analyses/:id/scores ──────────────────────────────────────────────

// handleGetAnalysisScores returns per-geography scores for an analysis.
func (p *PolicyPlugin) handleGetAnalysisScores(c *gin.Context) {
	id := c.Param("id")
	tier := c.Query("tier")
	scores, err := p.store.QueryAnalysisScores(c.Request.Context(), id, tier)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "query failed", Detail: err.Error()})
		return
	}
	items := make([]ScoreResponse, 0, len(scores))
	for _, s := range scores {
		items = append(items, ScoreResponse{
			AnalysisID: s.AnalysisID,
			GEOID:      s.GEOID,
			Score:      s.Score,
			Rank:       s.Rank,
			Percentile: s.Percentile,
			Tier:       s.Tier,
			Details:    s.Details,
		})
	}
	c.JSON(http.StatusOK, gin.H{"scores": items, "total": len(items)})
}

// ── POST /aggregate ───────────────────────────────────────────────────────

// handleAggregate runs a statistical aggregation over a variable.
func (p *PolicyPlugin) handleAggregate(c *gin.Context) {
	var req struct {
		VariableID string `json:"variable_id" binding:"required"`
		Level      string `json:"level" binding:"required"`
		StateFIPS  string `json:"state_fips"`
		Function   string `json:"function" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid request", Detail: err.Error()})
		return
	}
	level, err := geo.LevelFromString(req.Level)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid level", Detail: err.Error()})
		return
	}
	result, err := p.store.Aggregate(c.Request.Context(), store.AggregateQuery{
		VariableID: req.VariableID,
		Level:      level,
		StateFIPS:  req.StateFIPS,
		Function:   req.Function,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "aggregate failed", Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"variable_id": req.VariableID,
		"function":    req.Function,
		"value":       result.Value,
		"count":       result.Count,
	})
}

// ── POST /composite ───────────────────────────────────────────────────────

// handleComposite computes a query-time composite score across one or more
// geographies with sensitivity analysis. Composites are never stored — they
// are computed fresh on every request from raw indicator values.
func (p *PolicyPlugin) handleComposite(c *gin.Context) {
	var req CompositeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid request body", Detail: err.Error()})
		return
	}

	if len(req.GEOIDs) < 1 {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "at least one geoid is required"})
		return
	}
	if len(req.VariableIDs) < 2 {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "at least two variable_ids are required for a composite"})
		return
	}

	method := req.Method
	if method == "" {
		method = "geometric_mean"
	}
	if method != "geometric_mean" && method != "weighted_zscore" {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:  "unknown method",
			Detail: "method must be 'geometric_mean' or 'weighted_zscore'",
		})
		return
	}

	perturbation := req.Perturbation
	if perturbation == 0 {
		perturbation = 0.20
	}

	ctx := c.Request.Context()

	// 1. Fetch indicators for all requested geoids × variables.
	indQ := store.IndicatorQuery{
		GEOIDs:      req.GEOIDs,
		VariableIDs: req.VariableIDs,
		LatestOnly:  true,
	}
	allInds, err := p.store.QueryIndicators(ctx, indQ)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "indicator query failed", Detail: err.Error()})
		return
	}

	// 2. Index indicators by geoid → variable_id → value.
	geoValues := make(map[string]map[string]*float64)
	for _, g := range req.GEOIDs {
		geoValues[g] = make(map[string]*float64)
	}
	for _, ind := range allInds {
		if m, ok := geoValues[ind.GEOID]; ok {
			m[ind.VariableID] = ind.Value
		}
	}

	// 3. Z-score each variable across all geographies.
	// Build per-variable slices aligned with geoid order.
	zScores := make(map[string]map[string]*float64) // variable_id → geoid → z-score
	for _, varID := range req.VariableIDs {
		values := make([]*float64, len(req.GEOIDs))
		for i, geoid := range req.GEOIDs {
			values[i] = geoValues[geoid][varID]
		}
		zs := stats.ZScore(values)
		zMap := make(map[string]*float64, len(req.GEOIDs))
		for i, geoid := range req.GEOIDs {
			zMap[geoid] = zs[i]
		}
		zScores[varID] = zMap
	}

	// 4. Shift z-scores to be positive for geometric mean.
	// Find the global minimum z-score across all variables and geographies.
	minZ := 0.0
	for _, zMap := range zScores {
		for _, z := range zMap {
			if z != nil && *z < minZ {
				minZ = *z
			}
		}
	}
	shift := -minZ + 1.0 // ensures all values are ≥ 1

	// 5. Build weights map. Default to equal weights if not provided.
	weights := make(map[string]float64, len(req.VariableIDs))
	if len(req.Weights) > 0 {
		for k, v := range req.Weights {
			weights[k] = v
		}
	} else {
		for _, varID := range req.VariableIDs {
			weights[varID] = 1.0 / float64(len(req.VariableIDs))
		}
	}

	// 6. Compute composite scores (geometric mean of shifted z-scores).
	geoidInputs := make([]stats.CompositeInput, len(req.GEOIDs))
	for i, geoid := range req.GEOIDs {
		shiftedVals := make(map[string]*float64, len(req.VariableIDs))
		for _, varID := range req.VariableIDs {
			if z, ok := zScores[varID][geoid]; ok && z != nil {
				s := *z + shift
				shiftedVals[varID] = &s
			}
		}
		geoidInputs[i] = stats.CompositeInput{
			GEOID:  geoid,
			Values: shiftedVals,
		}
	}

	// 7. Run sensitivity analysis.
	sensResult, err := stats.CompositeSensitivity(geoidInputs, weights, perturbation)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "sensitivity analysis failed", Detail: err.Error()})
		return
	}

	// 8. Build response.
	scores := make([]CompositeScoreEntry, len(sensResult.BaseScores))
	for i, cs := range sensResult.BaseScores {
		scores[i] = CompositeScoreEntry{
			GEOID:       cs.GEOID,
			Score:       cs.Score,
			ContribVars: cs.ContribVars,
			MissingVars: cs.MissingVars,
		}
	}

	var sensInfo *SensitivityInfo
	if len(sensResult.Scenarios) > 0 {
		scenarioEntries := make([]PerturbedScenarioEntry, len(sensResult.Scenarios))
		for i, sc := range sensResult.Scenarios {
			scoresForScenario := make([]CompositeScoreEntry, len(sc.Scores))
			for j, cs := range sc.Scores {
				scoresForScenario[j] = CompositeScoreEntry{
					GEOID:       cs.GEOID,
					Score:       cs.Score,
					ContribVars: cs.ContribVars,
					MissingVars: cs.MissingVars,
				}
			}
			scenarioEntries[i] = PerturbedScenarioEntry{
				PerturbedVar: sc.PerturbedVar,
				Direction:    sc.Direction,
				Perturbation: sc.Perturbation,
				Scores:       scoresForScenario,
			}
		}
		sensInfo = &SensitivityInfo{
			Perturbation: sensResult.Perturbation,
			Stability:    sensResult.Stability,
			Scenarios:    scenarioEntries,
		}
	}

	resp := CompositeResponse{
		Scores:      scores,
		Sensitivity: sensInfo,
		Method:      method,
		VariableIDs: req.VariableIDs,
	}

	c.JSON(http.StatusOK, resp)
}

// ── Internal helpers ────────────────────────────────────────────────────────

// normalizeDirection standardizes direction values from the database
// (which may use higher_better/lower_better/neutral) to the canonical
// higher_is_better/lower_is_better form expected by the frontend.
func normalizeDirection(d string) string {
	switch d {
	case "higher_better":
		return "higher_is_better"
	case "lower_better":
		return "lower_is_better"
	default:
		return d
	}
}

// indicatorToResponse converts a store.Indicator to an IndicatorResponse,
// enriching it with human-readable metadata from the plugin's varMeta cache.
func (p *PolicyPlugin) indicatorToResponse(ind store.Indicator) IndicatorResponse {
	resp := IndicatorResponse{
		VariableID:    ind.VariableID,
		Vintage:       ind.Vintage,
		Value:         ind.Value,
		MarginOfError: ind.MarginOfError,
		CV:            ind.CV,
		Reliability:   ind.Reliability,
		RawValue:      ind.RawValue,
	}
	if meta, ok := p.varMeta[ind.VariableID]; ok {
		resp.Name = meta.Name
		resp.Unit = meta.Unit
		resp.Direction = normalizeDirection(meta.Direction)
	}
	return resp
}

// geoFromStore converts a geo.Geography and optional store slices to the HTTP
// response shape, enriching each indicator with metadata from the plugin's
// varMeta cache.
func (p *PolicyPlugin) geoFromStore(g geo.Geography, inds []store.Indicator, scores []store.AnalysisScore) GeographyResponse {
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
		resp.Indicators = append(resp.Indicators, p.indicatorToResponse(ind))
	}
	for _, s := range scores {
		resp.Scores = append(resp.Scores, ScoreResponse{
			AnalysisID: s.AnalysisID,
			GEOID:      s.GEOID,
			Score:      s.Score,
			Rank:       s.Rank,
			Percentile: s.Percentile,
			Tier:       s.Tier,
			Details:    s.Details,
		})
	}
	return resp
}

// isNotFound returns true when the error indicates a missing row.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	// pgx/v5 returns pgx.ErrNoRows for single-row scans.
	return errors.Is(err, pgx.ErrNoRows) || errors.Is(err, io.EOF) || strings.Contains(err.Error(), "not found")
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

// ── GET /policies ────────────────────────────────────────────────────────────

// handleListPolicies returns policy records filtered by optional query
// parameters: candidate, category, state, limit (default 100), offset.
func (p *PolicyPlugin) handleListPolicies(c *gin.Context) {
	q := store.PolicyQuery{
		Candidate: c.Query("candidate"),
		Category:  c.Query("category"),
		State:     c.Query("state"),
		Limit:     100,
		Offset:    0,
	}

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

	policies, err := p.store.QueryPolicies(c.Request.Context(), q)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "policy query failed", Detail: err.Error()})
		return
	}

	items := make([]PolicyResponse, 0, len(policies))
	for _, pol := range policies {
		items = append(items, policyToResponse(pol))
	}

	c.JSON(http.StatusOK, PolicyListResponse{
		Policies: items,
		Total:    len(items),
	})
}

// ── GET /policies/:id ────────────────────────────────────────────────────────

// handleGetPolicy returns a single policy record by its id.
func (p *PolicyPlugin) handleGetPolicy(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "id path parameter required"})
		return
	}

	pol, err := p.store.GetPolicy(c.Request.Context(), id)
	if err != nil {
		if isNotFound(err) {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "policy not found", Detail: id})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "policy lookup failed", Detail: err.Error()})
		return
	}

	c.JSON(http.StatusOK, policyToResponse(*pol))
}

// policyToResponse converts a store.PolicyRecord to the HTTP response shape.
func policyToResponse(p store.PolicyRecord) PolicyResponse {
	return PolicyResponse{
		ID:              p.ID,
		Candidate:       p.Candidate,
		Office:          p.Office,
		State:           p.State,
		Category:        p.Category,
		Title:           p.Title,
		Description:     p.Description,
		EquityDimension: p.EquityDimension,
		GeographicScope: p.GeographicScope,
	}
}

// ── GET /evidence-cards ──────────────────────────────────────────────────────

// handleListEvidenceCards returns evidence cards filtered by optional query
// parameters: category, equity_dimension, policy_id, limit (default 50), offset.
func (p *PolicyPlugin) handleListEvidenceCards(c *gin.Context) {
	f := store.EvidenceCardFilter{
		Category:        c.Query("category"),
		EquityDimension: c.Query("equity_dimension"),
		PolicyID:        c.Query("policy_id"),
		Limit:           50,
		Offset:          0,
	}

	if lim := c.Query("limit"); lim != "" {
		n, err := strconv.Atoi(lim)
		if err != nil || n < 1 {
			c.JSON(http.StatusBadRequest, ErrorResponse{Error: "limit must be a positive integer"})
			return
		}
		if n > 1000 {
			n = 1000
		}
		f.Limit = n
	}
	if off := c.Query("offset"); off != "" {
		n, err := strconv.Atoi(off)
		if err != nil || n < 0 {
			c.JSON(http.StatusBadRequest, ErrorResponse{Error: "offset must be a non-negative integer"})
			return
		}
		f.Offset = n
	}

	cards, err := p.store.QueryEvidenceCards(c.Request.Context(), f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "evidence card query failed", Detail: err.Error()})
		return
	}

	items := make([]EvidenceCardResponse, 0, len(cards))
	for _, card := range cards {
		items = append(items, evidenceCardToResponse(card))
	}

	c.JSON(http.StatusOK, EvidenceCardListResponse{
		Cards:  items,
		Total:  len(items),
		Limit:  f.Limit,
		Offset: f.Offset,
	})
}

// evidenceCardToResponse converts a store.EvidenceCard to the API response shape,
// decoding JSONB fields into raw JSON objects for the frontend.
func evidenceCardToResponse(card store.EvidenceCard) EvidenceCardResponse {
	resp := EvidenceCardResponse{
		PolicyID:        card.PolicyID,
		PolicyTitle:     card.PolicyTitle,
		Category:        card.Category,
		EquityDimension: card.EquityDimension,
		Title:           card.Title,
		KeyFinding:      card.KeyFinding,
		DataQuality:     card.DataQuality,
	}
	// Decode JSONB fields — on failure, leave as nil so caller gets an empty result.
	if len(card.Findings) > 0 {
		var v interface{}
		if err := json.Unmarshal(card.Findings, &v); err == nil {
			resp.Findings = v
		}
	}
	if len(card.Indicators) > 0 {
		var v interface{}
		if err := json.Unmarshal(card.Indicators, &v); err == nil {
			resp.Indicators = v
		}
	}
	if len(card.StatewideContext) > 0 {
		var v interface{}
		if err := json.Unmarshal(card.StatewideContext, &v); err == nil {
			resp.StatewideContext = v
		}
	}
	if len(card.CountyVariation) > 0 {
		var v interface{}
		if err := json.Unmarshal(card.CountyVariation, &v); err == nil {
			resp.CountyVariation = v
		}
	}
	if len(card.TopNeedCounties) > 0 {
		var v interface{}
		if err := json.Unmarshal(card.TopNeedCounties, &v); err == nil {
			resp.TopNeedCounties = v
		}
	}
	if len(card.BottomNeedCounties) > 0 {
		var v interface{}
		if err := json.Unmarshal(card.BottomNeedCounties, &v); err == nil {
			resp.BottomNeedCounties = v
		}
	}
	return resp
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

// ── GET /videos/:name ────────────────────────────────────────────────────────

// handleGetVideoCaptions returns personalized overlay captions for a pre-rendered
// explainer video. When ?geoid=X is provided, county-specific stats are injected
// into the caption text. Otherwise generic fallback captions are returned.
func (p *PolicyPlugin) handleGetVideoCaptions(c *gin.Context) {
	videoName := c.Param("name")
	if videoName == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "video name is required"})
		return
	}

	geoid := c.Query("geoid")

	// ── Fallback: no geoid → generic captions ─────────────────────────
	if geoid == "" {
		c.JSON(http.StatusOK, buildGenericCaptions(videoName))
		return
	}

	// ── Personalized: fetch county data ───────────────────────────────
	ctx := c.Request.Context()

	g, err := p.store.GetGeography(ctx, geoid)
	if err != nil {
		// If geography not found, fall back to generic
		c.JSON(http.StatusOK, buildGenericCaptions(videoName))
		return
	}

	// Fetch indicators for this county
	inds, err := p.store.QueryIndicators(ctx, store.IndicatorQuery{
		GEOIDs:     []string{geoid},
		LatestOnly: true,
	})
	if err != nil {
		inds = nil
	}

	// Fetch LISA profile
	lisaProfile, _ := p.store.QueryLISACountyProfile(ctx, geoid)

	resp := buildPersonalizedCaptions(videoName, geoid, g.Name, inds, lisaProfile)
	c.JSON(http.StatusOK, resp)
}

// buildGenericCaptions returns a fallback VideoCaptionResponse with no
// county-specific data — generic explainer captions only.
func buildGenericCaptions(videoName string) VideoCaptionResponse {
	return VideoCaptionResponse{
		VideoName:    videoName,
		Fallback:     true,
		OverlayStats: []VideoOverlayStat{},
		Captions:     genericCaptionsForVideo(videoName),
	}
}

// buildPersonalizedCaptions constructs county-specific overlay stats and
// captions for a given video, injecting local data into the caption text.
func buildPersonalizedCaptions(
	videoName, geoid, countyName string,
	indicators []store.Indicator,
	lisaProfile *store.LISACountyProfile,
) VideoCaptionResponse {
	resp := VideoCaptionResponse{
		VideoName:  videoName,
		GEOID:      geoid,
		CountyName: countyName,
		Fallback:   false,
	}

	// Build a fast lookup map for indicator values
	indMap := make(map[string]*float64)
	for _, ind := range indicators {
		indMap[ind.VariableID] = ind.Value
	}
	getVal := func(id string) *float64 { return indMap[id] }

	switch videoName {
	case "zscore":
		resp.OverlayStats = buildZScoreOverlay(countyName, getVal)
		resp.Captions = buildZScoreCaptions(countyName, getVal)
	case "ice":
		resp.OverlayStats = buildICEOverlay(countyName, getVal)
		resp.Captions = buildICECaptions(countyName, getVal)
	case "lisa_cluster_map":
		resp.OverlayStats = buildLISAOverlay(countyName, lisaProfile)
		resp.Captions = buildLISACaptions(countyName, lisaProfile)
	default:
		// Unrecognized video — use generic captions
		resp.OverlayStats = []VideoOverlayStat{}
		resp.Captions = genericCaptionsForVideo(videoName)
	}

	return resp
}

// ── Overlay stat builders per video type ─────────────────────────────────────

func buildZScoreOverlay(countyName string, getVal func(string) *float64) []VideoOverlayStat {
	pov := getVal("poverty_rate")
	inc := getVal("median_household_income")
	unins := getVal("uninsured_rate")

	var stats []VideoOverlayStat
	if pov != nil {
		stats = append(stats, VideoOverlayStat{
			Label:      "Poverty Rate",
			Value:      fmt.Sprintf("%.1f%%", *pov),
			Comparison: "WI avg: 11.2%",
			Accent:     *pov > 11.2,
		})
	}
	if inc != nil {
		stats = append(stats, VideoOverlayStat{
			Label:      "Median Income",
			Value:      fmt.Sprintf("$%.0f", *inc),
			Comparison: "WI median: $72,458",
			Accent:     *inc < 72458,
		})
	}
	if unins != nil {
		stats = append(stats, VideoOverlayStat{
			Label:      "Uninsured",
			Value:      fmt.Sprintf("%.1f%%", *unins),
			Comparison: "WI avg: 5.7%",
			Accent:     *unins > 5.7,
		})
	}
	return stats
}

func buildICEOverlay(countyName string, getVal func(string) *float64) []VideoOverlayStat {
	// Compute ICE from population data
	popTotal := getVal("total_population")
	poverty := getVal("poverty_rate")
	popWhite := getVal("pop_white_non_hispanic")
	if popWhite == nil {
		popWhite = getVal("pop_non_hispanic_white")
	}

	var stats []VideoOverlayStat

	if popTotal != nil && popWhite != nil && poverty != nil && *popTotal > 0 {
		pocPct := ((*popTotal - *popWhite) / *popTotal) * 100
		povPct := *poverty
		priv := (1 - pocPct/100) * (1 - povPct/100) * *popTotal
		dep := (pocPct / 100) * (povPct / 100) * *popTotal
		var ice float64
		if priv+dep > 0 {
			ice = (priv - dep) / (priv + dep)
		}
		stats = append(stats,
			VideoOverlayStat{
				Label:  "ICE Score",
				Value:  fmt.Sprintf("%.3f", ice),
				Accent: true,
			},
			VideoOverlayStat{
				Label:      "% People of Color",
				Value:      fmt.Sprintf("%.1f%%", pocPct),
				Comparison: "WI avg: 18.8%",
				Accent:     false,
			},
			VideoOverlayStat{
				Label:      "Poverty Rate",
				Value:      fmt.Sprintf("%.1f%%", *poverty),
				Comparison: "WI avg: 11.2%",
				Accent:     false,
			},
		)
	}

	return stats
}

func buildLISAOverlay(countyName string, lisaProfile *store.LISACountyProfile) []VideoOverlayStat {
	var stats []VideoOverlayStat

	if lisaProfile != nil {
		stats = append(stats, VideoOverlayStat{
			Label:  "Total Tracts",
			Value:  fmt.Sprintf("%d", lisaProfile.TotalTracts),
			Accent: false,
		})
		for _, entry := range lisaProfile.Clusters {
			stats = append(stats, VideoOverlayStat{
				Label:  fmt.Sprintf("Cluster %s", entry.Cluster),
				Value:  fmt.Sprintf("%d tracts", entry.Count),
				Accent: entry.Cluster == "HH" || entry.Cluster == "LL",
			})
		}
	}

	return stats
}

// ── Caption builders per video type ──────────────────────────────────────────

func buildZScoreCaptions(countyName string, getVal func(string) *float64) []VideoCaption {
	pov := getVal("poverty_rate")
	inc := getVal("median_household_income")

	captions := []VideoCaption{
		{Text: "The z-score tells us how far a county is from the state average.", StartSec: 0, EndSec: 4},
		{Text: "Measured in standard deviations — a score of 0 means exactly average.", StartSec: 4, EndSec: 8},
	}

	if countyName != "" && pov != nil {
		povZ := (*pov - 11.2) / 4.5 // rough z approximation
		where := "above"
		if povZ < 0 {
			where = "below"
		}
		captions = append(captions, VideoCaption{
			Text:       fmt.Sprintf("%s sits at z=%.1f — %s the state average for poverty.", countyName, povZ, where),
			StartSec:   8,
			EndSec:     13,
			AccentText: countyName,
		})
	} else {
		captions = append(captions, VideoCaption{
			Text: "Your county's position on the distribution shows where it stands.", StartSec: 8, EndSec: 13,
		})
	}

	if inc != nil {
		incZ := (*inc - 72458) / 15000
		where := "above"
		if incZ < 0 {
			where = "below"
		}
		captions = append(captions, VideoCaption{
			Text:       fmt.Sprintf("%s's median income is $%.0f — %s the WI median.", countyName, *inc, where),
			StartSec:   13,
			EndSec:     19,
			AccentText: countyName,
		})
	}

	return captions
}

func buildICECaptions(countyName string, getVal func(string) *float64) []VideoCaption {
	popTotal := getVal("total_population")
	poverty := getVal("poverty_rate")
	popWhite := getVal("pop_white_non_hispanic")
	if popWhite == nil {
		popWhite = getVal("pop_non_hispanic_white")
	}

	captions := []VideoCaption{
		{Text: "ICE — Index of Concentration at the Extremes — measures polarization.", StartSec: 0, EndSec: 5},
	}

	if countyName != "" && popTotal != nil && popWhite != nil && poverty != nil && *popTotal > 0 {
		pocPct := ((*popTotal - *popWhite) / *popTotal) * 100
		povPct := *poverty
		priv := (1 - pocPct/100) * (1 - povPct/100) * *popTotal
		dep := (pocPct / 100) * (povPct / 100) * *popTotal
		var ice float64
		if priv+dep > 0 {
			ice = (priv - dep) / (priv + dep)
		}
		desc := "moderately balanced"
		if ice > 0.3 {
			desc = "concentrated privilege"
		} else if ice < -0.3 {
			desc = "concentrated deprivation"
		} else if ice > 0 {
			desc = "slightly privileged"
		} else if ice < 0 {
			desc = "slightly deprived"
		}
		captions = append(captions,
			VideoCaption{
				Text:       fmt.Sprintf("%s's ICE score is %.3f — indicating %s.", countyName, ice, desc),
				StartSec:   5,
				EndSec:     11,
				AccentText: countyName,
			},
			VideoCaption{
				Text:       fmt.Sprintf("%.1f%% people of color, %.1f%% poverty rate — the extremes that shape the score.", pocPct, povPct),
				StartSec:   11,
				EndSec:     17,
				AccentText: fmt.Sprintf("%.1f%% people of color", pocPct),
			},
		)
	} else {
		captions = append(captions, VideoCaption{
			Text: "The score ranges from -1 (all deprived) to +1 (all privileged).", StartSec: 5, EndSec: 11,
		})
	}

	return captions
}

func buildLISACaptions(countyName string, lisaProfile *store.LISACountyProfile) []VideoCaption {
	captions := []VideoCaption{
		{Text: "LISA — Local Indicators of Spatial Association — reveals geographic clusters.", StartSec: 0, EndSec: 5},
	}

	if countyName != "" && lisaProfile != nil {
		totalTracts := lisaProfile.TotalTracts
		clusterSummary := ""
		for i, entry := range lisaProfile.Clusters {
			if i > 0 {
				clusterSummary += ", "
			}
			clusterSummary += fmt.Sprintf("%s (%d)", entry.Cluster, entry.Count)
		}
		if clusterSummary == "" {
			clusterSummary = "no significant clusters"
		}
		captions = append(captions,
			VideoCaption{
				Text:       fmt.Sprintf("%s has %d tracts with LISA clusters: %s.", countyName, totalTracts, clusterSummary),
				StartSec:   5,
				EndSec:     12,
				AccentText: countyName,
			},
		)

		// Highlight the dominant cluster
		dominant := ""
		maxCount := 0
		for _, entry := range lisaProfile.Clusters {
			if entry.Count > maxCount {
				maxCount = entry.Count
				dominant = entry.Cluster
			}
		}
		if dominant != "" {
			meaning := map[string]string{
				"HH": "High-High: concentrated advantage",
				"LL": "Low-Low: concentrated disadvantage",
				"HL": "High-Low: an island of advantage",
				"LH": "Low-High: an island of disadvantage",
			}
			captions = append(captions, VideoCaption{
				Text:       fmt.Sprintf("Dominant pattern: %s — %s.", dominant, meaning[dominant]),
				StartSec:   12,
				EndSec:     18,
				AccentText: dominant,
			})
		}
	} else {
		captions = append(captions,
			VideoCaption{Text: "Each tract is classified by how it relates to its neighbors.", StartSec: 5, EndSec: 11},
			VideoCaption{Text: "High-High: hot spots. Low-Low: cold spots. HL/LH: spatial outliers.", StartSec: 11, EndSec: 17},
		)
	}

	return captions
}

// parseCSV splits a comma-separated query param into a slice, trimming whitespace.
// Used for multi-vintage queries: ?vintage=2019,2021,2023
// queryBool reads an optional boolean query parameter. An absent, empty, or
// unparseable value yields false, so a malformed flag degrades to the safe
// default rather than rejecting the request.
func queryBool(c *gin.Context, key string) bool {
	v := c.Query(key)
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}

func parseCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
func genericCaptionsForVideo(videoName string) []VideoCaption {
	switch videoName {
	case "zscore":
		return []VideoCaption{
			{Text: "The z-score tells us how far a county is from the state average.", StartSec: 0, EndSec: 4},
			{Text: "Measured in standard deviations — a score of 0 means exactly average.", StartSec: 4, EndSec: 8},
			{Text: "Select a county to see where it falls on this distribution.", StartSec: 8, EndSec: 13},
			{Text: "A z-score of +1.5 means the county is well above the state average.", StartSec: 13, EndSec: 18},
		}
	case "ice":
		return []VideoCaption{
			{Text: "ICE — Index of Concentration at the Extremes — measures polarization.", StartSec: 0, EndSec: 5},
			{Text: "The score ranges from -1 (all deprived) to +1 (all privileged).", StartSec: 5, EndSec: 11},
			{Text: "It combines race and income into one measure of structural inequality.", StartSec: 11, EndSec: 17},
		}
	case "lisa_cluster_map":
		return []VideoCaption{
			{Text: "LISA — Local Indicators of Spatial Association — reveals geographic clusters.", StartSec: 0, EndSec: 5},
			{Text: "Each tract is classified by how it relates to its neighbors.", StartSec: 5, EndSec: 11},
			{Text: "High-High: hot spots. Low-Low: cold spots. HL/LH: spatial outliers.", StartSec: 11, EndSec: 17},
		}
	default:
		return []VideoCaption{
			{Text: "Select a county to see personalized data for this video.", StartSec: 0, EndSec: 5},
		}
	}
}

// ── GET /geographies/:geoid/lisa-profile ────────────────────────────────────

// handleGetLISACountyProfile returns a county-level summary of LISA spatial
// autocorrelation clusters aggregated from tract-level analysis_scores.
// The GEOID must be a 5-character county FIPS code.
func (p *PolicyPlugin) handleGetLISACountyProfile(c *gin.Context) {
	geoid := c.Param("geoid")

	// LISA profile only makes sense for county-level GEOIDs (tracts within county).
	if len(geoid) != 5 {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:  "invalid GEOID for LISA profile",
			Detail: "LISA county profile requires a 5-character county GEOID",
		})
		return
	}

	profile, err := p.store.QueryLISACountyProfile(c.Request.Context(), geoid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:  "LISA profile query failed",
			Detail: err.Error(),
		})
		return
	}

	clusters := make([]LISAClusterEntryResponse, 0, len(profile.Clusters))
	for _, entry := range profile.Clusters {
		clusters = append(clusters, LISAClusterEntryResponse{
			Cluster: entry.Cluster,
			Count:   entry.Count,
		})
	}

	c.JSON(http.StatusOK, LISACountyProfileResponse{
		GEOID:       profile.GEOID,
		Clusters:    clusters,
		TotalTracts: profile.TotalTracts,
	})
}
