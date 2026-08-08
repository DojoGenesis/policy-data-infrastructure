package gateway

// POST /analyses + GET /analyses/runs/:id — the queued-run surface
// (ADR-014 D3/D9/D10). The POST is cache-first: an identical prior analysis
// returns immediately with cached=true and no run is spent. Order of gates:
// auth (middleware) → validation → cache → budget → enqueue. The cache check
// deliberately precedes the budget so a cache hit costs nobody their
// allowance — shared results are the point of the cache.

import (
	"fmt"
	"net/http"
	"regexp"

	"github.com/gin-gonic/gin"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// CreateAnalysisRunRequest is the POST /analyses body.
type CreateAnalysisRunRequest struct {
	// Type is a registered run type (see runExecutors).
	Type string `json:"type" binding:"required"`
	// ScopeLevel is "county" or "state".
	ScopeLevel string `json:"scope_level" binding:"required"`
	// ScopeGEOID is the 5-digit county or 2-digit state FIPS.
	ScopeGEOID string `json:"scope_geoid" binding:"required"`
	// Vintage pins the primary variable's vintage. Empty = resolve latest
	// at enqueue (the resolved value, not "latest", enters the cache key).
	Vintage string `json:"vintage"`
	// Parameters are run-type specific; see each type's canon.
	Parameters map[string]interface{} `json:"parameters"`
}

var scopeGEOIDPattern = regexp.MustCompile(`^\d{2}$|^\d{5}$`)

// handleCreateAnalysisRun enqueues an analysis run (or returns the cached
// analysis).
func (p *PolicyPlugin) handleCreateAnalysisRun(c *gin.Context) {
	var req CreateAnalysisRunRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid request body", Detail: err.Error()})
		return
	}

	exec, ok := runExecutors[req.Type]
	if !ok {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:  "unknown analysis type",
			Detail: fmt.Sprintf("%q is not a registered run type; available: %s", req.Type, registeredRunTypes()),
		})
		return
	}

	switch req.ScopeLevel {
	case "county", "state":
	default:
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid scope_level", Detail: "scope_level must be county or state"})
		return
	}
	if !scopeGEOIDPattern.MatchString(req.ScopeGEOID) ||
		(req.ScopeLevel == "county" && len(req.ScopeGEOID) != 5) ||
		(req.ScopeLevel == "state" && len(req.ScopeGEOID) != 2) {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid scope_geoid", Detail: "county scope needs a 5-digit FIPS, state scope a 2-digit FIPS"})
		return
	}

	if req.Parameters == nil {
		req.Parameters = map[string]interface{}{}
	}
	params, err := exec.canon(req.Parameters)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid parameters", Detail: err.Error()})
		return
	}

	ctx := c.Request.Context()

	// Resolve an omitted vintage to a concrete one NOW — the cache key never
	// contains "latest" (ADR-014 D9).
	vintage := req.Vintage
	if vintage == "" {
		primary := exec.primaryVariable(params)
		v, err := p.store.LatestVintageForVariable(ctx, primary)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "vintage resolution failed", Detail: err.Error()})
			return
		}
		if v == "" {
			c.JSON(http.StatusUnprocessableEntity, ErrorResponse{
				Error:  "no data for variable",
				Detail: fmt.Sprintf("variable %q has no loaded indicator rows at any vintage", primary),
			})
			return
		}
		vintage = v
	}

	key := store.AnalysisKey{
		Type:       req.Type,
		ScopeGEOID: req.ScopeGEOID,
		ScopeLevel: req.ScopeLevel,
		Vintage:    vintage,
		Parameters: params,
	}
	if hit, err := p.store.FindAnalysisByKey(ctx, key); err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "cache lookup failed", Detail: err.Error()})
		return
	} else if hit != nil {
		c.JSON(http.StatusOK, gin.H{
			"analysis_id": hit.ID,
			"cached":      true,
			"computed_at": hit.ComputedAt,
			"vintage":     hit.Vintage,
			"score_count": hit.ScoreCount,
		})
		return
	}

	clientKey, _ := ClientKey(c)
	active, err := p.store.CountActiveRuns(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "queue depth check failed", Detail: err.Error()})
		return
	}
	if denial := p.runBudget.Admit(clientKey, active); denial != nil {
		denial.Abort(c)
		return
	}

	runID, err := p.store.CreateAnalysisRun(ctx, store.AnalysisRun{
		RunType:    req.Type,
		ScopeGEOID: req.ScopeGEOID,
		ScopeLevel: req.ScopeLevel,
		Vintage:    vintage,
		Parameters: params,
		ClientKey:  clientKey,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "enqueue failed", Detail: err.Error()})
		return
	}
	if p.runner != nil {
		p.runner.Wake()
	}

	c.Header("Location", "/v1/policy/analyses/runs/"+runID)
	c.JSON(http.StatusAccepted, gin.H{
		"run_id":  runID,
		"status":  "queued",
		"vintage": vintage,
	})
}

// handleGetAnalysisRun reports a run's status; when done it carries the
// analysis_id to fetch results from the existing analyses endpoints.
func (p *PolicyPlugin) handleGetAnalysisRun(c *gin.Context) {
	run, err := p.store.GetAnalysisRun(c.Request.Context(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "run not found", Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"run_id":       run.ID,
		"type":         run.RunType,
		"status":       run.Status,
		"error":        run.Error,
		"analysis_id":  run.AnalysisID,
		"scope_level":  run.ScopeLevel,
		"scope_geoid":  run.ScopeGEOID,
		"vintage":      run.Vintage,
		"parameters":   run.Parameters,
		"requested_at": run.RequestedAt,
		"started_at":   run.StartedAt,
		"finished_at":  run.FinishedAt,
	})
}

func registeredRunTypes() string {
	names := make([]string, 0, len(runExecutors))
	for name := range runExecutors {
		names = append(names, name)
	}
	// Deterministic order for error messages and tests.
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ", "
		}
		out += n
	}
	return out
}
