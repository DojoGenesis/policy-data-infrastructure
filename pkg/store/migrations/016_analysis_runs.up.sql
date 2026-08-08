-- ADR-014 D3: user-triggered analyses are queued, not synchronous — a run
-- request returns a job handle immediately and results are fetched when
-- ready. The queue is DB-backed so it survives restarts, and rows are
-- claimed with FOR UPDATE SKIP LOCKED so multiple workers never double-run.
CREATE TABLE IF NOT EXISTS analysis_runs (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_type     TEXT NOT NULL,
    scope_geoid  TEXT REFERENCES geographies(geoid),
    scope_level  geo_level,
    vintage      TEXT,
    parameters   JSONB NOT NULL DEFAULT '{}'::jsonb,
    status       TEXT NOT NULL DEFAULT 'queued'
                 CHECK (status IN ('queued', 'running', 'done', 'failed')),
    error        TEXT,
    -- The completed run's cache entry. SET NULL (not CASCADE): deleting an
    -- analysis invalidates the pointer, never the run's audit trail.
    analysis_id  UUID REFERENCES analyses(id) ON DELETE SET NULL,
    client_key   TEXT,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_analysis_runs_status_requested
    ON analysis_runs (status, requested_at);
