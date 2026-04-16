CREATE TABLE IF NOT EXISTS policies (
    id TEXT PRIMARY KEY,
    candidate TEXT NOT NULL,
    office TEXT,
    state TEXT,
    category TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    bill_references TEXT,
    claims_empirical TEXT,
    equity_dimension TEXT,
    geographic_scope TEXT,
    data_sources_needed TEXT,
    source_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_policies_candidate ON policies(candidate);
CREATE INDEX IF NOT EXISTS idx_policies_category ON policies(category);
CREATE INDEX IF NOT EXISTS idx_policies_state ON policies(state);
