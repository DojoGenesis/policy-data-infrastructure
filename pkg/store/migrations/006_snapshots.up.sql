CREATE TABLE snapshots (
    id          TEXT PRIMARY KEY,
    tag         TEXT UNIQUE,
    description TEXT,
    row_counts  JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
