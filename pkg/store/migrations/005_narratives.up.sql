CREATE TABLE IF NOT EXISTS narrative_templates (
    name        TEXT PRIMARY KEY,
    description TEXT,
    template    TEXT NOT NULL,
    slots       JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS generated_narratives (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    template     TEXT REFERENCES narrative_templates(name),
    scope_geoid  TEXT REFERENCES geographies(geoid),
    title        TEXT,
    html         TEXT,
    data_hash    TEXT,
    metadata     JSONB,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_narratives_template    ON generated_narratives(template);
CREATE INDEX IF NOT EXISTS idx_narratives_scope_geoid ON generated_narratives(scope_geoid);
CREATE INDEX IF NOT EXISTS idx_narratives_generated_at ON generated_narratives(generated_at);
