CREATE TABLE IF NOT EXISTS evidence_cards (
  id SERIAL PRIMARY KEY,
  policy_id TEXT NOT NULL UNIQUE,
  policy_title TEXT NOT NULL,
  category TEXT NOT NULL,
  equity_dimension TEXT NOT NULL,
  title TEXT,
  key_finding TEXT,
  data_quality TEXT,
  findings JSONB DEFAULT '[]',
  indicators JSONB DEFAULT '[]',
  statewide_context JSONB DEFAULT '{}',
  county_variation JSONB DEFAULT '{}',
  top_need_counties JSONB DEFAULT '[]',
  bottom_need_counties JSONB DEFAULT '[]',
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_evidence_cards_policy ON evidence_cards(policy_id);
CREATE INDEX IF NOT EXISTS idx_evidence_cards_category ON evidence_cards(category);
CREATE INDEX IF NOT EXISTS idx_evidence_cards_equity ON evidence_cards(equity_dimension);