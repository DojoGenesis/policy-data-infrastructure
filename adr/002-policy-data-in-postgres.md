# ADR-002: Move Policy Data from CSV to PostgreSQL

**Status:** Proposed
**Date:** 2026-04-15
**Deciders:** Alfonso Morales, Claude

## Context

Policy positions (70 records for Francesca Hong) live in `data/policies/*.csv` and are
only accessible via the Python `evidence_cards.py` script. The Go API has no policy
endpoints. Adding candidates requires manual CSV creation and Python re-runs.

For v1, the site needs to display policy positions linked to indicator data. The frontend
needs `GET /v1/policies`, `GET /v1/policies/:id/evidence`, and candidate filtering.

## Decision

Add a `policies` table (migration 008) and expose policy data through the Go API.

Schema:
```sql
CREATE TABLE IF NOT EXISTS policies (
    id TEXT PRIMARY KEY,
    candidate TEXT NOT NULL,
    office TEXT,
    state TEXT,
    category TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    equity_dimension TEXT,
    geographic_scope TEXT,
    source_url TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);
```

New Store methods: `PutPolicies`, `QueryPolicies`, `GetPolicy`.
New API endpoints: `GET /v1/policy/policies`, `GET /v1/policy/policies/:id`.
Evidence card generation remains Python for now; output JSON is served as a static
endpoint or loaded into a new `evidence_cards` table in a subsequent ADR.

## Consequences

- **Positive:** Policies queryable via API. Frontend can build candidate tracker.
- **Positive:** Adding a candidate becomes: write CSV → `pdi load-policies data/policies/*.csv`
- **Negative:** Dual source of truth until CSV loading is automated.
- **Negative:** Evidence cards remain Python-generated; full Go-native path is future work.
