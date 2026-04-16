# ADR-003: Add Indicator Metadata Endpoint

**Status:** Proposed
**Date:** 2026-04-15
**Deciders:** Alfonso Morales, Claude

## Context

The API returns indicator values with `variable_id` (e.g. `poverty_rate`, `cdc_obesity`)
but no human-readable label, unit, or direction. A frontend developer cannot build a
labeled dashboard without hardcoding variable metadata client-side.

The `indicator_meta` table already exists in the schema (migration 003) with columns:
`variable_id`, `source_id`, `name`, `description`, `unit`, `direction`. The `seed_sources.sql`
migration seeds `indicator_sources` rows. Variable metadata is seeded during ingest.

## Decision

1. Add `GET /v1/policy/variables` endpoint returning the full `indicator_meta` catalog
   with joined source info. Response: `{variables: [{id, name, description, unit,
   direction, source_id, source_name}]}`.

2. Enrich indicator responses: when indicators are embedded in geography responses
   (`handleGetGeography`, `handleGetIndicators`), include `name`, `unit`, and `direction`
   alongside `variable_id` and `value`. This requires a single JOIN or an in-memory
   lookup cache populated at startup.

## Consequences

- **Positive:** Frontend can render labeled indicator dashboards without hardcoded metadata.
- **Positive:** Variables endpoint serves as self-documenting API catalog.
- **Negative:** Slightly larger response payloads for geography profiles.
- **Negative:** Requires seeding `indicator_meta` on VPS (currently may be empty — verify).
