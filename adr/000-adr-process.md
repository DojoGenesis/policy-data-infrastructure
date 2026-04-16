# ADR-000: Use Architecture Decision Records

**Status:** Accepted
**Date:** 2026-04-15
**Deciders:** Alfonso Morales

## Context

PDI is growing from a proof-of-concept into a production platform. Decisions about
data models, API surface, deployment architecture, and frontend strategy are being
made in agent sessions that lose context between conversations. We need a durable,
scannable record of architectural decisions that informs both human contributors
and autonomous agents.

## Decision

Adopt Lightweight Architecture Decision Records in `adr/` at the repository root.

Format: `NNN-short-title.md` (zero-padded 3-digit sequence).
Template: Status, Date, Context, Decision, Consequences.
Status values: `Proposed` | `Accepted` | `Superseded by ADR-NNN` | `Deprecated`.

Every agent session that makes a structural choice (new table, new endpoint, new
integration pattern) must create or update an ADR.

## Consequences

- Decisions are discoverable via `ls adr/`
- New agents can scan ADRs in seconds instead of reading thousands of lines of source
- Superseded decisions stay in the record for archaeology
- Overhead is ~5 minutes per decision
