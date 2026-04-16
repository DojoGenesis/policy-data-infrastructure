# ADR-001: Embed Frontend in Go Binary

**Status:** Accepted
**Date:** 2026-04-15
**Deciders:** Alfonso Morales, Claude

## Context

The PDI API is deployed on a VPS at `api.policydatainfrastructure.com`. The marketing
site at `policydatainfrastructure.com` is a static GitHub Pages site with zero API calls.
We need a data explorer frontend. Three options were evaluated:

1. **Separate SPA** (React/Svelte/Next) — requires build toolchain, CORS, two deploy targets
2. **Embedded static files** (Alpine.js + `//go:embed`) — same-origin, one binary, no build step
3. **Server-rendered** (Go templates) — tighter coupling, slower iteration

## Decision

Embed the frontend in the Go binary via `//go:embed frontend/*`. Use Alpine.js for
reactivity (loaded from CDN). No npm, no Vite, no frontend build step.

The existing `docs/index.html` Alpine.js pattern is the starting point. API calls
use `fetch()` against same-origin `/v1/policy/*` endpoints (no CORS needed for the
primary use case). CORS is configured for cross-origin consumers (dev, partners).

## Consequences

- **Positive:** Single binary deployment. No CORS for primary frontend. No frontend build toolchain.
  Solo operator constraint respected.
- **Positive:** Marketing page evolves into app shell — no throwaway work.
- **Negative:** No hot-reload during frontend development (must rebuild Go binary).
  Mitigated: use `go run ./cmd/pdi serve` during dev, edit HTML directly.
- **Negative:** Alpine.js is less capable than React/Svelte for complex UI.
  Acceptable for a data explorer + evidence card gallery. Revisit if we need
  client-side routing or complex state management.
