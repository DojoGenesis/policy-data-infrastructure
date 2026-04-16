# ADR-005: Gateway-Powered Chat Interface for v2

**Status:** Proposed
**Date:** 2026-04-15
**Deciders:** Alfonso Morales

## Context

The v1 data explorer (Alpine.js, 6 pages, static data cards) works but is a
read-only dashboard. The user's actual vision is a conversational interface where
policy analysts can ask questions and get answers backed by real data:

- "Show me the counties with the highest poverty rates"
- "Compare Dane and Milwaukee on health outcomes"
- "Generate a policy brief for county 55025"
- "What does the data say about food deserts in rural Wisconsin?"

The Dojo Gateway is already running on the VPS (port 7340) with Anthropic API keys,
chat endpoint, and SSE streaming. The PDI API (port 8340) has all the data endpoints.
The missing piece is wiring them together.

## Decision (Proposed)

Replace the static data explorer with a gateway-powered chat application:

1. **Chat frontend** — clean textarea + message list + SSE streaming, embedded in
   the Go binary alongside the existing data explorer pages.

2. **Gateway as LLM proxy** — the chat frontend connects to the Dojo Gateway's `/chat`
   endpoint. The Gateway handles model routing, streaming, and tool calling.

3. **PDI API as MCP tool source** — configure the Gateway to register PDI API endpoints
   as callable tools. When the LLM decides it needs county data, it calls the
   `query_counties` tool which hits `GET /v1/policy/geographies`.

4. **System prompt** — a rich prompt grounding the LLM in PDI's methodology, data sources,
   variable definitions (from `/v1/policy/variables`), and the 85 policy positions.
   The prompt is maintained as a markdown file in the repo and loaded at startup.

## Tool Definitions for the Gateway

```json
[
  {"name": "query_counties", "description": "Search and list Wisconsin counties with population data", "endpoint": "GET /v1/policy/geographies?level=county&state_fips=55"},
  {"name": "get_county_profile", "description": "Get full indicator profile for a county by GEOID", "endpoint": "GET /v1/policy/geographies/:geoid"},
  {"name": "compare_counties", "description": "Side-by-side comparison of two counties", "endpoint": "POST /v1/policy/compare"},
  {"name": "list_variables", "description": "List all available indicator variables with metadata", "endpoint": "GET /v1/policy/variables"},
  {"name": "list_policies", "description": "List candidate policy positions", "endpoint": "GET /v1/policy/policies"},
  {"name": "generate_narrative", "description": "Generate a policy narrative for a geography", "endpoint": "POST /v1/policy/generate/narrative"},
  {"name": "generate_deliverable", "description": "Generate an HTML policy brief", "endpoint": "POST /v1/policy/generate/deliverable"}
]
```

## Architecture

```
Browser → policydatainfrastructure.com → Go binary (port 8340)
                                           ├── /          → chat UI (HTML)
                                           ├── /v1/       → PDI API endpoints
                                           └── /chat      → proxy to Dojo Gateway (port 7340)
                                                              ├── LLM routing (Anthropic)
                                                              └── Tool calls → PDI API (localhost:8340)
```

## Consequences

- **Positive:** Natural language interface for policy analysts — no SQL, no API knowledge needed
- **Positive:** Reuses existing infrastructure (Gateway + PDI API both already deployed)
- **Positive:** The LLM can combine multiple data queries in a single conversation
- **Positive:** HTML deliverables can be generated conversationally
- **Negative:** Requires Gateway configuration for PDI-specific tools (new MCP registration)
- **Negative:** LLM responses may hallucinate statistics — need grounding via tool calls
- **Negative:** Anthropic API costs for every chat interaction

## Prerequisites

- Gateway must support registering HTTP API endpoints as tools
- OR: build a lightweight PDI MCP server that wraps the API
- Chat frontend needs SSE streaming support
- System prompt needs to be comprehensive but not exceed context limits
