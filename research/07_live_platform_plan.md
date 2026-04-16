# Live Platform Plan — policydatainfrastructure.com

> Date: 2026-04-15 | Author: Alfonso + Claude | Status: DRAFT — awaiting audit signoff
> Related: research/06_recommendations.md, research/05_refactor_plan.md

## Vision

policydatainfrastructure.com becomes a live, VPS-driven policy data platform:
county explorer, evidence card gallery, candidate policy tracker, and narrative
generation — all backed by real government data in PostGIS, served by the Go API.

## Current State (audited)

### What Works
- **PDI Go API**: 12 endpoints, 380 tests, all passing. Binary is 25MB, fully self-contained
  (migrations + narrative templates embedded via `//go:embed`). Gin release mode hardcoded.
- **VPS PostGIS**: 72 WI counties, 1,652 tracts, 34 variables, 21K+ indicator rows. Healthy.
- **Dojo Gateway**: Running on VPS port 7340 via systemd. Caddy proxies `pdi.trespies.dev` to it.
- **policydatainfrastructure.com**: GitHub Pages static site. Marketing page with Alpine.js widgets
  and 8 hardcoded evidence cards. Not connected to live data.
- **Policy data**: 1 candidate (Francesca Hong), 70 policies, 38 equity dimensions, 70 evidence cards.
- **Goreleaser**: Produces linux/amd64 binaries correctly.

### What Doesn't Exist Yet
| Gap | Severity | Phase |
|-----|----------|-------|
| No PDI binary deployed to VPS | Blocker | 1 |
| No systemd service for `pdi serve` | Blocker | 1 |
| No Caddy block for PDI API domain | Blocker | 1 |
| No CORS middleware in gateway | Blocker for browser clients | 1 |
| No static file serving in Go binary | Needed for embedded frontend | 2 |
| No `/readyz` endpoint (DB health) | Medium | 1 |
| No deploy automation (manual scp) | Medium | 1 |
| No DB table for policies (CSV-only) | Needed for API-served policy data | 3 |
| No Go API endpoint for policies or evidence cards | Needed for frontend | 3 |
| `evidence_cards.py` hardcoded to one candidate CSV | Needs parameterization | 3 |
| `nginx.conf` in repo has placeholder domain | Low — VPS uses Caddy, not nginx | -- |

## Architecture Decision: Embedded Frontend vs Separate SPA

**Recommended: Embed frontend in the Go binary.**

Rationale:
- Same-origin serving eliminates CORS entirely — API calls from `/v1/*` work without middleware
- Single binary deployment: one file, one systemd service, one port
- No build toolchain (no npm, no Vite) — Alpine.js from CDN + `//go:embed docs/*`
- Matches the existing `docs/index.html` pattern (Alpine.js + self-contained HTML)
- The marketing page evolves into the app shell

Alternative (separate SPA) was rejected because:
- Requires CORS configuration and testing
- Two deploy targets (static site + API)
- More infrastructure to maintain for a solo operator

## Phased Implementation

### Phase 1: Deploy PDI API to VPS (grant-critical)

**Goal:** `api.policydatainfrastructure.com` returns live data from PostGIS.

1. **Add CORS middleware** — `gin-contrib/cors` with configurable allowed origins. Even with embedded
   frontend (same-origin), CORS is needed for: local development, partner integrations, grant demos
   from other domains.

2. **Add `/readyz` endpoint** — pings the pgx pool. Separates liveness (`/health`) from readiness.

3. **Cross-compile binary** — `GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o pdi-linux ./cmd/pdi`

4. **Create systemd service file** — `deploy/pdi.service`:
   ```ini
   [Unit]
   Description=Policy Data Infrastructure API
   After=postgresql.service
   
   [Service]
   Type=simple
   User=dojo
   ExecStart=/usr/local/bin/pdi serve --port 8340
   EnvironmentFile=/etc/pdi/env
   Restart=on-failure
   
   [Install]
   WantedBy=multi-user.target
   ```

5. **Add Caddy block** — append to VPS Caddyfile:
   ```
   api.policydatainfrastructure.com {
       reverse_proxy localhost:8340
   }
   ```
   Or: use path-based routing on the main domain (`policydatainfrastructure.com/v1/*`).

6. **Upload and start** — `scp pdi-linux root@5.161.84.125:/usr/local/bin/pdi`,
   copy service file, `systemctl enable --now pdi`.

7. **Verify** — `curl https://api.policydatainfrastructure.com/v1/geographies?level=county&limit=3`

**Estimated effort:** 2-3 hours. No new Go code beyond CORS + readyz (< 30 lines).

### Phase 2: Data Explorer Frontend (demo-critical)

**Goal:** policydatainfrastructure.com shows live data, not static cards.

1. **Embed static assets** — add `//go:embed frontend/*` to serve.go. The `frontend/` directory
   contains `index.html`, `app.js`, `style.css` (all Alpine.js, no build step).

2. **County Explorer page** — fetches `/v1/geographies?level=county&state_fips=55` on load.
   Click a county → drill into `/v1/geographies/:geoid` with full indicator profile.
   Ranked indicator bars (percentile within state).

3. **Evidence Card Gallery** — fetch from a new `/v1/evidence-cards` endpoint (or serve
   the pre-generated JSON). Filter by category, candidate, equity dimension.

4. **Compare Tool** — select two counties → `/v1/compare` → side-by-side indicator table
   with delta highlighting.

5. **Narrative Generator** — click "Generate Brief" → `/v1/generate/narrative` → rendered
   HTML policy brief for that geography.

**Estimated effort:** 1-2 sessions. The API endpoints already exist; this is frontend-only.

### Phase 3: Candidate Policy Tracker + National Scale

**Goal:** Multi-candidate platform with evidence-linked policy positions.

1. **Add policies DB table** — migration 008:
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
       data_sources_needed TEXT,
       created_at TIMESTAMPTZ DEFAULT now()
   );
   ```

2. **Add Store methods** — `PutPolicies`, `QueryPolicies`, `GetPolicy`

3. **Add API endpoints** — `GET /v1/policies`, `GET /v1/policies/:id`, `GET /v1/policies/:id/evidence`

4. **Generalize evidence_cards.py** — accept `--candidate` flag or `--all` for all CSVs in
   `data/policies/`. Parameterize the hardcoded `POLICIES_CSV` path.

5. **Add 2-3 progressive candidates** — research their public platforms, create CSVs,
   generate evidence cards. Target candidates whose platforms touch our 13 data dimensions.

6. **National-scale data fetch** — run pipeline for all 50 states. Rate limit budget:
   - Census ACS: ~500 req/min with key (feasible in hours)
   - CDC PLACES: 100 req/min with Socrata token (feasible in hours)
   - BLS LAUS: 25 queries/day unregistered → need registered key for national scale
   - USDA: single bulk download (national, minutes)

**Estimated effort:** 2-3 sessions. The candidate CSV creation is research-heavy.

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| VPS disk fills during national fetch | Low (34GB free) | High | Monitor `df -h` during fetch; USDA + CDC are largest |
| BLS rate limit blocks national fetch | High (25/day unregistered) | Medium | Use registered API key from Infisical |
| Government API URL changes break adapters | Medium | Medium | URL fallback lists already implemented in adapters |
| Caddy cert issuance fails for new subdomain | Low | High | Pre-verify DNS before adding Caddy block |
| Evidence card quality degrades with proxy metrics | Already happening | Medium | 16 of 38 dimensions use poverty_rate as proxy |

## DNS Plan

| Domain | Points To | Purpose |
|--------|-----------|---------|
| `policydatainfrastructure.com` | VPS (Caddy) | Main site + embedded frontend |
| `api.policydatainfrastructure.com` | VPS (Caddy) | Explicit API subdomain (optional) |

This requires changing DNS from GitHub Pages to VPS IP. Caddy handles TLS automatically via Let's Encrypt.

## Success Criteria

- [ ] `curl https://policydatainfrastructure.com/v1/geographies?level=county` returns live data
- [ ] Browser can load the county explorer and see all 72 WI counties
- [ ] At least 2 progressive candidates have policy positions with evidence cards
- [ ] Compare tool works in browser for any two counties
- [ ] Narrative generation produces readable output for any county GEOID
