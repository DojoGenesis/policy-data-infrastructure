# Plan: Policy Data Infrastructure — From v1 Ground Truth to MCF LOI

**Created:** 2026-04-26
**Source Files:** `STATUS.md`, `TODO.md`, `CLAUDE.md`, `CHANGELOG.md`, `adr/004-v1-launch-plan.md`, `adr/005-gateway-powered-chat.md`, `handoffs/2026-04-14_pdi_data_pipeline_continuation.md`, `git log -10`
**Mode:** Context Ingestion (no files uploaded — ingested the live repo state because the request was a topic directive, not file-bound)
**Repo:** `/Users/alfonsomorales/ZenflowProjects/policy-data-infra` (HEAD: `d858215`)

---

## File Catalog (current state)

### `STATUS.md` (Apr 15, stale)
- **Type:** Repo health snapshot
- **Claims:** HEAD `2922e3a`, 380 tests, frontend "no JavaScript calls the API", 6 pages "NOT live"
- **Reality drift:** HEAD is now `d858215`; frontend has been rebuilt (`9f7735d`), Alpine init fixed (`e0e05d8`), CSS+JS aligned (`1a2f202`), design system saved (`9dc35da`), chat live (`9ffcf7e` → `ea26794`)

### `TODO.md` (Apr 15)
- **P1 grant-critical:** finish `policydatainfrastructure.com` real app, MCF LOI (Jun 3), Arnold (~May)
- **P2:** transitive dep audit, pipeline 30.4% coverage, gateway 41.2% coverage
- **P3:** WI DPI Go integration, national scale, GTFS/EPA-TRI mock-HTTP tests

### `adr/004-v1-launch-plan.md` (Accepted, Apr 15)
- **4 waves:** Wave 1 API enrichment (4h), Wave 2 frontend (12h), Wave 3 candidates (8h), Wave 4 national+map (10h) = 34h revised
- **5 Wave 1 endpoints called out:** `/v1/policy/variables`, `/v1/policy/policies`, `/v1/policy/evidence-cards`, `/v1/policy/analyses`, indicator-response enrichment
- **6 audit findings:** materialized view refresh required, no analyses run, indicator_meta has 42 rows, Wave 2 effort undersized, DNS migration creates visibility gap, evidence cards not in DB

### `adr/005-gateway-powered-chat.md` (Proposed, Apr 15)
- 7 Gateway tools defined wrapping `/v1/policy/*` endpoints
- **Status drift:** ADR is "Proposed" but commits `9ffcf7e` (direct Anthropic) and `ea26794` (restored Gateway proxy) indicate it was built and pivoted

### `handoffs/2026-04-14_pdi_data_pipeline_continuation.md`
- 10-item track set; per Apr 15 CHANGELOG all targets confirmed complete
- Useful as model for this handoff's structure

### `git log -10` (last 10 commits)
- `d858215` fix(nightly-sweep): bound HRSA live HTTP probe with 5s timeout
- `ea26794` fix: restore Gateway proxy for chat — remove direct Anthropic hack
- `9ffcf7e` feat: direct Anthropic API for chat — bypass Gateway for grounded responses
- `723da4a`, `9e611f5`, `e0e05d8`, `f864dc0`, `1a2f202` — frontend bug fixes from user testing
- `9dc35da` docs: save interface design system — activist data journalism + clean data tool
- `9f7735d` feat: nuclear CSS rebuild

---

## Constraints (extracted from files)

1. **Grant timeline:** MCF LOI due 2026-06-03 (38 days from 2026-04-26). Arnold decision ~May 2026, no action. (`STATUS.md:78-81`, `TODO.md:9-11`, `adr/004:13-14`)
2. **Solo-operator stack:** Alpine.js + Tailwind CDN, Go binary embed, no build step, no new infrastructure. (`adr/004:46-49`, `adr/004:101-103`)
3. **VPS-only deploy target:** `5.161.84.125` (Hetzner), `pdi.service` systemd, Caddy + Cloudflare. PostGIS local on VPS. (`STATUS.md:14`, `CLAUDE.md:339-342`)
4. **Frontend served from Go binary** via `//go:embed frontend/*` (`adr/004:39-40`, Wave 2A)
5. **Test gate before commit:** `go build ./...` + `go test ./... -short` MUST pass. 380+ test floor. (`CLAUDE.md:226-228`, `CLAUDE.md:313-315`)
6. **Materialized view discipline:** `REFRESH MATERIALIZED VIEW CONCURRENTLY indicators_latest` after any indicator load — silent stale data without it. (`CLAUDE.md:128-132`, `adr/004:108-115`)
7. **GEOID strings always zero-padded** — never integers. Validation regex enforced. (`CLAUDE.md:140-147`)
8. **Gateway port 7340, PDI port 8340** — chat proxy crosses these. (`CLAUDE.md:339`, `adr/005:19-20`, root CLAUDE.md)
9. **Push protocol:** `gh auth switch --user DojoGenesis` before push to `DojoGenesis/policy-data-infrastructure`. (`CLAUDE.md:265-266`)
10. **No commits from agents:** orchestrator commits after independent verification. (`CLAUDE.md:319`)

---

## Contradictions (must resolve before planning Phase 1)

### C1 — STATUS.md frontend claim vs. recent commits
- **STATUS.md:53-60** says: "no JavaScript calls the API" + 6 explorer pages "NOT live"
- **Reality:** 8+ commits since then touched frontend, Alpine init, CSS/JS alignment, chat
- **Impact:** Plan can't trust STATUS.md to identify what's actually missing
- **Resolution:** Phase 0 audits the live binary's actual served routes + frontend behavior

### C2 — ADR-005 status "Proposed" vs. shipped chat code
- **adr/005:3** says Status: Proposed
- **Reality:** `9ffcf7e` shipped chat with direct Anthropic, `ea26794` restored Gateway proxy
- **Impact:** Without resolution, future agents waste cycles re-implementing
- **Resolution:** Phase 0 flips ADR-005 to Accepted (with amendments documenting the direct→Gateway pivot) OR creates ADR-006 superseding it

### C3 — DNS migration ambiguity
- **STATUS.md:16** says marketing site at `policydatainfrastructure.com` is GitHub Pages "not connected to API"
- **adr/004:46** Wave 2F "DNS migration: `policydatainfrastructure.com` → VPS"
- **Impact:** Unclear whether the Apr 15+ frontend work was deployed under DNS or just behind `api.*` subdomain
- **Resolution:** Phase 0 verifies what each domain serves right now

### C4 — Wave 1 API endpoint completion
- **TODO.md:14** + **STATUS.md:62-67** list 5 Wave 1 endpoints as not-done
- **Recent frontend commits** imply data is being fetched from somewhere (CSS aligned to JS methods)
- **Impact:** Could be calling existing endpoints + faking labels; could be partial Wave 1; could be hardcoded JSON
- **Resolution:** Phase 0 greps `frontend/` for `fetch(`/`axios`/`xhr` and lists endpoints actually called

---

## Plan

### Phase 0 — Reconcile reality with documentation (~2h)

**Why this comes first:** Four documented contradictions mean any agent dispatched on Phase 1+ work risks duplicating shipped code or fixing already-fixed bugs. Cheap to verify; expensive to skip.

**Actions:**
1. Walk `frontend/` (whatever exists post-`9f7735d`) and grep for `fetch(`, `await fetch`, `/v1/policy/` to enumerate endpoints actually called by JS
2. Hit live endpoints from terminal: `curl https://api.policydatainfrastructure.com/v1/policy/{variables,policies,evidence-cards,analyses}` and record which return 200 vs 404
3. `dig policydatainfrastructure.com` and `dig api.policydatainfrastructure.com` — confirm DNS targets
4. SSH to VPS, `systemctl status pdi.service`, check served routes
5. Update `STATUS.md` with HEAD `d858215`, current test count, real frontend state, real DNS state
6. Update `adr/005` status: flip to Accepted, append amendment recording the direct-Anthropic→Gateway-proxy pivot from `9ffcf7e`/`ea26794`
7. Reconcile `TODO.md` — strike completed items, add discovered gaps from steps 1-3

**Deliverables:**
- Updated `STATUS.md` matching repo HEAD
- Updated `adr/005-gateway-powered-chat.md` (Accepted + pivot amendment)
- Reconciled `TODO.md`
- Plain-text `phase0-reconciliation-notes.md` in `handoffs/` listing actual endpoints called, actual DNS targets, actual tests passing

**Success Criteria:**
- [ ] `STATUS.md` HEAD field matches `git rev-parse HEAD`
- [ ] All endpoints in updated TODO are independently confirmed not-200 via curl
- [ ] ADR-005 status field is no longer "Proposed"
- [ ] `git status` clean except for the doc updates above

---

### Phase 1 — Close grant-critical Wave 1 API gaps (~4h, parallel-friendly)

**Why:** Phase 0 will reveal what's actually missing. Plan assumes the documented Wave 1 gaps from `adr/004:25-29` survive reconciliation. Adjust if Phase 0 finds otherwise.

**Actions (subject to Phase 0 trim):**
1. **1A — `GET /v1/policy/variables`** indicator metadata catalog: register handler in `pkg/gateway/plugin.go`, implement in `pkg/gateway/handlers.go`, add `Store.ListVariableMeta()` in `pkg/store/store.go` + `pkg/store/postgres.go`. Coverage gate: handler test in `pkg/gateway/`.
2. **1B — Enrich indicator responses** with name/unit/direction: extend the existing geography-profile handler to JOIN `indicator_meta`. Backwards-compat: keep raw value field; add metadata sub-object.
3. **1C — `GET /v1/policy/analyses`** list endpoint: query `analyses` table; expose run_id + timestamp + variable_count.
4. **1D — `pdi analyze` CLI subcommand** (per ADR-004 Finding 2): runs AnalyzeStage + SynthesizeStage without re-fetch. Add to `cmd/pdi/`. Run on VPS once landed.
5. **1E — Refresh materialized view** `indicators_latest` post-load (CLAUDE.md:128-132). Add to pipeline FetchStage post-completion hook OR add `pdi refresh-views` subcommand.

**Parallelism:**
- 1A + 1C are independent → dispatch 2 Sonnet agents
- 1B touches existing handler → main thread to avoid merge conflicts
- 1D + 1E are CLI work → 1 Sonnet agent

**Deliverables:**
- 3 new endpoints serving 200 with non-empty payloads from VPS data
- 1 enriched endpoint
- 2 new CLI subcommands (`analyze`, optionally `refresh-views`)
- Tests passing; coverage on `pkg/gateway/` rises above 41.2%

**Success Criteria:**
- [ ] `curl https://api.policydatainfrastructure.com/v1/policy/variables` returns ≥34 variables with name/unit/direction
- [ ] `curl https://api.policydatainfrastructure.com/v1/policy/analyses` returns ≥1 run
- [ ] Geography profile response includes `meta.{name,unit,direction}` for each indicator
- [ ] `go test ./... -short` passes; total tests ≥380

---

### Phase 2 — Wire ADR-005 chat to the data layer (~6h)

**Why:** Recent commits show the chat was the user's actual front door — the product, not the dashboard. Per ADR-005:67-70 it removes the SQL-knowledge barrier for grant reviewers. Status: built but not yet tool-grounded.

**Actions:**
1. Verify Phase 0 amendment: which Gateway path is currently used? (`9ffcf7e` direct, `ea26794` restored Gateway)
2. Register the 7 ADR-005 tools (`adr/005:42-52`) on the Gateway. If Gateway lacks HTTP-tool registration: scaffold a thin PDI MCP server in `pkg/mcp/` wrapping the same endpoints (CLAUDE.md `MCP repo` precedent).
3. Author system prompt at `data/prompts/pdi_system.md` — methodology summary + variable list (loaded from `/v1/policy/variables`) + 70 evidence-card synopsis.
4. Server: load prompt at startup, inject into Gateway `/chat` requests.
5. Frontend: ensure SSE streaming is functional (per `adr/005:79`); verify against Phase 0 chat-page state.
6. Smoke-test 4 prompts from `adr/005:13-16`: poverty-rates lookup, Dane vs Milwaukee compare, brief for `55025`, food-desert query. Each must resolve via tool calls, not hallucinated numbers.

**Deliverables:**
- 7 Gateway tools registered (or PDI MCP server live)
- `data/prompts/pdi_system.md` committed
- Chat answers 4 ADR-005 prompts using real data

**Success Criteria:**
- [ ] Chat response for "compare Dane and Milwaukee on health outcomes" cites at least 2 indicator values that match `/v1/policy/geographies/{55025,55079}`
- [ ] Chat does not hallucinate variable IDs not in `/v1/policy/variables`
- [ ] System prompt < 8K tokens (so cache stays warm)

---

### Phase 3 — Candidate + policy layer for grant narrative (~6h)

**Why:** ADR-004 Wave 3 — adds the "story" grant reviewers need. Differentiates PDI from a generic data API.

**Actions (Wave 3 from `adr/004:60-69`):**
1. **3A** — Migration `pkg/store/migrations/008_policies.up.sql` per ADR-002. Idempotent; matching `008_policies.down.sql`.
2. **3B** — `Store.ListPolicies()`, `Store.GetPolicy(id)`, handlers, route registration.
3. **3C** — Research 2 progressive candidates (beyond Francesca Hong); commit CSVs to `data/policies/`.
4. **3D** — Generalize `analysis/evidence_cards.py` to multi-candidate: parameterize on candidate slug.
5. **3E** — Frontend: candidate tracker page in `frontend/`. Hash-route per `adr/004:50-55`.
6. **3F** — Wire `handleGenerateDeliverable` to full narrative engine (currently stub per `STATUS.md:67`).

**Parallelism:**
- 3A→3B sequential (FK)
- 3C is research, main thread
- 3D + 3E can parallelize after 3B

**Deliverables:**
- 2-3 candidates with CSVs in `data/policies/`
- `policies` table seeded
- Frontend candidate tracker live
- Full narrative engine wired

**Success Criteria:**
- [ ] `GET /v1/policy/policies` returns ≥3 candidates' positions
- [ ] `POST /v1/policy/generate/deliverable` returns HTML with at least 5 indicator citations (not stubs)
- [ ] Evidence cards regenerate per candidate, file count grows from 70 → ≥150

---

### Phase 4 — Pre-grant submission polish (~3h, week of MCF LOI)

**Why:** Final pre-Jun-3 buffer. Wave 4 national + map deferred to post-grant per `adr/004:84-85`.

**Actions:**
1. OpenAPI 3.0 spec — `docs/openapi.yaml`. Hand-written from current handler set; lighter than generated.
2. README badges + screenshots of chat answering grant-relevant prompts
3. `STATUS.md` final update with Phase 1-3 deltas
4. Pre-deploy smoke: 4 chat prompts from Phase 2 + 5 frontend pages all return 200 + grant-reviewer dry-run
5. Tag release `v1.0.0` (only after ALL `go test ./... -short` pass + manual smoke clean)

**Deliverables:**
- `docs/openapi.yaml`
- Updated README + STATUS
- Tagged `v1.0.0`

**Success Criteria:**
- [ ] OpenAPI lints clean (`spectral lint docs/openapi.yaml` or equivalent)
- [ ] All Phase 1-3 endpoints documented
- [ ] Tag pushed to `DojoGenesis/policy-data-infrastructure`

---

### Deferred (post-grant, post-MCF-LOI)

- **Wave 4 national pipeline** (`TODO.md:23`, `adr/004:74-81`) — 50-state run + rate-limit budget plan
- **Map visualization** (`adr/004:80`, ADR-004 Finding 7) — Leaflet choropleth; needs TIGER shapefile load (boundaries are NULL today)
- **Coverage uplift** — `pkg/pipeline/` 30.4% → ≥60%; `pkg/gateway/` 41.2% → ≥70% (`TODO.md:18-19`)
- **Transitive dep audit** — `quic-go`, `mongo-driver` (`TODO.md:17`)
- **GTFS + EPA-TRI mock HTTP tests** (`TODO.md:25`)

---

## Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Phase 0 reveals more drift than 2h budget allows | Med | Med | Time-box; if >3h, cut Phase 4 instead of Phase 0 — reconciled state matters more than polish |
| Gateway lacks HTTP-tool registration → ADR-005 needs MCP server scaffold | Med | High | Phase 2 step 2 includes MCP fallback; dispatch parallel scout while Phase 1 runs |
| MCF LOI Justice review (open in MEMORY.md) eats Phase 3 time | Med | High | Phase 3 candidates + narrative are grant-narrative leverage — protect this phase ahead of polish |
| `indicators_latest` materialized view goes stale silently mid-grant-review | High | High | Phase 1 step 5 makes refresh automatic; verify on VPS within 24h of landing |
| Chat hallucinates statistics during reviewer demo | Med | High | Phase 2 success criterion 1 is mandatory; do not demo until tool-grounded |
| Coverage drop trips CI on parallel agent merge | Med | Low | Each agent runs `go test ./... -short` before reporting done; main thread re-verifies post-integration |
| DNS cutover (`policydatainfrastructure.com` → VPS) creates downtime | Low | Med | Per ADR-004 Finding 5 — port marketing page into binary FIRST; verify locally before DNS flip |

---

## Dispatch Notes

- **Model assignment** (per CLAUDE.md:215-216):
  - Sonnet: Phase 1 (handlers, store methods, CLI subcommands), Phase 3D (evidence_cards.py generalization), Phase 4 (OpenAPI hand-write)
  - Opus: Phase 0 reconciliation (judgment-heavy), Phase 2 (system prompt design, tool grounding architecture), Phase 3A+3F (narrative engine wiring)
- **Verification gate** (CLAUDE.md:225-228): after EVERY phase, run `go build ./...` + `go test ./... -short`; check `git status` matches expected manifest
- **No commits from agents** (CLAUDE.md:319): orchestrator commits after independent verification
- **Push gate** (CLAUDE.md:265-266): `gh auth switch --user DojoGenesis` before any push to remote

---

## Mode Routing Note

Skill router selected **Context Ingestion** (this skill) because:
- The argument was a topic directive (`on policy-data-infra`), not file uploads or a `/spec` / `/synthesize` keyword
- 7 source files were ingested from disk (the live repo state) rather than uploaded — adapted the workflow accordingly
- The 4 detected contradictions made grounding-before-planning the highest-leverage move

If the user wants execution rather than further planning, the next move is `Phase 0` dispatch — a single Opus agent reconciling docs against `git log` and the live VPS in parallel with low-risk cleanups.
