# Contract: The Policy Experiment Page

**Status:** FROZEN — 2026-08-13
**Authority:** ADR-016. Where this file and a prompt disagree, this file wins; a gate can
verify a file and cannot verify a prompt.
**Normative Go source:** `pkg/experiment/types.go`. Where this file and that file disagree
about a field name or type, the Go source wins and this file is the bug.
**Depends on:** ADR-006 (grounded chat), ADR-014 (completeness, run layer), ADR-015 (rollups),
`research/08_mvp_public_groceries_rock_county.md` (tier 1 source of truth).

Every track building this page reads this file first. Nothing here is a suggestion. The
sections marked **OPEN** at the bottom are the only things not settled, and they are flagged
rather than guessed because a wrong freeze is worse than a named gap.

---

## 0. What the page is, in one paragraph

One route, `/policy?id=<policy_id>&geoid=<geoid>`, showing three layers about one policy in one
place. Tier 1 **REACH** measures who the policy's own eligibility rule covers. Tier 2
**EXPERIMENT** lets a reader set assumptions and does arithmetic over real denominators. Tier 3
**EVIDENCE** cites what happened where the policy was actually tried, from external published
sources. The page is bilingual, printable, and has two disclosure registers — a public layer
that expands into an analyst layer.

### The constraint that forecloses the obvious design

A live county's indicators return exactly one vintage per source: `ACS-2024-5yr`, `2022`
(CDC PLACES), `USDA-FARA-2019`. **The data is cross-sectional. There is no time series.**
PDI therefore cannot show that a policy caused an outcome, and any design that implies causal
effect from PDI's own data is wrong and must be refused rather than caveated.

This is enforced structurally, not by copy: `Value` carries a single `vintage` string and there
is no `prior_value`, no `change`, no `pct_change` and no series field anywhere in these
payloads. `schemas/analysis.schema.json` has all four of those, because a county analysis can
span vintages. This page cannot, so it does not get the fields.

---

## 1. Types

The Go declarations in `pkg/experiment/types.go` are normative. The gateway MUST import and
reuse them; it must not redeclare a parallel set in `pkg/gateway/types.go`. Two structs for one
payload is how the three-store split in ADR-014 happened.

### 1.1 Enumerations — every member, frozen

| Go type | Go constant | JSON value | Meaning |
|---|---|---|---|
| `Tier` | `TierReach` | `reach` | Measured coverage of a policy's own eligibility rule |
| | `TierExperiment` | `experiment` | Deterministic arithmetic over user-set assumptions |
| | `TierEvidence` | `evidence` | External published findings, cited, never computed here |
| `Kind` | `KindMeasured` | `measured` | Published figure from a loaded dataset |
| | `KindAssumed` | `assumed` | A number a human chose |
| | `KindDerived` | `derived` | Result of an operation over other values |
| `ClaimClass` | `ClassMeasurement` | `measurement` | Every input measured. May be stated as fact. |
| | `ClassProjection` | `projection` | At least one input assumed. Never a finding. |
| `Register` | `RegPublic` | `public` | Shown without interaction |
| | `RegAnalyst` | `analyst` | Shown on expand; strictly additive |
| `Reliability` | `RelHigh` | `high` | CV ≤ 15% |
| | `RelMedium` | `medium` | 15% < CV ≤ 30% |
| | `RelLow` | `low` | CV > 30% |
| | `RelUnknown` | `unknown` | No published MOE, or estimate is 0 |
| `Unit` | `UnitCount` | `count` | |
| | `UnitPeople` | `people` | |
| | `UnitHouseholds` | `households` | |
| | `UnitUSD` | `usd` | |
| | `UnitUSDPerYear` | `usd_per_year` | |
| | `UnitPercentPoints` | `percent_points` | **0–100** |
| | `UnitShare` | `share` | **0–1** |
| | `UnitMiles` | `miles` | |
| | `UnitDays` | `days` | |
| | `UnitRatio` | `ratio` | |
| `Op` | `OpRef` | `ref` | Named input |
| | `OpConst` | `const` | Literal |
| | `OpAdd` | `add` | Sum of all terms |
| | `OpSub` | `sub` | First term minus every subsequent term |
| | `OpMul` | `mul` | Product of all terms |
| | `OpDiv` | `div` | First term divided by every subsequent term |
| | `OpMin` | `min` | Smallest present term |
| | `OpMax` | `max` | Largest present term |
| `MissingReason` | `MissingNone` | `""` | Value is present |
| | `MissingNotLoaded` | `not_loaded` | No rows for this variable here. A **gap**. |
| | `MissingSuppressed` | `suppressed` | Publisher withheld it. A gap in the source. |
| | `MissingWithheld` | `withheld` | PDI refuses to publish it (ADR-014 D2). A **refusal**. |
| | `MissingBelowCoverage` | `below_coverage_threshold` | Aggregate coverage under threshold |
| | `MissingDivideByZero` | `division_by_zero` | Zero divisor |
| | `MissingNonFinite` | `non_finite` | NaN or ±Inf |
| | `MissingUnresolvedRef` | `unresolved_ref` | Ref names nothing in context |
| | `MissingUpstream` | `upstream_missing` | An input was missing |
| | `MissingNoModel` | `no_model` | Policy has no model file |
| | `MissingGeoLevelUnsupported` | `geo_level_unsupported` | Model does not declare this level |
| `ParamSource` | `SrcUser` | `user` | Reader set it |
| | `SrcDefault` | `default` | Omitted; model's authored default used |
| `GeoSource` | `GeoRequested` | `requested` | Request named it |
| | `GeoModelDefault` | `model_default` | Model's `default_geoid` |
| | `GeoStateFallback` | `state_fallback` | Statewide `"55"` |
| `EvidenceDirection` | — | `increase` `decrease` `no_change` `mixed` `not_reported` | What the source reported |
| `ClaimCheck` | — | `supports` `complicates` `contradicts` `not_applicable` | How it bears on the candidate's claim |

`MissingNotLoaded` vs `MissingWithheld` is load-bearing and must not be collapsed. ADR-014 D2
requires the API distinguish "no data loaded" from "cannot be computed defensibly." A reader who
cannot tell a gap from a refusal learns the wrong lesson from both.

### 1.2 `Expr` and `Term`

```
Expr {
  op            Op        `json:"op"`
  ref           string    `json:"ref,omitempty"`          // op=ref
  const         *float64  `json:"const,omitempty"`        // op=const
  const_kind    Kind      `json:"const_kind,omitempty"`   // measured|assumed, default assumed
  const_source  string    `json:"const_source,omitempty"` // required iff const_kind=measured
  terms         []Term    `json:"terms,omitempty"`        // every op except ref/const
}

Term {
  expr       Expr    `json:"expr"`
  label_key  string  `json:"label_key,omitempty"`
}
```

**The ref namespace is frozen.** Two tracks will otherwise each invent one.

| Prefix | Resolves to | Kind |
|---|---|---|
| `param:<param_id>` | A reader-set assumption | `assumed` |
| `indicator:<variable_id>` | A loaded indicator at the requested geography | `measured` |
| `reach:<field_id>` | A tier-1 measured field of this brief | `measured` |
| `output:<output_id>` | An output declared **earlier** in the same model | inherits |

Forward references and cycles are refused at model **load**, not resolved at evaluation.

`Term` exists so the analyst register can render the arithmetic as a sentence a reader can check
by hand. An unlabeled expression tree renders as a number with no derivation, and the derivation
is the whole argument for trusting a projection.

### 1.3 `Param`, `Output`, `PolicyModel`

Field-for-field in `pkg/experiment/types.go` and `schemas/policy-model.schema.json`. The rules
that are not obvious from the field list:

- **`min`/`max` are rejection bounds, not clamps.** A submitted value outside them is HTTP 400.
  Clamping substitutes a different assumption than the reader made and then attributes the
  result to them.
- **`default` is required and `default_source_key` is required with it.** A default with no
  stated basis is a recommendation the page is making without admitting it.
- **`precision` is 0–2, hard.** `pkg/grounding.Verify` registers result values at 0, 1 and 2
  decimals (`verify.go:58–69`). A figure rendered at 3 decimals would be rejected as
  unsupported in generated prose that quoted the page *correctly*.
- **`unit` is declared on every output, never inferred.** The evaluator does no unit algebra.
  A declared unit that disagrees with the arithmetic is a model-lint failure at author time,
  not a runtime coercion.
- **`refusal_keys` is required and non-empty on every model.**

### 1.4 `Value` — and where `claim_class` comes from

`Value` has **no `claim_class` field**. There is nothing to set wrongly.

```
Value {
  id               string          `json:"id"`
  label_key        string          `json:"label_key"`
  value            *float64        `json:"value"`            // nil ⟺ missing
  unit             Unit            `json:"unit"`
  precision        int             `json:"precision"`
  kind             Kind            `json:"kind"`
  tier             Tier            `json:"tier"`
  register         Register        `json:"register"`
  missing_reason   MissingReason   `json:"missing_reason"`
  missing_detail   string          `json:"missing_detail,omitempty"`
  assumed_inputs   []string        `json:"assumed_inputs"`    // sorted, deduped
  measured_inputs  []string        `json:"measured_inputs"`
  vintage          string          `json:"vintage,omitempty"`
  moe              *float64        `json:"moe,omitempty"`
  reliability      Reliability     `json:"reliability,omitempty"`
  source_ids       []string        `json:"source_ids,omitempty"`
  // claim_class is EMITTED on the wire, COMPUTED from assumed_inputs.
}
```

Two invariants, both checkable in one line by anybody:

```
claim_class == "projection"   ⟺   len(assumed_inputs) > 0
value == null                 ⟺   missing_reason != ""
```

`Value.MarshalJSON` emits the computed class; `Value.UnmarshalJSON` **errors** on a payload that
violates either. Verified 2026-08-13 against a lying payload (rejected), an honest payload
(accepted), and a zero-with-a-missing-reason payload (rejected).

`moe` is present on measured leaves only and is **nil on every value with an assumed input**.
A margin of error describes sampling error in an estimate of something that exists. Multiplied
by an assumption it describes the sampling error of a hypothetical, which is not a quantity.
Publishing an interval around a projection dresses the least certain number on the page as the
best characterized one. **Do not propagate MOE through tier 2. Ever.**

### 1.5 `EvalResult`

```
EvalResult {
  policy_id      string          claim_class   ClaimClass  (computed, emitted)
  model_version  string          params        []ParamBinding
  geoid          string          inputs        []Value
  level          string          outputs       []Value
  geo_name       string          steps         []Step
  computed_at    time.Time       absences      []Absence
                                 refusal_keys  []string
                                 caveat_keys   []string
                                 sources       []SourceRef
}
```

`EvalResult.ClaimClass()` is `Join` over every output's class — a projection if any output is
one. Same enforcement as `Value`: computed, emitted, and rejected on unmarshal if a payload
disagrees.

`computed_at` is **not a vintage** and must never be rendered as one. It says when arithmetic
happened, not when the world was measured.

---

## 2. The taint algorithm

Precise enough that two people implement it identically. `resolve(node, ctx) -> Value`.

```
LEAF — op = ref
  target := ctx.lookup(node.ref)
  if target does not exist:
      return { value: nil, missing_reason: unresolved_ref, kind: derived,
               assumed_inputs: [], measured_inputs: [] }
  switch namespace(node.ref):
    "param"                       kind := assumed
                                  assumed_inputs  := [param_id]
                                  measured_inputs := []
    "indicator", "reach"          kind := measured
                                  assumed_inputs  := []
                                  measured_inputs := [variable_id]
                                  moe, vintage, reliability := from the source row
    "output"                      return the already-resolved Value verbatim,
                                  inheriting its input lists unchanged
  if target has no value:
      return { value: nil, missing_reason: target.reason, kind, inputs as above }
  return { value: target.value, kind, inputs as above }

LEAF — op = const
  kind := node.const_kind, defaulting to "assumed" when absent
  if kind == "measured" and node.const_source == "":  MODEL LINT ERROR at load
  assumed_inputs  := kind == "assumed" ? ["const:<output_id>:<path>"] : []
  measured_inputs := kind == "measured" ? ["const:<output_id>:<path>"] : []
  return { value: node.const, kind, inputs as above }

NODE — add | sub | mul | div | min | max
  children := [resolve(t.expr, ctx) for t in node.terms]
  if len(children) == 0:                       MODEL LINT ERROR at load

  assumed_inputs  := sorted(dedupe(union of every child's assumed_inputs))
  measured_inputs := sorted(dedupe(union of every child's measured_inputs))
  kind := derived

  # MISSING dominates. It is checked BEFORE the arithmetic, so no operation
  # ever runs on a substituted zero.
  first := the first child (in term order) whose value is nil
  if first exists:
      return { value: nil, missing_reason: upstream_missing,
               missing_detail: first.id + ":" + string(first.missing_reason),
               kind, assumed_inputs, measured_inputs }

  if op == div and any divisor (children[1:]) == 0:
      return { value: nil, missing_reason: division_by_zero, kind, inputs }

  n := apply(op, child values)
  if isNaN(n) or isInf(n):
      return { value: nil, missing_reason: non_finite, kind, inputs }

  # vintage: carried only when every measured leaf agrees; otherwise "" and the
  # disagreement is reported in EvalResult.sources, never resolved by picking one.
  # moe: ALWAYS nil on a derived value that has any assumed input.
  return { value: n, kind, assumed_inputs, measured_inputs, vintage, moe }

CLASS — never stored, always derived
  claim_class(v) := len(v.assumed_inputs) > 0 ? projection : measurement
```

**Why the taint is total.** The operation set is closed at eight members. Every member is either
a leaf that declares its own inputs or a node that takes the union of its children's. There is
no operation that discards an input list, so there is no path by which a value acquires a number
without acquiring its provenance alongside it. Adding a ninth operation requires touching this
algorithm, which is the review point.

**Why it cannot be dropped in transit.** Because `claim_class` is a function of a field the
payload also carries, and both the server's marshaller and any client's unmarshaller recompute
it. Faking it means faking `assumed_inputs`, and `assumed_inputs` is checkable against the model
file that produced the value.

**Rounding.** Round for display only, at `precision`, at the last step. Never round an
intermediate — the arithmetic trace in the analyst register must reproduce the output exactly,
and a reader who redoes the multiplication and gets a different answer has caught the page
lying whether or not it was.

---

## 3. `GET /v1/policy/briefs/:policy_id`

Read-only, no token, cache-friendly. Query: `?geoid=<geoid>&level=<state|county|tract>`.
Both optional; see S4.

**Statuses:** `200` on success. `404` when `policy_id` is not in `data/policies/*.csv`.
`400` on a malformed geoid (must match `^\d{2}$|^\d{5}$|^\d{11}$`). `422` when the geoid is
well-formed but not in `geographies`. A policy that exists with no model and no evidence is a
**200 with absences**, not a 404 — the absence is the content.

### Response

```jsonc
{
  "policy": {
    "policy_id": "FH-TRANSIT-008",
    "title": "Public groceries",                    // verbatim CSV, English-authored
    "description": "Enable publicly-owned grocery stores in food deserts",
    "claim_empirical": "",                          // verbatim CSV; "" on 54 of 70 Hong positions
    "candidate": "Francesca Hong",                  // attribution register, not the headline
    "office": "Governor",
    "bill_references": "AB1221 (2025)",
    "category": "Cities Towns Transit"
  },
  "geography": {
    "geoid": "55105", "name": "Rock County",
    "level": "county", "source": "requested"        // requested | model_default | state_fallback
  },
  "reach": {                                         // null only when no rule is expressible
    "rule": {
      "rule_id": "usda_fara_lila",
      "authority_key": "pex.rule.usda_fara_lila.authority",
      "statement_key": "pex.rule.usda_fara_lila.statement",
      "vintage": "USDA-FARA-2019",
      "geo_vintage": "2010-tract",
      "source_ids": ["usda-food-access"]
    },
    "totals": [ Value, ... ],                        // tier=reach, kind=measured
    "areas": [ { "geoid": "...", "name": "...", "level": "tract",
                 "designated_share": 0.0-1.0|null, "values": [Value,...] } ],
    "designated_ids": ["55105000100", "..."],        // authority's OWN geo vintage
    "map_layer_key": "pex.map.designated_share.legend",
    "sources": [ SourceRef, ... ]
  },
  "model": {                                         // null when no model file
    "model_version": "1",
    "title_key": "...", "summary_key": "...",
    "geo_levels": ["county"],
    "params":  [ Param,  ... ],                      // full declarations, for the controls
    "outputs": [ Output-without-expr, ... ],         // id/label/unit/precision/register ONLY
    "refusal_keys": [...], "caveat_keys": [...]
  },
  "evidence": [ EvidenceItem, ... ],                 // [] when no evidence file
  "absences": [ { "tier": "experiment", "reason": "no_model",
                  "reason_key": "pex.absence.no_model", "detail": "" } ],
  "refusal_keys": ["pex.refusal.no_causal_claim", "pex.refusal.no_time_series", "..."],
  "sources": [ SourceRef, ... ],
  "generated_at": "2026-08-13T00:00:00Z"
}
```

**`model.outputs` carries no `expr`.** The expression tree is a model-authoring artifact.
Shipping it to the browser invites a second, divergent evaluator written in JavaScript, and then
two answers to the same question — the exact failure ADR-014 D6 removed at the SQL layer.

**Aggregations route through the run layer.** Any tier-1 total that is a tract→county rollup is
obtained via `POST /v1/policy/analyses` (`tract_rollup`), so coverage thresholds, bootstrap CI,
and D2 withholding apply for free. This endpoint does not reinvent statistics; a rollup that is
withheld arrives here as `value: null, missing_reason: "below_coverage_threshold"`.

---

## 4. `POST /v1/policy/simulate`

A pure function. No database write, no run row, no cache row, no budget reservation, **no run
token** — it is arithmetic over numbers the brief already published, and gating it behind the
`POST /analyses` token would gate the page's whole interactive layer behind a secret. It is
rate-limited by `CF-Connecting-IP` on the same mechanism as the other read endpoints.

Determinism is a requirement, not a property: the same
`(policy_id, model_version, geoid, level, canonical params)` must always produce byte-identical
outputs. Canonical params = every declared param, in declaration order, each snapped to its
`step` grid and rendered at its `precision`. That tuple is also the `ETag`.

### Request

```json
{
  "policy_id": "FH-TRANSIT-008",
  "geoid": "55105",
  "level": "county",
  "model_version": "1",
  "params": { "weekly_spend": 60 }
}
```

`level` empty → the model's first declared level. `model_version` empty → the loaded version;
the response always echoes what was actually used. Omitted params use their authored default and
come back with `"source": "default"`. Partial submissions are legal.

**Statuses:** `200` on success. `404` unknown `policy_id`. `422` + `{"missing_reason":
"no_model"}` when the policy exists with no model. `422` + `geo_level_unsupported` when the model
does not declare `level`. `400` when a param is unknown, non-numeric, or outside `[min, max]` —
**never a clamp, never a silent drop**. `422` unknown geoid.

### Response — `EvalResult`

```jsonc
{
  "policy_id": "FH-TRANSIT-008", "model_version": "1",
  "geoid": "55105", "level": "county", "geo_name": "Rock County",
  "claim_class": "projection",                       // computed from outputs
  "params": [ { "id": "weekly_spend", "label_key": "...", "value": 60,
                "unit": "usd", "precision": 0, "source": "user", "linked": false } ],
  "inputs":  [ Value, ... ],                         // measured leaves, analyst register
  "outputs": [ Value, ... ],                         // declaration order
  "steps":   [ { "output_id": "...", "label_key": "...", "op": "mul",
                 "operands": [Value,...], "result": Value } ],
  "absences": [], "refusal_keys": [...], "caveat_keys": [...],
  "sources": [ SourceRef, ... ],
  "computed_at": "2026-08-13T00:00:00Z"
}
```

---

## 5. Tier 3 — `data/policies/evidence/<policy_id>.csv`

One file per policy. Absent file = no tier 3, which is the state of all 70 policies today.
**PDI never computes a row in this file.** The loader refuses a row with an empty `source_url`,
`citation`, or `transfer_note`.

Column order is frozen. All columns are required non-empty except `period_end`,
`effect_reported`, and `relates_to_claim`.

| # | Column | Values / rule |
|---|---|---|
| 1 | `policy_id` | Must match the filename stem and a CSV policy record |
| 2 | `evidence_id` | Stable slug, unique within the file, `^[a-z0-9_]+$` |
| 3 | `jurisdiction` | Where it was actually tried, as the source names it |
| 4 | `geo_scale` | `city` \| `county` \| `state` \| `national` \| `other` |
| 5 | `period_start` | `YYYY` or `YYYY-MM` |
| 6 | `period_end` | `YYYY`, `YYYY-MM`, or `ongoing`. May be empty. |
| 7 | `intervention` | What was done — not what was proposed |
| 8 | `outcome` | What the source measured |
| 9 | `direction` | `increase` \| `decrease` \| `no_change` \| `mixed` \| `not_reported` |
| 10 | `finding` | One sentence, faithful to the source. Not PDI's interpretation. |
| 11 | `effect_reported` | The number or range **as published**, free text. May be empty. |
| 12 | `study_design` | `rct` \| `quasi_experimental` \| `pre_post` \| `descriptive` \| `case_study` \| `review` \| `program_report` |
| 13 | `evidence_strength` | `strong` \| `moderate` \| `limited` \| `contested` — authored, requires a reviewer |
| 14 | `source_title` | |
| 15 | `source_org` | |
| 16 | `source_url` | Required. A finding with nowhere to check it does not ship. |
| 17 | `source_year` | `YYYY` |
| 18 | `accessed_date` | `YYYY-MM-DD` |
| 19 | `citation` | Full text citation, as it would appear in a bibliography |
| 20 | `relates_to_claim` | Empty, or `candidate_claim` when it bears on the policy's `claims_empirical` |
| 21 | `claim_check` | `supports` \| `complicates` \| `contradicts` \| `not_applicable` |
| 22 | `transfer_note` | **Required.** Why this may or may not carry to Wisconsin. |
| 23 | `reviewed_by` | |
| 24 | `reviewed_date` | `YYYY-MM-DD` |

`transfer_note` is required because an uncontextualized result from another jurisdiction is the
most persuasive wrong thing this page could show. Nothing else on the page is as easy to
screenshot or as easy to misread.

Evidence text is **never translated by PDI** (see S10).

`claims_empirical` is a real unused asset: it is plumbed end-to-end (`pkg/policy/policy.go:21`,
store, API) and rendered in zero frontend files. Verified 2026-08-13: **16 of 70** Hong positions
carry one — `FH-EDU-001..005, FH-WRK-002, FH-HC-001, FH-HC-002, FH-TAX-001, FH-CARE-002,
FH-ENV-002, FH-LABOR-002, FH-CJ-002, FH-CJ-005, FH-GUN-001, FH-DISABILITY-002`. Re-derive with:

```bash
python3 -c "import csv;r=list(csv.DictReader(open('data/policies/francesca_hong_2026.csv')));print(len(r),sum(1 for x in r if x['claims_empirical'].strip()))"
```

**The pilot policy is not one of them.** `FH-TRANSIT-008` has an empty `claims_empirical`, so the
claim-check lane renders as a stated absence on the pilot. See OPEN-3.

---

## 6. Register split

Every payload field is `public`, `analyst`, or `both`. The analyst register is **strictly
additive**: expanding it never changes a number that was already on screen. A page where
expanding changes the answer was lying in one of the two states.

| Field | Register | Note |
|---|---|---|
| `policy.title`, `policy.description` | both | |
| `policy.candidate`, `.office`, `.bill_references` | both | Attribution block, subordinate (S8) |
| `policy.claim_empirical` | both | Rendered as the candidate's claim, attributed |
| `geography.name`, `.level` | both | |
| `geography.geoid` | analyst | |
| `geography.source` | both | Public shows it as a sentence when ≠ `requested` |
| `reach.rule.statement_key` | both | The eligibility test in plain words |
| `reach.rule.vintage`, `.geo_vintage`, `.source_ids` | analyst | |
| `reach.totals[].value`, `.unit`, `.label_key` | both | |
| `reach.totals[].reliability` | public | As a **phrase** (S7) |
| `reach.totals[].moe` | analyst | The number |
| `reach.totals[].vintage`, `.source_ids`, `.measured_inputs` | analyst | |
| `reach.totals[].missing_reason` | both | Public as a sentence, analyst as the code |
| `reach.areas[]` (the per-area table) | analyst | Public shows the map + totals |
| `reach.designated_ids` | analyst | |
| `reach.map_layer_key` | both | **Verbatim in both registers** (S6) |
| `model.params[]` (all fields) | both | The controls are the point |
| `model.params[].default_source_key` | public | Shown next to the control |
| `outputs[].value`, `.unit`, `.label_key` | both | Public shows `register: "public"` outputs only |
| `outputs[].claim_class` | both | Public as the projection banner; analyst as the token |
| `outputs[].assumed_inputs`, `.measured_inputs` | analyst | |
| `steps[]` (arithmetic trace) | analyst | |
| `inputs[]` (resolved measured leaves) | analyst | |
| `evidence[]` cards | both | |
| `evidence[].study_design`, `.evidence_strength`, `.citation` | analyst | |
| `evidence[].source_url`, `.source_org`, `.transfer_note` | both | |
| `absences[]` | both | Public as a sentence, analyst with `reason` + `detail` |
| `refusal_keys`, `caveat_keys` | both | |
| `sources[]` provenance block | both | Prints in both registers (S12) |
| `computed_at`, `generated_at` | analyst | Never rendered as a vintage |

---

## 7. Settled seam decisions

Each is a decision two tracks would otherwise each make differently, with the reason in one line.

**S1 — Route.** `/policy?id=<policy_id>&geoid=<geoid>`. Extensionless; `.html` 404s. No
`/experiment` route and no alias. *Reason: `research/08` already specs this form as the tier-1
source of truth, and two routes to one page is two pages to keep in sync.*

**S2 — `/policy` with no `?id=`.** The page renders a policy chooser from
`GET /v1/policy/policies`; the brief endpoint itself is not called. *Reason: a 400 on the page's
own bare URL is a dead end for the most likely first visit.*

**S3 — Panels with no data: hide the apparatus, never hide the absence.** When a tier has no
data, its interactive apparatus is **not rendered**, and an entry appears in `absences[]` which
the page renders as one sentence in a standing "What is not here" block. *Reason: an empty
interactive panel on 70 policies is noise, and a silently omitted one erases PDI's refusal — the
most defensible thing on the page. One mechanism, one rule, both failure modes closed.* A brief
with `model: null` and no matching `absences` entry is **malformed**.

**S4 — Default geography.** Absent `?geoid=` → the model's `default_geoid`; absent that →
`"55"` (Wisconsin, state level). The response always reports `geography.source`, and the page
states it in words whenever it is not `requested`. *Reason: a reader who believes they are
looking at their own county and is not has been misinformed by a default.*

**S5 — Public register shows a phrase; analyst adds the numbers.** Public gets a plain-language
reliability phrase and never an MOE or a CI. Analyst adds `moe`, the interval, `n`, coverage,
denominator, vintage, source, and the arithmetic trace. *Reason: an MOE that a reader cannot
convert into a decision is decoration, and decoration next to a number reads as precision.*

**S6 — The crosswalked-share wording is verbatim and identical in both registers and both
languages.** `research/08` §4: the USDA designation is a 2010-tract fact and the people-data is
2020 tracts, so the crosswalk yields a **0–1 population share**, not a flag, and the repo holds
no 2010 tract geometry.

> EN: *"Share of this tract's population living in an area USDA designated low-income and
> low-access"*
> ES: *"Porcentaje de la población de este sector censal que vive en una zona designada por el
> USDA como de bajos ingresos y bajo acceso"*

The page **never** prints "your tract is a food desert" from a crosswalked share, in either
language, in either register, in the map legend, in a tooltip, or in generated prose.
The 12 designated tracts are listed separately at their native 2010 vintage and labeled as such.

**S7 — Reliability phrases are frozen.** Bands are the Census Bureau's own CV convention;
`CV = (MOE / 1.645) / |estimate| × 100`.

| Band | EN | ES |
|---|---|---|
| `high` | This figure is well measured. | Esta cifra está bien medida. |
| `medium` | This figure is approximate. | Esta cifra es aproximada. |
| `low` | This figure is uncertain — read it as a rough order of size. | Esta cifra es incierta — tómala como un orden de magnitud. |
| `unknown` | This figure has no published margin of error. | Esta cifra no tiene margen de error publicado. |

*Reason: a threshold PDI invented would have to be defended; this one already is.* `unknown` is
deliberately distinct from `high` — not knowing the error is not the same as knowing it is small.

**S8 — The candidate's name is subordinate, not the headline. Civic framing.** `h1` is the
policy and the place. Candidate, office and bill appear in an attribution line inside the
provenance block. *Reason: PDI is grant-facing (Arnold Ventures, MCF), the platform already
carries two candidates' policy sets (Hong 70, Mamdani 15), and a measurement page that leads
with a candidate reads as advocacy — which undermines the exact defensibility the refusals exist
to establish.* Overridable by the operator; see OPEN-2.

**S9 — The user-facing word is "explore", never "experiment", in EN or ES.** The architectural
name stays `experiment` (this contract, ADR-016, `pkg/experiment`, `TierExperiment`,
`refusal_keys`). Frozen user-facing verbs:

| Slot | EN | ES |
|---|---|---|
| Panel heading | Try your own numbers | Prueba tus propios números |
| Action | Recalculate | Recalcular |
| Reset | Back to the starting numbers | Volver a los números iniciales |

*Reason: "experimento" in Spanish reads as a trial with subjects and a causal test, which is
precisely what PDI cannot run; and "experiment" in English invites the reader to take the output
as a result.* **Do not "fix" the mismatch between the package name and the copy — it is the
decision.**

**S10 — The server never sends display prose. Keys only.** Every translatable string in every
payload is an i18n key resolved client-side. Three exceptions, each explicit:

1. **Proper nouns** — place names, source names, candidate name, publisher names. Not translated.
2. **Verbatim CSV policy text** (`title`, `description`, `claim_empirical`) — English-authored.
   Rendered with `lang="en"` and, in ES, preceded by the standing note
   `pex.note.original_english`. A per-policy override may supply a translated title at
   `pex.policy.<policy_id>.title`; the pilot ships one.
3. **Evidence text** (tier 3) — a quotation of somebody else's published work. Rendered with the
   source's own `lang`; PDI translating a finding would make PDI the author of a claim it is
   explicitly not making. The scaffolding around the card is translated; the card is not.

*Reason: a server-supplied English string is invisible to the `data-en`/`data-es` swap layer and
ships an untranslated page that passes every backend test.*

**S11 — No Alpine `x-text` on any translatable string on this page.** Static text uses
`data-en`/`data-es` attributes. Dynamic text is written by a render function that resolves keys
through `window.LangToggle.t()` and re-runs on the `tp:langchange` event. *Reason: the known ES
parity gap — Alpine bindings bypass the swap layer entirely, so an ES page renders correct
numbers with English labels and looks fine to anyone testing in English.*

**S12 — Print renders the public register plus refusals plus the full provenance block.** The
analyst register prints only if it was expanded at print time. *Reason: the artifact's venue is a
folding table at a community meeting, and a number that travels without its footnote is how a
figure ends up in a grant application without its caveat.*

**S13 — Comparison mode `?id=A,B` shares assumptions by `(id, unit)` and nothing else.** Params
whose `id` **and** `unit` match across both models are driven by one control and marked
`"linked": true` by the server. Everything else is per-policy. *Reason: a comparison whose two
sides use different assumptions is not a comparison, and forcing a shared value onto a param the
other model does not have is nonsense.*

**S14 — Comparison never subtracts one policy's outputs from another's.** No difference row, no
"A reaches X more than B", no ranking. Each policy's projections are shown side by side under a
shared assumption set. *Reason: a difference between two projections built on different models
reads as a finding about which policy is better, and PDI has no basis for that claim.*
Tier 3 evidence is listed per policy and never merged.

**S15 — Titles and headings, frozen, both languages.**

| Slot | EN | ES |
|---|---|---|
| `<title>` | `{policy title} — {place} · Policy Data Infrastructure` | `{título} — {lugar} · Policy Data Infrastructure` |
| `h1` | `{policy title} — {place}` | `{título} — {lugar}` |
| Tier 1 `h2` | Does this reach my block? | ¿Esto llega a mi cuadra? |
| Tier 2 `h2` | Try your own numbers | Prueba tus propios números |
| Tier 3 `h2` | Where this has been tried | Dónde se ha intentado esto |
| Refusals `h2` | What we cannot tell you | Lo que no podemos decirte |
| Provenance `h2` | Where every number came from | De dónde viene cada número |
| Chooser `h1` | Policy positions | Posiciones de política |

**S16 — Number formatting is server-declared, client-applied.** The server sends the raw
`value` and a `precision`; the client formats with `Intl.NumberFormat` using locale `en-US` for
EN and `es-US` for ES. The client never chooses a precision. *Reason: two tracks rounding
independently produce two different numbers for the same field, and generated prose then fails
`grounding.Verify` against the page a reader is looking at.*

**S17 — Percent encoding.** `percent_points` is 0–100 and `share` is 0–1, always, on the wire
and in every model file. No field is ever "sometimes a fraction". *Reason: an off-by-exactly-100
error is invisible in a payload; this class has already cost two agents a day on a spreadsheet.*

**S18 — Share links carry assumptions in the query string.** `p.<param_id>=<value>`, one
parameter each, values snapped to `step`. Plus `mv=<model_version>`. *Reason: a link whose
model version is implicit renders different numbers after a model revision and nobody knows
when it changed.*

**S19 — The AI surface is one action, over an already-computed payload.** "Explain this in plain
language" runs the ADR-006 pipeline: the model may plan and may write prose, and may never be
the source of a number. `Verify` runs against the **number ledger** — defined as the union of
every `Value.value` in the payload and every `ParamBinding.value` — and a draft with an
unsupported figure is refused, falling back to the deterministic `facts.go`-style rendering.
The model additionally may not: choose or suggest a param value, produce a sentence with a
causal verb about PDI's own data, or introduce a jurisdiction not present in `evidence[]`.
*Reason: the floor must exist before the model is asked; a feature whose only output path runs
through a model has no floor.*

**S20 — `POST /v1/policy/simulate` needs no run token; `POST /v1/policy/analyses` still does.**
See §4. Aggregations go through the run layer; arithmetic does not.

**S21 — The eligibility rule is attributed to whoever actually defined it.** Where a bill's
eligibility language differs from the dataset PDI holds, the page presents USDA's test **as
USDA's**, never as the bill's. `EligibilityRule.authority_key` is required for this reason.
*Reason: `research/08` §11 — AB1221's text is unverified, and designing around a CSV summary is
how a page ends up attributing a federal agency's definition to a candidate.* See OPEN-1.

---

## 8. i18n key convention

Namespace: **`pex.`** (policy experiment). Grep-safe, and no existing key collides.

```
pex.<section>.<element>[.<variant>]
```

| Pattern | Example |
|---|---|
| Page chrome | `pex.title`, `pex.h1`, `pex.nav.back` |
| Tier headings | `pex.reach.question`, `pex.experiment.heading`, `pex.evidence.heading` |
| Rule text | `pex.rule.<rule_id>.authority`, `pex.rule.<rule_id>.statement` |
| Map legend | `pex.map.<layer_id>.legend` |
| Params | `pex.param.<param_id>.label` / `.help` / `.default_source` |
| Outputs | `pex.output.<output_id>.label` |
| Terms | `pex.term.<term_id>` |
| Models | `pex.model.<policy_id>.title` / `.summary` |
| Refusals | `pex.refusal.<slug>` |
| Caveats | `pex.caveat.<slug>` |
| Absences | `pex.absence.<missing_reason>` |
| Missing values | `pex.missing.<missing_reason>` |
| Reliability | `pex.reliability.<band>` |
| Policy overrides | `pex.policy.<policy_id>.title` |
| Standing notes | `pex.note.original_english` |

Rules: lowercase `[a-z0-9_.]` only; a key never contains a number that is part of a claim; every
key resolves in **both** `en` and `es` or the ES parity gate fails. The key set is validated
against `schemas/policy-model.schema.json`'s `i18n_key` pattern
(`^pex\.[a-z0-9_]+(\.[a-z0-9_]+)*$`), so a model file cannot introduce an off-namespace key.

Catalog: extend the `T` map in `cmd/pdi/frontend/lang-toggle.js` (the existing
`window.LangToggle.t()` mechanism) rather than introducing a second i18n system.

---

## 9. File ownership map

| Path | Contains | Absent means |
|---|---|---|
| `data/policies/models/<policy_id>.json` | Tier 2 model | No tier 2 for that policy |
| `data/policies/evidence/<policy_id>.csv` | Tier 3 evidence | No tier 3 for that policy |
| `data/policies/*.csv` | The policy records themselves | Policy does not exist → 404 |
| `schemas/policy-model.schema.json` | Model file schema | — |
| `pkg/experiment/types.go` | Normative Go types | — |

---

## 10. Verification these tracks owe

Not self-certification. Each is a command someone else can run.

1. `go build ./...` and `go test ./... -short`.
2. **Rebuild the binary before believing any frontend change.** `//go:embed` freezes HTML/JS at
   compile time; a running `pdi serve` serves whatever it was built with.
3. Load `/policy?id=FH-TRANSIT-008&geoid=55105` in a browser and look at it. A green suite, a
   200 from the API, and correct SQL have all coexisted here with a page that rendered nothing.
4. **Every check needs a known-bad control.** If the page renders the same with the data source
   pointed at nothing, the check proved nothing. The taint checks in §1.4 were verified this way.
5. Measure the interactive page **after** moving a slider. An empty default state has no
   overflow and passes every audit while being unusable the moment anyone uses it.
6. 375 / 768 / 1280, both themes, both languages. Spanish runs 15–20% longer.
7. Verify `/policy` itself, never the shared CSS layer — every page carries its own `<style>`
   block loading after `styles.css` at equal specificity.

---

## OPEN — operator must decide

These are not settled and must not be guessed at by any track.

**OPEN-1 — AB1221's actual eligibility language is unverified.** `research/08` §7 made this
Monday task 1 and it was never done. Until it is, the page presents USDA's LILA test attributed
to USDA (S21). If the bill defines eligibility differently, tier 1's rule statement changes and
the pilot's `EligibilityRule` needs a second entry. *Blocks: nothing. Changes: `reach.rule`.*

**OPEN-2 — Civic vs campaign framing (S8).** I froze civic/subordinate on the reasoning above.
This is a positioning call, not a technical one, and it forks every copy string in both
languages. If the operator wants campaign framing, S8 and S15 are what change.

**OPEN-3 — The pilot policy has no empirical claim.** `FH-TRANSIT-008`'s `claims_empirical` is
empty (verified; command in §5), so the claim-check lane renders as a stated absence on the one
policy being built. If the operator wants that lane exercised at launch, a second policy from
the 16 that carry one must be picked. `FH-EDU-001` (Free school meals, `food_access`, same
equity dimension) is the natural candidate.

**OPEN-4 — Who authors and reviews tier-3 evidence.** Zero evidence CSVs exist. `reviewed_by`
and `evidence_strength` are human judgments the schema requires and no track can supply. Without
an answer, tier 3 ships empty on every policy and the page is two tiers, not three.

**OPEN-5 — Coverage threshold value.** ADR-014 open question 1 is still open. This contract uses
ADR-015's recorded default of `0.8` wherever a rollup is requested. It needs a defensible basis,
not a round number.

**OPEN-6 — Whether params should have defaults at all.** I froze "required default + required
`default_source_key`" (S/§1.3) because a panel that opens blank cannot be tested, screenshotted,
or printed. The operator may prefer the panel open un-run with every output showing `—` until
the reader moves something, which is the strictest reading of "the model never picks an
assumption." That is a one-field change (`default` becomes optional) plus a page state.

**OPEN-7 — Spanish translation of policy titles.** 70 Hong + 15 Mamdani titles are
English-authored CSV text. S10 exception 2 covers the fallback, but who translates them, and
whether a machine translation is acceptable on a page whose entire argument is provenance, is
not mine to decide.

**OPEN-8 — Does comparison mode (`?id=A,B`) ship in v1?** S13 and S14 settle *how* it behaves so
nothing is invented later. Whether it is in scope for the first build is a scheduling decision;
`research/08` scoped one policy, one county, one week.

**OPEN-9 — Rate-limit budget for `/simulate`.** S20 says no run token and IP-based limiting.
The specific ceiling is not set. `chatbudget.go`'s reserve-then-settle shape is the precedent,
but simulate costs no money — only CPU — so the $/day model does not transfer directly.
