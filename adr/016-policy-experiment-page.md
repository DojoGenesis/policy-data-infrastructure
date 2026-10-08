# ADR-016: The Policy Experiment Page — Three Tiers, Two Registers, One Taint Rule

**Status:** Proposed
**Date:** 2026-08-13
**Deciders:** Cruz Morales
**Depends on:** ADR-006 (grounded chat / structured query), ADR-014 (data completeness and
user-run analyses), ADR-015 (county factor rollup)
**Extends:** ADR-011 (experience redesign) — adds a destination its IA does not contain
**Narrows:** ADR-013 (visual design) — see D11
**Frozen contract:** `docs/contracts/experiment-contract.md`

## Context

### What PDI was built to be, and what it is becoming

PDI was assembled as an offering to the Francesca Hong campaign: 70 policy positions, an
evidence card for each, a county explorer, a map, a chat. The operator's decision on 2026-08-13
is to converge that surface area into one page that demonstrates the outcomes of an individual
policy — with comparison between policies later, and AI integrated. Maximal scope: all three
tiers, and both audiences layered, a public-facing top layer that expands into an analyst layer
on demand.

The interesting problem is not building it. It is that the most natural version of it would be
wrong, and would look right.

### The constraint that forecloses the natural design

Verified against the live API on 2026-08-13: a live county's indicators return exactly one
vintage per source — `ACS-2024-5yr`, `2022` (CDC PLACES), `USDA-FARA-2019`.

**The data is cross-sectional. There is no time series anywhere in it.**

So there is no before and after, no counterfactual, and no comparison group. PDI cannot show
that a policy caused an outcome, and it cannot show that a policy would. Every design that
implies causal effect from PDI's own data is not a design that needs a caveat; it is a design
that has to be refused.

This forecloses, permanently and by construction:

- "A store here would cut food insecurity by X%" — no counterfactual exists.
- "This policy would help/harm you" — an assessment, not a measurement.
- Any trend, change, or percent-change field on this page's payloads.
- Siting simulation — PDI holds access *flags* at 1 and 10 miles, not distances or store
  locations.
- Any binary "you live in a food desert" derived from a crosswalked share (D6).

What it does not foreclose is the thing worth building: measuring who a policy's own rule
covers, letting a reader do arithmetic with their own assumptions over real denominators, and
citing what happened where the policy was actually tried.

### Why a caveat is not enough

ADR-014 D2 already settled the general form of this argument, and it applies here with more
force: *a caveat is read by roughly nobody and survives no copy-paste. The number, once
rendered, travels without its footnote — into a grant application, a briefing, a screenshot.*

This repository has been bitten by the specific failure four times that ADR-014 counted, and
twice more since: a spike that self-certified "✓ COMPLETE / Real Data" whose fabricated numbers
reached a grant-facing homepage, and a demolition-cost column of $1.00 placeholders that summed
into a plausible $2.6M decade trend. In both, the wrong number was more persuasive than the
right one because it was cleaner.

A page whose entire purpose is to let a reader multiply a real population by a number they
invented is that failure mode with a slider attached. The distinction between what PDI measured
and what the reader assumed cannot be a copy convention, a CSS class, or a note the frontend is
trusted to remember. It has to be a property of the values themselves.

## Decision

### D1 — Three tiers, and what each is permitted to claim

| Tier | Question | Who computes it | Claim status |
|---|---|---|---|
| **1 REACH** | Who does this policy's own eligibility rule cover, where, how many? | PDI, from loaded data | **Measurement** — carries vintage, source, MOE |
| **2 EXPERIMENT** | If you assume X, what does the arithmetic say? | PDI, deterministically, over real denominators from tier 1 | **Projection** — never a finding |
| **3 EVIDENCE** | What happened where this was actually tried? | Nobody at PDI. External published sources, cited. | **Attributed** — never computed here |

Tier 1 is `research/08_mvp_public_groceries_rock_county.md` in full, which stops deliberately at
"exposure and eligibility — never effect." Tiers 2 and 3 extend it without loosening it. Tier 2
is bounded by the same refusal — it produces the consequence of an assumption, not evidence
about the world. Tier 3 is the only tier on the page that may speak about effects, and it does
so in somebody else's voice with a URL attached.

The tier is a property of the claim, not of the layout. An output cannot change tier by being
rendered somewhere else.

### D2 — The absence of a time axis is expressed as the absence of fields

`schemas/analysis.schema.json` carries `comparison_year`, `prior_value`, `change` and
`pct_change`, because a county analysis can legitimately span vintages. This page cannot, so its
payloads do not get those fields, and `experiment.Value` carries a single `vintage` string rather
than a slice.

An author who wanted to imply a trend here would have to add a field and be seen doing it in
review. That is the enforcement: not a rule about what to write, a shape that has nowhere to
write it.

### D3 — Every value carries provenance; taint propagates and cannot be dropped

Every value carries a provenance `Kind` (`measured` | `assumed` | `derived`) and resolves to an
epistemic `ClaimClass` (`measurement` | `projection`). **A derived value with any assumed input
is a projection, permanently.** The class set has exactly two members; there is deliberately no
middle term, because "estimate" and "modeled" are precisely the vocabulary that lets a
projection be read as a finding.

This is enforced by arithmetic rather than by discipline, because a privately constructed
invariant is undone by the first generated `Deserialize` that walks around it:

- `ClaimClass` is **not a field**. There is nothing to set wrongly.
- It is a method computed from `assumed_inputs`, the sorted list of param ids the value descends
  from.
- The wire form is emitted from that method, and unmarshalling **errors** on a payload whose
  `claim_class` disagrees with its own `assumed_inputs`.
- The biconditional a third party checks in one line:
  `claim_class == "projection"  ⟺  len(assumed_inputs) > 0`.

Faking the class requires faking the input list, and the input list is checkable against the
model file that produced the value. The frontend is never asked to remember anything.

### D4 — The expression language is closed at eight operations

A per-policy model file declares its arithmetic as a small tagged union —
`ref` `const` `add` `sub` `mul` `div` `min` `max` — with no general expression parser and no
string ever evaluated. `pkg/grounding/intent.go` states the principle for the query planner:
*"Keeping it closed is what lets an unanswerable question be refused instead of guessed at."*

The same reasoning applies, and it buys a second thing here. Every operation is either a leaf
that declares its own provenance or a node that takes the union of its children's. No operation
discards an input list, so **the taint is provably total** rather than probably total. A ninth
operation requires editing the propagation rule, which is the review point.

Adding a general expression evaluator would make D3 an aspiration.

### D5 — Missing propagates as MISSING with a stated reason. Never zero.

A missing input, a zero divisor, a non-finite result and an unresolved reference each yield a
value with no number and a reason from a closed set. Never zero, never infinity, never a
silently dropped term, never a substituted 1. `value == null ⟺ missing_reason != ""` is a hard
invariant, checked on unmarshal.

The reason vocabulary preserves ADR-014 D2's distinction: `not_loaded` and `suppressed` are
**gaps**; `withheld` and `below_coverage_threshold` are **refusals**. A reader who cannot tell a
gap from a refusal learns the wrong lesson from both.

MOE is present on measured leaves only and is **nil on every value with an assumed input**. A
margin of error describes sampling error in an estimate of something that exists; multiplied by
an assumption it describes the sampling error of a hypothetical, which is not a quantity.
Publishing an interval around a projection would dress the least certain number on the page as
the best characterized one.

### D6 — The vintage collision is stated, not smoothed

USDA's designation is a 2010-tract fact; the people-data is 2020 tracts; the crosswalk allocates
by population and therefore turns a categorical flag into a **0–1 population share**. The repo
holds no 2010 tract geometry.

The map layer is therefore worded, verbatim and identically in both registers and both
languages: *"share of this tract's population living in an area USDA designated low-income and
low-access."* The designated tracts are listed separately at their native vintage and labeled as
such. **The page never prints "your tract is a food desert" from a crosswalked share** — not in
the legend, not in a tooltip, not in generated prose.

Where a bill's eligibility language differs from the dataset PDI holds, the page presents USDA's
test **as USDA's**, never as the bill's.

### D7 — Two registers, strictly additive

The page shows a public register and expands into an analyst register on demand. The analyst
register is a **superset**: no figure changes value when a reader expands it. A page where
expanding changes the answer was lying in one of the two states.

The public register shows a plain-language reliability phrase and never a margin of error or an
interval. The analyst register adds MOE, intervals, n, coverage, denominator, vintage, source,
and the arithmetic trace. Reliability bands use the Census Bureau's own coefficient-of-variation
convention (≤15% / ≤30% / above), because a threshold PDI invented would have to be defended
and that one already is.

The arithmetic trace is not a nicety. A projection with a hidden derivation asks the reader for
trust; a projection with a visible one asks them to disagree with an input, which is the only
useful thing a person can do with an assumption.

### D8 — Civic framing: the candidate's name is subordinate

The `h1` is the policy and the place. Candidate, office and bill reference appear in an
attribution line inside the provenance block.

Rationale: PDI is grant-facing (Arnold Ventures $591K, MCF LOI), the platform already carries two
candidates' policy sets (Hong 70 positions, Mamdani 15), and a measurement page that leads with a
candidate reads as advocacy — which undermines exactly the defensibility that D1–D6 exist to
establish. This is a positioning call rather than a technical one and the operator may overrule
it; it forks every copy string in both languages, so it is recorded as a decision rather than
left to whoever writes the first heading.

Relatedly: the user-facing word is **"explore"**, never "experiment", in either language. The
architectural name stays `experiment` — this ADR, `pkg/experiment`, the contract. Spanish
*"experimento"* reads as a trial with subjects and a causal test, which is precisely what PDI
cannot run, and English "experiment" invites the reader to take the output as a result. The
mismatch between the package name and the copy is the decision, not a bug to fix.

### D9 — One route, and absences are content

`/policy?id=<policy_id>&geoid=<geoid>` — extensionless, as the stack requires. No second route
and no alias.

Most of the 70 positions will have tier 1 and nothing else. When a tier has no data, its
interactive apparatus is **not rendered**, and an entry appears in the payload's `absences` list
which the page states in one sentence. An empty interactive panel on 70 policies is noise; a
silently omitted one erases PDI's refusal, which is the most defensible thing on the page. One
mechanism closes both failure modes: **hide the apparatus, never hide the absence.** A payload
with a missing tier and no matching absence entry is malformed.

Aggregations route through the existing run layer (`POST /v1/policy/analyses`, ADR-014/015) so
coverage thresholds, bootstrap CI and withholding apply for free. This page invents no
statistics. `POST /v1/policy/simulate` is pure arithmetic over already-published numbers and is
deliberately outside the run layer: it writes nothing, costs nothing, and needs no token.

### D10 — The contract is a file, not this ADR and not a prompt

`docs/contracts/experiment-contract.md` carries every type name, every JSON field, both endpoint
shapes, the tier-3 CSV columns, the register split, the frozen copy strings in both languages,
the i18n convention, and the taint algorithm in enough detail that two people implement it
identically.

It is a committed file because a gate can verify a file and cannot verify a prompt. Parallel
tracks do not couple on files, they couple on decisions neither has made yet; every such
decision is settled there as a numbered seam with a one-line reason, and the ones that could not
be settled from evidence are listed as OPEN rather than guessed.

### D11 — What this narrows in ADR-013, and adds to ADR-011

**ADR-013 is narrowed in one place.** Its §2A assigns categorical meaning to the Rainbow scale
— emerald (`--tp-cat-4`) for "positive/improvement", red (`--tp-cat-5`) for "negative/concern" —
and its principle 2 states that colour carries that meaning. On this page, **that vocabulary may
not be applied to any tier-2 output.** Colouring a projection green for improvement renders an
arithmetic consequence of the reader's own assumption as a value judgment about the policy,
which is the causal claim D1 forbids, delivered by a colour token instead of a sentence.
Tier-1 measurements and tier-3 published findings may use the scale as ADR-013 specifies.
Everything else in ADR-013 stands.

**ADR-011 is extended, not superseded.** Its §1B information architecture lists ten pages in
three groups and contains no policy-scoped destination; this adds one. Its §1C decision —
one HTML file per route, `data-en`/`data-es` attributes, a JS swap layer — is the mechanism this
page uses, with one hard consequence recorded here because it has already cost ES parity
elsewhere: **no Alpine `x-text` binding on any translatable string.** Alpine bindings bypass the
swap layer entirely, so an ES page renders correct numbers with English labels and looks fine to
anyone testing in English. Dynamic strings on this page resolve through `LangToggle.t()` in a
render function subscribed to `tp:langchange`.

Corollary, and the reason the API surface is shaped the way it is: **the server never sends
display prose.** Payloads carry i18n keys. A server-supplied English string is invisible to the
swap layer and ships an untranslated page that passes every backend test. The three exceptions —
proper nouns, verbatim English-authored CSV policy text, and quoted external evidence — are
enumerated in the contract, and the third is a refusal in its own right: PDI translating somebody
else's published finding would make PDI the author of a claim it is explicitly not making.

### D12 — The AI surface is bound by ADR-006, over an already-computed payload

One action: "explain this in plain language", over a payload that already exists. The ADR-006
pipeline applies unchanged — the model plans and writes prose and **may never be the source of a
number** — with `Verify` run against the union of every value in the payload and every bound
assumption. A draft carrying an unsupported figure is refused and the deterministic rendering
ships instead, because a feature whose only output path runs through a model has no floor.

Three additional refusals specific to this page: the model may not choose or suggest an
assumption, may not produce a sentence with a causal verb about PDI's own data, and may not
introduce a jurisdiction that is not in the cited evidence.

## Consequences

### Positive

- **The distinction survives a screenshot.** A projection is labeled as one by a property of the
  value, in the payload, in both registers, in both languages, in print.
- **PDI can host an interactive model without acquiring a causal claim.** The reader supplies the
  assumption and can see it; the platform supplies the denominator and can prove it.
- **The 70 positions become one page instead of 70 dead ends.** Tier 1 is expressible for any
  policy whose rule maps to loaded data; the rest state their absence honestly.
- **`claims_empirical` finally renders.** It is plumbed end-to-end and drawn nowhere; it becomes
  the claim-check lane in tier 3, where the candidate's own empirical claim meets external
  published evidence with a citation.
- **No new statistics.** Aggregations route through ADR-014/015's run layer, so coverage
  thresholds, intervals and withholding are inherited rather than reimplemented.

### Negative and accepted

- **Most policies will show one tier.** Tier 2 needs an authored model and tier 3 needs authored,
  reviewed, cited evidence. Coverage will look worse than a page that generated something for
  every policy. That is D5 and ADR-014 D2 working.
- **Tier 3 is human labour that no track can automate.** `evidence_strength` and `reviewed_by`
  are judgments. Zero evidence files exist today; without an author, the page is two tiers.
- **The taint invariant costs a custom marshaller** on two types, and any future type carrying a
  claim needs the same treatment. The alternative is a settable field, which is the thing being
  prevented.
- **No confidence interval on any projection.** Readers accustomed to seeing error bars will
  read their absence as sloppiness rather than as the refusal it is. The analyst register shows
  the input MOEs, which is the honest version of what they wanted.
- **A closed operation set will be too small for some policy.** That model gets refused and the
  set gets extended in review, which is slower than an escape hatch and is the point.

### Risks

- **A projection escaping into a screenshot without its label** is the highest-severity risk and
  the reason D3 is enforced by arithmetic instead of by copy.
- **Colour carrying a causal claim** is the second, and the least likely to be caught by a code
  review, because it arrives as a design token rather than as a sentence. D11 names it.
- **An authored default reading as a recommendation.** Every parameter's default requires a
  stated basis for exactly this reason, and it may still not be enough — see OPEN-6.
- **The eligibility rule being attributed to the wrong authority.** AB1221's text is still
  unverified; the mitigation is attribution, not silence.
- **The community as a subject rather than a partner.** Unchanged from `research/08` §11 and
  still the highest non-technical risk: do not ship a brief about Beloit without someone from
  Beloit reading it first. That is a gate on the launch, not on the build.

## Open questions

The full list, with what each one blocks, is the OPEN section of
`docs/contracts/experiment-contract.md`. The four that need an operator answer before the first
build lands:

1. **AB1221's actual eligibility language** is unverified (`research/08` §7, Monday task 1,
   never done). Until then the page presents USDA's test attributed to USDA.
2. **Civic vs campaign framing** (D8). Frozen civic; it forks every copy string, so an override
   is cheapest now.
3. **The pilot policy has no empirical claim.** `FH-TRANSIT-008`'s `claims_empirical` is empty —
   16 of the 70 Hong positions carry one and it is not among them — so the claim-check lane
   renders as a stated absence on the one policy being built. `FH-EDU-001` is the natural second
   policy if that lane should be exercised at launch.
4. **Who authors and reviews tier-3 evidence.** No file, no author, no reviewer, and the schema
   requires all three.
