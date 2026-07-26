# ADR-006: Ground chat in structured query generation, not vector retrieval

**Status:** Proposed
**Date:** 2026-07-26
**Deciders:** Cruz Morales
**Refines:** ADR-005 (Gateway-Powered Chat Interface) — keeps its architecture, closes
the hole it names in its own Consequences: *"LLM responses may hallucinate statistics."*

## Context

PIP-92 asks for chat over the MEDI/Atlas datasets where "users ask natural-language
questions and get answers backed by real data, not LLM confabulation," and asks
explicitly that the grounding approach be scouted before anything is built. The two
candidates named were vector embeddings of tract indicators and structured query
generation.

The requirement is unusually sharp for a chat feature: **a confabulated number is a
failure, not a rough edge.** That is a much stronger bar than "usually accurate," and
it is the bar the design has to be chosen against.

What the data actually is matters more here than what is fashionable. After PIP-91
and PIP-78 the corpus is:

- 1,542 tracts x 11 indicators, plus 72 counties x 11 — roughly 17,000 numeric facts
- every fact is a `(geography, indicator, vintage) -> number` tuple with a fixed schema
- plus ~140 district features, 132 officeholders, and 70 policy positions
- plus a small body of genuine prose: indicator definitions, method notes, source
  descriptions, and the framing rules in `policy-catalog.json`

Almost all of it is **numbers in a closed schema**. Very little of it is prose.

## Options considered

### A. Vector embeddings over the indicator data (RAG)

Embed per-tract or per-indicator text chunks, retrieve top-k by similarity, let the
model compose an answer from what came back.

Rejected. Not because RAG is bad, but because it is a poor fit for this corpus and,
specifically, it cannot meet the no-confabulated-number bar:

1. **Similarity is not correctness.** Retrieval returns chunks that *resemble* the
   query. For "which county has the highest cost burden," resemblance is not the
   relation being asked about — the answer requires a sort over all 72 counties, and
   nothing about top-k similarity guarantees the true maximum is in the k returned.
2. **Aggregation is impossible.** Median, rank, count, threshold, and comparison are
   the operations people actually ask of this data. None of them can be performed by
   retrieval; the model would have to do arithmetic on retrieved text, which is
   exactly the surface where wrong numbers are produced.
3. **Numbers embed badly.** Tokenizers fragment numerals, and embedding distance
   between "8.8" and "88" is not meaningfully larger than between "8.8" and "8.9".
   A retrieval layer over 17,000 numeric facts is being asked to do the one thing
   the representation is worst at.
4. **New infrastructure, new drift.** An embedding model plus a vector store, both of
   which must be rebuilt on every ACS vintage or they silently answer from last
   year's data. The current stack has neither and needs neither.

Embeddings remain the right tool for the prose — see the decision.

### B. Structured query generation

The model translates the question into a **typed intent** over a closed schema. The
intent is executed deterministically against the real data. The model never emits a
figure; it emits a query.

This fits the corpus exactly: the data is already a closed schema, and the operations
people ask for (lookup, rank, compare, aggregate, threshold, representation) are a
small, enumerable set.

### C. Tool calling (ADR-005 as written)

Structurally this is option B — a tool call is a structured query, and executing it is
deterministic. ADR-005 is therefore not wrong; it is **incomplete in one specific
place**, and that place is where the requirement lives.

In ADR-005's flow the model still writes the final prose. Correct tool output does not
constrain the sentence that follows it. A model handed `poverty_rate: 8.8` can still
write "just under 9 percent, up from 7.2 in the previous vintage" — inventing both a
comparison and a number that no tool returned. Grounding the *input* does not ground
the *output*.

## Decision

**Adopt structured query generation as the spine, and add an output verification stage
that ADR-005 does not have.** Three stages, with the model confined to the first and
last, and never trusted for a value:

1. **Plan.** The model maps the question to a typed `Intent` — operation, indicator,
   geography, filters — validated against a closed schema. An intent that does not
   validate is rejected, and the honest answer "I can't answer that from this data"
   is returned instead of a plausible one. The model's output here is a query.

2. **Execute.** Deterministic Go executes the intent against the Atlas bundle and
   returns a `Result`: the values, and a citation for each — indicator id, geography,
   vintage, and source. No model involvement. This is ordinary data access.

3. **Compose and verify.** The model writes prose *given the result*. Then every
   number in that prose is extracted and checked against the values the query actually
   returned. **A number with no match in the result set fails the response.** The
   failure is caught by the system, not by the reader.

Stage 3 is the load-bearing addition. Stages 1 and 2 make a wrong number unlikely;
stage 3 makes it *detectable*, which is what the requirement actually demands. It is
cheap — a regex over the draft and a set membership test — and it converts the
guarantee from a hope about model behaviour into a property of the pipeline.

**Retrieval keeps one job: prose.** Definitional questions ("what does cost-burdened
mean," "why is a tract missing data," "what does this page not claim") are answered
from the documentation corpus — indicator descriptions, method notes, the framing in
`policy-catalog.json`. That corpus is small enough that keyword matching over a few
dozen documents is sufficient and an embedding model is not yet earned. Numbers never
come from this path.

## Model lanes

Per PIP-92: OpenRouter for quality, Ollama for cost, behind one interface. The lane
choice is invisible to grounding — both lanes see the same intent schema and the same
verification. A cheaper model that plans slightly worse produces *rejected intents*,
not wrong numbers, which is the correct way for a cost decision to degrade.

## Consequences

**Positive**
- A wrong number is structurally hard to produce and mechanically detected if produced.
- Citations are free: the intent names its geography, indicator, and vintage, so every
  answer can say where each figure came from without the model being asked to remember.
- No new infrastructure. No embedding model, no vector store, nothing to re-index when
  the ACS vintage rolls.
- Refusals are honest and specific — "that indicator isn't in this dataset" rather than
  a confident guess.
- Testable without a model in the loop: intent execution and verification are pure
  functions over fixtures, so the guarantee is covered by `go test`.

**Negative**
- Only questions expressible in the schema get numeric answers. Open-ended "why" and
  causal questions fall to the prose path or to a refusal. This is a real limit and
  the right one — this dataset cannot support causal claims anyway.
- The intent schema is now a maintained surface: a new indicator or operation means
  extending it, and forgetting to means silent loss of coverage (a refusal, not a
  wrong answer — the safe direction).
- Verification is lexical. It catches invented figures; it does not catch a correctly-
  quoted number used in a wrong sentence. Narrowing that is future work, not a reason
  to skip the cheap 90%.

## Prerequisites

- The Atlas bundle from PIP-91/PIP-78 as the query target (present).
- An intent schema covering the operations the data supports.
- One model lane configured; the interface admits the second without changes.
