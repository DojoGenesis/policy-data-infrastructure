# Tier 3 evidence — curation rules

One file per policy: `data/policies/evidence/<policy_id>.csv`. Absent file means no tier 3 for
that policy, which is the state of all 70 Hong positions and 15 Mamdani positions except the
one(s) built here. This is normal, not a defect — see
`docs/contracts/experiment-contract.md` §5 and ADR-016 D1.

**PDI never computes a row in this file.** Every number, direction, and finding in a tier-3 row
comes from somewhere else, published, with a citation. Tier 3 is the only place on the policy
experiment page allowed to use the word "outcome," and it earns that permission by not
computing anything.

The contract (`docs/contracts/experiment-contract.md` §5) is normative for the column list,
types, and required/optional fields. This file is the *why*, and the checklist a curator should
run before a row ships.

## The rule that matters most

**A fabricated or half-remembered citation on a civic page aimed at voters is the worst possible
failure mode of this entire project.** This repository has already shipped fabricated numbers to
a grant-facing page once (a spike that self-certified "COMPLETE / Real Data"). Tier 3 sits one
level closer to the public than that spike did — it is cited by name to argue about a candidate's
policy position. Do not let that happen here.

**If you cannot resolve a citation — fetch it, read it, confirm the numbers you're about to
quote are actually in it — drop the row.** A thin evidence file that is entirely real beats a
thick one with a single row nobody actually checked. This file shipped with 7 rows, not 30, on
that principle. Quality over count.

## Checklist for every row

1. **Fetch the source yourself.** Every `source_url` in this file was actually opened (or its
   DOI resolved) during authoring, not recalled from training data. If you can't fetch it, you
   don't have it — search results and abstracts describing a paper are not the paper.
   **Every figure must be verified against fetched source text, not recalled — including
   figures you are confident about, and including rows nobody has flagged.** A 2026-08-14
   re-verification pass fetched all seven sources in this file and found real drift even in
   rows the previous audit had *not* flagged: a paper's own abstract said "about ten percent"
   where the row said "9 percent" (`allcott_2019_qje`), and a supermarket's opening date was
   off by seven months because nobody had checked it against the study's own stated survey
   dates (`elbel_bronx_2015`). Confidence is not evidence. If you did not just now read the
   sentence a number came from, do not type that number.
   **A DOI that redirects to a paywall is not the end of the search.** `mah_review_2019` was
   flagged by a prior audit as having "unverifiable" statistics because its DOI resolves to a
   paywalled Springer page — but the same paper has a full, free, NIH-hosted mirror on PubMed
   Central, and every one of the flagged statistics checked out exactly against it. Before
   writing "paywalled, could not verify," search for `site:ncbi.nlm.nih.gov/pmc <title>` or an
   author's own working-paper PDF (university faculty pages routinely host the accepted
   manuscript free). Only fall back to abstract-only sourcing, or drop the figure, once that
   search has genuinely failed — and say explicitly in `reviewed_by` which you did.
2. **Record the real study design, not the one that sounds most persuasive.** `study_design` is
   a closed set — `rct | quasi_experimental | pre_post | descriptive | case_study | review |
   program_report` — and a descriptive case study is not a causal estimate. Flattening the two
   is exactly the failure this column exists to prevent. State the stated limitation too, in
   `transfer_note` or `finding` — every study here has one (small sample, short follow-up,
   no true control, self-report, publication era).
3. **Actively look for disconfirming and null findings. Do not curate a case for the policy.**
   If you only search for evidence that a policy works, you will only find it — that is a
   property of search, not of the world. Four of this file's seven rows are quasi-experimental
   studies that found *no* significant diet or weight change from a new store opening
   (Dubowitz 2015, Cummins 2014, Elbel 2015), or that found supply-side access explains only a
   small share of the outcome gap the policy implicitly targets (Allcott et al. 2019, the
   single most rigorous paper in the set). If the strongest evidence available says an
   intervention underperforms expectations, that is the row that most needs to be here, not the
   one to leave out.
4. **State transferability honestly, every time.** `transfer_note` is required by the loader
   (empty `source_url`, `citation`, or `transfer_note` gets the row refused) and it is not
   boilerplate — a Florida town of 1,400 people is not Beloit, Wisconsin, and a Bronx
   neighborhood inside a dense five-borough retail market is not a small-metro county. Say what
   differs (population scale, ownership model, retail density, follow-up window) and say it in
   the row, not just in your head. An uncontextualized result from another jurisdiction is the
   single most persuasive wrong thing this page could show.
5. **`claim_check` and `relates_to_claim` are about the *candidate's own* `claims_empirical`
   field on that policy record — not about whether the evidence supports the policy in general.**
   Most Hong/Mamdani positions carry an empty `claims_empirical` (54 of 70 Hong positions,
   including the pilot `FH-TRANSIT-008` — verified via the command in the contract §5). When
   the policy has no candidate-authored empirical claim to check a source against, every row's
   `claim_check` is `not_applicable` and `relates_to_claim` is empty. This is correct, not a
   placeholder — do not invent a claim to check against.
6. **`evidence_strength` needs a human reviewer, and no curation pass to date has supplied
   one.** The contract's OPEN-4 says so directly: this is a human judgment no automated or
   single-pass track can authoritatively make. `evidence_strength` values here are a good-faith
   draft rating against ordinary evidence-hierarchy conventions (an RCT or a quasi-experimental
   design with a real comparison group generally rates higher than a single-site case study),
   **not a final determination.** Do not ship this file to production, and do not let a page
   render `evidence_strength` as if authoritative, until a real reviewer has confirmed or
   corrected it. This is a known, flagged gap, not an oversight.

   **`reviewed_by`/`reviewed_date` now record agent-level fact verification, which is real but
   is not the same thing as operator sign-off.** As of 2026-08-14, every row's `reviewed_by`
   states which source was actually fetched, what was confirmed against it word-for-word, and
   what was corrected or removed because it could not be confirmed — e.g. `"agent-verified
   against the open-access full text (PMC6904419) 2026-08-14; confirmed verbatim: ..."`. This
   replaces the earlier placeholder text (`UNREVIEWED - pending ...` / `PENDING`), which existed
   only so the loader's non-empty-column requirement was satisfied without a fabricated name
   standing in for a review that had not happened — a real problem, since a placeholder and a
   genuine verification look identical to the loader. The distinction that still matters:
   **agent-verified means the *facts* were checked against fetched source text. It does not
   mean a human has read the row, agreed with `evidence_strength`, or authorized it to ship.**
   A future curator (human or agent) who re-touches a row must write a `reviewed_by` that says,
   specifically, what was fetched and what was confirmed or corrected against it — not a name,
   and not language implying sign-off that has not happened. If you did not fetch the source
   yourself this pass, do not overwrite an existing `reviewed_by` that says someone did.
7. **Percent and share values are as-published, in `effect_reported`, as free text — never
   reformatted into `percent_points` vs `share` here.** Tier 3 is prose evidence, not a `Value`;
   the 0–100 vs 0–1 convention (contract S17) governs tier 1 and tier 2 payload fields, not this
   CSV. Quote the number the way the source stated it.
8. **Evidence text is never translated by PDI (contract S10).** `finding`, `outcome`,
   `intervention`, and `citation` stay in the language of the source (English, for everything in
   this file). The page's rendering layer translates the scaffolding around a card, never the
   quotation inside it — do not pre-translate a row to make the frontend's job easier.

## What this file is not

It is not a literature review and it is not exhaustive. It is the minimum defensible set of real,
checkable, sourced rows that lets one policy's tier 3 panel render honestly, including its own
uncertainty. A future curator extending this file, or starting one for a different policy, should
run every item on the checklist above again — none of it is a one-time cost.
