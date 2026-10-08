/* policy-app.js — the Policy Experiment Page (ADR-016, docs/contracts/experiment-contract.md)
   Stack-authored. Route: /policy?id=<policy_id>&geoid=<geoid>[&level=][&p.<param_id>=][&mv=]

   ── WHY NO ALPINE ────────────────────────────────────────────────────────
   Contract S11: no Alpine x-text on any translatable string on this page —
   Alpine bindings bypass lang-toggle.js's swap layer entirely (verified
   elsewhere in this stack: ~200 strings stayed English in Spanish mode on
   the ten-page build). Rather than police that discipline field by field,
   this page does not load Alpine at all. Every dynamic node is built by a
   render() function that resolves i18n keys through window.LangToggle.t()
   and is re-run in full on the `tp:langchange` event, which structurally
   forecloses the bug class instead of relying on nobody writing an x-text.

   ── THE TAINT INVARIANT, CLIENT SIDE ─────────────────────────────────────
   Every experiment.Value on this page carries its own claim_class, kind,
   register and missing_reason from the server (pkg/experiment/types.go).
   This file never infers, recolors, or upgrades any of those — it renders
   what the payload says. A tier-2 output's claim_class is always
   "projection" and is always rendered with the PROJECTION badge; nothing
   here has a code path that could print a tier-2 number without it.

   ── REGISTER SPLIT ────────────────────────────────────────────────────────
   One global "show the numbers behind this" toggle (STATE.analystOpen)
   gates every element carrying [data-pex-register="analyst"]. Expanding it
   only reveals more DOM — no function that renders a public-register number
   is ever called differently depending on this flag, which is what keeps
   the expansion strictly additive per contract §6.
   ══════════════════════════════════════════════════════════════════════ */
(function (window, document) {
  'use strict';

  var API_BASE = '/v1/policy';
  var TRACTS_URL = '/static/tracts.geojson';

  // ── tiny helpers ─────────────────────────────────────────────────────
  function t(key) { return window.LangToggle ? window.LangToggle.t(key) : key; }
  function lang() { return window.LangToggle ? window.LangToggle.getLang() : 'en'; }
  function locale() { return lang() === 'es' ? 'es-US' : 'en-US'; }
  function qs(sel, root) { return (root || document).querySelector(sel); }
  function qsa(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }
  function clear(node) { while (node.firstChild) { node.removeChild(node.firstChild); } }
  function el(tag, attrs, kids) {
    var n = document.createElement(tag);
    attrs = attrs || {};
    for (var k in attrs) {
      if (!Object.prototype.hasOwnProperty.call(attrs, k)) { continue; }
      if (k === 'text') { n.textContent = attrs[k]; }
      else if (k === 'html_SAFE_STATIC') { n.innerHTML = attrs[k]; /* only ever passed a literal from THIS file */ }
      else if (k.indexOf('on') === 0 && typeof attrs[k] === 'function') { n.addEventListener(k.slice(2), attrs[k]); }
      else { n.setAttribute(k, attrs[k]); }
    }
    (kids || []).forEach(function (c) { if (c) { n.appendChild(c); } });
    return n;
  }
  function txt(tag, className, text) { return el(tag, { class: className, text: text }); }

  function paramFromQuery(name) {
    var m = new URLSearchParams(window.location.search);
    return m.get(name);
  }

  function fetchJSON(url, opts) {
    return fetch(url, opts).then(function (r) {
      return r.text().then(function (body) {
        var parsed = null;
        try { parsed = body ? JSON.parse(body) : null; } catch (e) { /* non-JSON error page */ }
        return { ok: r.ok, status: r.status, body: parsed };
      });
    });
  }

  // FIX 2 (contract S10 — "the server never sends display prose"): the Go
  // handlers in pkg/gateway send ErrorResponse.Error/.Detail as free-form
  // English sentences ("param %q value %v is outside [%v, %v]",
  // "policy not found", …) — that is a wire-level fact this file cannot
  // change (out of this track's ownership manifest) and never a key the
  // lang-toggle swap layer can resolve. So this page never reads
  // res.body.error or res.body.detail for display, in any language. The
  // one field on that shape this page DOES trust is the closed,
  // contract-frozen `missing_reason` enum (contract §1.1) — every member
  // already has a pex.missing.* key registered above — and everything
  // else collapses to the generic, already-bilingual error string. A
  // reader on the Spanish page now never sees an English sentence the
  // backend wrote, no matter what status or prose comes back.
  function errorKeyFromResponse(res) {
    var reason = res && res.body && res.body.missing_reason;
    if (reason && STRINGS['pex.missing.' + reason]) { return 'pex.missing.' + reason; }
    return 'pex.ui.error_generic';
  }

  // ── i18n: page-scoped strings (contract §8, namespace pex.) ───────────
  // Every key the live API sends for FH-TRANSIT-008 + the generic keys
  // every one of the 70 positions can hit (tier headings, refusal keys,
  // missing/absence sentences, reliability bands — all frozen or closed
  // sets) is registered here. A policy-specific key this page has never
  // seen (a future policy's own param labels) falls back to raw-key
  // rendering via LangToggle.t()'s own fallback — see humanizeKey() below
  // for how this page softens that fallback instead of printing a dotted
  // key verbatim.
  var STRINGS = {
    // ── page chrome ──
    'pex.nav.back': { en: '← All policy positions', es: '← Todas las posiciones de política' },
    'pex.h1.title_place': { en: '{title} — {place}', es: '{title} — {place}' },
    'pex.title.suffix': { en: '· Policy Data Infrastructure', es: '· Policy Data Infrastructure' },

    // ── S15 tier headings, frozen ──
    'pex.reach.question': { en: 'Does this reach my block?', es: '¿Esto llega a mi cuadra?' },
    'pex.experiment.heading': { en: 'Try your own numbers', es: 'Prueba tus propios números' },
    'pex.evidence.heading': { en: 'Where this has been tried', es: 'Dónde se ha intentado esto' },
    'pex.refusals.heading': { en: 'What we cannot tell you', es: 'Lo que no podemos decirte' },
    'pex.provenance.heading': { en: 'Where every number came from', es: 'De dónde viene cada número' },
    'pex.claimcheck.heading': { en: 'Checking the candidate’s claim', es: 'Verificando la afirmación del candidato' },
    'pex.chooser.h1': { en: 'Policy positions', es: 'Posiciones de política' },

    // ── S9 frozen verbs (architectural name stays "experiment"; user word is "explore" — never used on its own here since the panel heading itself is the frozen "Try your own numbers") ──
    'pex.experiment.action': { en: 'Recalculate', es: 'Recalcular' },
    'pex.experiment.reset': { en: 'Back to the starting numbers', es: 'Volver a los números iniciales' },

    // ── S7 reliability phrases, frozen ──
    'pex.reliability.high': { en: 'This figure is well measured.', es: 'Esta cifra está bien medida.' },
    'pex.reliability.medium': { en: 'This figure is approximate.', es: 'Esta cifra es aproximada.' },
    'pex.reliability.low': { en: 'This figure is uncertain — read it as a rough order of size.', es: 'Esta cifra es incierta — tómala como un orden de magnitud.' },
    'pex.reliability.unknown': { en: 'This figure has no published margin of error.', es: 'Esta cifra no tiene margen de error publicado.' },

    // ── S6 crosswalk wording (map legend), verbatim across languages/registers ──
    'pex.map.designated_share.legend': {
      en: 'Share of this tract’s population living in an area USDA designated low-income and low-access',
      es: 'Porcentaje de la población de este sector censal que vive en una zona designada por el USDA como de bajos ingresos y bajo acceso'
    },
    // Same wording, parameterized to a whole requested geography (used on
    // the headline stat, not the map legend itself — the map legend key
    // above stays verbatim per S6). {place} is substituted at render time.
    'pex.reach.share_sentence': {
      en: 'Share of {place}’s population living in an area USDA designated low-income and low-access',
      es: 'Porcentaje de la población de {place} que vive en una zona designada por el USDA como de bajos ingresos y bajo acceso'
    },

    // ── rule usda_fara_lila ──
    'pex.rule.usda_fara_lila.authority': { en: 'USDA Economic Research Service', es: 'Servicio de Investigación Económica del USDA' },
    'pex.rule.usda_fara_lila.statement': {
      en: 'A tract is designated low-income and low-access (LILA) when a meaningful share of its residents are both low-income and far from the nearest full-service grocery store.',
      es: 'Un sector censal se designa de bajos ingresos y bajo acceso (LILA) cuando una parte importante de sus residentes tienen bajos ingresos y viven lejos de la tienda de comestibles de servicio completo más cercana.'
    },
    'pex.method.usda_fara_crosswalk': {
      en: 'USDA’s designation is a 2010-tract fact; the population figures on this page are 2020 tracts. The crosswalk allocates by population share, which is why this page reports a 0–100% share rather than a yes/no flag.',
      es: 'La designación del USDA es un hecho del sector censal de 2010; las cifras de población de esta página son de sectores censales de 2020. El cruce asigna por porcentaje de población, por eso esta página informa un porcentaje de 0–100% en vez de una bandera de sí/no.'
    },

    // ── reach output labels ──
    'pex.output.designated_share.label': { en: 'Share of the population in a designated area', es: 'Porcentaje de la población en una zona designada' },
    'pex.output.designated_population.label': { en: 'People living in a designated area', es: 'Personas que viven en una zona designada' },
    'pex.output.total_tract_count.label': { en: 'Census tracts in this geography', es: 'Sectores censales en esta zona' },
    'pex.output.designated_tract_count.label': { en: 'Tracts USDA designated', es: 'Sectores designados por el USDA' },
    'pex.output.tract_snap.label': { en: 'SNAP households', es: 'Hogares con SNAP' },
    'pex.output.tract_population.label': { en: 'Population', es: 'Población' },

    // ── model title/summary ──
    'pex.model.fh_transit_008.title': { en: 'What a public grocery store would cost and could reach', es: 'Qué costaría una tienda de abarrotes pública y a quién podría alcanzar' },
    'pex.model.fh_transit_008.summary': {
      en: 'Set how many stores, what they would cost, and how much of the designated population you assume they would serve. The arithmetic is fixed; the assumptions are yours.',
      es: 'Elige cuántas tiendas, cuánto costarían y qué parte de la población designada supones que servirían. La aritmética es fija; los supuestos son tuyos.'
    },

    // ── params ──
    'pex.param.num_stores.label': { en: 'Number of stores', es: 'Número de tiendas' },
    'pex.param.num_stores.help': { en: 'How many publicly owned stores you’re assuming would open.', es: 'Cuántas tiendas de propiedad pública supones que abrirían.' },
    'pex.param.num_stores.default_source': { en: 'Starting point: one pilot store. Not a recommendation.', es: 'Punto de partida: una tienda piloto. No es una recomendación.' },
    'pex.param.capital_cost_per_store.label': { en: 'Capital cost per store', es: 'Costo de capital por tienda' },
    'pex.param.capital_cost_per_store.help': { en: 'One-time cost to build or acquire and equip a full-service grocery store.', es: 'Costo único para construir o adquirir y equipar una tienda de comestibles de servicio completo.' },
    'pex.param.capital_cost_per_store.default_source': { en: 'Starting point: a round mid-range estimate for a small-format grocery build-out. Not a recommendation.', es: 'Punto de partida: una estimación redonda de rango medio para una tienda de formato pequeño. No es una recomendación.' },
    'pex.param.annual_operating_subsidy_per_store.label': { en: 'Annual operating subsidy per store', es: 'Subsidio operativo anual por tienda' },
    'pex.param.annual_operating_subsidy_per_store.help': { en: 'Public money assumed needed each year to keep one store open, on top of its own sales.', es: 'Dinero público que se supone necesario cada año para mantener abierta una tienda, además de sus propias ventas.' },
    'pex.param.annual_operating_subsidy_per_store.default_source': { en: 'Starting point: an arbitrary round figure, not drawn from a specific operating case. Not a recommendation.', es: 'Punto de partida: una cifra redonda arbitraria, no tomada de un caso operativo específico. No es una recomendación.' },
    'pex.param.target_population_share_served.label': { en: 'Share of the designated population you assume is served', es: 'Porcentaje de la población designada que supones sirven' },
    'pex.param.target_population_share_served.help': { en: 'Not every person in a designated area would shop at or benefit from a new store. This is your assumption about how much of that population it would reach.', es: 'No todas las personas en una zona designada comprarían o se beneficiarían de una tienda nueva. Este es tu supuesto sobre qué parte de esa población alcanzaría.' },
    'pex.param.target_population_share_served.default_source': { en: 'Starting point: an arbitrary round quarter-share. Not a recommendation.', es: 'Punto de partida: un cuarto redondo arbitrario. No es una recomendación.' },
    'pex.param.planning_horizon_years.label': { en: 'Planning horizon (years)', es: 'Horizonte de planificación (años)' },
    'pex.param.planning_horizon_years.help': { en: 'How many years of operating subsidy to add to the one-time capital cost.', es: 'Cuántos años de subsidio operativo se suman al costo de capital único.' },
    'pex.param.planning_horizon_years.default_source': { en: 'Starting point: a five-year planning window, an arbitrary round figure. Not a recommendation.', es: 'Punto de partida: una ventana de planificación de cinco años, una cifra redonda arbitraria. No es una recomendación.' },

    // ── tier-2 output labels ──
    'pex.output.total_capital_cost.label': { en: 'Total capital cost', es: 'Costo de capital total' },
    'pex.output.total_annual_operating_subsidy.label': { en: 'Total annual operating subsidy', es: 'Subsidio operativo anual total' },
    'pex.output.total_public_cost_over_horizon.label': { en: 'Total public cost over the planning horizon', es: 'Costo público total durante el horizonte de planificación' },
    'pex.output.population_potentially_served.label': { en: 'Population your assumptions say could be served', es: 'Población que, según tus supuestos, podría ser atendida' },

    // ── arithmetic-trace term labels ──
    'pex.term.num_stores': { en: 'number of stores', es: 'número de tiendas' },
    'pex.term.capital_cost_per_store': { en: 'capital cost per store', es: 'costo de capital por tienda' },
    'pex.term.annual_operating_subsidy_per_store': { en: 'annual operating subsidy per store', es: 'subsidio operativo anual por tienda' },
    'pex.term.target_population_share_served': { en: 'share of designated population served (your assumption)', es: 'porcentaje de población designada servida (tu supuesto)' },
    'pex.term.planning_horizon_years': { en: 'planning horizon, years', es: 'horizonte de planificación, años' },
    'pex.term.total_capital_cost': { en: 'total capital cost', es: 'costo de capital total' },
    'pex.term.total_annual_operating_subsidy': { en: 'total annual operating subsidy', es: 'subsidio operativo anual total' },
    'pex.term.operating_cost_over_horizon': { en: 'operating subsidy over the horizon', es: 'subsidio operativo durante el horizonte' },
    // FIX (serial gate, 2026-08-14): the model-instance track's denominator
    // fix (data/policies/models/FH-TRANSIT-008.json) replaced the
    // population_potentially_served expr's ref to
    // "indicator:usda_food_low_access_1mi" with two new refs +  one derived
    // node — "indicator:usda_food_desert", "indicator:usda_food_population",
    // and the wrapping node labeled "pex.term.lila_designated_population" —
    // in parallel with this file's own track. Without these three entries,
    // translate() (lang-toggle.js) falls back to returning the raw key
    // literal, so the analyst-register arithmetic trace would render the
    // strings "pex.term.usda_food_desert" etc. verbatim in both languages
    // instead of a label — the exact ES-parity failure S10/S11 exist to
    // prevent. pex.term.usda_food_low_access_1mi below is now unused (no
    // model file references it) but left in place as a harmless orphan
    // rather than deleted outside this fix's scope.
    'pex.term.usda_food_low_access_1mi': { en: 'people with low food access (1 mile), measured', es: 'personas con bajo acceso a alimentos (1 milla), medido' },
    'pex.term.usda_food_desert': { en: 'share of the population in a USDA-designated low-income, low-access area, measured', es: 'porcentaje de la población en una zona designada por el USDA como de bajos ingresos y bajo acceso, medido' },
    'pex.term.usda_food_population': { en: 'total population, measured', es: 'población total, medida' },
    'pex.term.lila_designated_population': { en: 'population living in a USDA-designated low-income, low-access area', es: 'población que vive en una zona designada por el USDA como de bajos ingresos y bajo acceso' },

    // ── refusals (page-level and model-level, closed set observed live) ──
    'pex.refusal.no_causal_claim': { en: 'PDI cannot show that this policy caused, or would cause, any outcome. The data has no before-and-after.', es: 'PDI no puede mostrar que esta política causó, o causaría, ningún resultado. Los datos no tienen un antes y después.' },
    'pex.refusal.no_time_series': { en: 'Every figure on this page is a single snapshot in time — there is no trend line anywhere on this page.', es: 'Cada cifra de esta página es una sola instantánea en el tiempo — no hay ninguna línea de tendencia en esta página.' },
    'pex.refusal.no_siting_simulation': { en: 'PDI does not simulate where a store would be sited or how far anyone would travel to it.', es: 'PDI no simula dónde se ubicaría una tienda ni qué tan lejos viajaría alguien hasta ella.' },
    'pex.refusal.no_food_desert_binary': { en: 'This page never labels a place a “food desert.” The underlying figure is a population share, not a yes/no flag — see the map note below.', es: 'Esta página nunca etiqueta un lugar como “desierto alimentario”. La cifra subyacente es un porcentaje de población, no una bandera de sí/no — vea la nota del mapa abajo.' },
    'pex.refusal.assumption_not_recommendation': { en: 'The starting numbers on the sliders below are not PDI’s recommendation. They are stated starting points you can and should change.', es: 'Los números iniciales de los controles abajo no son una recomendación de PDI. Son puntos de partida declarados que puedes y debes cambiar.' },

    // ── caveats ──
    'pex.caveat.usda_low_access_not_designated_flag': { en: 'The “people with low food access” figure used in your own-numbers panel is USDA’s low-access count, which is broader than the designated-tract flag shown on the map above.', es: 'La cifra de “personas con bajo acceso a alimentos” usada en el panel de tus propios números es el conteo de bajo acceso del USDA, más amplio que la bandera de sector designado que se muestra arriba en el mapa.' },
    'pex.caveat.vintage_collision_county_rollup': { en: 'This county-level total combines a 2010-vintage USDA designation with 2020-vintage population counts through a population-weighted crosswalk. See “Where every number came from.”', es: 'Este total a nivel de condado combina una designación del USDA de la versión 2010 con conteos de población de la versión 2020 mediante un cruce ponderado por población. Vea “De dónde viene cada número”.' },

    // ── missing_reason sentences (closed set, contract §1.1) ──
    'pex.missing.not_loaded': { en: 'PDI has not loaded this figure for this geography.', es: 'PDI no ha cargado esta cifra para esta zona.' },
    'pex.missing.suppressed': { en: 'The publisher withheld this figure, typically to protect a small population from being identified.', es: 'El publicador retuvo esta cifra, normalmente para proteger a una población pequeña de ser identificada.' },
    'pex.missing.withheld': { en: 'PDI is refusing to publish this figure because it would not be defensible at this geography.', es: 'PDI se niega a publicar esta cifra porque no sería defendible en esta zona.' },
    'pex.missing.below_coverage_threshold': { en: 'Too little of the underlying data was available to publish a reliable total.', es: 'Había muy pocos datos subyacentes disponibles para publicar un total confiable.' },
    'pex.missing.division_by_zero': { en: 'This figure would require dividing by zero, so PDI is not showing a number.', es: 'Esta cifra requeriría dividir entre cero, así que PDI no muestra un número.' },
    'pex.missing.non_finite': { en: 'This calculation did not produce a valid number.', es: 'Este cálculo no produjo un número válido.' },
    'pex.missing.unresolved_ref': { en: 'This figure refers to something PDI could not find. This is a model-authoring error, not a data gap.', es: 'Esta cifra hace referencia a algo que PDI no pudo encontrar. Es un error de autoría del modelo, no una brecha de datos.' },
    'pex.missing.upstream_missing': { en: 'One of the numbers this figure depends on is missing.', es: 'Falta uno de los números de los que depende esta cifra.' },
    'pex.missing.no_model': { en: 'This policy has no interactive model yet.', es: 'Esta política todavía no tiene un modelo interactivo.' },
    'pex.missing.geo_level_unsupported': { en: 'This model does not support the requested geography level.', es: 'Este modelo no admite el nivel de zona geográfica solicitado.' },

    // ── absence sentences (whole-tier, distinct phrasing from a single-value miss) ──
    'pex.absence.not_loaded': { en: 'PDI has no data loaded for this tier on this policy.', es: 'PDI no tiene datos cargados para este nivel en esta política.' },
    'pex.absence.no_model': { en: 'This policy has no interactive “try your own numbers” model yet.', es: 'Esta política todavía no tiene un modelo interactivo de “prueba tus propios números”.' },
    'pex.absence.suppressed': { en: 'The publisher withheld the data this tier would need.', es: 'El publicador retuvo los datos que este nivel necesitaría.' },
    'pex.absence.withheld': { en: 'PDI is refusing to publish this tier because it would not be defensible.', es: 'PDI se niega a publicar este nivel porque no sería defendible.' },
    'pex.absence.below_coverage_threshold': { en: 'Too little underlying data was available for this tier.', es: 'Había muy pocos datos subyacentes disponibles para este nivel.' },
    'pex.absence.geo_level_unsupported': { en: 'This tier does not support the requested geography level.', es: 'Este nivel no admite el nivel de zona geográfica solicitado.' },

    // ── standing note ──
    'pex.note.original_english': { en: '', es: 'Texto original en inglés, tal como lo escribió la campaña:' },

    // ── pilot title override (ships with the pilot per contract §S10 exception 2) ──
    'pex.policy.fh_transit_008.title': { en: '', es: 'Abarrotes públicos' },

    // ── claim check ──
    'pex.claimcheck.no_claim': { en: 'The candidate’s platform does not state a specific measurable claim for this policy. There is nothing here for PDI to check.', es: 'La plataforma del candidato no plantea una afirmación medible específica para esta política. No hay nada aquí para que PDI verifique.' },
    'pex.claimcheck.the_claim': { en: 'The candidate’s stated claim:', es: 'La afirmación declarada del candidato:' },
    'pex.claimcheck.no_evidence_yet': { en: 'No external evidence has been reviewed against this claim yet.', es: 'Aún no se ha revisado evidencia externa frente a esta afirmación.' },
    'pex.claimcheck.related_rows': { en: 'Evidence rows that bear on this claim:', es: 'Filas de evidencia que se relacionan con esta afirmación:' },

    // ── evidence enums ──
    'pex.evidence.direction.increase': { en: 'increase', es: 'aumento' },
    'pex.evidence.direction.decrease': { en: 'decrease', es: 'disminución' },
    'pex.evidence.direction.no_change': { en: 'no change', es: 'sin cambio' },
    'pex.evidence.direction.mixed': { en: 'mixed', es: 'mixto' },
    'pex.evidence.direction.not_reported': { en: 'not reported', es: 'no reportado' },
    'pex.evidence.claim_check.supports': { en: 'supports the claim', es: 'respalda la afirmación' },
    'pex.evidence.claim_check.complicates': { en: 'complicates the claim', es: 'complica la afirmación' },
    'pex.evidence.claim_check.contradicts': { en: 'contradicts the claim', es: 'contradice la afirmación' },
    'pex.evidence.claim_check.not_applicable': { en: 'not applicable to the claim', es: 'no aplica a la afirmación' },
    'pex.evidence.study_design.rct': { en: 'randomized controlled trial', es: 'ensayo controlado aleatorizado' },
    'pex.evidence.study_design.quasi_experimental': { en: 'quasi-experimental', es: 'cuasiexperimental' },
    'pex.evidence.study_design.pre_post': { en: 'before/after comparison', es: 'comparación antes/después' },
    'pex.evidence.study_design.descriptive': { en: 'descriptive', es: 'descriptivo' },
    'pex.evidence.study_design.case_study': { en: 'case study', es: 'estudio de caso' },
    'pex.evidence.study_design.review': { en: 'literature review', es: 'revisión de literatura' },
    'pex.evidence.study_design.program_report': { en: 'program report', es: 'informe de programa' },
    'pex.evidence.strength.strong': { en: 'strong evidence', es: 'evidencia sólida' },
    'pex.evidence.strength.moderate': { en: 'moderate evidence', es: 'evidencia moderada' },
    'pex.evidence.strength.limited': { en: 'limited evidence', es: 'evidencia limitada' },
    'pex.evidence.strength.contested': { en: 'contested evidence', es: 'evidencia disputada' },
    'pex.evidence.geo_scale.city': { en: 'city', es: 'ciudad' },
    'pex.evidence.geo_scale.county': { en: 'county', es: 'condado' },
    'pex.evidence.geo_scale.state': { en: 'state', es: 'estado' },
    'pex.evidence.geo_scale.national': { en: 'national', es: 'nacional' },
    'pex.evidence.geo_scale.other': { en: 'other', es: 'otro' },
    'pex.evidence.ongoing': { en: 'ongoing', es: 'en curso' },
    'pex.evidence.transfer_note_label': { en: 'Does this transfer here?', es: '¿Esto aplica aquí?' },
    'pex.evidence.bears_on_claim': { en: 'Bears on the candidate’s claim', es: 'Se relaciona con la afirmación del candidato' },
    'pex.evidence.reviewed_pending': { en: 'review pending', es: 'revisión pendiente' },

    // ── units ──
    'pex.unit.people': { en: 'people', es: 'personas' },
    'pex.unit.households': { en: 'households', es: 'hogares' },
    'pex.unit.miles': { en: 'mi', es: 'mi' },
    'pex.unit.days': { en: 'days', es: 'días' },
    'pex.unit.per_year_suffix': { en: '/yr', es: '/año' },

    // ── generic UI chrome this page invents ──
    'pex.ui.show_details': { en: 'Show the numbers behind this', es: 'Mostrar los números detrás de esto' },
    'pex.ui.hide_details': { en: 'Hide the numbers behind this', es: 'Ocultar los números detrás de esto' },
    'pex.ui.projection_badge': { en: 'PROJECTION', es: 'PROYECCIÓN' },
    'pex.ui.measurement_badge': { en: 'MEASURED', es: 'MEDIDO' },
    'pex.ui.projection_disclaimer': { en: 'Every number below is what your assumptions add up to — not a finding, not a prediction. Change a slider and the arithmetic changes with it.', es: 'Cada número de abajo es lo que suman tus supuestos — no un hallazgo, no una predicción. Cambia un control y la aritmética cambia con él.' },
    'pex.ui.starting_numbers_note': { en: 'Showing the starting numbers. Move a control and recalculate.', es: 'Mostrando los números iniciales. Mueve un control y recalcula.' },
    'pex.ui.recalculating': { en: 'Recalculating…', es: 'Recalculando…' },
    'pex.ui.error_generic': { en: 'This didn’t load. Try again.', es: 'Esto no cargó. Inténtalo de nuevo.' },
    'pex.ui.retry': { en: 'Retry', es: 'Reintentar' },
    'pex.ui.policy_not_found': { en: 'PDI doesn’t have a policy by that ID.', es: 'PDI no tiene una política con ese ID.' },
    'pex.ui.search_placeholder': { en: 'Search policy positions…', es: 'Buscar posiciones de política…' },
    'pex.ui.candidate_all': { en: 'All candidates', es: 'Todos los candidatos' },
    'pex.ui.category_all': { en: 'All categories', es: 'Todas las categorías' },
    'pex.ui.no_results': { en: 'No policy positions match.', es: 'Ninguna posición de política coincide.' },
    'pex.ui.per_area_heading': { en: 'By census tract', es: 'Por sector censal' },
    'pex.ui.designated_ids_heading': { en: 'USDA-designated tracts (2010 vintage)', es: 'Sectores designados por el USDA (versión 2010)' },
    'pex.ui.map_unavailable': { en: 'Map unavailable — see the table below.', es: 'Mapa no disponible — vea la tabla abajo.' },
    'pex.ui.geo_source_model_default': { en: 'You didn’t specify a place, so this is the pilot geography this model was built for: {place}.', es: 'No especificaste un lugar, así que esta es la zona piloto para la que se construyó este modelo: {place}.' },
    'pex.ui.geo_source_state_fallback': { en: 'You didn’t specify a place, so this shows Wisconsin statewide.', es: 'No especificaste un lugar, así que esto muestra todo el estado de Wisconsin.' },
    'pex.ui.print': { en: 'Print / save this brief', es: 'Imprimir / guardar este informe' },
    'pex.ui.copy_link': { en: 'Copy link to these numbers', es: 'Copiar enlace a estos números' },
    'pex.ui.link_copied': { en: 'Link copied.', es: 'Enlace copiado.' },
    'pex.ui.attribution_from': { en: 'From the platform of', es: 'De la plataforma de' },
    'pex.ui.bill': { en: 'Bill', es: 'Proyecto de ley' },
    'pex.ui.office': { en: 'Running for', es: 'Se postula para' },
    'pex.ui.assembled_at': { en: 'This page was assembled at', es: 'Esta página se ensambló el' },
    'pex.ui.not_a_vintage': { en: '(when the arithmetic ran — not when the world was measured)', es: '(cuando se ejecutó el cálculo — no cuando se midió el mundo)' },
    'pex.ui.vintage': { en: 'Vintage', es: 'Versión' },
    'pex.ui.method': { en: 'Method', es: 'Método' },
    'pex.ui.comparison_unavailable': { en: 'Comparing two policies side by side isn’t built yet. Showing the first one.', es: 'Comparar dos políticas lado a lado aún no está disponible. Mostrando la primera.' },
    'pex.ui.what_is_absent': { en: 'What isn’t here', es: 'Lo que no está aquí' },
    'pex.ui.standing_refusals': { en: 'What this page never does', es: 'Lo que esta página nunca hace' },
    'pex.ui.model_caveats': { en: 'Standing caveats on these numbers', es: 'Advertencias permanentes sobre estos números' },
    'pex.ui.arithmetic_trace': { en: 'Show the arithmetic', es: 'Mostrar la aritmética' },
    'pex.ui.measured_inputs': { en: 'Measured figures behind this panel', es: 'Cifras medidas detrás de este panel' }
  };

  if (window.LangToggle) { window.LangToggle.addStrings(STRINGS); }

  function humanizeKey(key) {
    if (!key) { return ''; }
    var parts = key.split('.');
    var last = parts[parts.length - 1];
    return last.replace(/_/g, ' ');
  }
  // For a raw ref id (contract §1.2's namespaced Expr.Ref form, e.g.
  // "indicator:usda_food_low_access_1mi") rather than an i18n dotted key.
  // Used as a display fallback only when the server sent an empty
  // label_key on a Value (observed live on EvalResult.Inputs — see
  // integrator report) — never as a substitute for a real translation.
  function humanizeRefId(id) {
    if (!id) { return ''; }
    return String(id).replace(/^(indicator|param|output|reach|const):/, '').replace(/_/g, ' ');
  }
  function tOr(key, fallback) {
    if (!key) { return fallback || ''; }
    var v = t(key);
    return (v === key) ? (fallback || humanizeKey(key)) : v;
  }
  function fill(templateKey, vars) {
    var s = tOr(templateKey, templateKey);
    for (var k in vars) { s = s.split('{' + k + '}').join(vars[k]); }
    return s;
  }

  // ── number formatting (contract S16 — server declares value+precision,
  // client applies locale + unit presentation only; precision itself is
  // NEVER chosen here) ──────────────────────────────────────────────────
  var UNIT_SUFFIX_KEY = { people: 'pex.unit.people', households: 'pex.unit.households', miles: 'pex.unit.miles', days: 'pex.unit.days' };

  function fmtValue(num, unit, precision) {
    if (num === null || num === undefined) { return null; }
    var p = (typeof precision === 'number') ? precision : 0;
    var loc = locale();
    if (unit === 'share') {
      return new Intl.NumberFormat(loc, { style: 'percent', minimumFractionDigits: p, maximumFractionDigits: p }).format(num);
    }
    if (unit === 'percent_points') {
      return new Intl.NumberFormat(loc, { minimumFractionDigits: p, maximumFractionDigits: p }).format(num) + '%';
    }
    if (unit === 'usd' || unit === 'usd_per_year') {
      var s = new Intl.NumberFormat(loc, { style: 'currency', currency: 'USD', minimumFractionDigits: p, maximumFractionDigits: p }).format(num);
      return unit === 'usd_per_year' ? s + t('pex.unit.per_year_suffix') : s;
    }
    var plain = new Intl.NumberFormat(loc, { minimumFractionDigits: p, maximumFractionDigits: p }).format(num);
    var suffixKey = UNIT_SUFFIX_KEY[unit];
    return suffixKey ? plain + ' ' + t(suffixKey) : plain;
  }

  function fmtDateTime(iso) {
    try {
      var d = new Date(iso);
      return new Intl.DateTimeFormat(locale(), { dateStyle: 'medium', timeStyle: 'short' }).format(d);
    } catch (e) { return iso; }
  }

  // ── STATE ────────────────────────────────────────────────────────────
  var STATE = {
    mode: null,           // 'chooser' | 'brief' | 'error'
    policyId: null,
    geoid: null,
    level: null,
    requestedComparison: false,
    brief: null,
    briefError: null,
    simResult: null,      // last EvalResult
    simPending: false,
    simError: null,
    paramValues: {},       // id -> number, current control state
    analystOpen: false
  };

  try { STATE.analystOpen = window.localStorage.getItem('pex-analyst-open') === '1'; } catch (e) { /* private mode */ }

  // ── sources dictionary (for enriching tier-2 outputs, whose own
  // EvalResult.sources[] ships empty — see integrator report; every
  // output Value still carries source_ids, and the brief's own sources[]
  // has the full SourceRef records, so this page joins them client-side
  // rather than inventing a second source of truth) ─────────────────────
  function sourceDict(brief) {
    var d = {};
    (brief.sources || []).forEach(function (s) { d[s.source_id] = s; });
    if (brief.reach && brief.reach.sources) {
      brief.reach.sources.forEach(function (s) { if (!d[s.source_id]) { d[s.source_id] = s; } });
    }
    return d;
  }

  // ═══════════════════════════════════════════════════════════════════
  //  RENDERING — value primitives
  // ═══════════════════════════════════════════════════════════════════

  // A wrapping sentence (S7), never a nowrap badge pill — see policy.html's
  // .pex-reliability-note comment for why that distinction is load-bearing.
  function reliabilityBadge(rel) {
    if (!rel) { return null; }
    var cls = 'pex-reliability-note pex-rel-' + rel;
    return el('span', { class: cls, text: tOr('pex.reliability.' + rel) });
  }

  // The ONE place that turns a Value's claim_class into a badge (FIX 1 —
  // this used to also exist, hardcoded to PROJECTION, at the tier-2 output
  // card call site below; that copy is gone, this is the only decision
  // point left, and it reads value.claim_class rather than assuming it).
  function claimBadge(value) {
    var isProj = value.claim_class === 'projection';
    return el('span', {
      class: 'pex-badge ' + (isProj ? 'pex-badge-projection' : 'pex-badge-measured'),
      text: tOr(isProj ? 'pex.ui.projection_badge' : 'pex.ui.measurement_badge')
    });
  }

  // Renders one experiment.Value as a labeled line. opts.big affects only
  // the badge/border shown — never whether the number is shown, and never
  // the number itself.
  function renderValue(value, opts) {
    opts = opts || {};
    var wrap = el('div', { class: 'pex-vline' + (opts.big ? ' pex-vline-big' : '') });
    var top = el('div', { class: 'pex-vline-top' });
    var label = txt('span', 'pex-vline-label', tOr(value.label_key, opts.fallbackLabel || value.id));
    top.appendChild(label);
    if (opts.showClaimBadge) { top.appendChild(claimBadge(value)); }
    wrap.appendChild(top);

    var numRow = el('div', { class: 'pex-vline-num' });
    if (value.value === null || value.value === undefined) {
      numRow.appendChild(txt('span', 'pex-missing-dash', '—'));
      var reason = value.missing_reason;
      if (reason) {
        numRow.appendChild(txt('span', 'pex-missing-note', tOr('pex.missing.' + reason)));
      }
    } else {
      numRow.appendChild(txt('span', 'pex-vline-value', fmtValue(value.value, value.unit, value.precision)));
      if (value.reliability) {
        var relLine = el('div', { class: 'pex-vline-reliability' });
        var b = reliabilityBadge(value.reliability);
        if (b) { relLine.appendChild(b); }
        numRow.appendChild(relLine);
      }
    }
    wrap.appendChild(numRow);

    if (STATE.analystOpen) {
      var analyst = el('div', { class: 'pex-analyst-block', 'data-pex-register': 'analyst' });
      if (value.vintage) { analyst.appendChild(metaLine(tOr('pex.ui.vintage'), value.vintage)); }
      if (typeof value.moe === 'number') { analyst.appendChild(metaLine('MOE', fmtValue(value.moe, value.unit, value.precision))); }
      if (value.measured_inputs && value.measured_inputs.length) {
        analyst.appendChild(metaLine(tOr('pex.ui.measured_inputs'), value.measured_inputs.join(', ')));
      }
      if (value.missing_detail) { analyst.appendChild(metaLine('detail', value.missing_detail)); }
      if (analyst.children.length) { wrap.appendChild(analyst); }
    }
    return wrap;
  }

  function metaLine(label, val) {
    var l = el('div', { class: 'pex-meta-line' });
    l.appendChild(txt('span', 'pex-meta-label', label + ':'));
    l.appendChild(txt('span', 'pex-meta-val', String(val)));
    return l;
  }

  // ═══════════════════════════════════════════════════════════════════
  //  PANEL: header / lede
  // ═══════════════════════════════════════════════════════════════════

  function renderHeader(root, brief) {
    var place = brief.geography.name;
    var titleOverrideKey = 'pex.policy.' + slug(brief.policy.policy_id) + '.title';
    var overrideTitle = t(titleOverrideKey);
    var showOriginalTitle = (lang() === 'es' && overrideTitle && overrideTitle !== titleOverrideKey);
    var displayTitle = showOriginalTitle ? overrideTitle : brief.policy.title;

    document.title = fill('pex.h1.title_place', { title: displayTitle, place: place }) + ' ' + tOr('pex.title.suffix');

    var h1 = qs('#pex-h1', root);
    clear(h1);
    h1.appendChild(document.createTextNode(displayTitle + ' — ' + place));

    var lede = qs('#pex-lede', root);
    clear(lede);
    var descWrap = el('p', { class: 'pex-lede-desc', lang: 'en' });
    descWrap.appendChild(document.createTextNode(brief.policy.description));
    lede.appendChild(descWrap);
    if (lang() === 'es') {
      var note = txt('p', 'pex-original-en-note', tOr('pex.note.original_english'));
      lede.insertBefore(note, descWrap);
    }

    var geoNote = qs('#pex-geo-source-note', root);
    clear(geoNote);
    if (brief.geography.source === 'model_default') {
      geoNote.appendChild(txt('p', 'pex-callout-info', fill('pex.ui.geo_source_model_default', { place: place })));
    } else if (brief.geography.source === 'state_fallback') {
      geoNote.appendChild(txt('p', 'pex-callout-info', tOr('pex.ui.geo_source_state_fallback')));
    }

    var backLink = qs('#pex-back-link', root);
    if (backLink) { backLink.textContent = tOr('pex.nav.back'); }
  }

  function slug(policyId) { return String(policyId || '').toLowerCase().replace(/-/g, '_'); }

  // ═══════════════════════════════════════════════════════════════════
  //  PANEL: Tier 1 — Reach
  // ═══════════════════════════════════════════════════════════════════

  function renderReach(root, brief) {
    var section = qs('#pex-reach', root);
    if (!brief.reach) { section.hidden = true; return; }
    section.hidden = false;
    var reach = brief.reach;

    var ruleBox = qs('#pex-reach-rule', root);
    clear(ruleBox);
    ruleBox.appendChild(txt('p', 'pex-rule-statement', tOr(reach.rule.statement_key)));
    ruleBox.appendChild(txt('p', 'pex-rule-authority', '— ' + tOr(reach.rule.authority_key)));

    // headline stats
    var statsBox = qs('#pex-reach-stats', root);
    clear(statsBox);
    reach.totals.forEach(function (v) {
      var card = el('div', { class: 'pex-stat-card' });
      if (v.id === 'reach.designated_share') {
        card.appendChild(txt('div', 'pex-stat-sentence', fill('pex.reach.share_sentence', { place: brief.geography.name })));
      } else {
        card.appendChild(txt('div', 'pex-stat-sentence', tOr(v.label_key)));
      }
      if (v.value === null) {
        card.appendChild(txt('div', 'pex-stat-value pex-missing-dash', '—'));
        card.appendChild(txt('div', 'pex-missing-note', tOr('pex.missing.' + v.missing_reason)));
      } else {
        card.appendChild(txt('div', 'pex-stat-value', fmtValue(v.value, v.unit, v.precision)));
        if (v.reliability) {
          var b = reliabilityBadge(v.reliability);
          if (b) { card.appendChild(b); }
        }
      }
      if (STATE.analystOpen && (v.vintage || (v.source_ids && v.source_ids.length))) {
        var meta = el('div', { class: 'pex-analyst-block', 'data-pex-register': 'analyst' });
        if (v.vintage) { meta.appendChild(metaLine(tOr('pex.ui.vintage'), v.vintage)); }
        statsBox.appendChild(card);
        card.appendChild(meta);
        return;
      }
      statsBox.appendChild(card);
    });

    // map
    initReachMap(qs('#pex-reach-map', root), reach.areas, brief.geography.geoid);

    // per-area analyst table
    var tableWrap = qs('#pex-reach-areas', root);
    clear(tableWrap);
    if (STATE.analystOpen) {
      tableWrap.hidden = false;
      tableWrap.appendChild(txt('h3', 'pex-subheading', tOr('pex.ui.per_area_heading')));
      var table = el('table', { class: 'data-table pex-scroll-table' });
      var thead = el('thead');
      var hr = el('tr');
      ['GEOID', tOr('pex.output.designated_share.label'), tOr('pex.output.tract_snap.label'), tOr('pex.output.tract_population.label')].forEach(function (h) {
        hr.appendChild(txt('th', null, h));
      });
      thead.appendChild(hr);
      table.appendChild(thead);
      var tbody = el('tbody');
      reach.areas.forEach(function (a) {
        var row = el('tr');
        row.appendChild(txt('td', null, a.geoid + ' (' + a.name + ')'));
        row.appendChild(txt('td', 'num', a.designated_share === null ? '—' : fmtValue(a.designated_share, 'share', 0)));
        var byId = {};
        (a.values || []).forEach(function (v) { byId[v.label_key] = v; });
        var snap = (a.values || []).filter(function (v) { return v.label_key === 'pex.output.tract_snap.label'; })[0];
        var pop = (a.values || []).filter(function (v) { return v.label_key === 'pex.output.tract_population.label'; })[0];
        row.appendChild(txt('td', 'num', snap && snap.value !== null ? fmtValue(snap.value, snap.unit, snap.precision) : '—'));
        row.appendChild(txt('td', 'num', pop && pop.value !== null ? fmtValue(pop.value, pop.unit, pop.precision) : '—'));
        tbody.appendChild(row);
      });
      table.appendChild(tbody);
      var scrollWrap = el('div', { class: 'pex-table-scroll' }, [table]);
      tableWrap.appendChild(scrollWrap);

      var idsBox = el('div', { class: 'pex-designated-ids' });
      idsBox.appendChild(txt('h3', 'pex-subheading', tOr('pex.ui.designated_ids_heading')));
      idsBox.appendChild(txt('p', 'pex-mono-list', reach.designated_ids.join(', ')));
      tableWrap.appendChild(idsBox);
    } else {
      tableWrap.hidden = true;
    }
  }

  var _mapInstance = null;
  var _mapReqToken = 0;
  function initReachMap(container, areas, requestedGeoid) {
    if (!container) { return; }
    var token = ++_mapReqToken;
    clear(container);
    if (!window.L || !areas || !areas.length) {
      container.appendChild(txt('p', 'pex-callout-info', tOr('pex.ui.map_unavailable')));
      return;
    }
    var mapHost = el('div', { class: 'pex-leaflet-host', role: 'img', 'aria-label': tOr('pex.map.designated_share.legend') });
    container.appendChild(mapHost);
    var legend = txt('p', 'pex-map-legend', tOr('pex.map.designated_share.legend'));
    container.appendChild(legend);

    var byGeoid = {};
    areas.forEach(function (a) { byGeoid[a.geoid] = a; });

    fetch(TRACTS_URL).then(function (r) { return r.json(); }).then(function (fc) {
      if (token !== _mapReqToken) { return; }
      var feats = fc.features.filter(function (f) {
        var g = (f.properties && f.properties.GEOID || '').toString().trim();
        return Object.prototype.hasOwnProperty.call(byGeoid, g);
      });
      if (!feats.length) {
        container.appendChild(txt('p', 'pex-callout-info', tOr('pex.ui.map_unavailable')));
        return;
      }
      if (_mapInstance) { try { _mapInstance.remove(); } catch (e) { /* noop */ } _mapInstance = null; }
      var map = window.L.map(mapHost, { scrollWheelZoom: false, attributionControl: false, zoomControl: true });
      _mapInstance = map;
      var layer = window.L.geoJSON({ type: 'FeatureCollection', features: feats }, {
        style: function (f) {
          var geoid = (f.properties.GEOID || '').toString().trim();
          var a = byGeoid[geoid];
          var share = a && a.designated_share !== null && a.designated_share !== undefined ? a.designated_share : 0;
          var color = (window.CasaDatosChoropleth && window.CasaDatosChoropleth.sequentialColor(share, '--tp-cream', '--tp-ocean').css) || '#888';
          return { fillColor: color, fillOpacity: 0.82, color: '#22303c', weight: 1 };
        },
        onEachFeature: function (f, lyr) {
          var geoid = (f.properties.GEOID || '').toString().trim();
          var a = byGeoid[geoid];
          var pct = (a && a.designated_share !== null && a.designated_share !== undefined) ? fmtValue(a.designated_share, 'share', 0) : '—';
          lyr.bindTooltip((f.properties.NAME || geoid) + ': ' + pct, { sticky: true });
        }
      }).addTo(map);
      try { map.fitBounds(layer.getBounds(), { padding: [10, 10] }); } catch (e) { /* single-feature edge case */ }
    }).catch(function () {
      if (token !== _mapReqToken) { return; }
      container.appendChild(txt('p', 'pex-callout-info', tOr('pex.ui.map_unavailable')));
    });
  }

  // ═══════════════════════════════════════════════════════════════════
  //  PANEL: Tier 2 — Experiment
  // ═══════════════════════════════════════════════════════════════════

  function renderExperimentShell(root, brief) {
    var section = qs('#pex-experiment', root);
    if (!brief.model) { section.hidden = true; return; }
    section.hidden = false;
    var model = brief.model;

    qs('#pex-experiment-title', root).textContent = tOr(model.title_key);
    qs('#pex-experiment-summary', root).textContent = tOr(model.summary_key);

    var controls = qs('#pex-experiment-controls', root);
    clear(controls);
    model.params.forEach(function (p) {
      if (!(p.id in STATE.paramValues)) { STATE.paramValues[p.id] = p.default; }
      controls.appendChild(renderParamControl(p));
    });

    var disclaimer = qs('#pex-experiment-disclaimer', root);
    disclaimer.textContent = tOr('pex.ui.projection_disclaimer');
  }

  function renderParamControl(p) {
    var wrap = el('div', { class: 'pex-param', 'data-param-id': p.id });
    var labelRow = el('div', { class: 'pex-param-label-row' });
    labelRow.appendChild(txt('label', 'pex-param-label', tOr(p.label_key)));
    var readout = txt('span', 'pex-param-readout', fmtValue(STATE.paramValues[p.id], p.unit, p.precision));
    readout.setAttribute('data-param-readout', p.id);
    labelRow.appendChild(readout);
    wrap.appendChild(labelRow);

    wrap.appendChild(txt('p', 'pex-param-help', tOr(p.help_key)));

    var inputRow = el('div', { class: 'pex-param-input-row' });
    var range = el('input', {
      type: 'range', min: p.min, max: p.max, step: p.step, value: STATE.paramValues[p.id],
      class: 'pex-param-range', 'aria-label': tOr(p.label_key)
    });
    range.addEventListener('input', function () { onParamInput(p, parseFloat(range.value)); });
    range.addEventListener('change', function () { onParamCommit(); });
    inputRow.appendChild(range);

    var num = el('input', {
      type: 'number', min: p.min, max: p.max, step: p.step, value: STATE.paramValues[p.id],
      class: 'pex-param-number', 'aria-label': tOr(p.label_key)
    });
    num.addEventListener('change', function () {
      var v = parseFloat(num.value);
      if (isNaN(v)) { v = p.default; }
      v = Math.min(p.max, Math.max(p.min, v));
      onParamInput(p, v, true);
      onParamCommit();
    });
    inputRow.appendChild(num);
    wrap.appendChild(inputRow);

    wrap.appendChild(txt('p', 'pex-param-source', tOr(p.default_source_key)));
    return wrap;
  }

  var _recalcTimer = null;
  function onParamInput(p, v, syncBoth) {
    STATE.paramValues[p.id] = v;
    var root = document;
    var readout = qs('[data-param-readout="' + p.id + '"]', root);
    if (readout) { readout.textContent = fmtValue(v, p.unit, p.precision); }
    if (!syncBoth) {
      var num = qs('.pex-param[data-param-id="' + p.id + '"] .pex-param-number', root);
      if (num) { num.value = v; }
    } else {
      var range = qs('.pex-param[data-param-id="' + p.id + '"] .pex-param-range', root);
      if (range) { range.value = v; }
    }
  }
  function onParamCommit() {
    if (_recalcTimer) { clearTimeout(_recalcTimer); }
    _recalcTimer = setTimeout(runSimulate, 250);
  }

  function runSimulate() {
    if (!STATE.brief || !STATE.brief.model) { return; }
    STATE.simPending = true;
    STATE.simError = null;
    renderExperimentOutputs(document, STATE.brief);
    var body = { policy_id: STATE.policyId, geoid: STATE.geoid || STATE.brief.geography.geoid, level: STATE.level || STATE.brief.geography.level, params: STATE.paramValues };
    fetchJSON(API_BASE + '/simulate', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
    }).then(function (res) {
      STATE.simPending = false;
      // STATE.simError holds an i18n KEY, never resolved text and never the
      // backend's own English prose — see errorKeyFromResponse above. It is
      // resolved with tOr() at render time so a language toggle re-renders
      // it correctly even without a new request.
      if (!res.ok || !res.body) { STATE.simError = errorKeyFromResponse(res); }
      else { STATE.simResult = res.body; updateShareLink(); }
      renderExperimentOutputs(document, STATE.brief);
    }).catch(function () {
      STATE.simPending = false;
      STATE.simError = 'pex.ui.error_generic';
      renderExperimentOutputs(document, STATE.brief);
    });
  }

  function resetParams() {
    if (!STATE.brief || !STATE.brief.model) { return; }
    STATE.brief.model.params.forEach(function (p) { STATE.paramValues[p.id] = p.default; });
    renderExperimentShell(document, STATE.brief);
    wireExperimentButtons(document);
    runSimulate();
  }

  function outputSourceRefs(output, dict) {
    return (output.source_ids || []).map(function (id) { return dict[id]; }).filter(Boolean);
  }

  function renderExperimentOutputs(root, brief) {
    var out = qs('#pex-experiment-outputs', root);
    clear(out);

    if (STATE.simPending) { out.appendChild(txt('p', 'pex-callout-info', tOr('pex.ui.recalculating'))); }
    if (STATE.simError) { out.appendChild(txt('p', 'state-error', tOr(STATE.simError))); }
    if (!STATE.simResult) {
      if (!STATE.simPending) { out.appendChild(txt('p', 'pex-callout-info', tOr('pex.ui.starting_numbers_note'))); }
      return;
    }
    var result = STATE.simResult;
    var dict = sourceDict(brief);

    var grid = el('div', { class: 'pex-output-grid' });
    (result.outputs || []).filter(function (o) { return o.register === 'public'; }).forEach(function (o) {
      var card = el('div', { class: 'pex-output-card' });
      card.appendChild(claimBadge(o));
      card.appendChild(txt('div', 'pex-output-label', tOr(o.label_key)));
      if (o.value === null) {
        card.appendChild(txt('div', 'pex-output-value pex-missing-dash', '—'));
        card.appendChild(txt('div', 'pex-missing-note', tOr('pex.missing.' + o.missing_reason)));
      } else {
        card.appendChild(txt('div', 'pex-output-value', fmtValue(o.value, o.unit, o.precision)));
      }
      var srcs = outputSourceRefs(o, dict);
      if (STATE.analystOpen && srcs.length) {
        var s = el('div', { class: 'pex-analyst-block', 'data-pex-register': 'analyst' });
        srcs.forEach(function (sr) { s.appendChild(metaLine(tOr('pex.ui.vintage'), sr.name + ' · ' + sr.vintage)); });
        card.appendChild(s);
      }
      grid.appendChild(card);
    });
    out.appendChild(grid);

    if (STATE.analystOpen) {
      var analystBox = el('div', { class: 'pex-analyst-block', 'data-pex-register': 'analyst' });

      if (result.inputs && result.inputs.length) {
        analystBox.appendChild(txt('h3', 'pex-subheading', tOr('pex.ui.measured_inputs')));
        result.inputs.forEach(function (v) { analystBox.appendChild(renderValue(v, { fallbackLabel: humanizeRefId(v.id) })); });
      }

      if (result.steps && result.steps.length) {
        analystBox.appendChild(txt('h3', 'pex-subheading', tOr('pex.ui.arithmetic_trace')));
        var byOutput = {};
        result.steps.forEach(function (st) { (byOutput[st.output_id] = byOutput[st.output_id] || []).push(st); });
        Object.keys(byOutput).forEach(function (outId) {
          var outMeta = (result.outputs || []).filter(function (o) { return o.id === outId; })[0];
          var group = el('div', { class: 'pex-trace-group' });
          group.appendChild(txt('div', 'pex-trace-heading', outMeta ? tOr(outMeta.label_key) : outId));
          byOutput[outId].forEach(function (st) { group.appendChild(renderStep(st)); });
          analystBox.appendChild(group);
        });
      }

      if (result.caveat_keys && result.caveat_keys.length) {
        analystBox.appendChild(txt('h3', 'pex-subheading', tOr('pex.ui.model_caveats')));
        var ul = el('ul', { class: 'pex-list' });
        result.caveat_keys.forEach(function (k) { ul.appendChild(txt('li', null, tOr(k))); });
        analystBox.appendChild(ul);
      }
      out.appendChild(analystBox);
    }
  }

  // renderStep prints the analyst arithmetic trace. It MUST route every
  // operand and the result through fmtValue — the unit/precision-aware
  // formatter — never through a hardcoded-decimals formatter. A share-typed
  // operand (unit "share", 0-1 on the wire) renders as a percentage via
  // fmtValue's unit=="share" branch; that is what lets the printed trace
  // reproduce by hand (contract §2 "Rounding": "a reader who redoes the
  // multiplication and gets a different answer has caught the page lying").
  // A prior version of this function built a correctly-formatted
  // `operandsText` via fmtValue and then never used it — the actual
  // `line.textContent` was built from a dead `trimNum(op.value)` path that
  // ignored unit and precision entirely and always printed 2 raw decimal
  // places. That is how "0.26 x 160,331 = 41,768" shipped: 0.26 is
  // usda_food_desert's raw 0-1 share truncated to 2 decimals with no percent
  // conversion, not the 26.05% the operand's declared unit and precision
  // actually call for. See docs/contracts/experiment-contract.md §2 and
  // pkg/experiment/eval.go's choosePrecision/displayScale for the backend
  // half of this fix.
  var OP_SYMBOL = { add: '+', sub: '−', mul: '×', div: '÷', min: 'min', max: 'max' };
  function renderStep(step) {
    var line = el('div', { class: 'pex-step' });
    var sym = OP_SYMBOL[step.op] || step.op;
    var text = step.operands.map(function (op) {
      var label = tOr(op.label_key, op.id);
      var num = (op.value === null) ? '—' : fmtValue(op.value, op.unit, op.precision);
      return label + ' = ' + num;
    }).join('; ') + '  →  ' + sym + '  =  ' +
      (step.result.value === null ? '—' : fmtValue(step.result.value, step.result.unit, step.result.precision));
    line.textContent = text;
    return line;
  }

  function wireExperimentButtons(root) {
    var recalcBtn = qs('#pex-recalc-btn', root);
    if (recalcBtn) { recalcBtn.onclick = runSimulate; }
    var resetBtn = qs('#pex-reset-btn', root);
    if (resetBtn) { resetBtn.onclick = resetParams; }
  }

  function updateShareLink() {
    if (!STATE.brief || !STATE.brief.model || !window.history || !window.history.replaceState) { return; }
    var url = new URL(window.location.href);
    Object.keys(STATE.paramValues).forEach(function (id) { url.searchParams.set('p.' + id, String(STATE.paramValues[id])); });
    url.searchParams.set('mv', STATE.brief.model.model_version);
    window.history.replaceState(null, '', url.toString());
  }

  // ═══════════════════════════════════════════════════════════════════
  //  PANEL: Tier 3 — Evidence
  // ═══════════════════════════════════════════════════════════════════

  function renderEvidence(root, brief) {
    var section = qs('#pex-evidence', root);
    var list = qs('#pex-evidence-list', root);
    clear(list);
    if (!brief.evidence || !brief.evidence.length) { section.hidden = true; return; }
    section.hidden = false;
    brief.evidence.forEach(function (ev) { list.appendChild(renderEvidenceCard(ev)); });
  }

  function renderEvidenceCard(ev) {
    var card = el('div', { class: 'pex-card pex-evidence-card' });
    var head = el('div', { class: 'pex-evidence-head' });
    head.appendChild(txt('span', 'pex-evidence-jurisdiction', ev.jurisdiction));
    head.appendChild(txt('span', 'pex-badge pex-badge-neutral', tOr('pex.evidence.geo_scale.' + ev.geo_scale)));
    var period = ev.period_start + '–' + (ev.period_end ? (ev.period_end === 'ongoing' ? tOr('pex.evidence.ongoing') : ev.period_end) : '');
    head.appendChild(txt('span', 'pex-evidence-period', period));
    if (ev.relates_to_claim === 'candidate_claim') {
      head.appendChild(txt('span', 'pex-badge pex-badge-info', tOr('pex.evidence.bears_on_claim')));
    }
    card.appendChild(head);

    card.appendChild(txt('p', 'pex-evidence-intervention', ev.intervention));

    var findingRow = el('div', { class: 'pex-evidence-finding' });
    findingRow.appendChild(txt('span', 'pex-badge pex-badge-neutral', tOr('pex.evidence.direction.' + ev.direction)));
    var findingText = el('p', { class: 'pex-evidence-finding-text', lang: 'en' });
    findingText.textContent = ev.finding;
    findingRow.appendChild(findingText);
    card.appendChild(findingRow);
    if (ev.effect_reported) { card.appendChild(txt('p', 'pex-evidence-effect', ev.effect_reported)); }

    var transfer = el('div', { class: 'pex-transfer-note' });
    transfer.appendChild(txt('div', 'pex-transfer-label', tOr('pex.evidence.transfer_note_label')));
    var transferText = el('p', { lang: 'en' }); transferText.textContent = ev.transfer_note;
    transfer.appendChild(transferText);
    card.appendChild(transfer);

    var srcLine = el('p', { class: 'pex-evidence-source' });
    var link = el('a', { href: ev.source_url, target: '_blank', rel: 'noopener noreferrer', text: ev.source_title + ' — ' + ev.source_org });
    srcLine.appendChild(link);
    card.appendChild(srcLine);

    if (STATE.analystOpen) {
      var analyst = el('div', { class: 'pex-analyst-block', 'data-pex-register': 'analyst' });
      analyst.appendChild(metaLine('Study design', tOr('pex.evidence.study_design.' + ev.study_design)));
      analyst.appendChild(metaLine('Evidence strength', tOr('pex.evidence.strength.' + ev.evidence_strength)));
      analyst.appendChild(metaLine('Citation', ev.citation));
      analyst.appendChild(metaLine('Reviewed by', ev.reviewed_by || tOr('pex.evidence.reviewed_pending')));
      card.appendChild(analyst);
    }
    return card;
  }

  // ═══════════════════════════════════════════════════════════════════
  //  PANEL: Claim check
  // ═══════════════════════════════════════════════════════════════════

  function renderClaimCheck(root, brief) {
    var box = qs('#pex-claimcheck-body', root);
    clear(box);
    var claim = (brief.policy.claim_empirical || '').trim();
    if (!claim) {
      box.appendChild(txt('p', 'pex-callout-info', tOr('pex.claimcheck.no_claim')));
      return;
    }
    box.appendChild(txt('p', 'pex-claim-label', tOr('pex.claimcheck.the_claim')));
    var claimText = el('blockquote', { class: 'pex-claim-text', lang: 'en' });
    claimText.textContent = claim;
    box.appendChild(claimText);
    if (lang() === 'es') { box.appendChild(txt('p', 'pex-original-en-note', tOr('pex.note.original_english'))); }

    var related = (brief.evidence || []).filter(function (ev) { return ev.relates_to_claim === 'candidate_claim'; });
    if (!related.length) {
      box.appendChild(txt('p', 'pex-callout-info', tOr('pex.claimcheck.no_evidence_yet')));
      return;
    }
    box.appendChild(txt('p', 'pex-claim-label', tOr('pex.claimcheck.related_rows')));
    var ul = el('ul', { class: 'pex-list' });
    related.forEach(function (ev) {
      var li = el('li');
      li.appendChild(document.createTextNode(ev.jurisdiction + ' — '));
      li.appendChild(el('strong', { text: tOr('pex.evidence.claim_check.' + ev.claim_check) }));
      ul.appendChild(li);
    });
    box.appendChild(ul);
  }

  // ═══════════════════════════════════════════════════════════════════
  //  PANEL: Refusals + absences
  // ═══════════════════════════════════════════════════════════════════

  function renderRefusals(root, brief) {
    var absBox = qs('#pex-absences', root);
    clear(absBox);
    if (brief.absences && brief.absences.length) {
      absBox.appendChild(txt('h3', 'pex-subheading', tOr('pex.ui.what_is_absent')));
      var ul = el('ul', { class: 'pex-list' });
      brief.absences.forEach(function (a) {
        var li = el('li');
        li.textContent = tOr(a.reason_key);
        if (STATE.analystOpen) {
          var meta = el('span', { class: 'pex-meta-inline', 'data-pex-register': 'analyst' });
          meta.textContent = ' [' + a.tier + ':' + a.reason + (a.detail ? ' — ' + a.detail : '') + ']';
          li.appendChild(meta);
        }
        ul.appendChild(li);
      });
      absBox.appendChild(ul);
    }

    var refBox = qs('#pex-refusal-list', root);
    clear(refBox);
    var keys = uniq((brief.refusal_keys || []).concat((brief.model && brief.model.refusal_keys) || []).concat((STATE.simResult && STATE.simResult.refusal_keys) || []));
    if (keys.length) {
      refBox.appendChild(txt('h3', 'pex-subheading', tOr('pex.ui.standing_refusals')));
      var ul2 = el('ul', { class: 'pex-list' });
      keys.forEach(function (k) { ul2.appendChild(txt('li', null, tOr(k))); });
      refBox.appendChild(ul2);
    }
  }
  function uniq(arr) { var seen = {}; return arr.filter(function (x) { if (seen[x]) { return false; } seen[x] = true; return true; }); }

  // ═══════════════════════════════════════════════════════════════════
  //  PANEL: Provenance
  // ═══════════════════════════════════════════════════════════════════

  function renderProvenance(root, brief) {
    var attr = qs('#pex-attribution', root);
    clear(attr);
    var p = brief.policy;
    var line1 = el('p', { class: 'pex-attribution-line' });
    line1.appendChild(document.createTextNode(tOr('pex.ui.attribution_from') + ' '));
    line1.appendChild(el('strong', { text: p.candidate }));
    line1.appendChild(document.createTextNode(' — ' + tOr('pex.ui.office') + ': ' + p.office));
    attr.appendChild(line1);
    if (p.bill_references) { attr.appendChild(txt('p', 'pex-attribution-bill', tOr('pex.ui.bill') + ': ' + p.bill_references)); }

    var sourcesBox = qs('#pex-sources', root);
    clear(sourcesBox);
    var dict = sourceDict(brief);
    Object.keys(dict).forEach(function (id) {
      var s = dict[id];
      var row = el('div', { class: 'pex-source-row' });
      var nameLine = el('p', { class: 'pex-source-name' });
      if (s.url) {
        nameLine.appendChild(el('a', { href: s.url, target: '_blank', rel: 'noopener noreferrer', text: s.name }));
      } else {
        nameLine.appendChild(document.createTextNode(s.name));
      }
      row.appendChild(nameLine);
      row.appendChild(metaLine(tOr('pex.ui.vintage'), s.vintage));
      if (s.method_key) { row.appendChild(txt('p', 'pex-source-method', tOr(s.method_key))); }
      sourcesBox.appendChild(row);
    });

    var stamp = qs('#pex-generated-at', root);
    stamp.textContent = tOr('pex.ui.assembled_at') + ' ' + fmtDateTime(brief.generated_at) + ' ' + tOr('pex.ui.not_a_vintage');
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Chooser (bare /policy, contract S2)
  // ═══════════════════════════════════════════════════════════════════

  var _chooserData = null;
  function renderChooser(root) {
    qs('#pex-brief-view', root).hidden = true;
    var chooser = qs('#pex-chooser-view', root);
    chooser.hidden = false;
    document.title = tOr('pex.chooser.h1') + ' ' + tOr('pex.title.suffix');

    var search = qs('#pex-chooser-search', root);
    var candSel = qs('#pex-chooser-candidate', root);
    var catSel = qs('#pex-chooser-category', root);

    function apply() {
      var q = (search.value || '').toLowerCase();
      var cand = candSel.value;
      var cat = catSel.value;
      var filtered = (_chooserData || []).filter(function (p) {
        if (cand && p.candidate !== cand) { return false; }
        if (cat && p.category !== cat) { return false; }
        if (q && (p.title + ' ' + p.description).toLowerCase().indexOf(q) === -1) { return false; }
        return true;
      });
      renderChooserGrid(filtered);
    }

    search.oninput = apply;
    candSel.onchange = apply;
    catSel.onchange = apply;

    if (_chooserData) { populateChooserFilters(); apply(); return; }

    fetchJSON(API_BASE + '/policies?limit=1000').then(function (res) {
      if (!res.ok || !res.body) {
        qs('#pex-chooser-grid', root).appendChild(txt('p', 'state-error', tOr('pex.ui.error_generic')));
        return;
      }
      _chooserData = res.body.policies || res.body.items || [];
      populateChooserFilters();
      apply();
    }).catch(function () {
      qs('#pex-chooser-grid', root).appendChild(txt('p', 'state-error', tOr('pex.ui.error_generic')));
    });
  }

  function populateChooserFilters() {
    var candSel = qs('#pex-chooser-candidate');
    var catSel = qs('#pex-chooser-category');
    var cands = uniq((_chooserData || []).map(function (p) { return p.candidate; })).sort();
    var cats = uniq((_chooserData || []).map(function (p) { return p.category; })).sort();
    clear(candSel); clear(catSel);
    candSel.appendChild(el('option', { value: '', text: tOr('pex.ui.candidate_all') }));
    cands.forEach(function (c) { candSel.appendChild(el('option', { value: c, text: c })); });
    catSel.appendChild(el('option', { value: '', text: tOr('pex.ui.category_all') }));
    cats.forEach(function (c) { catSel.appendChild(el('option', { value: c, text: c })); });
  }

  function renderChooserGrid(items) {
    var grid = qs('#pex-chooser-grid');
    clear(grid);
    if (!items.length) { grid.appendChild(txt('p', 'state-empty', tOr('pex.ui.no_results'))); return; }
    items.forEach(function (p) {
      var card = el('a', { class: 'pex-card pex-chooser-card', href: '/policy?id=' + encodeURIComponent(p.id) });
      card.appendChild(txt('div', 'pex-chooser-card-title', p.title));
      card.appendChild(txt('div', 'pex-chooser-card-desc', p.description));
      var meta = el('div', { class: 'pex-chooser-card-meta' });
      meta.appendChild(txt('span', 'pex-badge pex-badge-neutral', p.category));
      meta.appendChild(txt('span', 'pex-chooser-card-cand', p.candidate));
      card.appendChild(meta);
      grid.appendChild(card);
    });
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Orchestration
  // ═══════════════════════════════════════════════════════════════════

  // FIX 3 — the refusals section ("What we cannot tell you") is authored
  // in policy.html (not owned by this track) as the second-to-last
  // section: Reach, Experiment, Evidence, Claim check, Refusals,
  // Provenance — ~19,000px below the tier-2 controls whose limits it
  // states. This file owns none of that markup, but it does own what runs
  // against the live DOM, so it moves the already-rendered #pex-refusals
  // node to sit immediately after #pex-experiment (tier 2) and before
  // #pex-evidence (tier 3), once, the first time a brief is shown. It does
  // not clone or rebuild the section — same node, same ids, same content
  // functions (renderRefusals still fills it) — so contract S12 ("print
  // renders the public register plus refusals plus the full provenance
  // block") is unaffected: the print media query in policy.html selects
  // by class, not DOM position.
  var _refusalsRepositioned = false;
  function repositionRefusals(root) {
    if (_refusalsRepositioned) { return; }
    var refusals = qs('#pex-refusals', root);
    var experiment = qs('#pex-experiment', root);
    if (refusals && experiment && experiment.parentNode && refusals.parentNode === experiment.parentNode) {
      experiment.parentNode.insertBefore(refusals, experiment.nextSibling);
      _refusalsRepositioned = true;
    }
  }

  function renderBrief(root) {
    qs('#pex-chooser-view', root).hidden = true;
    var view = qs('#pex-brief-view', root);
    view.hidden = false;
    repositionRefusals(root);

    var comparisonNote = qs('#pex-comparison-note', root);
    comparisonNote.hidden = !STATE.requestedComparison;
    if (STATE.requestedComparison) { comparisonNote.textContent = tOr('pex.ui.comparison_unavailable'); }

    if (STATE.briefError) {
      qs('#pex-brief-error', root).hidden = false;
      qs('#pex-brief-error', root).textContent = tOr(STATE.briefError);
      qs('#pex-brief-content', root).hidden = true;
      qs('#pex-brief-loading', root).hidden = true;
      return;
    }
    qs('#pex-brief-error', root).hidden = true;

    if (!STATE.brief) {
      // Fetch in flight — nothing to render yet. Guard here rather than
      // letting renderHeader et al. run against a null brief, which is
      // exactly the null-deref this page must never ship (verified: it
      // threw "Cannot read properties of null (reading 'geography')"
      // before this guard existed).
      qs('#pex-brief-content', root).hidden = true;
      qs('#pex-brief-loading', root).hidden = false;
      return;
    }
    qs('#pex-brief-loading', root).hidden = true;
    qs('#pex-brief-content', root).hidden = false;

    var brief = STATE.brief;
    renderHeader(root, brief);
    renderReach(root, brief);
    renderExperimentShell(root, brief);
    wireExperimentButtons(root);
    renderExperimentOutputs(root, brief);
    renderEvidence(root, brief);
    renderClaimCheck(root, brief);
    renderRefusals(root, brief);
    renderProvenance(root, brief);

    var toggle = qs('#pex-analyst-toggle', root);
    toggle.textContent = tOr(STATE.analystOpen ? 'pex.ui.hide_details' : 'pex.ui.show_details');
    toggle.setAttribute('aria-pressed', String(STATE.analystOpen));
    document.documentElement.classList.toggle('pex-analyst-open', STATE.analystOpen);
  }

  function renderAll() {
    var root = document;
    if (STATE.mode === 'chooser') { renderChooser(root); }
    else if (STATE.mode === 'brief') { renderBrief(root); }
  }

  function loadFromLocation() {
    var idsRaw = paramFromQuery('id');
    var geoid = paramFromQuery('geoid');
    var level = paramFromQuery('level');
    STATE.geoid = geoid;
    STATE.level = level;

    if (!idsRaw) {
      STATE.mode = 'chooser';
      renderAll();
      return;
    }

    var ids = idsRaw.split(',').map(function (s) { return s.trim(); }).filter(Boolean);
    STATE.requestedComparison = ids.length > 1;
    STATE.policyId = ids[0];
    STATE.mode = 'brief';
    STATE.brief = null;
    STATE.briefError = null;
    STATE.simResult = null;
    STATE.simError = null;
    STATE.paramValues = {};

    var url = API_BASE + '/briefs/' + encodeURIComponent(STATE.policyId) + '?';
    var qparts = [];
    if (geoid) { qparts.push('geoid=' + encodeURIComponent(geoid)); }
    if (level) { qparts.push('level=' + encodeURIComponent(level)); }
    url += qparts.join('&');

    renderAll(); // show shell / loading-safe empty state immediately

    fetchJSON(url).then(function (res) {
      if (!res.ok || !res.body) {
        // Same rule as errorKeyFromResponse (FIX 2): never store the
        // backend's own English prose, even when nothing currently renders
        // it — an unused landmine is still a landmine. Key only.
        STATE.briefError = 'pex.ui.policy_not_found';
        renderAll();
        return;
      }
      STATE.brief = res.body;
      // seed param values from query string (?p.<id>=), else model defaults.
      // FIX 2, second half: a shared link can carry a value outside
      // [min, max] (a stale link after a model's bounds changed, a hand-
      // edited query string). The <input type=range> DOM element clamps
      // its own reported .value to [min, max] regardless of what we hand
      // it, but the readout span and the value this page sends to
      // /simulate were both built from the UNCLAMPED number — so the
      // slider silently disagreed with its own label, and the initial
      // simulate() call sent an out-of-range assumption the server
      // rejects with a 400 (contract §4: min/max are the server's
      // rejection bounds, never a clamp — it is correct to reject a
      // reader's own submitted value; it is not correct to have already
      // shown that reader a different number than the one being
      // submitted). Clamping the query-string value here, once, before it
      // ever becomes STATE.paramValues, is the same clamp this file
      // already applies to a hand-typed number-input value (see the
      // number input's onchange handler above) — not a new policy, the
      // existing one applied at the one place it was missing.
      if (STATE.brief.model) {
        STATE.brief.model.params.forEach(function (p) {
          var qv = paramFromQuery('p.' + p.id);
          var v = (qv !== null && qv !== '' && !isNaN(parseFloat(qv))) ? parseFloat(qv) : p.default;
          v = Math.min(p.max, Math.max(p.min, v));
          STATE.paramValues[p.id] = v;
        });
      }
      renderAll();
      if (STATE.brief.model) { runSimulate(); }
    }).catch(function () {
      STATE.briefError = 'pex.ui.policy_not_found';
      renderAll();
    });
  }

  function wireGlobalControls() {
    var toggle = qs('#pex-analyst-toggle');
    if (toggle) {
      toggle.addEventListener('click', function () {
        STATE.analystOpen = !STATE.analystOpen;
        try { window.localStorage.setItem('pex-analyst-open', STATE.analystOpen ? '1' : '0'); } catch (e) { /* private mode */ }
        renderAll();
      });
    }
    var printBtn = qs('#pex-print-btn');
    if (printBtn) { printBtn.addEventListener('click', function () { window.print(); }); }
    var copyBtn = qs('#pex-copy-link-btn');
    if (copyBtn) {
      copyBtn.addEventListener('click', function () {
        updateShareLink();
        if (navigator.clipboard) {
          navigator.clipboard.writeText(window.location.href).then(function () {
            copyBtn.textContent = tOr('pex.ui.link_copied');
            setTimeout(function () { copyBtn.textContent = tOr('pex.ui.copy_link'); }, 1600);
          });
        }
      });
    }
  }

  document.addEventListener('DOMContentLoaded', function () {
    wireGlobalControls();
    loadFromLocation();
  });
  document.addEventListener('tp:langchange', function () { renderAll(); });

  window.PolicyExplorer = { STATE: STATE, reload: loadFromLocation };
})(window, document);
