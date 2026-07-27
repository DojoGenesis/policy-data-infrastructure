/* @spine-kit i18n/bilingual-data.js · v1
   Canonical source: spine/kits/i18n/bilingual-data.js
   Paste-verbatim. Fix the spine, then re-vendor — do not edit this
   copy in place.

   ONE bilingual engine, one data array, both language halves — the
   pattern generalized from cruzromeromorales-site/app.js (PROJECTS +
   CLUSTERS, lines ~30-90) and trespiesdesign-site. ES2018, no
   modules, single IIFE (spine/README.md Authoring rule 4).

   How it works
   ------------
   - <html lang="en"|"es"> is set once, at author time, on each
     physical page half (kits/i18n/head-block.html + the twin-page
     pattern in I18N-RULES.md). Nothing in this file writes it — it
     only reads it, once, at load.
   - Any field in a data record may carry a `<field>Es` sibling —
     desc/descEs, title/titleEs, label/labelEs, href/hrefEs. L()
     prefers the Es sibling on the Spanish half and falls back to
     English when the sibling is missing, so a half-translated
     record degrades gracefully instead of rendering "undefined".
   - T(key) reads UI strings the SCRIPT ITSELF writes into the DOM
     (button labels, status words, aria text) — never page content.
     Page content belongs in the data array, read through L().
   - One array drives both language halves of the same page. Add a
     record once, with its Es siblings alongside; never fork the file.
   ═══════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  // ── Language: one switch, read once ──────────────────────────────
  const LANG = document.documentElement.lang === 'es' ? 'es' : 'en';

  // L(obj, key) — bilingual field accessor. Spanish half + a
  // `key + 'Es'` sibling present on obj → that sibling. Otherwise →
  // obj[key]. Never throws on a missing obj; never surfaces
  // "undefined" to the page.
  const L = (obj, key) => {
    if (!obj) return '';
    if (LANG === 'es' && obj[key + 'Es']) return obj[key + 'Es'];
    return obj[key] || '';
  };

  // T(key) — UI-string accessor for copy the SCRIPT writes (not the
  // data array). Add every such string here. Falls back to English,
  // then to ''.
  const UI = {
    en: {
      allItems:  'All items',
      openCard:  'Open item card.',
      closeCard: 'Close card',
      noLink:    'no public link yet — ask',
    },
    es: {
      allItems:  'Todos los elementos',
      openCard:  'Abre la ficha del elemento.',
      closeCard: 'Cerrar ficha',
      noLink:    'todavía no hay enlace público — pregunta',
    },
  };
  const T = key => (UI[LANG] && UI[LANG][key]) || UI.en[key] || '';

  /* ── Worked example — one record, both languages ──────────────────
     Add a record = add one object, its Es siblings alongside. This
     is the whole trick: no second template, no second render path,
     no per-language build step. Delete this array; keep the engine
     (LANG/L/T) and point it at your own data. */
  const ITEMS = [
    { id: 'example-item',
      title: 'Example item', titleEs: 'Elemento de ejemplo',
      desc: 'Rendered from one array — L() reads the language half.',
      descEs: 'Generado desde un solo arreglo — L() elige el idioma.',
      href: 'https://example.com', status: 'live' },
  ];

  // Worked read (delete once your own render() calls L()/T() directly):
  //   const item = ITEMS[0];
  //   el.textContent = L(item, 'title') + ' — ' + L(item, 'desc');
  //   btn.setAttribute('aria-label', T('openCard'));

  // Expose to the page script that renders from ITEMS. Rename the
  // global to fit the page; this line is the only thing every
  // consumer edits.
  window.Bilingual = { LANG, L, T, ITEMS };

})();
