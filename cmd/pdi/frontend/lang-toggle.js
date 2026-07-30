/* @spine-kit tokens/lang-toggle.js · v1
   Canonical source: PDI frontend lang-toggle
   
   Replaces the ES twin files (10 separate HTML files) with a single
   page per route. Every element with visible text carries both
   data-en and data-es attributes; this script walks the DOM and
   swaps textContent in-place without a page reload.

   ── HOW IT WORKS ────────────────────────────────────────────────
   Each translatable element:
     <span data-en="Counties" data-es="Condados">Counties</span>

   The toggle walks [data-<lang>] and sets textContent from the
   attribute. Elements without a translation for the target language
   keep their current textContent (the English fallback).

   ── MOUNT POINT ─────────────────────────────────────────────────
   Creates a <span data-agent-lang-pair> container in the site
   header. The theme-toggle.js (spine-kit) mounts its theme button
   INSIDE this container, so both toggles sit side-by-side without
   spending an extra flex gap.

   ── EVENTS ──────────────────────────────────────────────────────
   Fires `tp:langchange` on document (detail: { lang }). Dynamic
   content generators (chat, API responses) listen for this to
   re-render labels in the selected language.

   ── PUBLIC API ──────────────────────────────────────────────────
   window.LangToggle.getLang()  → 'en' | 'es'
   window.LangToggle.setLang(l) → switch to 'en' or 'es'
   window.LangToggle.isES()     → boolean
   window.LangToggle.isEN()     → boolean
   window.LangToggle.t(key)     → lookup a key from the translation map
   ════════════════════════════════════════════════════════════════ */
(function (w, d) {
  'use strict';

  var KEY = 'tp-lang';
  var SUPPORTED = ['en', 'es'];

  /* ── Translation map for dynamic content (JS-generated strings) ── */
  var T = {
    'api.error':    { en: 'API error',          es: 'Error de API' },
    'load.failed':  { en: 'Failed to load counties', es: 'Error al cargar condados' },
    'retry':        { en: 'Retry',              es: 'Reintentar' },
    'no.match':     { en: 'No counties match',  es: 'Ningún condado coincide con' },
    'of':           { en: 'of',                 es: 'de' },
    'counties':     { en: 'counties',           es: 'condados' },
    'chat.ask':     { en: 'Ask about this page…', es: 'Pregunta sobre esta página…' },
    'chat.placeholder': { en: 'Ask about this page…', es: 'Pregunta sobre esta página…' },
  };

  var LABELS = { en: 'ES', es: 'EN' };
  var ARIA = {
    en: 'Cambiar a español',
    es: 'Switch to English'
  };

  /* ── Language detection ─────────────────────────────────────── */
  function detectLang() {
    try {
      var stored = w.localStorage.getItem(KEY);
      if (stored && SUPPORTED.indexOf(stored) !== -1) return stored;
    } catch (e) { /* private mode */ }
    /* URL path prefix — /es/ or /es */
    var path = w.location.pathname;
    if (path === '/es' || path.indexOf('/es/') === 0) return 'es';
    /* html[lang] */
    var htmlLang = (d.documentElement.getAttribute('lang') || 'en').slice(0, 2).toLowerCase();
    if (SUPPORTED.indexOf(htmlLang) !== -1) return htmlLang;
    return 'en';
  }

  var currentLang = detectLang();

  function store(lang) {
    try { w.localStorage.setItem(KEY, lang); } catch (e) { /* private mode */ }
  }

  /* ── DOM swap ──────────────────────────────────────────────────
     Walks all elements that carry the target language attribute
     and sets textContent. Elements without a translation retain
     their current textContent (English fallback). */
  /* Attributes that can carry user-visible text and therefore need a
     translated pair. `placeholder` was the only one supported until
     2026-07-29, which left every aria-label and title on every page
     stuck in English regardless of the selected language — an
     accessibility hole for Spanish screen-reader users, not a polish
     issue: a sighted user could at least read the translated label
     next to the control, while a screen-reader user got only the
     untranslated one.

     Convention: data-<lang>-<attr>, e.g.
       <button aria-label="Dismiss"
               data-en-aria-label="Dismiss"
               data-es-aria-label="Descartar">
     `placeholder` keeps working exactly as before — it is just one
     entry in this list now rather than a special case. */
  var TRANSLATABLE_ATTRS = ['placeholder', 'aria-label', 'title', 'alt'];

  /* `root` scopes the walk. Defaults to the whole document; the mutation
     observer below passes just-added subtrees so a page that renders 70
     cards does not re-walk the entire document 70 times. querySelectorAll
     only returns DESCENDANTS, so the root itself is checked separately —
     without that, an added node that IS the translatable element (rather
     than containing one) would be skipped. */
  function swapDOM(lang, root) {
    var scope = root || d;
    var attr = 'data-' + lang;

    function applyText(el) {
      var text = el.getAttribute(attr);
      if (text !== null && text !== '') { el.textContent = text; }
    }
    function applyAttrs(el) {
      for (var a = 0; a < TRANSLATABLE_ATTRS.length; a++) {
        var name = TRANSLATABLE_ATTRS[a];
        var val = el.getAttribute(attr + '-' + name);
        if (val !== null) { el.setAttribute(name, val); }
      }
    }

    if (scope.nodeType === 1) {
      if (scope.hasAttribute(attr)) { applyText(scope); }
      applyAttrs(scope);
    }
    var els = scope.querySelectorAll('[' + attr + ']');
    for (var i = 0; i < els.length; i++) { applyText(els[i]); }
    for (var b = 0; b < TRANSLATABLE_ATTRS.length; b++) {
      var sel = '[' + attr + '-' + TRANSLATABLE_ATTRS[b] + ']';
      var nodes = scope.querySelectorAll(sel);
      for (var j = 0; j < nodes.length; j++) { applyAttrs(nodes[j]); }
    }
  }

  /* ── Late-arriving content ─────────────────────────────────────
     swapDOM used to run exactly once per language change, which meant
     any data-en/data-es pair that entered the DOM AFTERWARDS was never
     translated. That is most of the interesting content on this site:
     anything inside <template x-if="!loading"> renders when a fetch
     resolves, long after the swap. The symptom was specific and easy
     to miss — a cold load with tp-lang=es already stored would render
     the chrome in Spanish and the freshly-fetched tables in English.

     A MutationObserver closes the whole class instead of requiring
     every async completion point to remember to call back in. The
     observer is disconnected while swapping: setting textContent is
     itself a mutation, so leaving it connected would re-enter on our
     own writes. */
  var observer = null;
  function observe() {
    if (!observer || !d.body) { return; }
    observer.observe(d.body, { childList: true, subtree: true });
  }
  function startObserver() {
    if (observer || typeof w.MutationObserver !== 'function' || !d.body) { return; }
    observer = new w.MutationObserver(function (muts) {
      var roots = [];
      for (var i = 0; i < muts.length; i++) {
        var added = muts[i].addedNodes;
        for (var j = 0; j < added.length; j++) {
          if (added[j].nodeType === 1) { roots.push(added[j]); }
        }
      }
      if (!roots.length) { return; }
      observer.disconnect();
      try {
        for (var k = 0; k < roots.length; k++) { swapDOM(currentLang, roots[k]); }
      } finally {
        observe();
      }
    });
    observe();
  }

  function setLang(lang) {
    if (lang === currentLang || SUPPORTED.indexOf(lang) === -1) return;
    currentLang = lang;
    d.documentElement.setAttribute('lang', lang);
    store(lang);
    swapDOM(lang);
    try {
      d.dispatchEvent(new CustomEvent('tp:langchange', { detail: { lang: lang } }));
    } catch (e) { /* CustomEvent constructor — safe in ES2018+ */ }
  }

  function toggleLang() {
    setLang(currentLang === 'en' ? 'es' : 'en');
  }

  /* ── Init ───────────────────────────────────────────────────── */
  /* True once init() has run a first swapDOM. addStrings() reads this to
     decide whether a late registration needs an immediate re-swap: before
     init the upcoming swap will pick the strings up anyway, after it the
     DOM has already been walked and needs a second pass. */
  var inited = false;

  function init() {
    /* Apply detected language */
    d.documentElement.setAttribute('lang', currentLang);
    swapDOM(currentLang);
    inited = true;
    startObserver();

    /* ── Build toggle button ────────────────────────────────── */
    var header = d.querySelector('.site-header-inner');
    if (!header) return;
    if (d.querySelector('[data-agent-lang-pair]')) return; /* already injected */

    var pair = d.createElement('span');
    pair.className = 'lang-pair';
    pair.setAttribute('data-agent-lang-pair', '');

    var other = currentLang === 'en' ? 'es' : 'en';

    var link = d.createElement('a');
    link.className = 'lang-link';
    link.href = '#';
    link.setAttribute('data-lang', other);
    link.setAttribute('aria-label', ARIA[other] || other.toUpperCase());
    link.setAttribute('role', 'button');
    link.textContent = LABELS[other] || other.toUpperCase();

    link.addEventListener('click', function (e) {
      e.preventDefault();
      toggleLang();
    });

    /* Update button on lang change */
    d.addEventListener('tp:langchange', function (e) {
      var lang = e.detail.lang;
      var o = lang === 'en' ? 'es' : 'en';
      link.textContent = LABELS[o] || o.toUpperCase();
      link.setAttribute('aria-label', ARIA[o] || o.toUpperCase());
      link.setAttribute('data-lang', o);
    });

    pair.appendChild(link);

    /* Insert before the chat toggle, or at end of header */
    var chatToggle = header.querySelector('.chat-toggle');
    if (chatToggle) {
      header.insertBefore(pair, chatToggle);
    } else {
      header.appendChild(pair);
    }

  }

  /* ── Alpine bridge ─────────────────────────────────────────────
     swapDOM cannot reach Alpine-rendered text. x-text overwrites
     textContent from its own expression on every reactive update, so
     an element carrying both x-text and a data-es pair renders the
     English expression result and silently discards the translation.
     That is why ~200 strings stayed English in Spanish mode across
     the ten pages while the surrounding chrome translated correctly.

     A store, not a helper function, is the fix. Alpine tracks store
     reads inside expressions, so any expression calling
     $store.i18n.t(...) re-evaluates automatically when .lang changes.
     A plain global t() would return the right string but would never
     tell Alpine to re-render, leaving the page in the old language
     until something else happened to touch the same component.

       <span x-text="$store.i18n.t('compare.tied')"></span>

     ORDERING — measured, not assumed. Alpine 3.14.9 loaded with defer
     does NOT wait for DOMContentLoaded: it starts as soon as its own
     deferred script executes, dispatching alpine:init at
     readyState === 'interactive'. Verified event order on this site:

       alpine:init (interactive) -> alpine:initialized -> DOMContentLoaded

     So an alpine:init listener registered by a script that runs after
     alpine.min.js never fires, and a store registered later is too
     late for Alpine's FIRST pass — every $store.i18n expression in the
     initial render would evaluate against an undefined store and that
     binding would be dead for the life of the page.

     The load order therefore matters: lang-toggle.js must appear
     BEFORE alpine.min.js in every page. Both are defer, and deferred
     scripts execute in document order, so tag position is what
     guarantees this. Both paths below are kept anyway — the listener
     for the correct order, the immediate call for a page that ever
     regresses to loading Alpine first (late registration still works;
     it just cannot rescue the first render). */
  /* Presence, not truthiness. This used to read
       entry[currentLang] || entry.en || key
     which treats an INTENTIONALLY EMPTY translation as missing and falls
     through — ultimately leaking the raw key name into the page.

     An empty string is a legitimate translation. It is how word-order
     differences get handled: English "County A value" takes a suffix,
     Spanish "Valor de Condado A" takes a prefix, so one side of the pair
     is deliberately ''. Under the old test those keys rendered as
     `compare.value_prefix_esCounty A value` — and the breakage was
     invisible to an identical-EN/ES-string check, because the two
     languages fail differently rather than matching each other. */
  function translate(key) {
    var entry = T[key];
    if (!entry) { return key; }
    if (Object.prototype.hasOwnProperty.call(entry, currentLang)) {
      return entry[currentLang];
    }
    if (Object.prototype.hasOwnProperty.call(entry, 'en')) { return entry.en; }
    return key;
  }

  var storeReady = false;
  function registerStore() {
    if (storeReady) { return; }
    if (!w.Alpine || typeof w.Alpine.store !== 'function') { return; }
    storeReady = true;
    w.Alpine.store('i18n', {
      lang: currentLang,
      /* Reading this.lang is what registers the reactive dependency —
         do not "optimize" it away by calling translate(key) directly,
         or expressions stop re-rendering on language change. */
      t: function (key) { return this.lang && translate(key); },
      is: function (l) { return this.lang === l; }
    });
  }

  d.addEventListener('alpine:init', registerStore);
  registerStore();

  /* ── Public API ────────────────────────────────────────────────
     Assigned at module level, NOT inside init(). init() is a
     DOMContentLoaded handler, so exposing the API there meant no
     inline page script could call LangToggle at parse time — which is
     exactly when a page needs to register its own strings, before
     Alpine evaluates the expressions that read them. */
  w.LangToggle = {
    getLang: function () { return currentLang; },
    setLang: setLang,
    toggle: toggleLang,
    isES:  function () { return currentLang === 'es'; },
    isEN:  function () { return currentLang === 'en'; },
    t: translate,

    /* Register page-scoped strings:

         LangToggle.addStrings({
           'compare.tied': { en: 'Tied', es: 'Empatado' }
         });

       Page strings live with the page rather than in one shared map
       here. That matches how this repo already scopes CSS, keeps a
       page's vocabulary readable next to the markup that uses it, and
       lets several pages be translated in parallel without every
       change landing in this file.

       Keys are namespaced by page (compare.*, county.*) by convention;
       nothing enforces it, but a collision silently overwrites, so
       keep the prefix. Re-swaps immediately if the DOM is already
       initialised, so a late registration still takes effect. */
    addStrings: function (map) {
      if (!map || typeof map !== 'object') { return; }
      for (var k in map) {
        if (Object.prototype.hasOwnProperty.call(map, k)) { T[k] = map[k]; }
      }
      if (inited) { swapDOM(currentLang); }
    },

    /* Re-walk and re-apply. Rarely needed now that a MutationObserver
       catches late-arriving nodes automatically, but kept as an explicit
       escape hatch for anything that mutates in a way the observer
       cannot see — and because calling addStrings({}) purely for its
       re-swap side effect (which is what pages had to do before the
       observer existed) reads as a bug rather than an intention. */
    refresh: function (root) { swapDOM(currentLang, root); }
  };

  /* Keep the store in step with the toggle. setLang fires this event
     after swapDOM, so static and Alpine text change in one frame
     rather than visibly staggering. */
  d.addEventListener('tp:langchange', function (e) {
    if (w.Alpine && typeof w.Alpine.store === 'function') {
      var s = w.Alpine.store('i18n');
      if (s) { s.lang = (e.detail && e.detail.lang) || currentLang; }
    }
  });

  if (d.readyState === 'loading') {
    d.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})(window, document);
