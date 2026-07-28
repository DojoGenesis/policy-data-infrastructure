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
  function swapDOM(lang) {
    var attr = 'data-' + lang;
    var els = d.querySelectorAll('[' + attr + ']');
    for (var i = 0; i < els.length; i++) {
      var el = els[i];
      var text = el.getAttribute(attr);
      if (text !== null && text !== '') {
        el.textContent = text;
      }
    }
    /* Input placeholders */
    var placeholderAttr = attr + '-placeholder';
    var inputs = d.querySelectorAll('[' + placeholderAttr + ']');
    for (var j = 0; j < inputs.length; j++) {
      var val = inputs[j].getAttribute(placeholderAttr);
      if (val !== null) { inputs[j].placeholder = val; }
    }
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
  function init() {
    /* Apply detected language */
    d.documentElement.setAttribute('lang', currentLang);
    swapDOM(currentLang);

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

    /* ── Expose public API ──────────────────────────────────── */
    w.LangToggle = {
      getLang: function () { return currentLang; },
      setLang: setLang,
      toggle: toggleLang,
      isES:  function () { return currentLang === 'es'; },
      isEN:  function () { return currentLang === 'en'; },
      t: function (key) {
        var entry = T[key];
        if (!entry) return key;
        return entry[currentLang] || entry.en || key;
      }
    };
  }

  if (d.readyState === 'loading') {
    d.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})(window, document);
