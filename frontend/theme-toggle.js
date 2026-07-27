/* @spine-kit tokens/theme-toggle.js · v1
   Canonical source: spine/kits/tokens/theme-toggle.js
   Paste-verbatim. Fix the spine, then re-vendor — do not edit this copy in place. */

/* ════════════════════════════════════════════════════════════════════
   THEME TOGGLE — the enhancement half of the theme layer
   ────────────────────────────────────────────────────────────────────
   tokens.css already gives a correct page in both themes from
   `prefers-color-scheme` alone, with zero JavaScript. That is the
   baseline and it is not negotiable. This file adds ONE thing on top: a
   reader who wants the other theme can say so, and be remembered.

   Three rules it is built to keep:

     1 · IT IS NEVER LOAD-BEARING. With JS off, no button is inserted,
         nothing is missing from the page, and the OS preference governs.
         Nothing else in the stack reads anything this file writes.

     2 · IT DOES NOT SET THE THEME ON LOAD. The attribute is restored by
         a synchronous snippet in each page's <head>, before the first
         stylesheet is applied — otherwise a reader whose stored choice
         disagrees with their OS would see the wrong theme for a frame.
         By the time this deferred script runs, the theme is already
         correct; this file only reads it and offers to change it.

     3 · IT DOES NOT MOUNT WHERE LIGHT CANNOT WORK. The light theme is
         gated on color-mix() (tokens.css explains why: --tp-paper is the
         lightest value the system has, so a light elevation ladder has
         to mix). On an engine without it, a "light" button would set an
         attribute that no rule matches, and the page would sit there
         looking dark while the control claimed otherwise. So the probe
         is checked first and the button simply does not appear.

   ── WHY IT BORROWS .lang-link AND ADDS NO CSS ──────────────────────
   The toggle sits next to the language flipper, and the flipper is
   already styled — differently — in every stack. Reusing .lang-link is
   not a shortcut: it is what makes the control belong to whichever nav
   it lands in, at whatever size and colour that nav uses, with hover and
   focus already handled. The only styling this file applies is a
   four-property <button> reset, which carries no colour of its own.

   ── THE EVENT ───────────────────────────────────────────────────────
   Switching theme fires `tp:themechange` on `document`
   (detail: { theme }). Generated art reads its palette from tokens at
   DRAW time, so an SVG or canvas already on the page keeps the colours
   of the theme it was drawn in until something redraws it. A stack with
   a generator listens for this and re-runs it. Decoration only — a stack
   that ignores the event still has a correct page, just art one theme
   behind until the next reseed.
   ════════════════════════════════════════════════════════════════════ */
(function (w, d) {
  'use strict';

  var KEY = 'tp-theme';
  var LIGHT_PROBE = ['color', 'color-mix(in srgb, currentColor 50%, transparent)'];

  /* Labels, in the two languages every stack in this palette ships. Read
     from <html lang> rather than guessed, so the ES twin gets ES. */
  var COPY = {
    en: { toLight: 'Switch to light theme', toDark: 'Switch to dark theme' },
    es: { toLight: 'Cambiar al tema claro', toDark: 'Cambiar al tema oscuro' }
  };

  function supported() {
    try { return !!(w.CSS && w.CSS.supports && w.CSS.supports(LIGHT_PROBE[0], LIGHT_PROBE[1])); }
    catch (e) { return false; }
  }
  function store(v) {
    try { w.localStorage.setItem(KEY, v); } catch (e) { /* private mode, file://, disabled — the toggle still works for this page */ }
  }
  function prefersLight() {
    try { return !!(w.matchMedia && w.matchMedia('(prefers-color-scheme: light)').matches); }
    catch (e) { return false; }
  }
  /* What the reader is actually looking at right now: the forced value if
     there is one, otherwise whatever the media query resolves to. */
  function effective() {
    var forced = d.documentElement.getAttribute('data-theme');
    if (forced === 'light' || forced === 'dark') { return forced; }
    return prefersLight() ? 'light' : 'dark';
  }

  /* Two 16px glyphs, currentColor only, no external asset. The button
     shows the theme it will switch TO, which is the convention a reader
     already has from every OS: the control names its destination. */
  var SUN = '<svg viewBox="0 0 16 16" width="15" height="15" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" aria-hidden="true" focusable="false">' +
    '<circle cx="8" cy="8" r="3.1"/><path d="M8 1v1.6M8 13.4V15M1 8h1.6M13.4 8H15M3.1 3.1l1.1 1.1M11.8 11.8l1.1 1.1M12.9 3.1l-1.1 1.1M4.2 11.8l-1.1 1.1"/></svg>';
  var MOON = '<svg viewBox="0 0 16 16" width="15" height="15" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" aria-hidden="true" focusable="false">' +
    '<path d="M13.2 9.6A5.6 5.6 0 0 1 6.4 2.8a5.6 5.6 0 1 0 6.8 6.8z"/></svg>';

  function mountPoint() {
    var all = d.querySelectorAll('[data-agent-lang-pair]');
    if (!all.length) { return null; }
    for (var i = 0; i < all.length; i++) {
      if (all[i].closest && all[i].closest('header, .site-head, .top-nav')) { return all[i]; }
    }
    return all[0];   /* no header flipper — take the first one anywhere */
  }

  function init() {
    if (!supported()) { return; }
    var anchor = mountPoint();
    if (!anchor || d.querySelector('[data-agent-theme-toggle]')) { return; }

    var lang = (d.documentElement.getAttribute('lang') || 'en').slice(0, 2).toLowerCase();
    var copy = COPY[lang] || COPY.en;

    var btn = d.createElement('button');
    btn.type = 'button';
    btn.className = anchor.className && anchor.className.indexOf('lang-link') > -1 ? 'lang-link theme-toggle' : 'theme-toggle lang-link';
    btn.setAttribute('data-agent-theme-toggle', '');
    /* A <button> brings its own UA background, border, padding and font.
       Stripped here rather than in a stylesheet so the control inherits
       .lang-link whole and this kit ships no CSS of its own. The padding
       is a var() with a fallback so a stack can tighten it at a narrow
       viewport — which two of them have to; see the 375px note in each
       styles.css. */
    btn.style.cssText = 'background:none;border:0;padding:0 0 0 var(--theme-toggle-gap, 10px);font:inherit;line-height:0;cursor:pointer;vertical-align:middle';

    function paint() {
      var now = effective();
      var next = now === 'light' ? 'dark' : 'light';
      btn.innerHTML = next === 'light' ? SUN : MOON;
      btn.setAttribute('aria-label', next === 'light' ? copy.toLight : copy.toDark);
      btn.title = next === 'light' ? copy.toLight : copy.toDark;
    }

    btn.addEventListener('click', function () {
      var next = effective() === 'light' ? 'dark' : 'light';
      d.documentElement.setAttribute('data-theme', next);
      store(next);
      paint();
      try { d.dispatchEvent(new CustomEvent('tp:themechange', { detail: { theme: next } })); }
      catch (e) {
        /* CustomEvent constructor is ES6; the stacks are ES2018 so this is
           safe, but a generator that never redraws is a cosmetic loss and
           must never take the toggle down with it. */
      }
    });

    /* Follow the OS while no explicit choice is in force, so the glyph
       never claims the opposite of what the reader sees. */
    try {
      var mq = w.matchMedia('(prefers-color-scheme: light)');
      var onChange = function () { if (!d.documentElement.getAttribute('data-theme')) { paint(); } };
      if (mq.addEventListener) { mq.addEventListener('change', onChange); }
      else if (mq.addListener) { mq.addListener(onChange); }
    } catch (e) { /* no matchMedia — the button still toggles */ }

    paint();
    /* INSIDE the flipper where the flipper is a container, beside it
       where it is the link itself. Not cosmetic: the flipper usually sits
       in a flex nav, so a sibling costs the button its own width PLUS a
       flex gap — enough, measured, to wrap the nav to a second line at
       375px and push the headline 28-35px down the first screen on the
       longer language half. Inside the container the gap is not spent.
       (An <a> gets the sibling treatment because a button inside an
       anchor is invalid HTML, not because it is better there.) */
    var container = /^(SPAN|LI|DIV|P|NAV)$/.test(anchor.tagName) ? anchor : null;
    if (container) { container.appendChild(btn); }
    else if (anchor.parentNode) { anchor.parentNode.insertBefore(btn, anchor.nextSibling); }
  }

  if (d.readyState === 'loading') { d.addEventListener('DOMContentLoaded', init); }
  else { init(); }
})(window, document);
