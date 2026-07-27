/* @spine-kit parametric/parametric.js · v1
   Canonical source: spine/kits/parametric/parametric.js
   Paste-verbatim. Fix the spine, then re-vendor — do not edit this copy in place. */

/* ═══════════════════════════════════════════════════════════════════
 * PARAMETRIC — the pattern, not the picture, in ten lines.
 *
 *   A stack keeps a small PARAMS object (a seed + a few knobs) in a
 *   footer "parámetros" panel, persisted per-session. A generator is a
 *   pure function build(params, rng) -> SVG string that samples its
 *   palette from CSS custom properties (token/sample below) and takes
 *   every random choice from a seeded rng (mulberry32) — same seed,
 *   same art, every time, in this session or the next. Scroll position
 *   derives one normalized signal (deriveFromScroll) that gets written
 *   to ONE custom property on <html> (bindProperty) for CSS and any
 *   canvas/WebGL layer to read — never scattered across many properties.
 *   This file is that machinery, stripped of any one site's art. It
 *   ships no color, no shape, no medallion, no door — see GENERATORS.md
 *   for how to build a generator on top of it.
 *
 *   Extracted from two stack-zero sites: trespiesdesign.com (AMANECER —
 *   the sunrise medallion) and cruzromeromorales.com (PUERTAS — the
 *   generated doors). Where both sites independently converged on the
 *   same shape (mulberry32, the PARAMS load/save block, the velocity/
 *   energy smoothing constants below), that convergence is the signal
 *   this is already correct — those pieces are lifted verbatim.
 * ═══════════════════════════════════════════════════════════════════ */

(function () {
  'use strict';

  /* ── tokens: the CSS custom properties ARE the palette API ────────
     A generator never hardcodes a hex value. It reads named custom
     properties off <html> (token), then interpolates between two or
     more of them (sample) — a re-theme is a :root edit, never a code
     change. Lifted from AMANECER's token()/hexPair()/lerpHex()/sample(),
     which were already stack-agnostic; only the WARM/WATER anchor
     arrays built from them were site art, and those stay out of here. */
  var _rootStyle = null;
  function rootStyle() {
    if (!_rootStyle) {
      try { _rootStyle = getComputedStyle(document.documentElement); }
      catch (e) { _rootStyle = null; }
    }
    return _rootStyle;
  }
  function token(name) {
    var cs = rootStyle();
    if (!cs) { return ''; }
    try { return cs.getPropertyValue(name).trim(); } catch (e) { return ''; }
  }
  function hexPair(hex) {
    var h = (hex || '').replace('#', '');
    return [parseInt(h.substr(0, 2), 16), parseInt(h.substr(2, 2), 16), parseInt(h.substr(4, 2), 16)];
  }
  function lerpHex(a, b, t) {
    var A = hexPair(a), B = hexPair(b), c = [0, 0, 0], i;
    for (i = 0; i < 3; i++) { c[i] = Math.round(A[i] + (B[i] - A[i]) * t); }
    return 'rgb(' + c[0] + ',' + c[1] + ',' + c[2] + ')';
  }
  function sample(anchors, t) {
    t = Math.min(1, Math.max(0, t));
    var span = anchors.length - 1;
    if (span < 1) { return anchors[0]; }
    var i = Math.min(span - 1, Math.floor(t * span));
    return lerpHex(anchors[i], anchors[i + 1], t * span - i);
  }

  /* ── seeded randomness: unique per visit, stable per session ──────
     mulberry32 + freshSeed, lifted verbatim — both sites carry the
     identical implementation, which is the tell that it's already
     correct and already stack-agnostic. Do not "improve" this; if a
     stronger PRNG is ever needed, add a second function, don't edit
     this one — reproducible art depends on this exact bit pattern. */
  function mulberry32(seed) {
    return function () {
      seed |= 0; seed = (seed + 0x6D2B79F5) | 0;
      var z = Math.imul(seed ^ (seed >>> 15), 1 | seed);
      z = (z + Math.imul(z ^ (z >>> 7), 61 | z)) ^ z;
      return ((z ^ (z >>> 14)) >>> 0) / 4294967296;
    };
  }
  function freshSeed() { return (Date.now() ^ (Math.random() * 0x7FFFFFFF)) >>> 0; }

  /* ── params: the persisted parameter surface ───────────────────────
     Generalizes the hand-rolled PARAMS block both sites carry (default
     object → try sessionStorage → per-field typeof-validated merge →
     save-wrapped-in-try/catch) into one call. `schema` is a map of
     key -> { type: 'number'|'boolean'|'string', seed: true? }: `type`
     is checked against `typeof` before a stored value is trusted, so
     one corrupted or hand-edited field can't take the rest of the
     object down with it, and only fields the schema names can ever be
     restored at all. `seed: true` marks which field(s) reset()/reseed()
     re-roll via freshSeed() — plain reset() would otherwise lock every
     visit to the literal default seed, which defeats "unique per visit".
     A field with no schema entry is never restored from storage; it
     always reads back as its literal default.
     Storage access itself is inside the outermost try/catch: Safari
     private-mode and locked-down embeds can throw on touching
     sessionStorage at all, not just on reading from it. When that
     happens every method below still works — reads/writes just stay
     in memory for the tab, which is the whole page's job to not
     notice or care about. */
  function createParams(opts) {
    opts = opts || {};
    var key = opts.key || 'tp-params';
    var defaults = opts.defaults || {};
    var schema = opts.schema || {};

    var store = null;
    try { store = window.sessionStorage; } catch (e) { /* stateless fallback */ }

    var params = {};
    var k;
    for (k in defaults) {
      if (Object.prototype.hasOwnProperty.call(defaults, k)) { params[k] = defaults[k]; }
    }

    function save() {
      if (!store) { return; }
      try { store.setItem(key, JSON.stringify(params)); }
      catch (e) { /* quota or blocked: memory-only from here on */ }
    }

    function load() {
      if (!store) { return; }
      try {
        var raw = store.getItem(key);
        var saved = raw ? JSON.parse(raw) : null;
        if (saved && typeof saved === 'object') {
          var sk;
          for (sk in schema) {
            if (Object.prototype.hasOwnProperty.call(schema, sk) &&
                Object.prototype.hasOwnProperty.call(saved, sk) &&
                typeof saved[sk] === schema[sk].type) {
              params[sk] = saved[sk];
            }
          }
        } else {
          save(); /* first visit this session: persist the computed defaults (incl. any fresh seed) */
        }
      } catch (e) { /* malformed JSON or blocked read: defaults stand */ }
    }

    function set(k2, value) {
      params[k2] = value;
      save();
      return params;
    }

    function reset() {
      var k2;
      for (k2 in defaults) {
        if (Object.prototype.hasOwnProperty.call(defaults, k2)) { params[k2] = defaults[k2]; }
      }
      for (k2 in schema) {
        if (Object.prototype.hasOwnProperty.call(schema, k2) && schema[k2].seed) { params[k2] = freshSeed(); }
      }
      save();
      return params;
    }

    function reseed() {
      var k2, any = false;
      for (k2 in schema) {
        if (Object.prototype.hasOwnProperty.call(schema, k2) && schema[k2].seed) { params[k2] = freshSeed(); any = true; }
      }
      if (any) { save(); }
      return params;
    }

    load();

    return { params: params, defaults: defaults, schema: schema, save: save, set: set, reset: reset, reseed: reseed };
  }

  /* ── binding: one derived scalar, one custom property ──────────────
     Every derived visual signal collapses to ONE write on <html> — CSS
     and any canvas/WebGL uniform both read it from there. Lifted from
     AMANECER's setDawn(), generalized off the literal "--dawn" name. */
  function bindProperty(name, value) {
    try { document.documentElement.style.setProperty(name, value); } catch (e) { /* no-op */ }
  }

  /* ── deriveFromScroll: scroll position → progress + velocity/energy ─
     Generalizes the update()/requestUpdate() pair both sites hand-roll
     (a `ticking` flag so N scroll events collapse into the next single
     rAF, never a compute per event) plus the velocity/energy EMA both
     sites' WebGL field code duplicates verbatim — same 0.0009 and
     dt*3/dt*6 rate constants in both files independently, which is why
     those constants are fixed here rather than exposed as options.
     `opts.speed` multiplies progress (AMANECER's dawnSpeed knob).
     `opts.smoothing` retunes only how twitchy vs. smooth velocity
     feels (default 6, matching source).

     HARD CONSTRAINT: honors prefers-reduced-motion itself. When it is
     set, this never attaches a scroll listener and never starts a rAF
     loop — it settles once at the "arrived" end state (progress 1, no
     velocity) so the page reads as complete rather than stuck mid-
     transition. A caller with its own explicit motion toggle (the
     panel's motion switch) may override with `{ force: true }` — that
     is the user opting back into motion they explicitly asked for, not
     this kit overriding their OS setting on its own. */
  function deriveFromScroll(opts) {
    opts = opts || {};
    var onUpdate = typeof opts.onUpdate === 'function' ? opts.onUpdate : function () {};
    var speed = typeof opts.speed === 'number' ? opts.speed : 1;
    var smoothing = typeof opts.smoothing === 'number' ? opts.smoothing : 6;
    var doc = document.documentElement;

    var reduceMotion = false;
    try { reduceMotion = !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches); }
    catch (e) { /* assume motion is fine */ }
    var active = opts.force ? true : !reduceMotion;

    function now() { return (window.performance && performance.now) ? performance.now() : Date.now(); }

    if (!active) {
      onUpdate({ progress: 1, velocity: 0, energy: 0 });
      return { update: function () {}, start: function () {}, stop: function () {}, active: false };
    }

    var ticking = false;
    var lastY = window.scrollY || window.pageYOffset || 0;
    var lastT = now();
    var vel = 0, energy = 0, running = true;

    function compute() {
      ticking = false;
      if (!running) { return; }
      var max = doc.scrollHeight - window.innerHeight;
      var y = window.scrollY || window.pageYOffset || 0;
      var progress = Math.min(1, Math.max(0, (max > 0 ? y / max : 1) * speed));

      var t = now();
      var dt = Math.max((t - lastT) / 1000, 1e-3);
      lastT = t;
      var instVel = (y - lastY) / dt;
      lastY = y;
      vel += (instVel - vel) * Math.min(dt * smoothing, 1);
      var e = Math.min(Math.abs(vel) * 0.0009, 1);
      energy += (e - energy) * Math.min(dt * 3, 1);

      onUpdate({ progress: progress, velocity: vel, energy: energy });
    }
    function requestUpdate() {
      if (!ticking) { ticking = true; window.requestAnimationFrame(compute); }
    }
    function onScroll() { requestUpdate(); }

    window.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('resize', onScroll, { passive: true });
    requestUpdate();

    return {
      update: requestUpdate,
      start: function () { running = true; requestUpdate(); },
      stop: function () {
        running = false;
        window.removeEventListener('scroll', onScroll);
        window.removeEventListener('resize', onScroll);
      },
      active: true
    };
  }

  /* ── SVG assembly: geometry plumbing only, never art ───────────────
     pt() is polar-to-cartesian, lifted verbatim from AMANECER — it was
     already generic (used for both sunburst rays and sky zigzags).
     polygon()/path() just stitch an attribute map onto a points/d
     string so a generator doesn't hand-concatenate quote marks; the
     geometry itself — how many points, what radius, what bounds — is
     always the generator's job. See GENERATORS.md for the constraint
     discipline (clip/bounds) these are meant to be used inside of. */
  function pt(cx, cy, angle, r) {
    return (cx + Math.cos(angle) * r).toFixed(1) + ',' + (cy + Math.sin(angle) * r).toFixed(1);
  }
  function attrString(attrs) {
    var out = '', k;
    if (!attrs) { return out; }
    for (k in attrs) {
      if (!Object.prototype.hasOwnProperty.call(attrs, k)) { continue; }
      if (attrs[k] === undefined || attrs[k] === null || attrs[k] === false) { continue; }
      out += ' ' + k + '="' + attrs[k] + '"';
    }
    return out;
  }
  function polygon(points, attrs) {
    var i, parts = [];
    for (i = 0; i < points.length; i++) {
      parts.push(Array.isArray(points[i]) ? (points[i][0] + ',' + points[i][1]) : points[i]);
    }
    return '<polygon points="' + parts.join(' ') + '"' + attrString(attrs) + '/>';
  }
  function path(segments, attrs) {
    var d = Array.isArray(segments) ? segments.join(' ') : segments;
    return '<path d="' + d + '"' + attrString(attrs) + '/>';
  }

  /* ── the visible parámetros panel: generic disclosure + row wiring ─
     Beyond the file-1 spec's literal ask, but the spine README lists
     "the visible parámetros panel" as part of what this kit gives a
     stack, and both sites hand-roll an almost-identical syncPanel/
     openPanel/closePanel/wireRange quartet — exactly the duplication a
     kit exists to remove. Neither helper knows what a param MEANS:
     bindPanel only manages disclosure + focus + Escape; wireControl
     only pipes one input's value into a createParams() controller + an
     output readout + your own callback. Which rows exist, and what
     changing them should do, stays entirely in the stack's own code —
     see params-panel.html and GENERATORS.md. */
  function bindPanel(toggle, panel) {
    if (!toggle || !panel) { return { open: function () {}, close: function () {} }; }

    function open() {
      panel.hidden = false;
      toggle.setAttribute('aria-expanded', 'true');
      var first = panel.querySelector('input, button, select, textarea');
      if (first) { first.focus(); }
    }
    function close(refocus) {
      panel.hidden = true;
      toggle.setAttribute('aria-expanded', 'false');
      if (refocus) { toggle.focus(); }
    }
    toggle.addEventListener('click', function () {
      if (panel.hidden) { open(); } else { close(false); }
    });
    panel.addEventListener('keydown', function (ev) {
      if (ev.key === 'Escape') { close(true); }
    });
    return { open: open, close: close };
  }

  function wireControl(ctrl, key, input, output, opts) {
    if (!ctrl || !input) { return; }
    opts = opts || {};
    var format = opts.format;
    var onChange = typeof opts.onChange === 'function' ? opts.onChange : null;
    var evt = (input.type === 'checkbox' || input.tagName === 'SELECT') ? 'change' : 'input';

    input.addEventListener(evt, function () {
      var value;
      if (input.type === 'checkbox') { value = input.checked; }
      else if (input.type === 'range' || input.type === 'number') { value = parseFloat(input.value); }
      else { value = input.value; }

      ctrl.params[key] = value;
      if (output) { output.textContent = format ? format(value) : String(value); }
      ctrl.save();
      if (onChange) { onChange(value, ctrl.params); }
    });
  }

  window.TPParametric = {
    /* seeded randomness */
    mulberry32: mulberry32,
    freshSeed: freshSeed,
    /* tokens */
    token: token,
    hexPair: hexPair,
    lerpHex: lerpHex,
    sample: sample,
    /* persisted params */
    createParams: createParams,
    /* binding + scroll derivation */
    bindProperty: bindProperty,
    deriveFromScroll: deriveFromScroll,
    /* SVG assembly */
    pt: pt,
    polygon: polygon,
    path: path,
    /* parámetros panel wiring */
    bindPanel: bindPanel,
    wireControl: wireControl
  };
})();
