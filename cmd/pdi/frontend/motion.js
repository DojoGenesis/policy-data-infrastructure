/* @spine-kit motion/motion.js · v1
   Canonical source: spine/kits/motion/motion.js
   Paste-verbatim. Fix the spine, then re-vendor — do not edit this copy in place. */

/* ═══════════════════════════════════════════════════════════════════
 * TPMotion — the spine's motion primitives, generalized from stack
 * zero (trespiesdesign-site + cruzromeromorales-site app.js).
 *
 * Seven entry points, one global (window.TPMotion):
 *   reveals(selector, opts)   IntersectionObserver reveal-on-scroll
 *   revealAll(selector)       escape hatch: make everything visible now
 *   governor({onChange})      reduced-motion + explicit override + persist
 *   flip(nodes, mutate, opts) First-Last-Invert-Play re-layout
 *   counters(scope, opts)     count-up numbers, instant mode when off
 *   onScroll(fn)              one shared rAF-batched scroll subscription
 *   spring(opts)              critically-damped numeric tween, no deps
 *
 * Every entry point that produces visible motion checks motionOn()
 * itself — a page can vendor this file and call only the pieces it
 * needs; nothing here requires governor() to have run first. Reduced
 * motion is honored from the OS preference alone until something
 * (governor, or a stack's own code) sets .no-motion / .force-motion
 * on <html>. See MOTION-RULES.md for the full policy this encodes.
 *
 * ES2018 IIFE. No modules, no imports, no export. Paste-verbatim.
 * ═══════════════════════════════════════════════════════════════════ */
(function (window, document) {
  'use strict';

  var doc = document.documentElement;

  /* ── failsafe teardown — runs the instant this file executes ──────
     The inline head snippet arms a timer (and, for a document already
     hidden at parse time, sets the class immediately) that adds
     .motion-failed so content is never left waiting on a script that
     never shows up — see motion.css's failsafe rule. Reaching this line
     at all means motion.js DID load and run, so clear that timer before
     it can ever fire. Do NOT unconditionally strip .motion-failed too,
     though: if the document is STILL hidden right now, DocumentTimeline
     is still suspended, so handing this visitor back into the hiding
     .reveal rule would only recreate the exact freeze the failsafe
     exists to prevent. Only a document that is visible right now is safe
     to hand back to the normal reveal/transition path. */
  if (window.__tpMotionFailsafe) {
    clearTimeout(window.__tpMotionFailsafe);
    window.__tpMotionFailsafe = null;
  }
  if (!document.hidden) {
    doc.classList.remove('motion-failed');
  }

  /* ── motion state — a pure read ─────────────────────────────────
     .no-motion    -> motion is OFF, for any reason: the OS default
                      (prefers-reduced-motion: reduce, no override
                      yet) or an explicit user opt-out.
     .force-motion -> motion is explicitly ON despite an OS-level
                      prefers-reduced-motion: reduce — the override
                      escape hatch for a user who opted back in.
     Neither class present -> the OS preference decides, live. This
     function is intentionally independent of governor() below, so
     reveals()/flip()/counters() work correctly even if a stack never
     calls governor() at all — reduced motion stays honored by
     default either way. */
  function prefersReduce() {
    return !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches);
  }
  function motionOn() {
    if (doc.classList.contains('force-motion')) { return true; }
    if (doc.classList.contains('no-motion')) { return false; }
    return !prefersReduce();
  }

  function cssVar(name, fallback) {
    var v = '';
    try { v = window.getComputedStyle(doc).getPropertyValue(name).trim(); } catch (e) { /* ignore */ }
    return v || fallback || '';
  }
  function toArray(nodes) {
    if (!nodes) { return []; }
    if (typeof nodes === 'string') { return Array.prototype.slice.call(document.querySelectorAll(nodes)); }
    if (typeof nodes.length !== 'number') { return [nodes]; }
    return Array.prototype.slice.call(nodes);
  }
  function safeRect(el) {
    if (!el || typeof el.getBoundingClientRect !== 'function') { return null; }
    try { return el.getBoundingClientRect(); } catch (e) { return null; }
  }

  /* ═══════════════ reveals / revealAll ═══════════════════════════ */
  var revealState = { selector: null, observer: null };

  function revealAll(selector) {
    var sel = selector || revealState.selector || '.reveal';
    var els = document.querySelectorAll(sel);
    for (var i = 0; i < els.length; i++) { els[i].classList.add('in'); }
  }

  function reveals(selector, opts) {
    opts = opts || {};
    var sel = selector || '.reveal';
    var onReveal = typeof opts.onReveal === 'function' ? opts.onReveal : null;
    revealState.selector = sel;

    if (revealState.observer) { revealState.observer.disconnect(); revealState.observer = null; }

    if (document.hidden) {
      /* DocumentTimeline is suspended for the whole time document.hidden
         is true, so a reveal transition begun in a background tab
         freezes at opacity 0 and never reaches 1 — and IntersectionObserver
         callbacks don't fire in a non-rendered document either, so an
         observer created here would never even add .in in the first
         place. Content correctness beats animation: reveal everything
         now, create no observer, and skip straight to the finished page
         for this visitor. */
      revealAll(sel);
      return null;
    }

    if (!motionOn() || !('IntersectionObserver' in window)) {
      /* Degraded path (motion off, or no IntersectionObserver support):
         reveal everything immediately, but still route each element
         through onReveal — a stack that wires counters via onReveal
         must get correct instant values here too, not just on the
         IO-driven path below. */
      var fallback = document.querySelectorAll(sel);
      for (var f = 0; f < fallback.length; f++) {
        fallback[f].classList.add('in');
        if (onReveal) { onReveal(fallback[f]); }
      }
      return null;
    }

    var obs = new window.IntersectionObserver(function (entries) {
      for (var i = 0; i < entries.length; i++) {
        if (entries[i].isIntersecting) {
          var el = entries[i].target;
          el.classList.add('in');
          obs.unobserve(el);
          if (onReveal) { onReveal(el); }
        }
      }
      /* threshold: 0, NOT a fraction — this is the fix that must
         survive every re-vendor. A fractional threshold (e.g. 0.18)
         requires that percentage of the element's box to be on
         screen at once. A .reveal block taller than the viewport can
         NEVER reach that ratio — its top can be visible, its bottom
         can be visible, but never enough of the whole box at the same
         time — so it would sit at opacity 0 forever: content
         permanently invisible, with no error and no way to recover
         short of a page reload with a shorter viewport. threshold 0
         fires on ANY intersection, however small; the negative bottom
         rootMargin ('0px 0px -8% 0px') still holds the trigger back
         until the element is genuinely on screen, so it doesn't fire
         the instant a single pixel clips the viewport's far edge. */
    }, { threshold: 0, rootMargin: '0px 0px -8% 0px' });

    var els = document.querySelectorAll(sel);
    for (var j = 0; j < els.length; j++) {
      /* skip elements a prior call already revealed — reveals() is
         safe to call again after a stack injects new .reveal nodes */
      if (!els[j].classList.contains('in')) { obs.observe(els[j]); }
    }
    revealState.observer = obs;
    return obs;
  }

  /* ═══════════════ flip ═══════════════════════════════════════════
     Generalized from puertas' applyFilter(): measure First, run the
     caller's mutate(), measure Last, Invert with a transform, Play.
     Assumes mutate() reorders, hides, or restyles the SAME element
     references passed in — that is what makes "measure before / after
     the same nodes" meaningful, and it is also what keeps focus safe:
     flip() never removes or replaces a node, so an element the user
     had focused stays exactly where the DOM left it. If a caller's
     mutate() destroys and recreates nodes instead, there is nothing
     for this helper to measure a Last state on, and any lost focus is
     a consequence of that recreation, not of flip() itself. */
  var liveAnimations = [];
  function trackAnimation(anim) {
    liveAnimations.push(anim);
    var untrack = function () {
      var i = liveAnimations.indexOf(anim);
      if (i > -1) { liveAnimations.splice(i, 1); }
    };
    if (anim.finished && typeof anim.finished.then === 'function') { anim.finished.then(untrack, untrack); }
    else { anim.onfinish = untrack; anim.oncancel = untrack; }
  }
  function finishTrackedAnimations() {
    var live = liveAnimations.slice();
    liveAnimations.length = 0;
    for (var i = 0; i < live.length; i++) {
      try { live[i].finish(); } catch (e1) { try { live[i].cancel(); } catch (e2) { /* ignore */ } }
    }
  }

  function flip(nodes, mutate, opts) {
    opts = opts || {};
    var list = toArray(nodes);

    if (typeof mutate !== 'function') { return; }

    if (!motionOn()) { mutate(); return; }

    var first = list.map(safeRect);
    var anyUsable = false;
    for (var k = 0; k < first.length; k++) {
      var r0 = first[k];
      if (r0 && (r0.width || r0.height || r0.top || r0.left)) { anyUsable = true; break; }
    }
    if (!list.length || !anyUsable) { mutate(); return; }

    mutate();

    var dur = opts.duration || parseFloat(cssVar('--motion-dur-flip', '380')) || 380;
    var ease = opts.easing || cssVar('--motion-ease-flip') || cssVar('--motion-ease') || 'cubic-bezier(.22,.9,.3,1)';

    list.forEach(function (el, i) {
      var f = first[i];
      var last = safeRect(el);
      if (!f || !last) { return; }

      var dx = f.left - last.left;
      var dy = f.top - last.top;
      var sx = last.width ? f.width / last.width : 1;
      var sy = last.height ? f.height / last.height : 1;
      var moved = Math.abs(dx) > 0.5 || Math.abs(dy) > 0.5;
      var scaled = Math.abs(sx - 1) > 0.01 || Math.abs(sy - 1) > 0.01;
      if (!moved && !scaled) { return; }

      if (typeof el.animate === 'function') {
        var anim = el.animate(
          [
            { transform: 'translate(' + dx.toFixed(2) + 'px,' + dy.toFixed(2) + 'px) scale(' + sx.toFixed(4) + ',' + sy.toFixed(4) + ')' },
            { transform: 'none' }
          ],
          { duration: dur, easing: ease }
        );
        trackAnimation(anim);
      } else {
        /* No Web Animations API: plain transition fallback. If motion
           gets switched off mid-flight, motion.css's .no-motion /
           reduced-motion transition-duration override finishes this
           almost instantly rather than leaving it stuck. */
        var prevTransition = el.style.transition;
        el.style.transition = 'none';
        el.style.transform = 'translate(' + dx.toFixed(2) + 'px,' + dy.toFixed(2) + 'px) scale(' + sx.toFixed(4) + ',' + sy.toFixed(4) + ')';
        el.getBoundingClientRect(); /* force reflow so the start transform paints before the transition is re-armed */
        el.style.transition = 'transform ' + dur + 'ms ' + ease;
        el.style.transform = '';
        el.addEventListener('transitionend', function cleanup() {
          el.style.transition = prevTransition;
          el.removeEventListener('transitionend', cleanup);
        });
      }
    });
  }

  /* ═══════════════ counters ════════════════════════════════════════
     Target value always comes from the DOM (data-count), never from a
     JS literal — so the number an author types into the markup is the
     one source of truth: it's what a JS-off visitor sees verbatim,
     and what JS animates from 0 (or snaps to instantly) toward. */
  function fmtNum(v, dec, locale) {
    return v.toLocaleString(locale || 'en-US', { minimumFractionDigits: dec, maximumFractionDigits: dec });
  }
  function finishCounter(el) {
    var target = parseFloat(el.getAttribute('data-count'));
    if (isNaN(target)) { return; }
    var dec = parseInt(el.getAttribute('data-decimals') || '0', 10);
    el.textContent = fmtNum(target, dec, el.getAttribute('data-locale'));
  }
  function animateCounter(el) {
    var target = parseFloat(el.getAttribute('data-count'));
    if (isNaN(target)) { return; }
    var dec = parseInt(el.getAttribute('data-decimals') || '0', 10);
    var dur = parseInt(el.getAttribute('data-duration') || '1200', 10);
    var locale = el.getAttribute('data-locale');
    var t0 = null;
    function frame(t) {
      if (!motionOn()) { finishCounter(el); return; } /* toggled off mid-flight: snap, never freeze */
      /* Backgrounded mid-count: rAF is about to stop being delivered, so
         snap to the real value now. Freezing here would leave a visibly
         wrong number on screen for as long as the tab stays hidden. */
      if (document.hidden) { finishCounter(el); return; }
      if (t0 === null) { t0 = t; }
      var k = Math.min(1, (t - t0) / dur);
      var eased = 1 - Math.pow(1 - k, 3);
      el.textContent = fmtNum(target * eased, dec, locale);
      if (k < 1) { window.requestAnimationFrame(frame); }
    }
    window.requestAnimationFrame(frame);
  }
  function counters(scope, opts) {
    opts = opts || {};
    var root = scope && typeof scope.querySelectorAll === 'function' ? scope : document;
    var sel = opts.selector || '.num[data-count]';
    var nums = root.querySelectorAll(sel);
    /* opts.instant can force instant-on, never force instant-off: a
       caller must not be able to use it to bypass motion-off.

       document.hidden forces instant too, and this is Rule 0 applied to
       TEXT. A counter animates 0 -> target across rAF frames; a hidden
       document suspends rAF, so the count freezes wherever it stopped and
       the page displays a number that is simply false. Observed on the
       live showcase: a "6" rendered as "1" and a "2" as "0" — not a
       missing value, a WRONG one, which is worse than blank because it
       reads as deliberate. The CSS failsafe cannot reach this: it governs
       opacity, and these are text nodes. */
    var instant = opts.instant === true || !motionOn() || document.hidden;
    for (var i = 0; i < nums.length; i++) {
      if (instant) { finishCounter(nums[i]); } else { animateCounter(nums[i]); }
    }
  }

  /* ═══════════════ onScroll ════════════════════════════════════════
     One shared listener + one shared "ticking" flag serves every
     subscriber, so N callers cost one rAF-batched read per frame, not
     N scroll-event handlers each forcing their own layout pass. Never
     call the callback directly off the scroll event — only from
     inside the rAF tick. */
  var scrollSubs = [];
  var scrollTicking = false;
  var scrollWired = false;
  function runScrollSubs() {
    scrollTicking = false;
    for (var i = 0; i < scrollSubs.length; i++) {
      try { scrollSubs[i](); } catch (e) { /* one bad subscriber must not break the rest */ }
    }
  }
  function requestScrollTick() {
    if (!scrollTicking) { scrollTicking = true; window.requestAnimationFrame(runScrollSubs); }
  }
  function onScroll(fn) {
    if (typeof fn !== 'function') { return function () {}; }
    scrollSubs.push(fn);
    if (!scrollWired) {
      scrollWired = true;
      window.addEventListener('scroll', requestScrollTick, { passive: true });
      window.addEventListener('resize', requestScrollTick, { passive: true });
    }
    return function unsubscribe() {
      var i = scrollSubs.indexOf(fn);
      if (i > -1) { scrollSubs.splice(i, 1); }
    };
  }

  /* ═══════════════ spring ══════════════════════════════════════════
     A small numeric spring-feel tween: semi-implicit Euler integration
     of a mass-spring-damper, critically damped by default (damping =
     2·sqrt(stiffness·mass), the ζ=1 condition — settles as fast as
     possible with no overshoot). Operates on plain numbers; the
     caller's onUpdate maps the number to whatever CSS/canvas value it
     needs. Pure JS, no dependency. */
  function spring(opts) {
    opts = opts || {};
    var from = typeof opts.from === 'number' ? opts.from : 0;
    var to = typeof opts.to === 'number' ? opts.to : 1;
    var mass = opts.mass || 1;
    var stiffness = opts.stiffness || 170;
    var damping = typeof opts.damping === 'number' ? opts.damping : 2 * Math.sqrt(stiffness * mass);
    var precision = opts.precision || 0.005;
    var onUpdate = typeof opts.onUpdate === 'function' ? opts.onUpdate : function () {};
    var onComplete = typeof opts.onComplete === 'function' ? opts.onComplete : function () {};

    if (!motionOn()) { onUpdate(to); onComplete(to); return { stop: function () {} }; }

    var pos = from;
    var vel = typeof opts.velocity === 'number' ? opts.velocity : 0;
    var last = null;
    var raf = null;
    var stopped = false;

    onUpdate(pos); /* paint the starting value immediately, before any physics runs */

    function step(t) {
      if (stopped) { return; }
      if (last === null) { last = t; raf = window.requestAnimationFrame(step); return; }
      if (!motionOn()) { pos = to; onUpdate(pos); onComplete(pos); stopped = true; return; } /* snap, never freeze */

      var dt = Math.min((t - last) / 1000, 1 / 30); /* clamp so a backgrounded tab can't blow up the integrator */
      last = t;

      var fSpring = -stiffness * (pos - to);
      var fDamp = -damping * vel;
      vel += (fSpring + fDamp) / mass * dt;
      pos += vel * dt;

      var settled = Math.abs(to - pos) < precision && Math.abs(vel) < precision;
      if (settled) { pos = to; onUpdate(pos); onComplete(pos); stopped = true; return; }

      onUpdate(pos);
      raf = window.requestAnimationFrame(step);
    }
    raf = window.requestAnimationFrame(step);

    return {
      stop: function () { stopped = true; if (raf) { window.cancelAnimationFrame(raf); } }
    };
  }

  /* ═══════════════ governor ════════════════════════════════════════
     Reads the OS preference, exposes an explicit user override,
     persists it, and calls back so a stack can start/stop its own
     loops (a WebGL field, a CSS animation driver, anything else) —
     generalized from applyMotion() in trespiesdesign-site/app.js.
     Class semantics (mirrored in motion.css):
       no-motion    -> motion is OFF (default-off or explicit opt-out)
       force-motion -> motion is ON despite prefers-reduced-motion:
                       reduce (an explicit opt-in override)
     At most one of the two is ever present; often neither is (motion
     on, OS has no preference — the unremarkable default case). */
  function governor(opts) {
    opts = opts || {};
    var onChange = typeof opts.onChange === 'function' ? opts.onChange : function () {};
    var storageKey = opts.storageKey || 'tp-motion';

    var store = null;
    try { store = window.localStorage; } catch (e) { /* blocked: private mode, locked-down embed, etc. */ }

    var reduce = prefersReduce();
    var override = null; /* true | false | null (= follow the OS preference) */
    if (store) {
      try {
        var raw = store.getItem(storageKey);
        if (raw === 'on') { override = true; } else if (raw === 'off') { override = false; }
      } catch (e) { /* ignore */ }
    }

    var on = override === null ? !reduce : override;

    function paint() {
      doc.classList.toggle('no-motion', !on);
      doc.classList.toggle('force-motion', on && reduce);
    }
    function persist() {
      if (!store) { return; }
      try { store.setItem(storageKey, on ? 'on' : 'off'); } catch (e) { /* ignore */ }
    }
    function settle() {
      /* Turning motion off must snap to the finished/static state,
         never freeze mid-animation. rAF-driven loops (spring,
         counters) already self-check motionOn() every tick and snap
         there on their own; this covers the two things that do NOT
         tick through this module's own code once started: elements
         still hidden and waiting on the IntersectionObserver, and any
         in-flight flip() WAAPI animation. */
      revealAll();
      finishTrackedAnimations();
    }
    function apply(next, silent) {
      on = !!next;
      paint();
      if (!on) { settle(); }
      if (!silent) { persist(); }
      onChange(on);
    }

    paint(); /* first paint, synchronous — no flash of the wrong state */

    if (window.matchMedia) {
      var mql = window.matchMedia('(prefers-reduced-motion: reduce)');
      var onMqlChange = function (e) {
        reduce = e.matches;
        if (override === null) { apply(!reduce, true); } else { paint(); }
      };
      if (mql.addEventListener) { mql.addEventListener('change', onMqlChange); }
      else if (mql.addListener) { mql.addListener(onMqlChange); } /* legacy Safari */
    }

    return {
      isOn: function () { return on; },
      set: function (next) { override = !!next; apply(override); },
      reset: function () {
        override = null;
        if (store) { try { store.removeItem(storageKey); } catch (e) { /* ignore */ } }
        apply(!reduce, true); /* silent: re-persisting here would immediately recreate the override we just cleared */
      }
    };
  }

  window.TPMotion = {
    reveals: reveals,
    revealAll: revealAll,
    governor: governor,
    flip: flip,
    counters: counters,
    onScroll: onScroll,
    spring: spring,
    isOn: motionOn
  };
})(window, document);
