// ═══════════════════════════════════════════════════════════════════
// PDI URL State Utility — url-state.js
// ──────────────────────────────────────────────────────────────────
// Shared helpers for deep-linkable URL state management.
// Every page that uses this gets:
//   1. readParams()            — returns current URLSearchParams
//   2. updateParam(key, value) — sets a single param (replaceState)
//   3. copyLink(buttonEl)      — copies current URL to clipboard
//
// Usage:
//   Load <script src="/static/lib/url-state.js"></script> before your page script.
//   Call URLState.readParams() to read all URL params.
//   Call URLState.updateParam('tab', 'economic') to update a single param.
//   Call URLState.copyLink(el) to copy the current URL.
//
// Note: This is a companion to deeplink.js (PDIDeepLink namespace).
// Pages that load deeplink.js can use PDIDeepLink directly.
// Pages that only need the basic three helpers can load this instead.
// ═══════════════════════════════════════════════════════════════════

(function () {
  'use strict';

  var URLState = window.URLState || {};

  // ── Read current URL params ────────────────────────────────────
  URLState.readParams = function () {
    return new URLSearchParams(window.location.search);
  };

  // ── Get a single param value ───────────────────────────────────
  URLState.getParam = function (name, defaultVal) {
    var params = new URLSearchParams(window.location.search);
    var val = params.get(name);
    return val != null ? val : (defaultVal !== undefined ? defaultVal : null);
  };

  // ── Update a single URL parameter (replaceState, no reload) ────
  // Pass null or '' to remove the parameter.
  URLState.updateParam = function (key, value) {
    var sp = new URLSearchParams(window.location.search);
    if (value != null && value !== '') {
      sp.set(key, String(value));
    } else {
      sp.delete(key);
    }
    var qs = sp.toString();
    var newUrl = window.location.pathname + (qs ? '?' + qs : '');
    if (window.location.search !== (qs ? '?' + qs : '')) {
      window.history.replaceState(null, '', newUrl);
    }
  };

  // ── Replace ALL URL params at once ─────────────────────────────
  URLState.replaceParams = function (params) {
    var sp = new URLSearchParams();
    if (params) {
      Object.keys(params).forEach(function (k) {
        var v = params[k];
        if (v != null && v !== '') {
          sp.set(k, String(v));
        }
      });
    }
    var qs = sp.toString();
    var newUrl = window.location.pathname + (qs ? '?' + qs : '');
    if (window.location.search !== (qs ? '?' + qs : '')) {
      window.history.replaceState(null, '', newUrl);
    }
  };

  // ── Copy current page URL to clipboard ─────────────────────────
  URLState.copyLink = function (buttonEl) {
    var url = window.location.href;
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(url).then(function () {
        URLState._flashCopied(buttonEl);
      }).catch(function () {
        URLState._fallbackCopy(url, buttonEl);
      });
    } else {
      URLState._fallbackCopy(url, buttonEl);
    }
  };

  // ── Flash "Copied!" on the button ──────────────────────────────
  URLState._flashCopied = function (el) {
    if (!el) return;
    var origHTML = el.innerHTML;
    el.innerHTML = '✓ Copied!';
    el.classList.add('copy-link--copied');
    el.style.pointerEvents = 'none';
    setTimeout(function () {
      el.innerHTML = origHTML;
      el.classList.remove('copy-link--copied');
      el.style.pointerEvents = '';
    }, 1800);
  };

  // ── Fallback copy using textarea (older browsers) ──────────────
  URLState._fallbackCopy = function (text, buttonEl) {
    var ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.left = '-9999px';
    ta.style.top = '-9999px';
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    try {
      document.execCommand('copy');
      URLState._flashCopied(buttonEl);
    } catch (e) {
      if (buttonEl) {
        buttonEl.textContent = 'Copy failed';
        setTimeout(function () {
          buttonEl.innerHTML = '📋 Copy Link';
        }, 2000);
      }
    }
    document.body.removeChild(ta);
  };

  // ── Render a "Copy Link" button into a container element ───────
  URLState.renderCopyButton = function (containerEl) {
    if (!containerEl) return;
    var btn = document.createElement('button');
    btn.className = 'copy-link-btn';
    btn.setAttribute('type', 'button');
    btn.setAttribute('title', 'Copy shareable link to clipboard');
    btn.setAttribute('aria-label', 'Copy shareable link to clipboard');
    btn.innerHTML = '📋 Copy Link';
    btn.addEventListener('click', function () {
      URLState.copyLink(btn);
    });
    containerEl.appendChild(btn);
  };

  // ── Auto-inject a copy-link button into common containers ──────
  URLState.autoInjectCopyButton = function () {
    var container = document.querySelector('.page-actions') ||
                    document.querySelector('.portrait-actions') ||
                    document.querySelector('.map-controls') ||
                    document.querySelector('.compare-toolbar') ||
                    document.querySelector('.filter-bar');
    if (container) {
      URLState.renderCopyButton(container);
    }
  };

  window.URLState = URLState;
})();
