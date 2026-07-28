// ═══════════════════════════════════════════════════════════════════
// PDI Deep-Link Utility — ADR-012 I4
// ──────────────────────────────────────────────────────────────────
// Shared helpers for URL state management and Copy Link buttons.
// Every page that uses this gets:
//   1. PDIDeepLink.syncURL(params)         — history.replaceState with given params
//   2. PDIDeepLink.readParams()            — returns current URLSearchParams
//   3. PDIDeepLink.copyLink(buttonEl)      — copies current URL to clipboard
//   4. PDIDeepLink.renderCopyButton(el)    — renders a "Copy Link" button into el
//
// Usage:
//   Load <script src="/static/lib/deeplink.js"></script> before your page script.
//   Call PDIDeepLink.renderCopyButton(containerEl) to insert the button.
//   In Alpine watchers, call PDIDeepLink.syncURL({key: val, ...}) to update the URL.
// ═══════════════════════════════════════════════════════════════════

(function () {
  'use strict';

  var PDIDeepLink = window.PDIDeepLink || {};

  // ── Copy Link SVG icon (inline, no external deps) ──────────────
  var LINK_ICON = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="flex-shrink:0;">' +
    '<path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/>' +
    '<path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>' +
    '</svg>';

  var CHECK_ICON = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="flex-shrink:0;">' +
    '<polyline points="20 6 9 17 4 12"/>' +
    '</svg>';

  // ── Read current URL params ────────────────────────────────────
  PDIDeepLink.readParams = function () {
    return new URLSearchParams(window.location.search);
  };

  // ── Sync URL with given params map (replaceState, no reload) ───
  // Pass an object of {key: value} pairs. Null/undefined/empty-string
  // values are omitted. Pass {} to clear all params.
  PDIDeepLink.syncURL = function (params) {
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

  // ── Get a single param value, with default ─────────────────────
  PDIDeepLink.getParam = function (name, defaultVal) {
    var params = new URLSearchParams(window.location.search);
    var val = params.get(name);
    return val != null ? val : (defaultVal !== undefined ? defaultVal : null);
  };

  // ── Copy current page URL to clipboard ─────────────────────────
  PDIDeepLink.copyLink = function (buttonEl) {
    var url = window.location.href;
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(url).then(function () {
        PDIDeepLink._flashCopied(buttonEl);
      }).catch(function () {
        PDIDeepLink._fallbackCopy(url, buttonEl);
      });
    } else {
      PDIDeepLink._fallbackCopy(url, buttonEl);
    }
  };

  // ── Flash "Copied!" on the button ──────────────────────────────
  PDIDeepLink._flashCopied = function (el) {
    if (!el) return;
    var origHTML = el.innerHTML;
    var origTitle = el.getAttribute('title') || '';
    el.innerHTML = CHECK_ICON + ' <span>Copied!</span>';
    el.setAttribute('title', 'Link copied to clipboard');
    el.classList.add('copy-link--copied');
    el.style.pointerEvents = 'none';
    setTimeout(function () {
      el.innerHTML = origHTML;
      el.setAttribute('title', origTitle);
      el.classList.remove('copy-link--copied');
      el.style.pointerEvents = '';
    }, 1800);
  };

  // ── Fallback copy using textarea (older browsers) ──────────────
  PDIDeepLink._fallbackCopy = function (text, buttonEl) {
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
      PDIDeepLink._flashCopied(buttonEl);
    } catch (e) {
      if (buttonEl) {
        buttonEl.textContent = 'Copy failed';
        setTimeout(function () {
          buttonEl.innerHTML = LINK_ICON + ' <span>Copy Link</span>';
        }, 2000);
      }
    }
    document.body.removeChild(ta);
  };

  // ── Render a "Copy Link" button into a container element ───────
  // Call once per page, e.g. PDIDeepLink.renderCopyButton(document.getElementById('copy-link-container'))
  PDIDeepLink.renderCopyButton = function (containerEl) {
    if (!containerEl) return;
    var btn = document.createElement('button');
    btn.className = 'copy-link-btn';
    btn.setAttribute('type', 'button');
    btn.setAttribute('title', 'Copy shareable link to clipboard');
    btn.setAttribute('aria-label', 'Copy shareable link to clipboard');
    btn.innerHTML = LINK_ICON + ' <span>Copy Link</span>';
    btn.addEventListener('click', function () {
      PDIDeepLink.copyLink(btn);
    });
    containerEl.appendChild(btn);
  };

  // ── Auto-inject a copy-link button into the first .page-actions
  //    or .portrait-actions container found on the page ──────────
  PDIDeepLink.autoInjectCopyButton = function () {
    var container = document.querySelector('.page-actions') ||
                    document.querySelector('.portrait-actions') ||
                    document.querySelector('.map-controls') ||
                    document.querySelector('.compare-toolbar') ||
                    document.querySelector('.filter-bar');
    if (container) {
      PDIDeepLink.renderCopyButton(container);
    }
  };

  window.PDIDeepLink = PDIDeepLink;
})();
