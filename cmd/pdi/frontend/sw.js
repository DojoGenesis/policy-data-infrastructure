// sw.js — PDI Service Worker
// NetworkFirst strategy: try network, fall back to cache.
// Caches static assets (CSS, JS, HTML) and API responses for 24h.
// Provides offline support for previously visited pages.
//
// ADR-012 I7: PWA offline support.

const CACHE_NAME = 'pdi-v1';
const STATIC_CACHE = 'pdi-static-v1';
const API_CACHE = 'pdi-api-v1';
const MAX_API_AGE = 24 * 60 * 60 * 1000; // 24 hours

// Resources to pre-cache on install.
// Every entry MUST resolve — cache.addAll() rejects the whole batch if any
// single request 404s, so one stale path silently precaches nothing at all.
// chat-drawer.js/.css were removed 2026-07-28 and deleted from disk; leaving
// them here would have failed the entire precache.
const PRECACHE_URLS = [
  '/',
  '/static/tokens.css',
  '/static/motion.css',
  '/static/styles.css',
  '/static/alpine.min.js',
  '/static/motion.js',
  '/static/charts.js',
  '/static/lang-toggle.js',
  '/static/theme-toggle.js',
  '/static/lib/api.js',
  '/static/lib/domain.js',
  '/static/lib/chat.js',
  '/static/lib/deeplink.js',
  '/static/pdi-icon.svg',
];

// ── Install: pre-cache static assets ──────────────────────────
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(STATIC_CACHE).then((cache) => {
      return cache.addAll(PRECACHE_URLS).catch((err) => {
        console.warn('[PDI SW] Pre-cache partial failure:', err.message);
      });
    }).then(() => self.skipWaiting())
  );
});

// ── Activate: clean old caches ────────────────────────────────
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) => {
      return Promise.all(
        keys
          .filter((k) => k !== STATIC_CACHE && k !== API_CACHE)
          .map((k) => caches.delete(k))
      );
    }).then(() => self.clients.claim())
  );
});

// ── Helpers ───────────────────────────────────────────────────

function isStaticAsset(url) {
  const path = new URL(url).pathname;
  return (
    path.startsWith('/static/') ||
    path.endsWith('.css') ||
    path.endsWith('.js') ||
    path.endsWith('.svg') ||
    path.endsWith('.ico') ||
    path.endsWith('.png') ||
    path.endsWith('.woff2')
  );
}

function isAPIRequest(url) {
  const path = new URL(url).pathname;
  return path.startsWith('/v1/');
}

function isHTMLPage(url) {
  const path = new URL(url).pathname;
  // Clean URLs served by PDI: /, /county, /compare, etc.
  if (path === '/' || path === '') return true;
  const pages = ['/county', '/compare', '/evidence', '/candidates',
                 '/map', '/narrative', '/about', '/composite', '/chat'];
  // Also match /es/<page>
  const cleanPath = path.replace(/\/$/, '');
  if (pages.includes(cleanPath)) return true;
  if (cleanPath.startsWith('/es/')) return true;
  return false;
}

// ── NetworkFirst strategy ─────────────────────────────────────
// Try network; if it fails (offline/timeout), serve from cache.
// On success, update cache.

async function networkFirst(request) {
  const cacheName = isAPIRequest(request.url) ? API_CACHE : STATIC_CACHE;

  try {
    const networkResponse = await fetch(request);

    // Only cache successful GET responses
    if (request.method === 'GET' && networkResponse.ok) {
      const cache = await caches.open(cacheName);
      // Clone the response — one goes to browser, one to cache
      cache.put(request, networkResponse.clone());
    }

    return networkResponse;
  } catch (error) {
    // Network failed — try cache
    const cachedResponse = await caches.match(request);
    if (cachedResponse) {
      // Check if API cache entry is too old
      if (isAPIRequest(request.url)) {
        const cachedDate = cachedResponse.headers.get('sw-cached-date');
        if (cachedDate) {
          const age = Date.now() - parseInt(cachedDate, 10);
          if (age > MAX_API_AGE) {
            console.debug('[PDI SW] Cached API response too old:', request.url);
          }
        }
      }
      return cachedResponse;
    }

    // No cache — return offline page for HTML requests
    if (isHTMLPage(request.url) || request.headers.get('Accept')?.includes('text/html')) {
      return new Response(
        '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">' +
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">' +
        '<title>PDI — Offline</title>' +
        '<link rel="stylesheet" href="/static/tokens.css">' +
        '<link rel="stylesheet" href="/static/styles.css">' +
        '<style>body{display:flex;align-items:center;justify-content:center;min-height:100vh;' +
        'text-align:center;font-family:var(--font-sans);background:var(--bg);color:var(--text);}' +
        '.offline-card{max-width:420px;padding:32px;}' +
        'h1{font-size:1.5rem;margin-bottom:8px;}p{color:var(--muted);font-size:0.9rem;line-height:1.5;}' +
        '.offline-icon{font-size:3rem;margin-bottom:16px;}' +
        '</style></head><body><div class="offline-card">' +
        '<div class="offline-icon">📡</div>' +
        '<h1>You\'re offline</h1>' +
        '<p>PDI is showing cached data from your last visit. ' +
        'Some features (search, chat, dynamic indicators) may not be available until you reconnect.</p>' +
        '<p style="margin-top:12px;font-size:0.78rem;">Reconnect to the internet and refresh for fresh data.</p>' +
        '</div></body></html>',
        {
          status: 503,
          statusText: 'Offline',
          headers: { 'Content-Type': 'text/html; charset=utf-8' }
        }
      );
    }

    // For non-HTML requests (API, etc.), return a JSON error
    if (isAPIRequest(request.url)) {
      return new Response(
        JSON.stringify({ error: 'offline', message: 'You are offline. Showing cached data where available.' }),
        {
          status: 503,
          headers: { 'Content-Type': 'application/json' }
        }
      );
    }

    throw error;
  }
}

// ── Fetch handler ─────────────────────────────────────────────
self.addEventListener('fetch', (event) => {
  const { request } = event;
  const url = new URL(request.url);

  // Only handle GET requests for our own origin
  if (request.method !== 'GET') return;
  if (url.origin !== self.location.origin) return;

  // Skip service worker itself and chrome-extension requests
  if (url.pathname === '/sw.js') return;

  // Use NetworkFirst for everything
  event.respondWith(networkFirst(request));
});

// ── Message channel for client ↔ SW communication ────────────
self.addEventListener('message', (event) => {
  if (event.data === 'SKIP_WAITING') {
    self.skipWaiting();
  }

  if (event.data === 'CHECK_ONLINE') {
    // Client can ping SW to check connectivity
    event.ports[0]?.postMessage({ online: self.navigator?.onLine ?? true });
  }
});
