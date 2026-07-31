/* app.js — PDI Direction A (Canvas-First Explorer)
   Interactive Wisconsin county map, real county geometry from the embedded
   GeoJSON (explorer-data.js). The detail sidebar merges two REAL data
   sources — the embedded GeoJSON's own ACS snapshot fields (ten metrics
   baked into explorer-data.js for all 72 counties) as a baseline, overlaid
   with GET /v1/policy/geographies/:geoid/indicators for the selected
   county's live indicators when the live API/DB actually has rows. Live
   data wins on overlap; every rendered row is visibly labeled which
   source it came from (see provenanceBadgeHtml()). GET /v1/policy/variables
   is fetched once for catalog metadata. Responsive design: map/sidebar
   side-by-side on desktop, overlay on mobile.

   ── History (read before changing this file again) ─────────────────────
   1. Originally this sidebar rendered PDI_VARIABLES, a hand-authored list
      of 34 variables, and fell back to Math.random() for any county/variable
      combination not present in the embedded static GeoJSON — i.e. it
      fabricated data for 24 of those 34 "variables" out of thin air. That
      was a straight-up bug and has been removed; it must never come back.
   2. The immediate fix after that added REAL_VARIABLE_IDS, an allowlist of
      the 10 fields that happen to be present in the explorer's embedded
      static GeoJSON, and hid everything else. That stopped the fabrication
      but permanently capped the explorer at 10 fields when the live
      backend actually has a ~51-variable catalog with ~24-40+ indicators
      per county. That allowlist (and PDI_VARIABLES/REAL_PDI_VARIABLES) is
      now gone too.
   3. The next version made the sidebar's variable list DERIVED entirely
      from whatever GET /v1/policy/geographies/:geoid/indicators returns —
      the same live endpoint county.html uses — grouped into categories
      with the same inferCategory() logic county.html uses (ported
      verbatim below, sourceId-fallback quirks and all, so the two pages
      agree on where a given variable lands). That fixed the previous
      hardcoded-10-field cap, but it went too far the other way: with a
      fresh/dev Postgres whose indicators table is empty (0 rows — not a
      problem with the fetched GeoJSON), every county's sidebar showed
      zero variable cards and evidence fell back to "No data available",
      even though window.PDI_GEOJSON_DATA — already loaded, already in
      memory — still carried the same 10 real ACS fields per county that
      step 2's allowlist used to surface. loadCountyData() (below) kept
      only total_population out of those ten and discarded the other nine
      before they ever reached the DOM.
   4. This version: loadCountyData() keeps all ten embedded ACS fields
      (ACS_EMBEDDED_FIELDS below), and loadCountyIndicators() renders the
      UNION of that embedded baseline with whatever the live API returns
      (mergeIndicators()) — live data wins on any variable_id collision,
      the embedded snapshot fills in the rest, and every row is visibly
      badged with which source it came from so an embedded value is never
      mistaken for a live one. A live-fetch failure now renders the
      embedded baseline with a non-blocking retry notice instead of wiping
      the sidebar — the empty-catalog case from step 3 no longer means an
      empty sidebar. Nothing here is synthesized: every number traces to
      either the live API response or a real ACS value baked into
      explorer-data.js. See ACS_EMBEDDED_FIELDS for exactly which fields,
      and why each one's canonical live variable_id is set or deliberately
      left null.
*/

(function () {
  'use strict';

  /* ══════════════════ API helper ═══════════════════════════════════
     Matches cmd/pdi/frontend/county.html's api() (~line 2417) exactly:
     relative path, throws on non-ok response. The Go binary serves this
     JS file and the /v1/policy/* API same-origin (see cmd/pdi/serve.go —
     r.Group("/v1").Group("/policy") mounts the PolicyPlugin, and the
     static file server serves this file under /static/), so a relative
     path resolves correctly in production. There is no dev-only base-URL
     branch here on purpose — if you need to point this at a different
     host while testing locally (e.g. the local Postgres has no rows),
     do that by serving these static files separately and monkey-patching
     window.fetch or this api() function from the console, NOT by baking
     an alternate base URL into shipped code.
  */
  function api(path) {
    return fetch('/v1/policy' + path).then(function (r) {
      if (!r.ok) throw new Error(r.status + ' ' + r.statusText);
      return r.json();
    });
  }

  /* ══════════════════ Category inference ═══════════════════════════
     Ported verbatim from county.html's inferCategory() (~line 2595) so
     the explorer and the county page agree on where every variable lands.
     Keep these two copies in sync if county.html's logic changes — this
     is deliberately not shared as a module because the two pages don't
     currently share a JS module boundary (see lib/api.js's header comment
     that lib/ is the shared adapter — a future cleanup could hoist this
     there, but that's out of scope for this fix).
  */
  var CATEGORY_ORDER = ['Economic', 'Health', 'Housing', 'Education', 'Demographic', 'Environment', 'Infrastructure', 'Food Access', 'Other'];

  function inferCategory(varId, name, sourceId) {
    var n = (name + ' ' + varId).toLowerCase();
    if (/poverty|income|gini|unemploy|labor|gdp|earning|wage/.test(n)) return 'Economic';
    if (/health|uninsured|diabet|obes|disability|morta|asthma|smoking/.test(n)) return 'Health';
    if (/housing|rent|mortgage|cost_burden|vacanc|homeowner|overcrowd/.test(n)) return 'Housing';
    if (/education|bachelor|school|enrollment|diploma/.test(n)) return 'Education';
    if (/pop|age|race|hispanic|white|black|asian|foreign|citizen|household|family/.test(n)) return 'Demographic';
    if (/environ|pollution|lead|epa|air|water|hazard/.test(n)) return 'Environment';
    if (/transp|commute|vehicle|broadband|internet/.test(n)) return 'Infrastructure';
    if (/food|snap|grocery|supermarket/.test(n)) return 'Food Access';
    if (sourceId === 'cdc') return 'Health';
    if (sourceId === 'epa') return 'Environment';
    if (sourceId === 'usda') return 'Food Access';
    return 'Other';
  }

  /* ══════════════════ Formatting ══════════════════════════════════
     fmtValue mirrors county.html's fmtValue() exactly, for consistent
     numeric display across the site's two data-driven views.
  */
  function fmtValue(v) {
    if (v === null || v === undefined) return '—';
    if (typeof v === 'number') {
      return Math.abs(v) < 10 ? v.toFixed(2) : Math.round(v).toLocaleString();
    }
    return v;
  }

  function fmtDollar(v) { return (v === null || v === undefined) ? '—' : '$' + Math.round(v).toLocaleString(); }
  function fmtPercent(v) { return (v === null || v === undefined) ? '—' : (Math.round(v * 10) / 10) + '%'; }
  function fmtPercentile(v) { return (v === null || v === undefined) ? '—' : Math.round(v * 100) + 'th pct'; }

  /* Picks the display formatter for an indicator-shaped object by its
     `formatter` hint (see ACS_EMBEDDED_FIELDS below) instead of ever
     string-matching a variable_id or name inline here. Live API rows
     don't carry a formatter hint (only embedded-baseline rows do — see
     embeddedIndicatorsFor()), so they fall through to the same generic
     fmtValue() this function always used, unchanged. */
  function formatIndicatorValue(v) {
    if (v.formatter === 'dollar') return fmtDollar(v.value);
    if (v.formatter === 'percent') return fmtPercent(v.value);
    return fmtValue(v.value);
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  /* ══════════════════ Embedded ACS Snapshot Fields ═══════════════════
     These ten properties are baked into the embedded static GeoJSON
     (explorer-data.js) for all 72 counties — real ACS 5-year estimates,
     not live-API data (see loadCountyData(), embeddedIndicatorsFor()).
     This table is the single source of truth for which GeoJSON
     properties are real ACS metrics, as opposed to geometry/labeling
     props (GEOID, NAME, BASENAME, INTPTLAT/LON, county_name) that
     loadCountyData() keeps separately and unconditionally. Per field:
       - prop:       the GeoJSON feature.properties key
       - name:       human display name for the sidebar row
       - unit:       human-readable unit, for documentation only (the
                     dollar/percent formatters below bake their own
                     symbol into the string, so this isn't rendered as a
                     separate suffix — see formatIndicatorValue())
       - formatter:  'dollar' | 'percent' | 'count' — which fmt* helper
                     renders the value (see formatIndicatorValue()); kept
                     as a per-field data hint so nothing downstream needs
                     to hardcode formatting logic keyed off a variable_id
                     or name
       - variableId: the canonical GET /v1/policy/variables catalog id
                     this metric corresponds to, cross-checked against
                     pkg/datasource/acs.go's real ACS variable defs — set
                     ONLY when the live catalog would report the exact
                     same statistic in the exact same unit, so a merge
                     collision (see mergeIndicators()) is a genuine
                     same-thing override rather than two different
                     measurements colliding by coincidence. Left null
                     when no live equivalent exists yet (e.g. the live
                     ACS source has no education-attainment table at all,
                     and no combined "severely cost-burdened" percentage)
                     or when the only related live variable is a
                     different unit — the live catalog's
                     pop_white_non_hispanic/pop_black/pop_hispanic_latino
                     are raw ACS counts, not the percentages this
                     snapshot carries, so collision-merging a percent row
                     with a raw-count live row under the same variable_id
                     would silently relabel a headcount as a percentage.
                     That's worse than leaving the two unmerged, so those
                     stay unmapped on purpose. When null,
                     embeddedIndicatorsFor() uses the GeoJSON prop name
                     itself as the row's variable_id — stable, unique,
                     and guaranteed not to collide with a real catalog id.
  */
  var ACS_EMBEDDED_FIELDS = [
    { prop: 'total_population',           name: 'Total Population',                  unit: 'count',   formatter: 'count',   variableId: 'total_population' },
    { prop: 'median_hh_income',           name: 'Median Household Income',           unit: 'dollars', formatter: 'dollar',  variableId: 'median_household_income' },
    { prop: 'poverty_rate',               name: 'Poverty Rate',                      unit: 'percent', formatter: 'percent', variableId: 'poverty_rate' },
    { prop: 'pct_cost_burdened',          name: 'Cost-Burdened Households',          unit: 'percent', formatter: 'percent', variableId: null },
    { prop: 'pct_severely_cost_burdened', name: 'Severely Cost-Burdened Households', unit: 'percent', formatter: 'percent', variableId: null },
    { prop: 'uninsured_rate',             name: 'Uninsured Rate',                    unit: 'percent', formatter: 'percent', variableId: 'uninsured_rate' },
    { prop: 'pct_bachelors_or_higher',    name: 'Bachelor\'s Degree or Higher',      unit: 'percent', formatter: 'percent', variableId: null },
    { prop: 'pct_poc',                    name: 'People of Color',                   unit: 'percent', formatter: 'percent', variableId: null },
    { prop: 'pct_hispanic',               name: 'Hispanic or Latino',                unit: 'percent', formatter: 'percent', variableId: null },
    { prop: 'pct_non_hispanic_black',     name: 'Black, Non-Hispanic',               unit: 'percent', formatter: 'percent', variableId: null },
  ];

  /* ══════════════════ County Data (loaded dynamically) ════════════════
  */
  var countyData = {};
  var countyGeometry = {};

  /* ══════════════════ App State ═════════════════════════════════════
  */
  var state = {
    selectedCounty: null,
    filteredCounties: [],
    theme: localStorage.getItem('tp-theme') || 'light',
    variableMeta: {},        // variable_id -> {name, unit, direction, description, sourceId}
    indicatorsCache: {},     // geoid -> indicators array, from the live API
    indicatorsRequestId: 0,  // guards against a stale response winning a race
  };

  /* ══════════════════ SVG Map Rendering (unchanged) ═══════════════════
     Geometry rendering is untouched by this fix — it stays sourced from
     the embedded static GeoJSON (explorer-data.js). Only the sidebar's
     data source changed, per this fix's scope.
  */
  function mercatorProject(lon, lat) {
    /* Web Mercator projection */
    var x = (lon + 180) / 360;
    var y = (1 - Math.log(Math.tan((lat + 90) * Math.PI / 360)) / Math.PI) / 2;
    return [x, y];
  }

  function renderMap() {
    var mapHost = document.getElementById('map-counties');
    if (!mapHost) return;
    mapHost.innerHTML = '';

    /* Calculate bounds from all county geometries */
    var minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    Object.keys(countyGeometry).forEach(function (id) {
      var geo = countyGeometry[id];
      if (geo.type === 'Polygon' || geo.type === 'MultiPolygon') {
        var coords = geo.type === 'Polygon' ? geo.coordinates[0] : [];
        if (geo.type === 'MultiPolygon') {
          geo.coordinates.forEach(function (p) {
            if (p[0]) coords = coords.concat(p[0]);
          });
        }
        coords.forEach(function (c) {
          var proj = mercatorProject(c[0], c[1]);
          minX = Math.min(minX, proj[0]);
          minY = Math.min(minY, proj[1]);
          maxX = Math.max(maxX, proj[0]);
          maxY = Math.max(maxY, proj[1]);
        });
      }
    });

    var padding = 0.05;
    var width = 0.9 * (maxX - minX) || 1;
    var height = 0.9 * (maxY - minY) || 1;
    var scale = Math.min((1 - 2 * padding) / width, (1 - 2 * padding) / height);

    /* Render each county */
    Object.keys(countyGeometry).forEach(function (id) {
      var geo = countyGeometry[id];
      var county = countyData[id];
      if (!county) return;

      var pathData = '';
      var polygons = geo.type === 'Polygon' ? [geo] : (geo.type === 'MultiPolygon' ?
        geo.coordinates.map(function (coords) { return { type: 'Polygon', coordinates: [coords] }; }) : []);

      polygons.forEach(function (poly) {
        poly.coordinates[0].forEach(function (c, i) {
          var proj = mercatorProject(c[0], c[1]);
          var x = padding + (proj[0] - minX) * scale;
          var y = padding + (proj[1] - minY) * scale;
          pathData += (i === 0 ? 'M' : 'L') + ' ' + (x * 960).toFixed(1) + ' ' + (y * 600).toFixed(1) + ' ';
        });
        pathData += 'Z ';
      });

      var path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('d', pathData);
      path.setAttribute('class', 'map-county');
      path.setAttribute('data-county-id', id);
      path.setAttribute('aria-label', county.name);
      path.setAttribute('role', 'button');
      path.setAttribute('tabindex', '0');
      path.addEventListener('click', function () { selectCounty(id); });
      path.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          selectCounty(id);
        }
      });

      mapHost.appendChild(path);
    });
  }

  /* ══════════════════ Indicator Fetch, Merge & Render ══════════════════
  */

  /* Turns a county's embedded ACS snapshot (ACS_EMBEDDED_FIELDS,
     loadCountyData()) into indicator-shaped objects — {variable_id, name,
     unit, value} — matching exactly what renderIndicators() and
     updateEvidenceCards() already consume from the live API, so the two
     sources can merge into one list and share a render path (see
     mergeIndicators() and loadCountyIndicators() below). Every row is
     tagged source: 'acs-embedded' so renderIndicators() can badge it
     distinctly from source: 'live' rows — this repo's whole ethos is
     that a number states where it came from, and an embedded snapshot
     value rendered indistinguishably from a live one is exactly that
     failure mode. Fields whose value is null/undefined are skipped
     outright — this function never emits a placeholder row for missing
     data, matching this file's zero-fabrication rule (see file header
     History). */
  function embeddedIndicatorsFor(id) {
    var county = countyData[id];
    var out = [];
    if (!county) return out;

    ACS_EMBEDDED_FIELDS.forEach(function (field) {
      var value = county[field.prop];
      if (value === null || value === undefined) return;
      out.push({
        variable_id: field.variableId || field.prop,
        name: field.name,
        unit: '', /* dollar/percent formatters bake in their own symbol; see formatIndicatorValue() */
        value: value,
        formatter: field.formatter,
        source: 'acs-embedded',
      });
    });
    return out;
  }

  /* Merges the embedded ACS baseline with live API indicators into one
     list, keyed by variable_id. Embedded rows seed the list (and its
     display order); a live row with the same variable_id REPLACES the
     embedded row in place — live data wins on any collision, per this
     fix's spec — and a live row with a new variable_id is appended
     after. Callers must tag every live indicator source: 'live' before
     calling this (loadCountyIndicators() does, right after the
     name/unit catalog backfill) so the merged rows stay
     provenance-labeled either way. */
  function mergeIndicators(embedded, live) {
    var byId = {};
    var order = [];

    embedded.forEach(function (ind) {
      if (!byId.hasOwnProperty(ind.variable_id)) order.push(ind.variable_id);
      byId[ind.variable_id] = ind;
    });
    (live || []).forEach(function (ind) {
      if (!byId.hasOwnProperty(ind.variable_id)) order.push(ind.variable_id);
      byId[ind.variable_id] = ind; /* live wins: overwrites the embedded entry, or adds a new one */
    });

    return order.map(function (varId) { return byId[varId]; });
  }

  /* Fetches this county's real indicators from the live API and renders
     the union of that data with the embedded ACS baseline (see
     embeddedIndicatorsFor(), mergeIndicators() above) — live data wins
     on any overlap, but the embedded snapshot means the sidebar is never
     empty just because the live catalog/DB is thin or briefly
     unreachable. Never fabricates: every rendered row traces to either
     the live API or the embedded static GeoJSON, and is visibly labeled
     which (see provenanceBadgeHtml()). A total fetch failure still
     renders the embedded baseline and surfaces a non-blocking retry
     notice, instead of wiping the sidebar down to an error message. */
  function loadCountyIndicators(id) {
    var loader = document.getElementById('sidebar-loader');
    var container = document.getElementById('variables-container');
    var errorEl = document.getElementById('sidebar-error');

    var requestId = ++state.indicatorsRequestId;

    if (errorEl) errorEl.style.display = 'none';
    container.style.display = 'none';
    if (loader) loader.style.display = '';

    api('/geographies/' + id + '/indicators').then(function (data) {
      if (requestId !== state.indicatorsRequestId) return; // a later selection already superseded this one
      var liveIndicators = data.indicators || [];

      /* Backfill name/unit from the variable catalog when the indicator
         payload doesn't already carry it, same enrichment county.html
         does after its Promise.all(), then tag provenance so both the
         merge and the badge below know this row is live. */
      liveIndicators.forEach(function (ind) {
        var meta = state.variableMeta[ind.variable_id];
        if (meta) {
          if (!ind.name) ind.name = meta.name;
          if (!ind.unit) ind.unit = meta.unit;
        }
        ind.source = 'live';
      });

      var merged = mergeIndicators(embeddedIndicatorsFor(id), liveIndicators);

      state.indicatorsCache[id] = merged;
      renderIndicators(merged);
      updateEvidenceCards(merged);

      if (loader) loader.style.display = 'none';
      container.style.display = 'grid';
    }).catch(function (e) {
      if (requestId !== state.indicatorsRequestId) return;
      console.error('Failed to load indicators for ' + id + ':', e);

      /* The live API is down/erroring, but the embedded ACS snapshot for
         this county is still real data already sitting in memory — show
         it, clearly labeled, instead of wiping the sidebar. The error
         becomes a non-blocking notice (Retry still re-runs this whole
         function, see init()) rather than a wall that hides real data
         the page already has. If a county genuinely has zero embedded
         fields too, embeddedIndicatorsFor() returns [] and
         renderIndicators()/updateEvidenceCards() fall through to their
         existing empty states below — still never fabricated. */
      var fallback = embeddedIndicatorsFor(id);
      state.indicatorsCache[id] = fallback;
      renderIndicators(fallback);
      updateEvidenceCards(fallback);

      if (loader) loader.style.display = 'none';
      container.style.display = fallback.length ? 'grid' : 'none';
      if (errorEl) {
        errorEl.style.display = '';
        var msgEl = document.getElementById('sidebar-error-message');
        if (msgEl) {
          msgEl.textContent = 'Live indicator data unavailable — showing the embedded ACS snapshot. (' + e.message + ')';
        }
      }
    });
  }

  /* Small provenance badge appended to every rendered indicator row —
     this repo's whole ethos is that a number states where it came from,
     so an embedded-snapshot value must never render indistinguishably
     from a live one. Self-contained inline styling rather than a
     stylesheet class, because this file does not own explorer-styles.css;
     the amber/emerald wash mirrors the existing #sidebar-error amber
     treatment (explorer-styles.css .state-error) rather than reaching
     for tokens.css's --gated/--live custom properties, for the same
     reason that block gives — this file can't verify those properties'
     contrast in both themes without also owning the stylesheet. Colors
     do reuse tokens.css's own --live/--gated hex values (#34d399 /
     #f59e0b) as the rgba() base, so they at least agree with this
     workspace's semantic color vocabulary even though they're inlined
     rather than referenced live. */
  function provenanceBadgeHtml(source) {
    var isLive = source === 'live';
    var label = isLive ? 'Live' : 'ACS snapshot';
    var title = isLive
      ? 'From the live platform API for this county'
      : 'From the embedded ACS county snapshot bundled with this page (not the live API)';
    var rgb = isLive ? '52,211,153' : '245,158,11'; /* --live : --gated, from tokens.css */
    return '<span class="var-source" title="' + escapeHtml(title) + '" ' +
      'style="display:inline-block;align-self:center;margin-left:var(--pdi-space-xs);' +
      'padding:1px 6px;border-radius:999px;font-size:0.625rem;font-weight:700;' +
      'text-transform:uppercase;letter-spacing:.04em;white-space:nowrap;' +
      'color:var(--pdi-text-secondary);background-color:rgba(' + rgb + ',0.14);' +
      'border:1px solid rgba(' + rgb + ',0.4);">' + escapeHtml(label) + '</span>';
  }

  /* Groups the merged indicator list (embedded ACS baseline + live API
     overlay — see mergeIndicators()) into categories via inferCategory()
     (matching county.html's taxonomy) and renders the sidebar. The
     variable list is entirely derived from what this county actually
     has, live and/or embedded — there is no hardcoded allowlist, so a
     county that's missing a given field simply doesn't show it, and a
     county with more data than another shows more rows. That's
     intentional. Every row is badged with which source it came from
     (see provenanceBadgeHtml()). */
  function renderIndicators(indicators) {
    var container = document.getElementById('variables-container');
    container.innerHTML = '';

    var groups = {};
    indicators.forEach(function (ind) {
      var meta = state.variableMeta[ind.variable_id];
      var sourceId = meta ? meta.sourceId : '';
      var name = ind.name || (meta && meta.name) || ind.variable_id;
      var cat = inferCategory(ind.variable_id, name, sourceId);
      if (!groups[cat]) groups[cat] = [];
      groups[cat].push({
        id: ind.variable_id,
        name: name,
        unit: ind.unit || (meta && meta.unit) || '',
        value: ind.value,
        formatter: ind.formatter,
        source: ind.source,
      });
    });

    var order = CATEGORY_ORDER.filter(function (c) { return groups[c]; });
    Object.keys(groups).forEach(function (c) { if (order.indexOf(c) === -1) order.push(c); });

    order.forEach(function (cat) {
      var catEl = document.createElement('div');
      catEl.className = 'var-category';
      catEl.id = 'cat-' + cat.toLowerCase().replace(/[^a-z0-9]+/g, '-');

      var heading = document.createElement('h3');
      heading.className = 'cat-title';
      heading.textContent = cat;
      catEl.appendChild(heading);

      var list = document.createElement('div');
      list.className = 'var-list';
      groups[cat].forEach(function (v) {
        var varEl = document.createElement('div');
        varEl.className = 'var-item';
        var valueHtml = escapeHtml(formatIndicatorValue(v)) +
          (v.unit ? ' <span class="var-unit">' + escapeHtml(v.unit) + '</span>' : '');
        varEl.innerHTML = '<span class="var-label">' + escapeHtml(v.name) + '</span>' +
          '<span class="var-value">' + valueHtml + '</span>' +
          provenanceBadgeHtml(v.source);
        list.appendChild(varEl);
      });
      catEl.appendChild(list);
      container.appendChild(catEl);
    });

    updateCoverageNote(indicators);
  }

  /* Coverage note in the sidebar header — tells the truth about BOTH
     data sources now that the sidebar can show embedded-baseline rows
     as well as live ones. With no argument (variable catalog just
     loaded, no county selected/rendered yet) it reports only the
     catalog size, same as before. Otherwise it's handed the exact
     merged array that got rendered and counts the embedded/live split
     itself from each row's `source` tag — fully runtime-derived, no
     hardcoded totals on either side. */
  function updateCoverageNote(indicators) {
    var coverageEl = document.getElementById('data-coverage-note');
    if (!coverageEl) return;
    var catalogTotal = Object.keys(state.variableMeta).length;

    if (indicators === undefined) {
      coverageEl.textContent = catalogTotal
        ? 'Live catalog: ' + catalogTotal + ' variables tracked platform-wide.'
        : '';
      return;
    }

    var embeddedCount = 0, liveCount = 0;
    (indicators || []).forEach(function (ind) {
      if (ind.source === 'live') liveCount++;
      else embeddedCount++;
    });

    coverageEl.textContent = embeddedCount + ' indicator' + (embeddedCount === 1 ? '' : 's') +
      ' from the embedded ACS snapshot · ' + liveCount + ' live' +
      (catalogTotal ? ' from the ' + catalogTotal + '-variable catalog' : '') + '.';
  }

  /* ══════════════════ County Selection ════════════════════════════════
  */
  function selectCounty(id) {
    if (!countyData[id]) return;

    state.selectedCounty = id;
    var county = countyData[id];

    /* Update map */
    document.querySelectorAll('.map-county').forEach(function (p) {
      p.classList.toggle('selected', p.getAttribute('data-county-id') === id);
    });

    /* Update sidebar header — name/population come from the embedded
       static GeoJSON, same as the indicator list's baseline below (see
       loadCountyIndicators()); the header itself doesn't merge in
       anything live. */
    var sidebar = document.getElementById('data-sidebar');
    sidebar.classList.add('active');
    document.getElementById('county-title').textContent = county.name;
    document.getElementById('county-meta').textContent = 'Population: ' + fmtValue(county.total_population);

    /* Update map status */
    document.getElementById('map-status').textContent = county.name + ' selected · ' +
      Math.round(county.total_population).toLocaleString() + ' population';

    /* Fetch (or re-fetch) this county's real indicators from the live API. */
    loadCountyIndicators(id);
  }

  function updateEvidenceCards(indicators) {
    var container = document.getElementById('evidence-cards');
    container.innerHTML = '';

    if (!indicators || indicators.length === 0) {
      var empty = document.createElement('article');
      empty.className = 'evidence-card reveal';
      empty.innerHTML = '<h3>No data available</h3><p>Indicator data could not be loaded for this county.</p>';
      container.appendChild(empty);
      document.querySelectorAll('.evidence-card').forEach(function (el) {
        el.offsetHeight; /* force reflow */
        el.classList.add('in');
      });
      return;
    }

    function find(varId) {
      for (var i = 0; i < indicators.length; i++) {
        if (indicators[i].variable_id === varId) return indicators[i];
      }
      return null;
    }
    function val(varId) {
      var ind = find(varId);
      return ind ? ind.value : null;
    }

    /* People-of-color share: prefer the embedded snapshot's own
       precomputed pct_poc (ACS_EMBEDDED_FIELDS) when it's present in the
       merged list — it's real ACS data, and unlike the fallback below
       it's available even when the live indicators table is empty. Fall
       back to the same derivation county.html's computeICE() uses —
       (total_population - pop_white_non_hispanic) / total_population —
       for the case where only those two live variables are available
       and pct_poc isn't (pct_poc currently has no live-catalog
       counterpart at all; see ACS_EMBEDDED_FIELDS). */
    var totalPop = val('total_population');
    var pocPctDirect = val('pct_poc');
    var whiteNH = val('pop_white_non_hispanic');
    var pocPct = (pocPctDirect !== null && pocPctDirect !== undefined) ? pocPctDirect :
      ((totalPop && whiteNH !== null && whiteNH !== undefined && totalPop > 0)
        ? ((totalPop - whiteNH) / totalPop) * 100
        : null);

    var metrics = [
      {
        title: 'Economic Status',
        stats: [
          { label: 'Median Household Income', value: fmtDollar(val('median_household_income')) },
          { label: 'Poverty Rate', value: fmtPercent(val('poverty_rate')) },
        ]
      },
      {
        title: 'Housing',
        stats: [
          { label: 'Housing Units (Cost Burden Universe)', value: fmtValue(val('housing_units_cost_burden')) },
          { label: 'SVI: Housing & Transportation', value: fmtPercentile(val('cdc_svi_housing_transport')) },
        ]
      },
      {
        title: 'Health & Vulnerability',
        stats: [
          { label: 'Uninsured Rate', value: fmtPercent(val('uninsured_rate')) },
          { label: 'SVI: Overall Vulnerability', value: fmtPercentile(val('cdc_svi_overall')) },
        ]
      },
      {
        title: 'Demographic Profile',
        stats: [
          { label: 'Population', value: fmtValue(totalPop) },
          { label: 'People of Color', value: pocPct !== null ? pocPct.toFixed(1) + '%' : '—' },
        ]
      },
    ];

    metrics.forEach(function (metric) {
      var card = document.createElement('article');
      card.className = 'evidence-card reveal';
      var html = '<h3>' + escapeHtml(metric.title) + '</h3>';
      metric.stats.forEach(function (s) {
        html += '<div class="metric"><span class="metric-label">' + escapeHtml(s.label) +
          '</span><span class="metric-value">' + escapeHtml(s.value) + '</span></div>';
      });
      card.innerHTML = html;
      container.appendChild(card);
    });

    /* Trigger reveal animation */
    document.querySelectorAll('.evidence-card').forEach(function (el) {
      el.offsetHeight; /* force reflow */
      el.classList.add('in');
    });
  }

  /* ══════════════════ Search & Filter (unchanged) ═════════════════════
  */
  function filterCounties(query) {
    var lowerQuery = query.toLowerCase();
    state.filteredCounties = Object.keys(countyData).filter(function (id) {
      return countyData[id].name.toLowerCase().includes(lowerQuery) ||
        countyData[id].basename.toLowerCase().includes(lowerQuery);
    });

    /* Highlight matching counties on map */
    document.querySelectorAll('.map-county').forEach(function (p) {
      var id = p.getAttribute('data-county-id');
      var isMatch = state.filteredCounties.includes(id) || state.filteredCounties.length === 0;
      p.style.opacity = isMatch ? '1' : '0.3';
    });
  }

  /* ══════════════════ Theme Toggle (unchanged) ════════════════════════
  */
  function toggleTheme() {
    var html = document.documentElement;
    var current = html.getAttribute('data-theme') || 'light';
    var next = current === 'light' ? 'dark' : 'light';
    html.setAttribute('data-theme', next);
    localStorage.setItem('tp-theme', next);
    document.getElementById('theme-toggle').textContent = next === 'dark' ? '☀️' : '🌙';
  }

  /* ══════════════════ Mobile Sidebar Close (unchanged) ════════════════
  */
  function closeSidebar() {
    var sidebar = document.getElementById('data-sidebar');
    sidebar.classList.remove('active');
  }

  /* ══════════════════ Load County Geometry & Basic Info ═══════════════
     Geometry, name, basename, and population come from the embedded
     static GeoJSON, same as before — but so do the other nine real ACS
     metrics in ACS_EMBEDDED_FIELDS now (they didn't, until this fix; see
     file header History #3 -> #4, which explains why that was a
     regression). Those embedded metrics are the sidebar's baseline;
     loadCountyIndicators() overlays live API data on top of them for
     the selected county. See file header.
  */
  function loadCountyData() {
    var data = window.PDI_GEOJSON_DATA;
    if (!data || !data.features) {
      console.error('County GeoJSON data not found');
      return;
    }

    data.features.forEach(function (feature) {
      var props = feature.properties;
      var id = props.GEOID;
      var county = {
        id: id,
        name: props.NAME,
        basename: props.BASENAME,
        total_population: props.total_population,
      };
      /* Retain the rest of the real embedded ACS metrics too (see
         ACS_EMBEDDED_FIELDS above). This loop is the actual fix: this
         object used to stop at total_population and silently discard
         the other nine fields before they ever reached the DOM. */
      ACS_EMBEDDED_FIELDS.forEach(function (field) {
        county[field.prop] = props[field.prop];
      });
      countyData[id] = county;
      countyGeometry[id] = feature.geometry;
    });
  }

  /* ══════════════════ Initialize ════════════════════════════════════
  */
  function init() {
    loadCountyData();
    renderMap();

    /* Full variable catalog, fetched once on init (not per-county), per
       the same pattern as county.html's Promise.all() — except this
       fetch runs independently of the first county's indicator fetch
       rather than blocking on it, since category inference only needs
       each indicator's own name/id (the catalog's sourceId is a fallback
       that, ported bug-compatible with county.html, doesn't currently
       change any real category anyway — see inferCategory above). */
    api('/variables').then(function (data) {
      var vars = data.variables || [];
      vars.forEach(function (v) {
        state.variableMeta[v.id] = {
          name: v.name,
          unit: v.unit,
          direction: v.direction,
          description: v.description,
          sourceId: v.source_id,
        };
      });
      updateCoverageNote(undefined);
    }).catch(function (e) {
      console.error('Failed to load variable catalog:', e);
      /* Non-fatal: per-county indicator rendering still works from each
         indicator's own name/id; only catalog-wide metadata (descriptions,
         the "N of 51" coverage note) is degraded. */
    });

    /* Event listeners */
    document.getElementById('theme-toggle').addEventListener('click', toggleTheme);
    document.getElementById('county-search').addEventListener('input', function (e) {
      filterCounties(e.target.value);
    });
    document.getElementById('close-sidebar').addEventListener('click', closeSidebar);
    var retryBtn = document.getElementById('sidebar-retry');
    if (retryBtn) {
      retryBtn.addEventListener('click', function () {
        if (state.selectedCounty) loadCountyIndicators(state.selectedCounty);
      });
    }

    /* Set initial theme icon */
    document.getElementById('theme-toggle').textContent = state.theme === 'dark' ? '☀️' : '🌙';

    /* Select first county on load */
    var firstId = Object.keys(countyData)[0];
    if (firstId) selectCounty(firstId);
  }

  /* DOM ready */
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  /* Expose to global for testing */
  window.PDI = {
    selectCounty: selectCounty,
    filterCounties: filterCounties,
    toggleTheme: toggleTheme,
    state: state,
    formatValue: fmtValue,
    api: api,
    inferCategory: inferCategory,
  };
})();
