/* choropleth.js — casa-datos choropleth primitive.
   Stack-authored, NOT a vendored @spine-kit file (see charts.js's header
   for why: zero dependencies, including this stack's other files).

   Renders a GeoJSON-shaped FeatureCollection of Polygon geometry to an
   SVG grid, coloring each feature by a sequential scale derived from
   two --tp-* tokens and labeling every region with its actual value —
   color is never the only signal (DATAVIZ-RULES.md rule 2).

   SYNTHETIC GEOMETRY ONLY. The geometry this file ships and demonstrates
   with (see app.js's SYNTHETIC_REGIONS) is an abstract grid of unit
   squares with invented row/column names ("A1".."D4") — not a real
   jurisdiction, boundary, or place. See "Swapping in real GeoJSON"
   below for what changes (and what does not) to point this renderer at
   an actual dataset. */
(function (window) {
  'use strict';

  var _rootStyle = null;
  function rootStyle() {
    if (!_rootStyle) {
      try { _rootStyle = getComputedStyle(document.documentElement); }
      catch (e) { _rootStyle = null; }
    }
    return _rootStyle;
  }
  function cssVar(name, fallback) {
    var cs = rootStyle();
    var v = '';
    if (cs) { try { v = cs.getPropertyValue(name).trim(); } catch (e) { /* ignore */ } }
    return v || fallback || '#888';
  }
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function fmt(n, decimals) {
    var d = typeof decimals === 'number' ? decimals : 0;
    return Number(n).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
  }

  /* ── tiny color-math trio, private to this file (see charts.js's
     header note on why these primitives don't lean on TPParametric) ── */
  function hexToRgb(hex) {
    var h = (hex || '').replace('#', '');
    return { r: parseInt(h.substr(0, 2), 16), g: parseInt(h.substr(2, 2), 16), b: parseInt(h.substr(4, 2), 16) };
  }
  function lerpRgb(a, b, t) {
    return { r: Math.round(a.r + (b.r - a.r) * t), g: Math.round(a.g + (b.g - a.g) * t), b: Math.round(a.b + (b.b - a.b) * t) };
  }
  function rgbToCss(c) { return 'rgb(' + c.r + ',' + c.g + ',' + c.b + ')'; }
  function srgbToLinear(c8) { var c = c8 / 255; return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4); }
  function relLuminance(c) { return 0.2126 * srgbToLinear(c.r) + 0.7152 * srgbToLinear(c.g) + 0.0722 * srgbToLinear(c.b); }
  function contrastRatio(a, b) {
    var la = relLuminance(a), lb = relLuminance(b);
    var hi = Math.max(la, lb), lo = Math.min(la, lb);
    return (hi + 0.05) / (lo + 0.05);
  }
  /** Pick whichever candidate foreground contrasts best against a given
      fill — this is what lets a single sequential scale (light anchor
      to dark anchor) carry a legible label at EVERY step, instead of
      one label color that only works at one end of the range. */
  function pickForeground(fillRgb, candidates) {
    var best = candidates[0], bestRatio = -1;
    candidates.forEach(function (c) {
      var r = contrastRatio(fillRgb, c.rgb);
      if (r > bestRatio) { bestRatio = r; best = c; }
    });
    return best;
  }

  /** sequentialColor(t, loToken, hiToken) -> { css, rgb } for t in [0,1] */
  function sequentialColor(t, loToken, hiToken) {
    t = Math.min(1, Math.max(0, t));
    var lo = hexToRgb(cssVar(loToken, '#FEDC82'));
    var hi = hexToRgb(cssVar(hiToken, '#0E6690'));
    var rgb = lerpRgb(lo, hi, t);
    return { css: rgbToCss(rgb), rgb: rgb };
  }

  /* ═══════════════════════════════════════════════════════════════
   * buildChoropleth(featureCollection, opts) -> SVG string
   *
   *   featureCollection: {
   *     type: "FeatureCollection",
   *     features: [{
   *       type: "Feature",
   *       properties: { id, name, value },
   *       geometry: { type: "Polygon", coordinates: [[[x,y], ...]] }
   *     }, ...]
   *   }
   *
   *   opts: { width, height, valueField, unit, decimals, ariaId,
   *           loToken, hiToken, legendTitle }
   *
   * Geometry coordinates are plain flat numbers in "map units" (this
   * stack's synthetic grid uses small integers, one unit per cell) —
   * NOT longitude/latitude. fitToViewBox() below is a linear
   * scale+translate, not a map projection; see "Swapping in real
   * GeoJSON" for what a real lon/lat dataset needs on top of this.
   * ═══════════════════════════════════════════════════════════════ */
  function collectAllPoints(fc) {
    var pts = [];
    fc.features.forEach(function (f) {
      f.geometry.coordinates.forEach(function (ring) {
        ring.forEach(function (pt) { pts.push(pt); });
      });
    });
    return pts;
  }

  function fitToViewBox(fc, W, H, pad) {
    var pts = collectAllPoints(fc);
    var minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
    pts.forEach(function (p) {
      minX = Math.min(minX, p[0]); maxX = Math.max(maxX, p[0]);
      minY = Math.min(minY, p[1]); maxY = Math.max(maxY, p[1]);
    });
    var spanX = Math.max(1e-6, maxX - minX);
    var spanY = Math.max(1e-6, maxY - minY);
    var availW = W - pad * 2, availH = H - pad * 2;
    var scale = Math.min(availW / spanX, availH / spanY);
    var offX = pad + (availW - spanX * scale) / 2;
    var offY = pad + (availH - spanY * scale) / 2;
    return function (p) {
      return [offX + (p[0] - minX) * scale, offY + (p[1] - minY) * scale];
    };
  }

  function ringToPoints(ring, project) {
    return ring.map(function (p) { var q = project(p); return q[0].toFixed(1) + ',' + q[1].toFixed(1); }).join(' ');
  }

  function buildChoropleth(fc, opts) {
    opts = opts || {};
    var W = opts.width || 420, H = opts.height || 360;
    var legendH = 56;
    var mapH = H - legendH;
    var valueField = opts.valueField || 'value';
    var loToken = opts.loToken || '--tp-cream';
    var hiToken = opts.hiToken || '--tp-ocean';
    var decimals = opts.decimals || 0;

    var values = fc.features.map(function (f) { return f.properties[valueField]; });
    var minV = Math.min.apply(null, values), maxV = Math.max.apply(null, values);
    var span = maxV - minV || 1;

    var project = fitToViewBox(fc, W, mapH, 14);
    var ink = { name: 'ink', rgb: hexToRgb(cssVar('--tp-ink', '#0C2222')) };
    var paper = { name: 'paper', rgb: hexToRgb(cssVar('--tp-paper', '#FAF7EF')) };

    var cells = '';
    fc.features.forEach(function (f) {
      var v = f.properties[valueField];
      var t = (v - minV) / span;
      var color = sequentialColor(t, loToken, hiToken);
      var fg = pickForeground(color.rgb, [ink, paper]);
      var pts = ringToPoints(f.geometry.coordinates[0], project);
      /* label anchor = centroid of the ring's own points (adequate for
         this stack's convex unit-square cells; a real polygon renderer
         swapping in irregular shapes should compute a proper polygon
         centroid instead — see "Swapping in real GeoJSON"). */
      var cx = 0, cy = 0, cn = f.geometry.coordinates[0].length - 1;
      for (var i = 0; i < cn; i++) { var q = project(f.geometry.coordinates[0][i]); cx += q[0]; cy += q[1]; }
      cx /= cn; cy /= cn;

      cells += '<polygon points="' + pts + '" fill="' + color.css + '" stroke="' + esc(cssVar('--bg', '#0a0a0f')) + '" stroke-width="2">' +
        '<title>' + esc(f.properties.name) + ': ' + esc(fmt(v, decimals)) + (opts.unit ? ' ' + esc(opts.unit) : '') + '</title>' +
        '</polygon>';
      cells += '<text x="' + cx.toFixed(1) + '" y="' + (cy - 3).toFixed(1) + '" text-anchor="middle" font-size="10.5" font-weight="600" font-family="' + esc(cssVar('--font-sans', 'sans-serif')) + '" fill="' + (fg.name === 'ink' ? esc(cssVar('--tp-ink')) : esc(cssVar('--tp-paper'))) + '">' + esc(f.properties.name) + '</text>';
      cells += '<text x="' + cx.toFixed(1) + '" y="' + (cy + 11).toFixed(1) + '" text-anchor="middle" font-size="10.5" font-family="' + esc(cssVar('--font-mono', 'monospace')) + '" fill="' + (fg.name === 'ink' ? esc(cssVar('--tp-ink')) : esc(cssVar('--tp-paper'))) + '">' + esc(fmt(v, decimals)) + '</text>';
    });

    /* ── legend: a swept gradient bar + numeric min/max labels — the
       label pairing is what keeps the legend itself compliant with
       "color is never the only signal": the two end VALUES are printed,
       not just the two end colors. ── */
    var legendX = 14, legendY = mapH + 22, legendW = W - 28, legendSteps = 24;
    var legendCells = '';
    for (var s = 0; s < legendSteps; s++) {
      var st = s / (legendSteps - 1);
      var sc = sequentialColor(st, loToken, hiToken);
      var sx = legendX + (legendW / legendSteps) * s;
      legendCells += '<rect x="' + sx.toFixed(1) + '" y="' + legendY.toFixed(1) + '" width="' + (legendW / legendSteps + 0.5).toFixed(1) + '" height="14" fill="' + sc.css + '"/>';
    }
    var legend = '<text x="' + legendX + '" y="' + (legendY - 6) + '" font-size="11" font-family="' + esc(cssVar('--font-sans', 'sans-serif')) + '" fill="' + esc(cssVar('--muted')) + '">' + esc(opts.legendTitle || 'Value (synthetic)') + '</text>' +
      legendCells +
      '<text x="' + legendX + '" y="' + (legendY + 28) + '" font-size="11" font-family="' + esc(cssVar('--font-mono', 'monospace')) + '" fill="' + esc(cssVar('--quiet', cssVar('--muted'))) + '">' + esc(fmt(minV, decimals)) + '</text>' +
      '<text x="' + (legendX + legendW) + '" y="' + (legendY + 28) + '" text-anchor="end" font-size="11" font-family="' + esc(cssVar('--font-mono', 'monospace')) + '" fill="' + esc(cssVar('--quiet', cssVar('--muted'))) + '">' + esc(fmt(maxV, decimals)) + '</text>';

    return '<svg viewBox="0 0 ' + W + ' ' + H + '" xmlns="http://www.w3.org/2000/svg" role="img" aria-labelledby="' + esc(opts.ariaId || 'choropleth') + '-title">' +
      '<title id="' + esc(opts.ariaId || 'choropleth') + '-title">' + esc(opts.title || 'Choropleth map') + '</title>' +
      cells + legend +
      '</svg>';
  }

  window.CasaDatosChoropleth = {
    buildChoropleth: buildChoropleth,
    sequentialColor: sequentialColor,
    pickForeground: pickForeground,
    hexToRgb: hexToRgb,
    esc: esc,
    fmt: fmt
  };

  /* ── Swapping in real GeoJSON ──────────────────────────────────────
   * What stays the same: the FeatureCollection shape, `properties.name`
   * + a numeric value field, the sequential-color + legend + label
   * pipeline, and pickForeground()'s per-cell contrast choice.
   *
   * What changes: real Polygon/MultiPolygon coordinates are
   * longitude/latitude pairs, not flat map units — fitToViewBox()'s
   * linear scale+translate is not a map projection and will DISTORT
   * real geography (it has no notion of the Earth's curvature). Add a
   * projection step before fitToViewBox() — even a simple equirectangular
   * one (x = lon, y = -lat) is enough for a small-area map; a
   * country- or world-scale map needs a real projection (e.g. Albers,
   * Mercator). MultiPolygon geometry needs one <path> per polygon
   * instead of this file's single <polygon> per feature, and the
   * centroid math (currently a plain vertex average, adequate for
   * convex unit squares) should become an area-weighted polygon
   * centroid for irregular real-world shapes. Everything downstream —
   * value → sequentialColor → legend → label — is unchanged.
   */
})(window);
