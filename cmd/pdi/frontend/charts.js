/* charts.js — casa-datos chart primitives.
   Stack-authored, NOT a vendored @spine-kit file (no tag; nothing here
   is copied from spine/kits/ — see casa-datos/CLAUDE.md's kit table for
   which files ARE vendored). Hand-rolled SVG, zero dependencies: no
   charting library, no DOM framework. Each builder is a pure function

     build(data, opts) -> SVG string

   following the token-and-bounds half of spine/kits/parametric/
   GENERATORS.md's discipline (palette from CSS custom properties, a
   declared viewBox bound with every mark clamped inside it) without
   the seeded-rng half — chart marks are driven by caller-supplied
   DATA, never by randomness. Randomness, where casa-datos uses it at
   all, lives in app.js's synthetic-dataset generator, one layer up
   from these primitives; see DATAVIZ-RULES.md.

   Deliberately dependency-free of window.TPParametric too: this file
   (and choropleth.js) each carry their own tiny cssVar() reader so
   either could be lifted into a project that never vendors the
   parametric kit at all. Zero dependencies means zero — including
   this stack's other own files. */
(function (window) {
  'use strict';

  /* ── tokens: read a custom property off <html> at call time ────── */
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
  /** The house categorical scale, in order, wrapping after 8 series —
      DATAVIZ-RULES.md documents why this order and why wrap-not-repeat
      is a documented limit rather than silently reusing colors past 8. */
  function catColor(i) {
    var n = ((i % 8) + 8) % 8 + 1;
    return cssVar('--tp-cat-' + n, '#888');
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
  function niceMax(v) {
    /* Round the top of a zero-baselined axis up to a "nice" number so
       gridlines land on readable values instead of e.g. 87.3. */
    if (v <= 0) { return 10; }
    var mag = Math.pow(10, Math.floor(Math.log(v) / Math.LN10));
    var norm = v / mag;
    var nice = norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 5 ? 5 : 10;
    return nice * mag;
  }

  /* Distinct marker shapes per series index — the non-color encoder a
     line chart pairs with its stroke color (DATAVIZ-RULES.md rule 2:
     color is never the only signal). Cycles after 4; a 5th+ series
     also gets a distinct dash pattern (see DASH_PATTERNS below), so
     two encoders combine before any repeat is possible. */
  function marker(cx, cy, shapeIndex, size, attrs) {
    var s = size || 5;
    var shape = ((shapeIndex % 4) + 4) % 4;
    if (shape === 0) { return '<circle cx="' + cx.toFixed(1) + '" cy="' + cy.toFixed(1) + '" r="' + s.toFixed(1) + '"' + attrString(attrs) + '/>'; }
    if (shape === 1) {
      var h = s * 1.15;
      return '<rect x="' + (cx - h).toFixed(1) + '" y="' + (cy - h).toFixed(1) + '" width="' + (h * 2).toFixed(1) + '" height="' + (h * 2).toFixed(1) + '"' + attrString(attrs) + '/>';
    }
    if (shape === 2) {
      var t = s * 1.3;
      return '<polygon points="' + cx.toFixed(1) + ',' + (cy - t).toFixed(1) + ' ' + (cx + t).toFixed(1) + ',' + (cy + t * 0.85).toFixed(1) + ' ' + (cx - t).toFixed(1) + ',' + (cy + t * 0.85).toFixed(1) + '"' + attrString(attrs) + '/>';
    }
    var r = s * 1.25;
    return '<polygon points="' + cx.toFixed(1) + ',' + (cy - r).toFixed(1) + ' ' + (cx + r).toFixed(1) + ',' + cy.toFixed(1) + ' ' + cx.toFixed(1) + ',' + (cy + r).toFixed(1) + ' ' + (cx - r).toFixed(1) + ',' + cy.toFixed(1) + '"' + attrString(attrs) + '/>';
  }
  var DASH_PATTERNS = [null, '2,4', '8,4', '1,3,6,3'];
  function dashFor(i) { return DASH_PATTERNS[Math.floor(i / 4) % DASH_PATTERNS.length]; }

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

  /* ═══════════════════════════════════════════════════════════════
   * buildBarChart(data, opts) -> SVG string
   *   data: [{ label, value }, ...]
   *   opts: { width, height, unit, valuePrefix, decimals, ariaId }
   *
   * Honest-scale rule (DATAVIZ-RULES.md): the value axis ALWAYS starts
   * at zero. There is no "start" option — a truncated bar-chart
   * baseline misrepresents ratios between bars, so the option to
   * misrepresent them does not exist in this function's signature.
   * ═══════════════════════════════════════════════════════════════ */
  function buildBarChart(data, opts) {
    opts = opts || {};
    var W = opts.width || 640, H = opts.height || 320;
    var padL = 56, padR = 20, padT = 24, padB = 48;
    var plotW = W - padL - padR, plotH = H - padT - padB;
    var n = data.length;
    var maxV = 0;
    for (var i = 0; i < n; i++) { maxV = Math.max(maxV, data[i].value); }
    var axisMax = niceMax(maxV * 1.12 || 1); /* zero baseline, headroom so the tallest bar's label clears the top */
    var gap = plotW / n * 0.28;
    var barW = (plotW / n) - gap;

    var gridlines = '';
    var ticks = 4;
    for (var g = 0; g <= ticks; g++) {
      var gv = axisMax * g / ticks;
      var gy = padT + plotH - (gv / axisMax) * plotH;
      gridlines += '<line x1="' + padL.toFixed(1) + '" y1="' + gy.toFixed(1) + '" x2="' + (W - padR).toFixed(1) + '" y2="' + gy.toFixed(1) + '" stroke="' + esc(cssVar('--border-strong')) + '" stroke-width="1"/>';
      gridlines += '<text x="' + (padL - 10).toFixed(1) + '" y="' + (gy + 4).toFixed(1) + '" text-anchor="end" font-size="11" font-family="' + esc(cssVar('--font-mono', 'monospace')) + '" fill="' + esc(cssVar('--quiet', cssVar('--muted'))) + '">' + esc(fmt(gv, opts.decimals)) + '</text>';
    }

    var bars = '';
    for (var b = 0; b < n; b++) {
      var d = data[b];
      var x = padL + b * (barW + gap) + gap / 2;
      var barH = axisMax > 0 ? (d.value / axisMax) * plotH : 0;
      var y = padT + plotH - barH;
      var color = catColor(b);
      bars += '<rect x="' + x.toFixed(1) + '" y="' + y.toFixed(1) + '" width="' + barW.toFixed(1) + '" height="' + Math.max(0, barH).toFixed(1) + '" rx="3" fill="' + esc(color) + '"><title>' + esc(d.label) + ': ' + esc(fmt(d.value, opts.decimals)) + (opts.unit ? ' ' + esc(opts.unit) : '') + '</title></rect>';
      /* value label ABOVE the bar, on the plot background, in the
         standard UI text color — never inside the colored fill, so no
         per-bar-color foreground contrast computation is needed
         (DATAVIZ-RULES.md, "axis/label legibility"). This label is
         also the non-color encoder pairing rule (color.md /
         CONTEXTS.md rule 5): the number is readable with the chart
         rendered in grayscale. */
      bars += '<text x="' + (x + barW / 2).toFixed(1) + '" y="' + (y - 8).toFixed(1) + '" text-anchor="middle" font-size="12" font-family="' + esc(cssVar('--font-mono', 'monospace')) + '" fill="' + esc(cssVar('--text')) + '">' + esc(fmt(d.value, opts.decimals)) + '</text>';
      bars += '<text x="' + (x + barW / 2).toFixed(1) + '" y="' + (padT + plotH + 20).toFixed(1) + '" text-anchor="middle" font-size="12" font-family="' + esc(cssVar('--font-sans', 'sans-serif')) + '" fill="' + esc(cssVar('--muted')) + '">' + esc(d.label) + '</text>';
    }

    var axisLine = '<line x1="' + padL.toFixed(1) + '" y1="' + (padT + plotH).toFixed(1) + '" x2="' + (W - padR).toFixed(1) + '" y2="' + (padT + plotH).toFixed(1) + '" stroke="' + esc(cssVar('--border-strong')) + '" stroke-width="1.5"/>';

    return '<svg viewBox="0 0 ' + W + ' ' + H + '" xmlns="http://www.w3.org/2000/svg" role="img" aria-labelledby="' + esc(opts.ariaId || 'bar-chart') + '-title">' +
      '<title id="' + esc(opts.ariaId || 'bar-chart') + '-title">' + esc(opts.title || 'Bar chart') + '</title>' +
      '<defs><clipPath id="' + esc(opts.ariaId || 'bar-chart') + '-clip"><rect x="0" y="0" width="' + W + '" height="' + H + '"/></clipPath></defs>' +
      '<g clip-path="url(#' + esc(opts.ariaId || 'bar-chart') + '-clip)">' +
      gridlines + bars + axisLine +
      '</g></svg>';
  }

  /* ═══════════════════════════════════════════════════════════════
   * buildLineChart(data, opts) -> SVG string
   *   data: [{ name, points: [{ x, y }, ...] }, ...]   (one entry per series)
   *   opts: { width, height, xLabels: [...], unit, decimals, ariaId, zeroBaseline }
   *
   * Each series gets a color (catColor) AND a distinct marker shape +
   * dash pattern (DATAVIZ-RULES.md rule 2) — never color alone. Ends
   * each line with its series name printed directly beside the last
   * point, so identifying a series never depends on a separate legend
   * matched purely by color.
   * ═══════════════════════════════════════════════════════════════ */
  function buildLineChart(data, opts) {
    opts = opts || {};
    var W = opts.width || 640, H = opts.height || 320;
    var padL = 56, padR = 118, padT = 24, padB = 40;
    var plotW = W - padL - padR, plotH = H - padT - padB;
    var xLabels = opts.xLabels || [];
    var stepCount = Math.max(1, xLabels.length - 1);

    var maxV = 0, minV = Infinity;
    data.forEach(function (s) { s.points.forEach(function (p) { maxV = Math.max(maxV, p.y); minV = Math.min(minV, p.y); }); });
    if (!isFinite(minV)) { minV = 0; }
    var zeroBase = opts.zeroBaseline !== false; /* default true — honest-scale rule for the demo line chart too */
    var axisMin = zeroBase ? 0 : niceMax(minV) * 0 + Math.floor(minV * 0.9);
    var axisMax = niceMax(Math.max(maxV * 1.1, axisMin + 1));

    function xAt(i) { return padL + (i / stepCount) * plotW; }
    function yAt(v) { return padT + plotH - ((v - axisMin) / (axisMax - axisMin)) * plotH; }

    var gridlines = '';
    var ticks = 4;
    for (var g = 0; g <= ticks; g++) {
      var gv = axisMin + (axisMax - axisMin) * g / ticks;
      var gy = yAt(gv);
      gridlines += '<line x1="' + padL.toFixed(1) + '" y1="' + gy.toFixed(1) + '" x2="' + (W - padR).toFixed(1) + '" y2="' + gy.toFixed(1) + '" stroke="' + esc(cssVar('--border-strong')) + '" stroke-width="1"/>';
      gridlines += '<text x="' + (padL - 10).toFixed(1) + '" y="' + (gy + 4).toFixed(1) + '" text-anchor="end" font-size="11" font-family="' + esc(cssVar('--font-mono', 'monospace')) + '" fill="' + esc(cssVar('--quiet', cssVar('--muted'))) + '">' + esc(fmt(gv, opts.decimals)) + '</text>';
    }
    var xTicks = '';
    xLabels.forEach(function (lbl, i) {
      xTicks += '<text x="' + xAt(i).toFixed(1) + '" y="' + (padT + plotH + 22).toFixed(1) + '" text-anchor="middle" font-size="11" font-family="' + esc(cssVar('--font-sans', 'sans-serif')) + '" fill="' + esc(cssVar('--muted')) + '">' + esc(lbl) + '</text>';
    });

    var lines = '';
    data.forEach(function (series, si) {
      var color = catColor(si);
      var d = '';
      series.points.forEach(function (p, i) {
        d += (i === 0 ? 'M' : 'L') + xAt(i).toFixed(1) + ',' + yAt(p.y).toFixed(1) + ' ';
      });
      var dash = dashFor(si);
      lines += '<path d="' + d.trim() + '" fill="none" stroke="' + esc(color) + '" stroke-width="2.5"' + (dash ? ' stroke-dasharray="' + dash + '"' : '') + ' stroke-linejoin="round" stroke-linecap="round"/>';
      series.points.forEach(function (p, i) {
        lines += marker(xAt(i), yAt(p.y), si, 4.5, { fill: color });
      });
      var last = series.points[series.points.length - 1];
      lines += '<text x="' + (xAt(series.points.length - 1) + 10).toFixed(1) + '" y="' + (yAt(last.y) + 4).toFixed(1) + '" font-size="12" font-family="' + esc(cssVar('--font-sans', 'sans-serif')) + '" fill="' + esc(cssVar('--text')) + '">' + esc(series.name) + '</text>';
    });

    var axisLine = '<line x1="' + padL.toFixed(1) + '" y1="' + (padT + plotH).toFixed(1) + '" x2="' + (W - padR).toFixed(1) + '" y2="' + (padT + plotH).toFixed(1) + '" stroke="' + esc(cssVar('--border-strong')) + '" stroke-width="1.5"/>';

    return '<svg viewBox="0 0 ' + W + ' ' + H + '" xmlns="http://www.w3.org/2000/svg" role="img" aria-labelledby="' + esc(opts.ariaId || 'line-chart') + '-title">' +
      '<title id="' + esc(opts.ariaId || 'line-chart') + '-title">' + esc(opts.title || 'Line chart') + '</title>' +
      '<defs><clipPath id="' + esc(opts.ariaId || 'line-chart') + '-clip"><rect x="0" y="0" width="' + W + '" height="' + H + '"/></clipPath></defs>' +
      '<g clip-path="url(#' + esc(opts.ariaId || 'line-chart') + '-clip)">' +
      gridlines + xTicks + lines +
      '</g></svg>';
  }

  /* ═══════════════════════════════════════════════════════════════
   * buildSmallMultiples(panels, opts) -> SVG string
   *   panels: [{ title, data: [{label, value}, ...] }, ...]
   *   opts: { width, cols, panelHeight, unit, decimals, ariaId }
   *
   * Every panel shares ONE y-scale, computed across ALL panels' data —
   * the correctness rule small multiples exist to serve: panels are
   * only honestly comparable at a glance if a taller bar always means
   * a bigger number, panel to panel (DATAVIZ-RULES.md). A per-panel
   * independent scale would let two visually-identical bars represent
   * very different values; this function structurally rules that out.
   * ═══════════════════════════════════════════════════════════════ */
  function buildSmallMultiples(panels, opts) {
    opts = opts || {};
    var cols = opts.cols || Math.min(4, panels.length) || 1;
    var rows = Math.ceil(panels.length / cols);
    var panelW = opts.panelWidth || 150;
    var panelH = opts.panelHeight || 130;
    var gapX = 18, gapY = 26;
    var padL = 34, padT = 8, padB = 30;
    var W = cols * panelW + (cols - 1) * gapX + padL + 12;
    var H = rows * (panelH + padT + padB) + (rows - 1) * gapY + 8;

    var maxV = 0;
    panels.forEach(function (p) { p.data.forEach(function (d) { maxV = Math.max(maxV, d.value); }); });
    var axisMax = niceMax(maxV * 1.15 || 1); /* one shared, zero-baselined scale for every panel */

    var out = '';
    panels.forEach(function (panel, pi) {
      var col = pi % cols, row = Math.floor(pi / cols);
      var ox = padL + col * (panelW + gapX);
      var oy = padT + row * (panelH + padT + padB + gapY);
      var n = panel.data.length;
      var barGap = panelW / n * 0.22;
      var barW = (panelW / n) - barGap;

      out += '<text x="' + (ox + panelW / 2).toFixed(1) + '" y="' + (oy - 2).toFixed(1) + '" text-anchor="middle" font-size="12" font-weight="600" font-family="' + esc(cssVar('--font-sans', 'sans-serif')) + '" fill="' + esc(cssVar('--text')) + '">' + esc(panel.title) + '</text>';
      out += '<line x1="' + ox.toFixed(1) + '" y1="' + (oy + panelH).toFixed(1) + '" x2="' + (ox + panelW).toFixed(1) + '" y2="' + (oy + panelH).toFixed(1) + '" stroke="' + esc(cssVar('--border-strong')) + '" stroke-width="1"/>';

      panel.data.forEach(function (d, bi) {
        var x = ox + bi * (barW + barGap) + barGap / 2;
        var barH = (d.value / axisMax) * panelH;
        var y = oy + panelH - barH;
        out += '<rect x="' + x.toFixed(1) + '" y="' + y.toFixed(1) + '" width="' + barW.toFixed(1) + '" height="' + Math.max(0, barH).toFixed(1) + '" rx="2" fill="' + esc(catColor(pi)) + '"><title>' + esc(panel.title) + ' — ' + esc(d.label) + ': ' + esc(fmt(d.value, opts.decimals)) + '</title></rect>';
        out += '<text x="' + (x + barW / 2).toFixed(1) + '" y="' + (oy + panelH + 14).toFixed(1) + '" text-anchor="middle" font-size="9.5" font-family="' + esc(cssVar('--font-mono', 'monospace')) + '" fill="' + esc(cssVar('--quiet', cssVar('--muted'))) + '">' + esc(d.label) + '</text>';
      });
    });

    return '<svg viewBox="0 0 ' + W + ' ' + H + '" xmlns="http://www.w3.org/2000/svg" role="img" aria-labelledby="' + esc(opts.ariaId || 'small-multiples') + '-title">' +
      '<title id="' + esc(opts.ariaId || 'small-multiples') + '-title">' + esc(opts.title || 'Small multiples') + '</title>' +
      out +
      '</svg>';
  }

  window.CasaDatosCharts = {
    buildBarChart: buildBarChart,
    buildLineChart: buildLineChart,
    buildSmallMultiples: buildSmallMultiples,
    catColor: catColor,
    cssVar: cssVar,
    esc: esc,
    fmt: fmt
  };
})(window);
