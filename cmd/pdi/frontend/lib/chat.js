// lib/chat.js — Chat adapter with full data grounding for the Dojo Gateway.
//
// EXPORT IDIOM: `ChatAdapter` is a top-level `const` in this classic
// (non-module) script — a script-scoped lexical binding, reachable by the
// bare name `ChatAdapter` from any other classic <script> on the same page
// (and via `typeof ChatAdapter !== 'undefined'` as the existence probe — see
// chat.html's `hasAdapter()`), but NEVER present as `window.ChatAdapter` on
// its own. This file ALSO assigns `window.ChatAdapter` below, for any
// consumer that specifically needs a window property. Both forms resolve to
// the SAME object; this is additive, not a replacement — every existing bare
// `ChatAdapter.` call site and `typeof ChatAdapter` probe in chat.html keeps
// working unchanged. Same idiom in lib/api.js (`PDI` / `window.PDIApi` —
// renamed there to avoid colliding with explorer-app.js's unrelated
// `window.PDI`) and lib/domain.js (`Domain` / `window.Domain`).
//
// NOTE: chat.html carries a comment (just above its hasAdapter() helper)
// stating that probing window.ChatAdapter "yields undefined forever." That
// was accurate before this change; it is now stale, since window.ChatAdapter
// resolves as of this file. chat.html is a consumer page outside this file's
// ownership, so the comment was left as-is rather than edited.
const ChatAdapter = {
  _sessionId: 'pdi-web-' + Date.now().toString(36),
  _proxyAvailable: null,
  _systemPrompt: null,  // Built lazily from live API data
  _pageContext: null,   // {page, geoid, name, indicator, geoid1, geoid2, name1, name2}
  _narrateMode: false,  // True when narrate() is active — enables spatial system prompt
  _navCommandsFound: [],  // Navigation commands extracted from last response

  setPageContext(ctx) {
    this._pageContext = ctx || null;
    this._systemPrompt = null;  // Invalidate cache so prompt rebuilds
  },

  // ── Rich Response Parser ────────────────────────────────────────────────────

  // Parses special formatting tokens into HTML components.
  // Tokens:
  //   {{stat:value:label}}              → stat-callout card
  //   {{chart:name1=val1,name2=val2}}   → horizontal bar chart
  //   {{table:h1|h2|r1c1|r1c2|...}}    → data table
  //   {{nav:label:url}}                 → navigation pill button
  // Plain text passes through unchanged (backward compatible).
  _parseRichResponse(text) {
    if (!text) return '';

    let html = text;

    // 1. Stat callouts: {{stat:value:label}}
    //    value is the stat number, label is the description
    html = html.replace(
      /\{\{stat:\s*([^:}]+?)\s*:\s*([^}]*?)\s*\}\}/g,
      function(match, value, label) {
        return '<div class="stat-callout"><span class="stat-value">' +
               ChatAdapter._escapeHtml(value.trim()) +
               '</span><span class="stat-label">' +
               ChatAdapter._escapeHtml(label.trim()) +
               '</span></div>';
      }
    );

    // 2. Mini bar charts: {{chart:name1=val1, name2=val2, ...}}
    //    Supports numeric values and optional formatting.
    html = html.replace(
      /\{\{chart:\s*([^}]+?)\s*\}\}/g,
      function(match, content) {
        var entries = ChatAdapter._parseChartEntries(content);
        if (entries.length === 0) return match;
        return ChatAdapter._buildMiniChart(entries);
      }
    );

    // 3. Data tables: {{table:h1|h2|r1c1|r1c2|r2c1|r2c2|...}}
    html = html.replace(
      /\{\{table:\s*([^}]+?)\s*\}\}/g,
      function(match, content) {
        // Two layouts are accepted on purpose.
        //
        // The documented one is a single flat pipe-separated list. But the
        // system prompt now ships its county/policy data as one pipe-delimited
        // record PER LINE, and the model demonstrably carries that convention
        // into its {{table:...}} output (measured: 3 of 4 tables came back
        // multi-line). Flat-chunking such content silently scrambles the
        // columns — the newline lands inside a cell, every subsequent cell
        // shifts, and the user sees a wrong-but-plausible table.
        //
        // So: if the content has line breaks, treat each line as a row. This
        // is strictly more robust than instructing the model not to do it,
        // which was tried first and did not hold.
        if (content.indexOf('\n') !== -1) {
          var rows = content.split('\n')
            .map(function(r) { return r.trim(); })
            .filter(function(r) { return r.length > 0; })
            .map(function(r) {
              return r.split('|').map(function(c) { return c.trim(); });
            });
          if (rows.length > 1) return ChatAdapter._buildDataTableRows(rows);
        }
        var cells = content.split('|').map(function(c) { return c.trim(); });
        // First row is headers; remaining cells split into rows of header-length
        // If only one row of data, treat all as a single-row table with first cells as headers
        return ChatAdapter._buildDataTable(cells);
      }
    );

    // 4. Navigation pills: {{nav:label:url}}
    //    Renders as a clickable button that navigates to the given URL.
    html = html.replace(
      /\{\{nav:\s*([^:}]+?)\s*:\s*([^}]*?)\s*\}\}/g,
      function(match, label, url) {
        var cleanLabel = label.trim();
        var cleanUrl = url.trim();
        return '<a href="' + ChatAdapter._escapeHtml(cleanUrl) +
               '" class="nav-pill">' +
               ChatAdapter._escapeHtml(cleanLabel) +
               '</a>';
      }
    );

    return html;
  },

  // Split chart content into {name, value} entries.
  // Handles formats: "name=42", "name=42.5", "name=$42K"
  _parseChartEntries(content) {
    var parts = content.split(',').map(function(p) { return p.trim(); }).filter(Boolean);
    var entries = [];
    for (var i = 0; i < parts.length; i++) {
      var eqIdx = parts[i].lastIndexOf('=');
      if (eqIdx === -1) {
        // Fallback: treat whole thing as name, value=0
        entries.push({ name: parts[i].trim(), value: 0 });
        continue;
      }
      var name = parts[i].substring(0, eqIdx).trim();
      var rawVal = parts[i].substring(eqIdx + 1).trim();
      // Extract numeric value, stripping $, %, commas
      var numVal = parseFloat(rawVal.replace(/[$%,]/g, '').replace(/,/g, ''));
      if (isNaN(numVal)) numVal = 0;
      entries.push({ name: name, value: numVal, display: rawVal });
    }
    return entries;
  },

  // Build an HTML horizontal bar chart from entries.
  _buildMiniChart(entries) {
    var maxVal = 1;
    for (var i = 0; i < entries.length; i++) {
      if (entries[i].value > maxVal) maxVal = entries[i].value;
    }
    if (maxVal === 0) maxVal = 1;

    var bars = '';
    for (var i = 0; i < entries.length; i++) {
      var e = entries[i];
      var pct = Math.round((e.value / maxVal) * 100);
      var display = e.display || e.value;
      bars += '<div class="mini-bar-row">' +
        '<span class="mini-bar-label">' + ChatAdapter._escapeHtml(e.name) + '</span>' +
        '<span class="mini-bar-track"><span class="mini-bar-fill" style="width:' + pct + '%"></span></span>' +
        '<span class="mini-bar-value">' + ChatAdapter._escapeHtml(String(display)) + '</span>' +
        '</div>';
    }

    return '<div class="mini-chart">' + bars + '</div>';
  },

  // Build an HTML data table from already-delimited rows (one array per row).
  // Row 0 is the header; every other row is padded/truncated to its width so a
  // ragged model response can never shift cells into the wrong column.
  _buildDataTableRows(rows) {
    if (!rows || rows.length === 0) return '';
    var cols = rows[0].length;
    if (cols === 0) return '';

    var html = '<table class="data-table"><thead><tr>';
    for (var i = 0; i < cols; i++) {
      html += '<th>' + ChatAdapter._escapeHtml(rows[0][i] || '') + '</th>';
    }
    html += '</tr></thead><tbody>';
    for (var r = 1; r < rows.length; r++) {
      html += '<tr>';
      for (var c = 0; c < cols; c++) {
        html += '<td>' + ChatAdapter._escapeHtml(rows[r][c] || '') + '</td>';
      }
      html += '</tr>';
    }
    html += '</tbody></table>';
    return html;
  },

  // Build an HTML data table from cell array.
  // First N cells are headers; rest fill rows of N columns.
  _buildDataTable(cells) {
    if (cells.length === 0) return '';

    // Determine column count: assume first row is headers
    // If only 2-3 cells, could be a 2-column table
    // Heuristic: if cells.length <= 6, try small table
    var cols, rows;
    if (cells.length === 2) {
      // Single key-value: treat as 2 columns, 1 data row
      cols = 2;
      rows = [cells];
    } else if (cells.length === 3) {
      cols = 3;
      rows = [cells];
    } else if (cells.length === 4) {
      // Could be 2 cols × 2 rows, or 4 cols × 1 row
      cols = 2;
      rows = [cells.slice(0, 2), cells.slice(2, 4)];
    } else {
      // Try to detect from first few cells
      cols = Math.min(cells.length, 5); // reasonable max
      // Find the smallest n where cells.length is divisible
      for (var n = 2; n <= Math.min(cells.length, 5); n++) {
        if (cells.length % n === 0) {
          cols = n;
          break;
        }
      }
      // Build rows
      rows = [];
      for (var i = 0; i < cells.length; i += cols) {
        var row = cells.slice(i, Math.min(i + cols, cells.length));
        // Pad last row if needed
        while (row.length < cols) row.push('');
        rows.push(row);
      }
    }

    var html = '<table class="data-table"><thead><tr>';
    var headers = rows[0];
    var dataStart = 1;
    // If only one row, use first col as header-like
    if (rows.length === 1 && cols === 2) {
      // Treat as key-value pairs: left col bold, right col value
      html = '<table class="data-table"><tbody>';
      html += '<tr><th>' + ChatAdapter._escapeHtml(cells[0]) + '</th><td>' + ChatAdapter._escapeHtml(cells[1]) + '</td></tr>';
      html += '</tbody></table>';
      return html;
    }

    for (var i = 0; i < cols; i++) {
      html += '<th>' + ChatAdapter._escapeHtml(headers[i] || '') + '</th>';
    }
    html += '</tr></thead><tbody>';
    for (var r = dataStart; r < rows.length; r++) {
      html += '<tr>';
      for (var c = 0; c < cols; c++) {
        var cell = rows[r][c] || '';
        html += '<td>' + ChatAdapter._escapeHtml(cell) + '</td>';
      }
      html += '</tr>';
    }
    html += '</tbody></table>';
    return html;
  },

  _escapeHtml(str) {
    return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  },

  // ── Navigation Command Parsing ───────────────────────────────────────────────

  // Extract navigation commands from response text. Returns {cleanText, commands}.
  // Commands: {{scroll:layer-N}}, {{map:indicator=X&zoom=N}}, {{highlight:card-N}}
  _parseNavigationCommands(text) {
    if (!text) return { cleanText: text, commands: [] };
    var commands = [];
    var cleanText = text;

    // 1. Scroll commands: {{scroll:layer-N}} or {{scroll:layer-N:description}}
    var scrollRe = /\{\{scroll:\s*([^:}]+?)(?::\s*([^}]*?))?\s*\}\}/g;
    cleanText = cleanText.replace(scrollRe, function(match, target, description) {
      commands.push({ type: 'scroll', target: target.trim(), description: description ? description.trim() : null });
      return '';  // Remove from displayed text
    });

    // 2. Map commands: {{map:indicator=X&zoom=N}} or {{map:geoid=X&zoom=N}}
    var mapRe = /\{\{map:\s*([^}]+?)\s*\}\}/g;
    cleanText = cleanText.replace(mapRe, function(match, params) {
      var parsed = {};
      var pairs = params.split('&');
      for (var i = 0; i < pairs.length; i++) {
        var eq = pairs[i].indexOf('=');
        if (eq > 0) {
          parsed[pairs[i].substring(0, eq).trim()] = pairs[i].substring(eq + 1).trim();
        }
      }
      commands.push({ type: 'map', params: parsed });
      return '';  // Remove from displayed text
    });

    // 3. Highlight commands: {{highlight:card-N}} or {{highlight:evidence-N}}
    var hlRe = /\{\{highlight:\s*([^}]+?)\s*\}\}/g;
    cleanText = cleanText.replace(hlRe, function(match, target) {
      commands.push({ type: 'highlight', target: target.trim() });
      return '';  // Remove from displayed text
    });

    // 4. Layer reveal commands (alternative format): {{layer:N}}
    var layerRe = /\{\{layer:\s*(\d+)\s*\}\}/g;
    cleanText = cleanText.replace(layerRe, function(match, num) {
      commands.push({ type: 'scroll', target: 'layer-' + num });
      return '';  // Remove from displayed text
    });

    // Clean up extra whitespace from removed commands
    cleanText = cleanText.replace(/\n{3,}/g, '\n\n').trim();

    return { cleanText: cleanText, commands: commands };
  },

  // ── Suggested Questions ─────────────────────────────────────────────────────

  // Returns context-aware suggested question prompts for the current page.
  _getSuggestedQuestions() {
    var ctx = this._pageContext;

    switch (ctx && ctx.page) {
      case 'county':
        return [
          'Walk me through ' + (ctx.name || 'this county'),
          'How does ' + (ctx.name || 'this county') + ' compare to the state average?',
          'Compare this county to the state average',
          'How has poverty changed in ' + (ctx.name || 'this county') + ' since 2019?',
          'What should I do about this?',
          'Which policies would help ' + (ctx.name || 'this county') + ' the most?',
          'What are the biggest disparities within ' + (ctx.name || 'this county') + '?'
        ];
      case 'map':
        return [
          'Why is this tract an outlier?',
          'Where are the biggest clusters?',
          'Explain why some tracts cluster on ' + (ctx.indicator || 'this indicator'),
          'What correlates most strongly with ' + (ctx.indicator || 'this indicator') + '?',
          'Show me the distribution of ' + (ctx.indicator || 'this indicator') + ' across all tracts'
        ];
      case 'compare':
        return [
          'Why does ' + (ctx.name1 || 'County A') + ' lead on health?',
          'What\'s the biggest gap between these two counties?',
          'Show me the biggest differences between ' + (ctx.name1 || 'County A') + ' and ' + (ctx.name2 || 'County B'),
          'What explains the differences between these two counties?',
          'How do these counties rank on key indicators statewide?'
        ];
      default:
        // Explorer or standalone chat page
        return [
          'Which county has the highest poverty rate?',
          'What are the top 5 counties by uninsured rate?',
          'Is poverty correlated with cost burden across Wisconsin?',
          'Compare Menominee County with Dane County',
          'Show me the distribution of median incomes across WI counties',
          'Which policies target the highest-burden counties?'
        ];
    }
  },

  // ── Page Context ────────────────────────────────────────────────────────────

  _buildPageContextBlock() {
    if (!this._pageContext) return '';
    const ctx = this._pageContext;
    switch (ctx.page) {
      case 'county':
        return `\nCURRENT PAGE CONTEXT: The user is currently viewing ${ctx.name || 'a county'} (GEOID: ${ctx.geoid || 'unknown'}). Contextualize your answer relative to this county unless asked otherwise.`;
      case 'map':
        return `\nCURRENT PAGE CONTEXT: The user is viewing the LISA cluster map for ${ctx.indicator || 'an indicator'}.`;
      case 'compare':
        return `\nCURRENT PAGE CONTEXT: The user is comparing ${ctx.name1 || 'County A'} and ${ctx.name2 || 'County B'}${ctx.geoid1 && ctx.geoid2 ? ' (' + ctx.geoid1 + ' vs ' + ctx.geoid2 + ')' : ''}.`;
      default:
        return '';
    }
  },

  // ── System Prompt ───────────────────────────────────────────────────────────

  async _buildSystemPrompt() {
    if (this._systemPrompt) return this._systemPrompt;

    // Fetch live data to ground the chat.
    //
    // ── Why this is tabular and not prose ──────────────────────────────────
    // This prompt is re-sent on EVERY message, so its size sets how many
    // people the public chat can serve under the operator's daily cap. The
    // prose "key=value" form used here previously spent ~45% of its bytes
    // re-stating column names and the same candidate identity on every row.
    // Both blocks are now header-declared and pipe-delimited: the header
    // names the columns once, the rows carry only values. This is LOSSLESS —
    // every field that was here before is still here. Verified safe against
    // the live payload: no county name or policy field contains a "|", and
    // all 72 county names carry the " County" suffix that the header hoists.
    let countyBlock = '';
    let policyBlock = '';
    let topUninsured = '';
    let countyCount = 0;
    let policyCount = 0;
    try {
      const [countyResp, policyResp] = await Promise.allSettled([
        PDI.counties(),
        PDI.policies()
      ]);

      if (countyResp.status === 'fulfilled') {
        const items = countyResp.value.items || [];
        countyCount = items.length;
        countyBlock = items.map(c => {
          const pov = Domain.indValue(c, 'poverty_rate');
          const inc = Domain.indValue(c, 'median_household_income');
          const uns = Domain.indValue(c, 'uninsured_rate');
          return [
            (c.name || '?').replace(/ County$/, ''),
            c.geoid || '?',
            c.population != null ? c.population : '?',
            pov != null ? pov : '?',
            inc != null ? Math.round(inc) : '?',
            uns != null ? uns : '?'
          ].join('|');
        }).join('\n');

        // Derive the highest-uninsured counties from the SAME live payload.
        //
        // These used to be hardcoded in the COST-SAVING framework below as
        // "Menominee (16.5%), Iron (11.2%), Florence (10.8%)". By 2026-07-29
        // the live data said Menominee 22.5%, Clark 21.3%, Vernon 14.7% —
        // Menominee was 6 points off and neither Iron nor Florence was still
        // in the top five. The model was being handed wrong figures and
        // presenting them as grounded fact.
        //
        // Deriving them means they cannot rot again. Same failure class as
        // the pinned model ID and the hardcoded exchange count: a literal
        // that was true once, kept asserting itself after it stopped being
        // true, and nothing failed loudly enough to notice.
        topUninsured = items
          .map(c => ({ n: (c.name || '?').replace(/ County$/, ''), v: Domain.indValue(c, 'uninsured_rate') }))
          .filter(r => r.v != null)
          .sort((a, b) => b.v - a.v)
          .slice(0, 3)
          .map(r => `${r.n} (${r.v}%)`)
          .join(', ');
      }

      if (policyResp.status === 'fulfilled') {
        const policies = policyResp.value || [];
        policyCount = policies.length;

        // Hoist the repeated "Candidate (Office, State)" into ONE header per
        // candidate rather than repeating it on every row.
        //
        // CAREFUL: this dataset is NOT single-candidate. As of 2026-07-29 it is
        // 70 Francesca Hong (Governor, WI) + 15 Zohran Mamdani (Mayor, NY).
        // Hoisting a single global candidate header would silently relabel all
        // 15 Mamdani positions as Hong's — a factual corruption invisible to
        // any structural check. Grouping is what keeps attribution correct, and
        // the grouping is computed from the data (not hardcoded), so a third
        // candidate would get their own header automatically.
        const groups = [];
        const byKey = Object.create(null);
        for (const p of policies) {
          const key = `${p.candidate || '?'} ${p.office || '?'} ${p.state || '?'}`;
          if (!byKey[key]) {
            byKey[key] = {
              candidate: p.candidate || '?',
              office: p.office || '?',
              state: p.state || '?',
              rows: []
            };
            groups.push(byKey[key]);
          }
          byKey[key].rows.push(p);
        }
        policyBlock = groups.map(g => {
          const prefixes = Array.from(
            new Set(g.rows.map(p => String(p.id || '').split('-')[0]).filter(Boolean))
          ).join('/');
          const header = `# ${g.candidate} — ${g.office}, ${g.state} — ${g.rows.length} positions` +
                         (prefixes ? ` (ids ${prefixes}-*)` : '');
          const body = g.rows.map(p => [
            p.id || '?',
            p.title || '?',
            p.equity_dimension || '?',
            p.description || ''
          ].join('|')).join('\n');
          return header + '\n' + body;
        }).join('\n');
      }
    } catch (_) {}

    this._systemPrompt = `You are the Policy Data Infrastructure assistant. You answer questions about Wisconsin county-level social determinants data, candidate policy positions, and their connections. You have COMPLETE ACCESS to the live dataset below. Use it to answer precisely. Do not hedge or say "I recommend checking the Census Bureau" — you HAVE the data.

INSTRUCTIONS:
- For a county, cite its exact poverty rate, income, and uninsured rate from the data below. Use specific numbers, never ranges.
- For a policy, name the equity dimension it addresses and which counties are worst on that dimension's indicators.
- For "which policies help which counties most", cross-reference policy equity_dimension against county indicators.
- For cost-saving questions, prioritize policies addressing the highest-burden counties (highest poverty, worst health outcomes, most cost-burdened).
- Cite sources: Census ACS 2023 5-Year (demographics), CDC PLACES 2022 (health outcomes), USDA FARA 2019 (food access).

QUERY OPERATIONS — plan each answer as one of: lookup (one geography-indicator value) · rank (top N, bottom N, statewide order) · compare (2+ geographies across indicators) · aggregate (mean, median, min, max) · threshold (filter above/below a cutoff) · distribution (range, quartiles, skew, shape) · correlation (direction and strength between two indicators) · explain (data + methodology: why a geography has this value) · time_series (across vintages, e.g. 2019 vs 2023).

RICH FORMATTING TOKENS — these render as styled components:
- {{stat:value:label}} — highlight one key statistic, e.g. {{stat:17.5%:poverty rate}}
- {{chart:name1=val1, name2=val2, ...}} — compare values across entities, e.g. {{chart:Menominee=17.5, Dane=9.2, State=11.3}}
- {{table:h1|h2|row1c1|row1c2|row2c1|row2c2|...}} — structured comparison, e.g. {{table:County|Poverty|Income|Menominee|17.5%|$45,200|Dane|9.2%|$78,900}}

CRITICAL — two hard rules for every {{...}} token. (1) Never nest one token inside another: a {{table:...}} or {{chart:...}} contains plain values only, never a {{stat:...}}. (2) Never put a line break inside a token — a {{table:...}} is ONE flat pipe-separated list on a SINGLE line: every header cell, then every data cell in row order, separated only by "|". The DATA BLOCKS below happen to use one pipe-delimited record per line; that is an input format for you to read, and must never be copied into an output token. Breaking either rule makes the table render as scrambled columns or as raw text.

Use them whenever presenting numeric comparisons, rankings, or key findings. Mix with narrative — never a block of bare tokens. Stat callouts inline with the explanation, tables after comparisons, charts for rankings of 3-6 items.

NAVIGATION COMMANDS — you control the page the user sees. Use sparingly, only when they add to the narrative; don't spam them.
- {{scroll:layer-N}} — scroll to Layer N of a county profile (N is 1-5)
- {{map:indicator=X&zoom=N}} — update the map to indicator X at zoom level N
- {{highlight:card-N}} — highlight evidence card N
- {{layer:N}} — shorthand for {{scroll:layer-N}}

SPATIAL NARRATION — when asked to "walk through," "compare," "explain," or "recommend," narrate the page section by section:
- WALK-THROUGH ("walk me through this county"): narrate layers 1-5 in order, 2-3 sentences each, describing what the user sees and citing key values, with {{scroll:layer-N}} to scroll to each. Order: introduce the county → L1 primary observation → L2 research-grounded measures → L3 derived structure → L4 geography as signal → L5 query-time construction → evidence cards / policy levers.
- COMPARISON: compute the values and show {{chart:...}} side-by-side. Cite specific numbers — "Dane County's poverty rate is 9.2% vs the state average of 11.3%." If comparing neighbors, name them and their values. End with which direction the gap runs and what it means.
- EXPLANATION ("why is this tract an outlier/cluster"): explain LISA methodology — "A High-High cluster means both this tract AND its neighbors have high values — it's not just this tract being high, it's the neighborhood being high together." Cite the tract's value and its neighbors'. Define spatial autocorrelation in plain language.
- RECOMMENDATION ("what should I do about this"): cross-reference the county's worst indicators with policy evidence cards, matching equity dimensions to its highest-burden areas. For each: (1) name the policy lever, (2) cite the county data justifying it, (3) state the equity dimension. Use {{highlight:card-N}} for relevant evidence cards.

EQUITY DIMENSION → INDICATOR MAPPING:
- housing_affordability, housing_stability → poverty_rate, median_household_income (cost-burdened counties)
- health_access, health_equity → uninsured_rate, poverty_rate (health underserved counties)
- food_access → poverty_rate (food desert concentration in high-poverty counties)
- income_equity, economic_equity → median_household_income, poverty_rate
- education_funding, education_equity → poverty_rate (school funding correlates with income)
- environmental_health, environmental_justice → poverty_rate (pollution burden concentrates in poor counties)
- transit_access → poverty_rate, median_household_income (transit deserts in rural poor counties)
- rural_equity → poverty_rate in northern/rural counties

WISCONSIN COUNTY DATA (${countyCount || 72} counties, Census ACS 2023 5-Year). Pipe-delimited; the trailing " County" is omitted from each name (row 1 is Adams County). Columns:
name|geoid|population|poverty_rate(%)|median_household_income($)|uninsured_rate(%)
${countyBlock || 'Data loading failed — provide general analysis based on known WI patterns'}

CANDIDATE POLICY POSITIONS (${policyCount || 85} total). Pipe-delimited and grouped by candidate: each "# Candidate — Office, State" header applies to EVERY row beneath it until the next such header. Columns:
id|title|equity_dimension|description
${policyBlock || 'Policy data loading failed'}

COST-SAVING ANALYSIS FRAMEWORK:
Interventions save the most money in counties with the highest poverty + uninsured rates, because:
1. Medicaid expansion (Hong's BadgerCare) saves most in high-uninsured counties: ${topUninsured || 'see the uninsured_rate column above'}
2. Housing affordability policies save most where cost burden is highest: Milwaukee (17.5% poverty + 939K pop = largest absolute burden)
3. Food access policies save most in high-poverty rural counties: Menominee, Ashland, Forest, Sawyer
4. Education funding saves most where chronic absence correlates with poverty: Milwaukee, Racine, Kenosha
${this._buildPageContextBlock()}`;

    return this._systemPrompt;
  },

  // ── Suggestion prompts ──────────────────────────────────────────────────────

  // Example questions, used ONLY as suggestion chips (chat.html falls back to
  // this list when _getSuggestedQuestions is unavailable).
  //
  // These are NOT answers and must never be emitted through send(). They used to
  // be streamed back as a fake reply whenever the backend was unreachable, which
  // is how a hard 401 on /v1/chat reached production unnoticed: the chat looked
  // like it was working while answering nothing. See _emitUnavailable().
  _placeholders: [
    "Try asking: 'Which policies will help Menominee County the most?' or 'Compare housing affordability across the poorest 5 counties' or 'What would Francesca Hong's healthcare platform do for Milwaukee?'",
    "I can cross-reference 85 policy positions with 72 counties of indicator data. Ask me which policies address which problems in which places.",
    "Try: 'Explain which policies will make a difference in which counties, starting with the most money-saving interventions.'"
  ],

  // ── Spatial Narrator ────────────────────────────────────────────────────────

  // narrate(command, pageContext): The spatial narrator entry point.
  // Sets page context, enhances the message with narration framing,
  // delegates to send(), and returns navigation commands extracted from the response.
  //
  // command examples: "Walk me through this county", "Compare to state"
  // pageContext: {page, geoid, name, indicator, ...} — same as setPageContext()
  //
  // Returns a promise resolving to {reply, navCommands} where:
  //   reply: the full response HTML (rich-parsed)
  //   navCommands: array of {type, target/params} extracted from response
  async narrate(command, pageContext) {
    this._narrateMode = true;
    this._navCommandsFound = [];
    if (pageContext) this.setPageContext(pageContext);

    // Build narration-framed message
    var narrationPrefix = '';
    var ctx = this._pageContext;
    if (ctx) {
      switch (ctx.page) {
        case 'county':
          narrationPrefix = '[NARRATION MODE: The user is on the ' + (ctx.name || 'county') + ' profile page. They see 5 data layers plus evidence cards. Guide them spatially. Use {{scroll:layer-N}} to scroll the page.] ';
          break;
        case 'map':
          narrationPrefix = '[NARRATION MODE: The user is on the LISA cluster map for ' + (ctx.indicator || 'an indicator') + '. Explain spatial patterns. Use {{map:...}} commands to zoom/recenter if helpful.] ';
          break;
        case 'compare':
          narrationPrefix = '[NARRATION MODE: The user is comparing ' + (ctx.name1 || 'County A') + ' and ' + (ctx.name2 || 'County B') + '. Narrate differences and similarities. Use visual comparisons.] ';
          break;
        default:
          narrationPrefix = '[NARRATION MODE: Guide the user through the data. Be spatial and contextual.] ';
      }
    }

    var fullMessage = narrationPrefix + command;
    var result = { reply: '', navCommands: [] };

    await this.send(fullMessage,
      function onChunk(chunk) {
        result.reply += chunk;
      },
      function onDone() { /* resolved below */ }
    );

    // Parse navigation commands from the response
    var parsed = ChatAdapter._parseRichResponse(result.reply);
    var navResult = ChatAdapter._parseNavigationCommands(result.reply);
    this._navCommandsFound = navResult.commands;
    result.navCommands = navResult.commands;
    result.reply = ChatAdapter._parseRichResponse(navResult.cleanText);

    this._narrateMode = false;
    return result;
  },

  // ── Chat Send ──────────────────────────────────────────────────────────────

  // There is deliberately no availability pre-flight here any more. The old
  // _checkProxy() burned a real /v1/chat round trip, cached the verdict for the
  // whole session, and — worst of all — routed every failure into a canned
  // "answer". send() now makes exactly one attempt and reports what actually
  // happened.

  // ── Grounded lane (2026-08-09) ──────────────────────────────────────────
  //
  // Data questions belong to the deterministic engine, not the prose model.
  // POST /v1/policy/chat/query executes a checked Intent against the live
  // database and returns an answer whose every figure came out of a query,
  // with a citation — no model, no spend, no Gateway round trip, and it keeps
  // answering when the Gateway does not. (A deploy severing a Gateway request
  // is exactly what produced the 502 on 2026-08-08; "which county has the
  // highest poverty rate" never needed that lane in the first place.)
  //
  // The Intent is built HERE from the vocabulary the server publishes at
  // /chat/schema — that endpoint exists so a caller can construct a valid
  // Intent without guessing. Matching is deliberately CONSERVATIVE: an
  // unrecognised shape returns null and the question falls through to the
  // Gateway, because a confidently wrong Intent would answer a question
  // nobody asked. The server validates whatever arrives and refuses anything
  // invalid, so the worst case of a bad match is a refusal we fall through on.
  //
  // Deliberately NOT mapped: "best" and "worst". Direction orders the VALUE,
  // not the goodness of it — the highest poverty rate is the worst place, and
  // pkg/grounding/intent.go keeps that distinction out of the schema on
  // purpose. Those questions go to the prose lane rather than have this
  // matcher decide what "best" means.

  _grounded: { loaded: false, schema: null, places: [], distinctive: null },

  async _loadGroundedVocab() {
    if (this._grounded.loaded) return this._grounded;
    this._grounded.loaded = true; // one attempt; a failure means "decline", not "retry forever"
    try {
      const [schema, geos] = await Promise.all([
        fetch('/v1/policy/chat/schema').then(r => (r.ok ? r.json() : null)).catch(() => null),
        fetch('/v1/policy/geographies?level=county&limit=500').then(r => (r.ok ? r.json() : null)).catch(() => null),
      ]);
      if (schema && Array.isArray(schema.indicators)) this._grounded.schema = schema;
      const items = (geos && (geos.items || geos.geographies)) || [];
      this._grounded.places = items.filter(g => g && g.name).map(g => ({ name: g.name }));

      // Tokens that identify exactly ONE indicator. They let "highest obesity"
      // match "Obesity Prevalence" without demanding the full label, while a
      // token shared by several indicators (e.g. "population") stays ambiguous
      // and is left to the stricter tiers.
      if (this._grounded.schema) {
        const counts = {};
        for (const ind of this._grounded.schema.indicators) {
          const toks = new Set(
            ((ind.id || '') + ' ' + (ind.label || ''))
              .toLowerCase().replace(/[^a-z0-9\s_]/g, ' ').replace(/_/g, ' ')
              .split(/\s+/).filter(w => w.length >= 6));
          for (const t of toks) counts[t] = (counts[t] || 0) + 1;
        }
        const distinctive = {};
        for (const ind of this._grounded.schema.indicators) {
          const toks = new Set(
            ((ind.id || '') + ' ' + (ind.label || ''))
              .toLowerCase().replace(/[^a-z0-9\s_]/g, ' ').replace(/_/g, ' ')
              .split(/\s+/).filter(w => w.length >= 6));
          for (const t of toks) if (counts[t] === 1) distinctive[t] = ind.id;
        }
        this._grounded.distinctive = distinctive;
      }
    } catch (_) { /* leaves schema null; the matcher declines */ }
    return this._grounded;
  },

  _matchIndicator(q, vocab) {
    let best = null, bestScore = 0;
    for (const ind of vocab.schema.indicators) {
      const id = String(ind.id || '').toLowerCase();
      const label = String(ind.label || '').toLowerCase();
      let score = 0;
      const idPhrase = id.replace(/_/g, ' ');
      if (idPhrase && q.indexOf(' ' + idPhrase + ' ') >= 0) score = 300 + idPhrase.length;
      else if (label && q.indexOf(' ' + label + ' ') >= 0) score = 200 + label.length;
      else if (label) {
        const words = label.replace(/[^a-z0-9\s]/g, ' ').split(/\s+/).filter(w => w.length >= 4);
        if (words.length && words.every(w => q.indexOf(w) >= 0)) score = 100 + words.join('').length;
      }
      if (score > bestScore) { bestScore = score; best = ind.id; }
    }
    if (best) return best;
    // Fall back to a token that names exactly one indicator.
    const dist = vocab.distinctive || {};
    for (const tok of Object.keys(dist)) {
      if (q.indexOf(' ' + tok) >= 0) return dist[tok];
    }
    return null;
  },

  _matchPlaces(q, places) {
    const found = [];
    for (const p of places) {
      const bare = String(p.name).replace(/\s+county$/i, '').toLowerCase();
      if (bare.length < 4) continue;
      const at = q.indexOf(' ' + bare + ' ');
      if (at >= 0) found.push({ name: p.name, at: at });
    }
    found.sort((a, b) => a.at - b.at);
    return found.map(f => f.name);
  },

  _matchIntent(question, vocab) {
    if (!vocab || !vocab.schema) return null;
    const q = ' ' + String(question || '').toLowerCase()
      .replace(/[^a-z0-9%.\s-]/g, ' ').replace(/\s+/g, ' ').trim() + ' ';
    if (q.trim().length < 3) return null;

    const indicator = this._matchIndicator(q, vocab);
    if (!indicator) return null;

    const level = /\btracts?\b/.test(q) ? 'tract' : 'county';
    const places = this._matchPlaces(q, vocab.places);

    let limit = 0;
    const mLimit = q.match(/\btop\s+(\d{1,2})\b/) ||
                   q.match(/\b(\d{1,2})\s+(?:highest|lowest)\b/) ||
                   q.match(/\b(?:highest|lowest)\s+(\d{1,2})\b/);
    if (mLimit) limit = Math.max(1, Math.min(parseInt(mLimit[1], 10), 50));

    const wantsHigh = /\b(highest|most|largest|greatest|biggest|top|maximum|max)\b/.test(q);
    const wantsLow  = /\b(lowest|least|smallest|fewest|bottom|minimum|min)\b/.test(q);
    const wantsAgg  = /\b(average|mean|median|typical|statewide|overall)\b/.test(q);
    const asksCount = /\bhow many\b|\bnumber of\b|\bcount\b/.test(q);

    const mThresh = q.match(/\b(above|over|more than|greater than|at least|below|under|less than|fewer than)\s+\$?([0-9][0-9,]*\.?[0-9]*)/);
    if (mThresh) {
      const above = /above|over|more than|greater than|at least/.test(mThresh[1]);
      const value = parseFloat(mThresh[2].replace(/,/g, ''));
      if (!isNaN(value)) {
        return { operation: 'threshold', indicator: indicator, level: level,
                 comparator: above ? 'above' : 'below', threshold: value };
      }
    }

    if (places.length >= 2) {
      return { operation: 'compare', indicator: indicator, level: level, places: places.slice(0, 4) };
    }
    if (places.length === 1) {
      return { operation: 'lookup', indicator: indicator, level: level, places: places };
    }
    if (wantsHigh || wantsLow) {
      const intent = { operation: 'rank', indicator: indicator, level: level,
                       direction: (wantsLow && !wantsHigh) ? 'lowest' : 'highest' };
      if (limit) intent.limit = limit;
      return intent;
    }
    if (wantsAgg || asksCount) {
      let aggregate = 'median';
      if (asksCount && !wantsAgg) aggregate = 'count';
      return { operation: 'aggregate', indicator: indicator, level: level, aggregate: aggregate };
    }
    return null;
  },

  // Emit an already-complete answer through the streaming contract the page
  // expects — in ONE chunk, with no timers.
  //
  // The Gateway path paces itself with setTimeout to simulate typing, which
  // makes sense while a remote model is the reason you are waiting. Here the
  // answer is already in hand: the pacing would be pure theatre, and it has a
  // real cost — background tabs clamp setTimeout to roughly once per second,
  // so a 33-chunk "stream" takes 33 seconds to finish in an unfocused tab
  // (observed while verifying this, and initially mistaken for a truncation
  // bug). One chunk is instant, honest, and immune to that clamp.
  _emitAnswer(text, onChunk, onDone) {
    onChunk(text);
    onDone();
  },

  // Returns true when the grounded engine answered and nothing further is needed.
  async _tryGrounded(userMessage, onChunk, onDone) {
    let vocab;
    try { vocab = await this._loadGroundedVocab(); } catch (_) { return false; }
    if (!vocab || !vocab.schema) return false;

    // 1. Structured lane — no model anywhere in the path.
    const intent = this._matchIntent(userMessage, vocab);
    if (intent) {
      const ans = await this._postGrounded('/v1/policy/chat/query', intent);
      if (ans && ans.answered && ans.text) {
        this._lastLane = 'grounded-structured';
        this._emitAnswer(ans.text +
          '\n\nAnswered from the dataset directly — no model produced these numbers.',
          onChunk, onDone);
        return true;
      }
    }

    // 2. Natural-language grounded lane, only where a planner exists. Without
    //    one the server refuses honestly, and asking anyway just burns a round
    //    trip — so the schema's capability flag gates it. When a planner is
    //    configured later, this starts working with no further change here.
    if (vocab.schema.planner_configured) {
      const ans = await this._postGrounded('/v1/policy/chat', { query: userMessage });
      if (ans && ans.answered && ans.text) {
        this._lastLane = 'grounded-nl';
        this._emitAnswer(ans.text, onChunk, onDone);
        return true;
      }
    }
    return false;
  },

  async _postGrounded(path, payload) {
    try {
      const r = await fetch(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!r.ok) return null; // 422 = intent refused; fall through to the Gateway
      return await r.json();
    } catch (_) {
      return null;
    }
  },

  // Which lane answered last, for debugging. Never rendered.
  _lastLane: null,

  async send(userMessage, onChunk, onDone) {
    // Deterministic first, prose second. A question the dataset can answer
    // exactly should never be paraphrased by a model.
    if (await this._tryGrounded(userMessage, onChunk, onDone)) return;
    this._lastLane = 'gateway';
    await this._sendToGateway(userMessage, onChunk, onDone);
  },

  async _sendToGateway(userMessage, onChunk, onDone) {
    let r;
    try {
      const systemPrompt = await this._buildSystemPrompt();

      r = await fetch('/v1/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        // ── Model routing ────────────────────────────────────────────────
        // Operator decision 2026-07-29: chat is open to the public and runs on
        // DeepSeek v4 Pro via OpenRouter rather than Anthropic, so free usage
        // does not spend Anthropic credits.
        //
        // These two values are a PINNED pair and pinning rots — the previous
        // pin ('claude-sonnet-4-20250514') was retired upstream and every
        // request 500'd with "model ... not found in provider anthropic" for an
        // unknown period, invisible because the UI answered with a canned
        // placeholder. The placeholder path is gone now, so the same failure
        // would surface immediately as "NOT AN ANSWER — HTTP 500".
        //
        // To change model: check what the Gateway actually offers first —
        //   GET /v1/models  (534 entries; 13 deepseek/*, plus anthropic/*)
        // Both `provider: 'openrouter'` and omitting provider entirely resolve
        // this model; the explicit provider is kept so the routing is legible.
        body: JSON.stringify({
          message: userMessage,
          session_id: this._sessionId,
          system_prompt: systemPrompt,
          provider: 'openrouter',
          model: 'deepseek/deepseek-v4-pro',
          stream: false
        })
      });
    } catch (err) {
      // No HTTP response at all — DNS, TLS, CORS, dropped connection, or the
      // device is offline. Distinct from "the server answered with an error".
      this._proxyAvailable = false;
      this._lastFailure = { kind: 'unreachable', detail: err && err.message };
      this._emitUnavailable('unreachable', { detail: err && err.message }, onChunk, onDone);
      return;
    }

    if (!r.ok) {
      // The server responded and said no. Surface its own words verbatim —
      // that is the signal that was previously swallowed by the placeholder.
      let raw = '';
      try { raw = await r.text(); } catch (_) {}
      this._proxyAvailable = false;
      this._lastFailure = { kind: 'server-error', status: r.status, detail: raw };
      this._emitUnavailable('server-error', { status: r.status, detail: this._extractServerReason(raw) }, onChunk, onDone);
      return;
    }

    let data = null;
    try {
      data = await r.json();
    } catch (err) {
      this._proxyAvailable = false;
      this._lastFailure = { kind: 'unreadable', detail: err && err.message };
      this._emitUnavailable('unreadable', { detail: 'response was not valid JSON' }, onChunk, onDone);
      return;
    }

    // A 200 with no usable text is still a non-answer. Never fall back to
    // JSON.stringify(data) — dumping the envelope into the bubble reads as
    // content and hides the fact that no answer arrived.
    const content = (data && (data.content || data.message)) || '';
    if (typeof content !== 'string' || content.trim() === '') {
      this._proxyAvailable = false;
      this._lastFailure = { kind: 'unreadable', detail: 'empty content field' };
      this._emitUnavailable('unreadable', { detail: 'the reply contained no answer text' }, onChunk, onDone);
      return;
    }

    this._proxyAvailable = true;
    this._lastFailure = null;

    // Stream for UX (character-by-character simulated typing)
    for (let i = 0; i < content.length; i += 5) {
      onChunk(content.substring(i, Math.min(i + 5, content.length)));
      await new Promise(resolve => setTimeout(resolve, 8));
    }
    onDone();
  },

  // ── Honest failure reporting ────────────────────────────────────────────────

  // Last failure recorded by _sendToGateway, for debugging. Never rendered raw.
  _lastFailure: null,

  // Pull the human-readable reason out of an error body. PDI's own ErrorResponse
  // ({error, detail}) and the Dojo Gateway's ({error, success}) both key on
  // "error", so one path covers both. Falls back to truncated raw text.
  _extractServerReason(raw) {
    if (!raw) return '';
    try {
      const j = JSON.parse(raw);
      if (j && typeof j.error === 'string') {
        return j.detail ? j.error + ' — ' + j.detail : j.error;
      }
    } catch (_) {}
    return raw.length > 300 ? raw.substring(0, 300) + '…' : raw;
  },

  // Emits a single, clearly-labelled non-answer notice and finishes.
  //
  // Sent as ONE chunk on purpose: chat.html re-renders innerHTML per chunk, so a
  // partially-streamed block would render as broken markup — and the simulated
  // typing animation is exactly what made the old canned reply feel like a real
  // response. A failure should not type itself out.
  //
  // kind: 'unreachable' | 'server-error' | 'unreadable'
  _emitUnavailable(kind, info, onChunk, onDone) {
    try {
      onChunk(this._unavailableHtml(kind, info || {}));
    } finally {
      if (typeof onDone === 'function') onDone();
    }
  },

  // Per-status plain-English cause. Operators and visitors both read this.
  _statusHint(status) {
    if (status === 401 || status === 403) {
      return 'The chat backend rejected this site’s credential, so the question never reached a model. This is a server configuration problem — rewording the question will not help.';
    }
    if (status === 404) {
      return 'This deployment has no chat backend: nothing is serving /v1/chat. Static builds of the Atlas ship without one.';
    }
    if (status === 429) {
      return 'The chat backend is rate-limited right now. Waiting a minute and asking again may work.';
    }
    if (status === 503) {
      return 'The chat backend is not configured or not running. An operator has to fix it — retrying will not.';
    }
    if (status >= 500) {
      return 'The chat backend failed while handling the question. This is a server-side fault, not a problem with what you asked.';
    }
    return 'The chat backend refused the request.';
  },

  // Builds the notice. Single line — no newlines anywhere — because chat.html
  // runs .replace(/\n/g,'<br>') over the rendered string, and a <br> injected
  // inside a tag would corrupt the markup. Every interpolated value is escaped.
  _unavailableHtml(kind, info) {
    var esc = ChatAdapter._escapeHtml;
    var title, lead, cause = '', reason = info.detail || '';

    if (kind === 'server-error') {
      title = 'Not an answer — chat backend returned an error';
      lead = 'Your question was sent but not answered. The chat backend replied with HTTP ' +
             esc(String(info.status)) + '. Nothing below this line comes from the dataset.';
      cause = ChatAdapter._statusHint(info.status);
    } else if (kind === 'unreadable') {
      title = 'Not an answer — unreadable reply from the chat backend';
      lead = 'The chat backend responded, but its reply contained no answer. Nothing here comes from the dataset.';
      cause = 'This usually means the backend is misconfigured or returned an unexpected payload shape.';
    } else {
      title = 'Not an answer — chat backend unreachable';
      lead = 'Your question was not delivered. The browser could not reach the chat backend, so there is no answer to give and nothing here comes from the dataset.';
      cause = 'You may be offline, or the chat backend may be down.';
    }

    var h = '<div class="chat-unavailable" role="alert" style="border-left:3px solid var(--tp-red);background:var(--tp-red-soft);border-radius:6px;padding:12px 14px;color:var(--text)">';
    h += '<div style="font-weight:700;text-transform:uppercase;letter-spacing:0.06em;font-size:0.7rem;margin-bottom:8px">' + esc(title) + '</div>';
    h += '<div style="margin-bottom:8px">' + esc(lead) + '</div>';
    h += '<div style="margin-bottom:8px">' + esc(cause) + '</div>';

    if (reason) {
      h += '<div style="font-size:0.78rem;opacity:0.85;margin-bottom:8px">Backend said: <code style="font-family:ui-monospace,SFMono-Regular,Menlo,monospace;word-break:break-word">' +
           esc(reason) + '</code></div>';
    }

    // The graceful part: what genuinely still works, and what the visitor will be
    // able to ask once chat is back. Framed in the future tense so it can never be
    // read as a reply to what they just asked.
    h += '<div style="font-size:0.82rem;opacity:0.9">The rest of the Atlas is unaffected — county profiles, the cluster map, comparisons, and evidence cards are all still live.</div>';

    var qs = [];
    try { qs = ChatAdapter._getSuggestedQuestions() || []; } catch (_) {}
    if (qs.length) {
      h += '<div style="font-size:0.82rem;opacity:0.9;margin-top:8px">Once chat is working again, questions like these will be answerable:</div>';
      h += '<ul style="margin:6px 0 0;padding-left:20px;font-size:0.82rem;opacity:0.9">';
      for (var i = 0; i < Math.min(qs.length, 3); i++) {
        h += '<li>' + esc(qs[i]) + '</li>';
      }
      h += '</ul>';
    }

    h += '</div>';
    return h;
  }
};

// See the EXPORT IDIOM note at the top of this file.
window.ChatAdapter = ChatAdapter;
