// lib/chat.js — Chat adapter with full data grounding for the Dojo Gateway.
const ChatAdapter = {
  _sessionId: 'pdi-web-' + Date.now().toString(36),
  _proxyAvailable: null,
  _systemPrompt: null,  // Built lazily from live API data
  _pageContext: null,   // {page, geoid, name, indicator, geoid1, geoid2, name1, name2}

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
        var cells = content.split('|').map(function(c) { return c.trim(); });
        // First row is headers; remaining cells split into rows of header-length
        // If only one row of data, treat all as a single-row table with first cells as headers
        return ChatAdapter._buildDataTable(cells);
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

  // ── Suggested Questions ─────────────────────────────────────────────────────

  // Returns context-aware suggested question prompts for the current page.
  _getSuggestedQuestions() {
    var ctx = this._pageContext;

    switch (ctx && ctx.page) {
      case 'county':
        return [
          'How does ' + (ctx.name || 'this county') + ' compare to the state average?',
          'What are the biggest disparities within ' + (ctx.name || 'this county') + '?',
          'Which policies would help ' + (ctx.name || 'this county') + ' the most?',
          'What is the distribution of poverty across tracts in ' + (ctx.name || 'this county') + '?',
          'Is poverty correlated with health outcomes in ' + (ctx.name || 'this county') + '?',
          'How has ' + (ctx.name || 'this county') + ' changed over time?'
        ];
      case 'map':
        return [
          'Where are the High-High clusters for ' + (ctx.indicator || 'this indicator') + '?',
          'Which tracts are spatial outliers?',
          'What correlates most strongly with ' + (ctx.indicator || 'this indicator') + '?',
          'Explain why some tracts cluster on ' + (ctx.indicator || 'this indicator'),
          'Show me the distribution of ' + (ctx.indicator || 'this indicator') + ' across all tracts'
        ];
      case 'compare':
        return [
          'Which county has better health outcomes?',
          'Show me the biggest differences between ' + (ctx.name1 || 'County A') + ' and ' + (ctx.name2 || 'County B'),
          'What explains the differences between these two counties?',
          'How do these counties rank on key indicators statewide?',
          'Which policies target the disparities between these counties?'
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

    // Fetch live data to ground the chat
    let countyLines = '';
    let policyLines = '';
    try {
      const [countyResp, policyResp] = await Promise.allSettled([
        PDI.counties(),
        PDI.policies()
      ]);

      if (countyResp.status === 'fulfilled') {
        const items = countyResp.value.items || [];
        countyLines = items.map(c => {
          const pov = Domain.indValue(c, 'poverty_rate');
          const inc = Domain.indValue(c, 'median_household_income');
          const uns = Domain.indValue(c, 'uninsured_rate');
          return `${c.name} (${c.geoid}): pop=${c.population?.toLocaleString() || '?'}, poverty=${pov != null ? pov + '%' : '?'}, income=$${inc ? Math.round(inc).toLocaleString() : '?'}, uninsured=${uns != null ? uns + '%' : '?'}`;
        }).join('\n');
      }

      if (policyResp.status === 'fulfilled') {
        const policies = policyResp.value || [];
        policyLines = policies.map(p =>
          `${p.id}: ${p.candidate} (${p.office || '?'}, ${p.state || '?'}) — ${p.title} [${p.equity_dimension || '?'}] — ${p.description || ''}`
        ).join('\n');
      }
    } catch (_) {}

    this._systemPrompt = `You are the Policy Data Infrastructure assistant. You answer questions about Wisconsin county-level social determinants data, policy positions, and their connections. You have COMPLETE ACCESS to the live dataset below. Use it to answer precisely. Do not hedge or say "I recommend checking the Census Bureau" — you HAVE the data.

INSTRUCTIONS:
- When asked about a county, cite its exact poverty rate, income, and uninsured rate from the data below
- When asked about policies, explain which equity dimensions they address and which counties have the worst indicators in those dimensions
- When asked "which policies will help which counties most", cross-reference the policy equity_dimensions with county indicators
- For cost-saving questions, prioritize policies addressing the highest-burden counties (highest poverty, worst health outcomes, most cost-burdened)
- Always cite the data source: Census ACS 2023 5-Year for demographics, CDC PLACES 2022 for health outcomes, USDA FARA 2019 for food access
- Use specific numbers, not ranges

QUERY OPERATIONS — When answering, plan responses using these analytical operations:
- lookup: find a specific value for a geography-indicator pair
- rank: order geographies by an indicator value (top N, bottom N, or statewide ranking)
- compare: side-by-side comparison of two or more geographies across multiple indicators
- aggregate: summary statistics (mean, median, min, max) across a set of geographies
- threshold: filter geographies above or below a cutoff value
- distribution: describe the spread of values — range, quartiles, skew, histogram shape
- correlation: identify relationships between two indicators across geographies (positive/negative, strength)
- explain: combine data with methodology to explain why a geography has a particular value (causal factors, context)
- time_series: compare data across vintage years (e.g., 2019 vs 2023 trends)

RICH FORMATTING TOKENS — Use these to present data visually. They render as styled components:
- Stat callouts: {{stat:value:label}} — for highlighting a key statistic (e.g., {{stat:17.5%:poverty rate}})
- Mini bar charts: {{chart:name1=val1, name2=val2, ...}} — for comparing values across entities (e.g., {{chart:Menominee=17.5, Dane=9.2, State=11.3}})
- Data tables: {{table:h1|h2|row1c1|row1c2|row2c1|row2c2|...}} — for structured comparisons (e.g., {{table:County|Poverty|Income|Menominee|17.5%|$45,200|Dane|9.2%|$78,900|State|11.3%|$72,400}})

Use these tokens whenever presenting numeric comparisons, rankings, or key findings. Mix them with narrative text — don't put all tokens in a block. Place stat callouts inline with explanations, tables after comparisons, and charts for rankings of 3-6 items.

EQUITY DIMENSION → INDICATOR MAPPING:
- housing_affordability, housing_stability → poverty_rate, median_household_income (cost-burdened counties)
- health_access, health_equity → uninsured_rate, poverty_rate (health underserved counties)
- food_access → poverty_rate (food desert concentration in high-poverty counties)
- income_equity, economic_equity → median_household_income, poverty_rate
- education_funding, education_equity → poverty_rate (school funding correlates with income)
- environmental_health, environmental_justice → poverty_rate (pollution burden concentrates in poor counties)
- transit_access → poverty_rate, median_household_income (transit deserts in rural poor counties)
- rural_equity → poverty_rate in northern/rural counties

WISCONSIN COUNTY DATA (72 counties, Census ACS 2023 5-Year):
${countyLines || 'Data loading failed — provide general analysis based on known WI patterns'}

CANDIDATE POLICY POSITIONS (85 total):
${policyLines || 'Policy data loading failed'}

COST-SAVING ANALYSIS FRAMEWORK:
The counties where policy interventions save the most money are those with the highest poverty + uninsured rates, because:
1. Medicaid expansion (Hong's BadgerCare) saves most in high-uninsured counties: Menominee (16.5%), Iron (11.2%), Florence (10.8%)
2. Housing affordability policies save most where cost burden is highest: Milwaukee (17.5% poverty + 939K pop = largest absolute burden)
3. Food access policies save most in high-poverty rural counties: Menominee, Ashland, Forest, Sawyer
4. Education funding saves most where chronic absence correlates with poverty: Milwaukee, Racine, Kenosha
${this._buildPageContextBlock()}`;

    return this._systemPrompt;
  },

  // ── Placeholders (fallback when chat proxy is unavailable) ──────────────────

  _placeholders: [
    "Try asking: 'Which policies will help Menominee County the most?' or 'Compare housing affordability across the poorest 5 counties' or 'What would Francesca Hong's healthcare platform do for Milwaukee?'",
    "I can cross-reference 85 policy positions with 72 counties of indicator data. Ask me which policies address which problems in which places.",
    "Try: 'Explain which policies will make a difference in which counties, starting with the most money-saving interventions.'"
  ],

  // ── Chat Send ──────────────────────────────────────────────────────────────

  async _checkProxy() {
    if (this._proxyAvailable !== null) return this._proxyAvailable;
    try {
      const r = await fetch('/v1/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: 'ping', session_id: this._sessionId, stream: false }),
        signal: AbortSignal.timeout(8000)
      });
      this._proxyAvailable = r.ok;
    } catch (_) {
      this._proxyAvailable = false;
    }
    return this._proxyAvailable;
  },

  async send(userMessage, onChunk, onDone) {
    const available = await this._checkProxy();
    if (available) {
      await this._sendToGateway(userMessage, onChunk, onDone);
    } else {
      await this._sendPlaceholder(userMessage, onChunk, onDone);
    }
  },

  async _sendToGateway(userMessage, onChunk, onDone) {
    try {
      const systemPrompt = await this._buildSystemPrompt();

      const r = await fetch('/v1/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: userMessage,
          session_id: this._sessionId,
          system_prompt: systemPrompt,
          provider: 'anthropic',
          model: 'claude-sonnet-4-20250514',
          stream: false
        })
      });

      if (!r.ok) {
        const errBody = await r.text();
        onChunk(`Error (${r.status}): ${errBody.substring(0, 200)}`);
        onDone();
        return;
      }

      const data = await r.json();
      const content = data.content || data.message || JSON.stringify(data);

      // Stream for UX (character-by-character simulated typing)
      for (let i = 0; i < content.length; i += 5) {
        onChunk(content.substring(i, Math.min(i + 5, content.length)));
        await new Promise(resolve => setTimeout(resolve, 8));
      }
      onDone();
    } catch (err) {
      onChunk(`Connection error: ${err.message}`);
      onDone();
    }
  },

  async _sendPlaceholder(userMessage, onChunk, onDone) {
    const response = this._placeholders[Math.floor(Math.random() * this._placeholders.length)];
    for (let i = 0; i < response.length; i += 4) {
      onChunk(response.substring(i, Math.min(i + 4, response.length)));
      await new Promise(resolve => setTimeout(resolve, 15));
    }
    onDone();
  }
};
