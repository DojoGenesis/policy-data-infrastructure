// lib/chat.js — Chat adapter with full data grounding for the Dojo Gateway.
const ChatAdapter = {
  _sessionId: 'pdi-web-' + Date.now().toString(36),
  _proxyAvailable: null,
  _systemPrompt: null,  // Built lazily from live API data

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
4. Education funding saves most where chronic absence correlates with poverty: Milwaukee, Racine, Kenosha`;

    return this._systemPrompt;
  },

  _placeholders: [
    "Try asking: 'Which policies will help Menominee County the most?' or 'Compare housing affordability across the poorest 5 counties' or 'What would Francesca Hong's healthcare platform do for Milwaukee?'",
    "I can cross-reference 85 policy positions with 72 counties of indicator data. Ask me which policies address which problems in which places.",
    "Try: 'Explain which policies will make a difference in which counties, starting with the most money-saving interventions.'"
  ],

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

      // Stream for UX
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
