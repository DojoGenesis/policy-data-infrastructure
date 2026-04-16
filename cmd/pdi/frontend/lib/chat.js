// lib/chat.js — Chat adapter for the Dojo Gateway via /v1/chat proxy.
// The Gateway expects: { message: string, session_id: string, stream: bool }
// NOT the OpenAI-style { messages: [...] } format.
const ChatAdapter = {
  _sessionId: 'pdi-web-' + Date.now().toString(36),
  _proxyAvailable: null,

  _systemPrompt: `You are the Policy Data Infrastructure assistant. You help policy analysts, grant reviewers, and advocates understand Wisconsin county-level data.

DATA AVAILABLE:
- 72 Wisconsin counties with Census ACS 2023 indicators (poverty rate, median household income, uninsured rate, population, race demographics, housing cost burden)
- 1,652 census tracts with CDC PLACES health outcomes (obesity, diabetes, mental health, blood pressure, asthma, smoking, physical health) and USDA food access indicators
- 85 policy positions from Francesca Hong (WI Governor candidate, Democratic Socialist) and Zohran Mamdani (NYC Mayor, DSA)
- 12 statistical analyses: composite disadvantage indices, correlation matrices across Wisconsin tracts

KEY FACTS:
- Average WI poverty rate: 10.5% (72 counties, ACS 2023)
- Highest poverty: Menominee County (29.8%), highest uninsured: Menominee (16.5%)
- Lowest poverty: Waukesha County (4.2%), highest income: Waukesha ($95,107)
- Dane County (Madison): population 564,777, median income $88,108, poverty 10.5%
- Milwaukee County: population 939,489, poverty 18.2%, highest absolute cost burden

METHODOLOGY:
- Indicators stored as raw values, NULL for suppressed data
- ICE (Index of Concentration at the Extremes) measures segregation: range [-1, +1]
- Pipeline validation rejects data loads with >30% null rate
- 13 Go datasource adapters, 380+ tests, PostGIS backend

When answering, cite the data source and vintage year. Be specific about geography (county name, FIPS code). If you don't have the exact data, say so rather than guessing.`,

  _placeholders: [
    "The chat interface connects to the Dojo Gateway for AI-powered data analysis. Try asking about Wisconsin counties, poverty rates, health outcomes, or policy positions.",
    "Try: 'What county has the highest poverty rate?' or 'Compare Dane and Milwaukee counties' or 'Tell me about Francesca Hong's housing policies'",
    "The platform tracks 42 indicator variables across 13 data sources covering 72 Wisconsin counties and 1,652 census tracts."
  ],

  async _checkProxy() {
    if (this._proxyAvailable !== null) return this._proxyAvailable;
    try {
      const r = await fetch('/v1/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: 'ping', session_id: this._sessionId, stream: false }),
        signal: AbortSignal.timeout(5000)
      });
      // 200 = working, 400 with "message is required" = wrong format, 502 = gateway down
      this._proxyAvailable = r.ok;
    } catch (_) {
      this._proxyAvailable = false;
    }
    return this._proxyAvailable;
  },

  // Send a message and stream the response.
  // userMessage is the latest user text. conversationHistory is ignored for now
  // (the Gateway manages session state via session_id).
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
      const r = await fetch('/v1/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: userMessage,
          session_id: this._sessionId,
          system_prompt: this._systemPrompt,
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
      // Gateway response: { type: "complete", content: "...", usage: {...} }
      const content = data.content || data.message || JSON.stringify(data);

      // Simulate streaming for UX consistency
      for (let i = 0; i < content.length; i += 5) {
        onChunk(content.substring(i, Math.min(i + 5, content.length)));
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      onDone();
    } catch (err) {
      onChunk(`Connection error: ${err.message}`);
      onDone();
    }
  },

  async _sendPlaceholder(userMessage, onChunk, onDone) {
    const q = (userMessage || '').toLowerCase();
    let response;

    if (q.includes('county') || q.includes('dane') || q.includes('milwaukee')) {
      response = "County-level data is available at #/counties. Each county shows poverty rate, median household income, and uninsured rate from Census ACS 2023. Click any county for a full profile with indicators grouped by health, housing, food access, and demographics.";
    } else if (q.includes('tract') || q.includes('census')) {
      response = "Census tract data covers 1,652 Wisconsin tracts with CDC PLACES health outcomes (8 indicators) and USDA food access data (6 indicators). Navigate to a county profile to explore its tracts.";
    } else if (q.includes('policy') || q.includes('candidate') || q.includes('hong') || q.includes('mamdani')) {
      response = "The platform tracks 85 policy positions from Francesca Hong (WI Governor candidate, DSA) and Zohran Mamdani (NYC Mayor, DSA). Visit #/candidates to browse and filter.";
    } else if (q.includes('poverty') || q.includes('income') || q.includes('rate')) {
      response = "The average poverty rate across Wisconsin's 72 counties is 10.5% (Census ACS 2023). Menominee County has the highest rate. Browse all counties at #/counties or compare two at #/compare.";
    } else {
      response = this._placeholders[Math.floor(Math.random() * this._placeholders.length)];
    }

    for (let i = 0; i < response.length; i += 4) {
      onChunk(response.substring(i, Math.min(i + 4, response.length)));
      await new Promise(resolve => setTimeout(resolve, 15));
    }
    onDone();
  }
};
