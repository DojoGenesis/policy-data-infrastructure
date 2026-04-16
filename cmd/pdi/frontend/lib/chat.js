// lib/chat.js — SSE streaming adapter for Dojo Gateway.
// Falls back to a helpful placeholder while the proxy endpoint is being wired.
const ChatAdapter = {

  // Placeholder responses shown before the Gateway proxy is connected.
  _placeholders: [
    "The chat interface connects to the Dojo Gateway for AI-powered data analysis. The Gateway proxy endpoint is being wired up — check back soon.",
    "While the live gateway connection is being configured, you can explore the data using the Counties, Compare, Evidence, and Analysis tabs.",
    "The Policy Data Infrastructure platform tracks 42 indicator variables across 13 federal and state data sources covering all 72 Wisconsin counties and 1,652 census tracts.",
    "Try browsing Wisconsin counties at #/counties, or compare two counties side-by-side at #/compare to see detailed indicator differences.",
    "The Analysis section (#/analysis) surfaces statistical results including OLS regression coefficients, correlation matrices, and composite disadvantage indices computed across all tracts."
  ],

  // Detect whether the PDI chat proxy endpoint is available.
  _proxyAvailable: null,

  async _checkProxy() {
    if (this._proxyAvailable !== null) return this._proxyAvailable;
    try {
      const r = await fetch('/v1/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ messages: [{ role: 'user', content: 'ping' }], stream: false }),
        signal: AbortSignal.timeout(2000)
      });
      this._proxyAvailable = r.status !== 404 && r.status !== 501;
    } catch (_) {
      this._proxyAvailable = false;
    }
    return this._proxyAvailable;
  },

  // Send messages and stream the response.
  // onChunk(text) called with each chunk; onDone() called when complete.
  async send(messages, onChunk, onDone) {
    const available = await this._checkProxy();

    if (available) {
      await this._sendToGateway(messages, onChunk, onDone);
    } else {
      await this._sendPlaceholder(messages, onChunk, onDone);
    }
  },

  // Real SSE streaming to the PDI chat proxy (→ Dojo Gateway → Anthropic).
  async _sendToGateway(messages, onChunk, onDone) {
    try {
      const r = await fetch('/v1/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ messages, stream: true })
      });

      if (!r.ok) {
        onChunk(`Error: HTTP ${r.status} from chat endpoint.`);
        onDone();
        return;
      }

      const reader = r.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop(); // keep incomplete last line

        for (const line of lines) {
          if (line.startsWith('data: ')) {
            const data = line.slice(6).trim();
            if (data === '[DONE]') { onDone(); return; }
            try {
              const parsed = JSON.parse(data);
              // Anthropic SSE delta format
              const delta = parsed?.delta?.text
                ?? parsed?.choices?.[0]?.delta?.content
                ?? '';
              if (delta) onChunk(delta);
            } catch (_) {
              // non-JSON SSE line — ignore
            }
          }
        }
      }

      onDone();
    } catch (err) {
      onChunk(`Connection error: ${err.message}`);
      onDone();
    }
  },

  // Placeholder fallback — simulates streaming for UX consistency.
  async _sendPlaceholder(messages, onChunk, onDone) {
    const last = messages[messages.length - 1]?.content?.toLowerCase() || '';
    let response;

    // Simple keyword routing for slightly smarter placeholder responses.
    if (last.includes('county') || last.includes('dane') || last.includes('milwaukee')) {
      response = "County-level data is available at #/counties. Each county card shows poverty rate, median household income, and uninsured rate. Click any county for a full profile with grouped indicators by health, housing, food access, and demographics.";
    } else if (last.includes('tract') || last.includes('census')) {
      response = "Census tract data is available for all 1,652 Wisconsin tracts. Navigate to a county profile and click 'View Tracts' to explore tract-level CDC PLACES health outcomes and USDA food access indicators.";
    } else if (last.includes('policy') || last.includes('candidate')) {
      response = "The platform tracks 85 policy positions from Wisconsin progressive candidates, crosswalked to equity dimensions. Visit #/candidates to filter by candidate or policy category.";
    } else if (last.includes('analysis') || last.includes('regression') || last.includes('correlation')) {
      response = "The Analysis section (#/analysis) surfaces computed statistical results: OLS regression models, pairwise correlation matrices, composite disadvantage indices, and tipping point analyses across all 1,652 tracts.";
    } else {
      const idx = Math.abs(last.length) % this._placeholders.length;
      response = this._placeholders[idx];
    }

    // Simulate streaming at ~50 chars/sec.
    for (let i = 0; i < response.length; i += 3) {
      onChunk(response.substring(i, Math.min(i + 3, response.length)));
      await new Promise(r => setTimeout(r, 20));
    }
    onDone();
  }
};
