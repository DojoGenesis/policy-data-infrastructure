// lib/api.js — API port adapter. ONLY file that calls fetch(). All calls are cached.
const PDI = {
  _cache: {},
  _varMeta: null,
  _policies: null,

  // ── Core HTTP helpers ──────────────────────────────────────────────────────

  async _get(url) {
    if (this._cache[url]) return this._cache[url];
    const r = await fetch(url);
    if (!r.ok) throw new Error(`HTTP ${r.status}: ${url}`);
    const d = await r.json();
    this._cache[url] = d;
    return d;
  },

  async _post(url, body) {
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}: ${url}`);
    return r.json();
  },

  // ── Geography ──────────────────────────────────────────────────────────────

  // Counties with key indicators — POST /query for enrichment
  async counties() {
    return this._post('/v1/policy/query', {
      level: 'county',
      state_fips: '55',
      limit: 100,
      variable_ids: ['poverty_rate', 'median_household_income', 'uninsured_rate']
    });
  },

  // Single geography with all indicators
  async geography(geoid) {
    return this._get(`/v1/policy/geographies/${geoid}`);
  },

  // Children of a geography (default: up to 200)
  async children(geoid, limit = 200) {
    return this._get(`/v1/policy/geographies/${geoid}/children?limit=${limit}`);
  },

  // ── Comparison ────────────────────────────────────────────────────────────

  async compare(geoid1, geoid2) {
    return this._post('/v1/policy/compare', { geoid1, geoid2 });
  },

  // ── Variable Metadata ─────────────────────────────────────────────────────

  // Returns an object keyed by variable_id. Cached after first fetch.
  async variables() {
    if (!this._varMeta) {
      const d = await this._get('/v1/policy/variables');
      this._varMeta = {};
      for (const v of (d.variables || [])) {
        this._varMeta[v.id] = v;
      }
    }
    return this._varMeta;
  },

  // ── Analyses ──────────────────────────────────────────────────────────────

  async analyses() {
    return this._get('/v1/policy/analyses');
  },

  async analysis(id) {
    return this._get(`/v1/policy/analyses/${id}`);
  },

  async analysisScores(id) {
    return this._get(`/v1/policy/analyses/${id}/scores`);
  },

  // ── Aggregate Queries ─────────────────────────────────────────────────────

  // fn: avg | min | max | stddev | count
  async aggregate(variableId, level, fn, stateFips = '55') {
    return this._post('/v1/policy/aggregate', {
      variable_id: variableId,
      level,
      function: fn,
      state_fips: stateFips
    });
  },

  // ── Policies ──────────────────────────────────────────────────────────────

  // Returns policies array (cached). Falls back to empty array on error.
  async policies() {
    if (!this._policies) {
      try {
        const d = await this._get('/v1/policy/policies?limit=200');
        this._policies = d.policies || [];
      } catch (_) {
        this._policies = [];
      }
    }
    return this._policies;
  },

  // ── Sources ───────────────────────────────────────────────────────────────

  async sources() {
    return this._get('/v1/policy/sources');
  },

  // ── Tracts with Indicators ────────────────────────────────────────────────

  // Fetches all tract children of a county, then queries their indicators.
  async tractIndicators(parentGeoid, variableIds) {
    const children = await this.children(parentGeoid, 500);
    const geoids = (children.items || []).map(t => t.geoid);
    if (geoids.length === 0) return { items: [] };
    return this._post('/v1/policy/query', {
      level: 'tract',
      state_fips: parentGeoid.substring(0, 2),
      limit: 2000,
      variable_ids: variableIds
    });
  },

  // ── Query ─────────────────────────────────────────────────────────────────

  // Generic POST /query — for callers that need custom variable sets.
  async query(params) {
    return this._post('/v1/policy/query', params);
  },

  // ── Cache Control ─────────────────────────────────────────────────────────

  clearCache() {
    this._cache = {};
    this._varMeta = null;
    this._policies = null;
  }
};
