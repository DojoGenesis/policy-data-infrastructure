// frontend/pages/profile.js — county profile dashboard
// Loaded by Alpine.js parent router when page === 'county'

document.addEventListener('alpine:init', () => {
  Alpine.data('countyProfile', () => ({
    county: null,
    indicators: [],
    tractCount: 0,
    loading: true,
    error: null,

    async init() {
      // Watch for route changes driven by the parent router
      this.$watch('$root.geoid', (val) => {
        if (val && this.$root.page === 'county') this.load(val);
      });
      // Initial load if already on this page
      if (this.$root.geoid && this.$root.page === 'county') {
        this.load(this.$root.geoid);
      }
    },

    async load(geoid) {
      this.loading = true;
      this.error = null;
      this.county = null;
      this.indicators = [];
      this.tractCount = 0;
      try {
        const resp = await fetch(`/v1/policy/geographies/${geoid}`);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        this.county = data;
        this.indicators = data.indicators || [];

        // Get child tract count (limit=1 to minimise payload)
        try {
          const childResp = await fetch(`/v1/policy/geographies/${geoid}/children?limit=1`);
          if (childResp.ok) {
            const childData = await childResp.json();
            this.tractCount = childData.total || 0;
          }
        } catch (_) {
          // Non-fatal — tract count is supplemental
        }
      } catch (e) {
        this.error = e.message;
      } finally {
        this.loading = false;
      }
    },

    // ── Computed ─────────────────────────────────────────────────────────────

    get grouped() {
      const groups = {};
      for (const ind of this.indicators) {
        const key = this.sourceGroup(ind.variable_id);
        if (!groups[key]) groups[key] = { name: key, items: [] };
        groups[key].items.push(ind);
      }
      // Sort items within each group alphabetically by indicator name
      for (const g of Object.values(groups)) {
        g.items.sort((a, b) => a.name.localeCompare(b.name));
      }
      return Object.values(groups);
    },

    get formattedPopulation() {
      if (!this.county || this.county.population == null) return '—';
      return Number(this.county.population).toLocaleString();
    },

    // ── Helpers ───────────────────────────────────────────────────────────────

    sourceGroup(varId) {
      if (varId.startsWith('cdc_')) return 'Health Outcomes (CDC PLACES)';
      if (varId.startsWith('usda_')) return 'Food Access (USDA)';
      if (
        varId.startsWith('owner_') ||
        varId.startsWith('renter_') ||
        varId.startsWith('housing_')
      ) return 'Housing (ACS)';
      return 'Demographics (Census ACS)';
    },

    // Max value across all indicators in the same source group (for dollar/count scaling)
    groupMax(ind) {
      const group = this.sourceGroup(ind.variable_id);
      const peers = this.indicators.filter(
        (i) => this.sourceGroup(i.variable_id) === group && i.value != null
      );
      if (peers.length === 0) return 1;
      return Math.max(...peers.map((i) => i.value), 1);
    },

    barWidth(ind) {
      if (ind.value == null) return '0%';
      if (ind.unit === 'percent') return Math.min(ind.value, 100).toFixed(1) + '%';
      if (ind.unit === 'count') return '0%'; // counts shown as number, no bar
      // dollars and any other numeric: scale relative to group max
      const pct = (ind.value / this.groupMax(ind)) * 100;
      return Math.min(pct, 100).toFixed(1) + '%';
    },

    barColor(ind) {
      if (ind.direction === 'lower_better') return 'bar-fill-amber';
      if (ind.direction === 'higher_better') return 'bar-fill-teal';
      return 'bar-fill-teal';
    },

    showBar(ind) {
      return ind.unit !== 'count';
    },

    fmtValue(ind) {
      if (ind.value == null) return '—';
      if (ind.unit === 'percent') return ind.value.toFixed(1) + '%';
      if (ind.unit === 'dollars') return '$' + Math.round(ind.value).toLocaleString();
      return Math.round(ind.value).toLocaleString();
    },

    fmtMOE(ind) {
      if (ind.margin_of_error == null) return null;
      if (ind.unit === 'percent') return '±' + ind.margin_of_error.toFixed(1) + '%';
      if (ind.unit === 'dollars') return '±$' + Math.round(ind.margin_of_error).toLocaleString();
      return '±' + Math.round(ind.margin_of_error).toLocaleString();
    },
  }));
});

// Inject the template into the countyProfile ref container and initialise Alpine
(function mountCountyProfile() {
  function tryMount() {
    // $root on body is the router(); find the ref from there
    const body = document.body;
    const rootData = body._x_dataStack && body._x_dataStack[0];
    const container = rootData && rootData.$refs && rootData.$refs.countyProfile;
    if (!container) return; // not ready yet

    if (container.hasAttribute('x-data')) return; // already mounted

    container.innerHTML = `
<div x-data="countyProfile()" x-init="init()" style="width:100%">

  <!-- Loading state -->
  <div x-show="loading" class="loading">Loading county profile&hellip;</div>

  <!-- Error state -->
  <div x-show="!loading && error" class="card" style="border-color:#fca5a5;background:#fef2f2;margin-top:1rem">
    <p style="color:#b91c1c;font-weight:600">Error loading county</p>
    <p x-text="error" style="color:#64748b;font-size:0.9rem;margin-top:0.25rem"></p>
    <a href="#/" class="btn btn-outline" style="margin-top:1rem">Back to Counties</a>
  </div>

  <!-- Profile content -->
  <div x-show="!loading && !error && county">

    <!-- Back link + title row -->
    <div style="display:flex;align-items:center;gap:1rem;margin-bottom:1.5rem;flex-wrap:wrap">
      <a href="#/" class="btn btn-outline btn-sm">← Back to Counties</a>
      <h1 x-text="county && county.name" style="font-size:1.6rem;font-weight:700;color:#1a2744;margin:0"></h1>
    </div>

    <!-- Top stats bar -->
    <div class="card" style="margin-bottom:1.5rem">
      <div style="display:flex;gap:3rem;flex-wrap:wrap;align-items:center">

        <div class="stat">
          <div class="stat-value" x-text="formattedPopulation"></div>
          <div class="stat-label">Total Population</div>
        </div>

        <template x-if="tractCount > 0">
          <div class="stat">
            <div class="stat-value" x-text="tractCount.toLocaleString()"></div>
            <div class="stat-label">Census Tracts</div>
          </div>
        </template>

        <template x-if="county && county.geoid">
          <div class="stat">
            <div class="stat-value" style="font-size:1.25rem" x-text="county.geoid"></div>
            <div class="stat-label">FIPS Code</div>
          </div>
        </template>

        <template x-if="indicators.length > 0">
          <div class="stat">
            <div class="stat-value" x-text="indicators.length"></div>
            <div class="stat-label">Indicators</div>
          </div>
        </template>

      </div>
    </div>

    <!-- Empty indicators notice -->
    <div x-show="indicators.length === 0" class="empty">
      <p>No indicator data available for this county.</p>
    </div>

    <!-- Indicator groups -->
    <template x-for="group in grouped" :key="group.name">
      <div class="card" style="margin-bottom:1.25rem">

        <!-- Group header -->
        <div style="display:flex;align-items:center;gap:0.75rem;margin-bottom:1.25rem">
          <h2 x-text="group.name" style="font-size:1.05rem;font-weight:700;color:#1a2744;margin:0"></h2>
          <span class="badge badge-teal" x-text="group.items.length + ' indicators'"></span>
        </div>

        <!-- Bar chart rows -->
        <div class="bar-chart">
          <template x-for="ind in group.items" :key="ind.variable_id">
            <div>

              <!-- Indicators with bars (percent / dollars) -->
              <template x-if="showBar(ind)">
                <div class="bar-row">
                  <div class="bar-label" :title="ind.name" x-text="ind.name"></div>
                  <div class="bar-track">
                    <div
                      class="bar-fill"
                      :class="barColor(ind)"
                      :style="'width:' + barWidth(ind)"
                      x-text="fmtValue(ind)"
                    ></div>
                  </div>
                  <template x-if="fmtMOE(ind)">
                    <div style="font-size:0.75rem;color:#94a3b8;flex-shrink:0;min-width:60px" x-text="fmtMOE(ind)"></div>
                  </template>
                </div>
              </template>

              <!-- Count indicators: display as number row, no bar -->
              <template x-if="!showBar(ind)">
                <div style="display:flex;align-items:center;gap:0.75rem;padding:0.3rem 0;border-bottom:1px solid #f1f5f9">
                  <div style="width:160px;font-size:0.85rem;text-align:right;flex-shrink:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#475569" :title="ind.name" x-text="ind.name"></div>
                  <div style="font-size:0.9rem;font-weight:600;color:#1a2744" x-text="fmtValue(ind)"></div>
                  <template x-if="ind.vintage">
                    <span style="font-size:0.75rem;color:#94a3b8" x-text="ind.vintage"></span>
                  </template>
                </div>
              </template>

            </div>
          </template>
        </div>

      </div>
    </template>

  </div><!-- /profile content -->
</div>
    `;

    // Let Alpine initialise the newly injected subtree
    if (window.Alpine && Alpine.initTree) {
      Alpine.initTree(container);
    }
  }

  // Try immediately (Alpine may already be initialised) and retry on DOMContentLoaded
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', tryMount);
  } else {
    tryMount();
  }
  // Also try after a tick in case Alpine refs aren't ready yet
  requestAnimationFrame(tryMount);
})();
