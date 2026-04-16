// frontend/pages/counties.js — County Explorer
// Loaded as a static script tag inside the Alpine.js `router()` component.
// Registers the `countyExplorer` Alpine component, then injects its template
// into the x-ref="countyExplorer" div that index.html provides.

document.addEventListener('alpine:init', () => {
  Alpine.data('countyExplorer', () => ({
    counties: [],
    search: '',
    loading: true,
    error: null,

    async init() {
      try {
        const resp = await fetch('/v1/policy/geographies?level=county&state_fips=55&limit=100');
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        this.counties = data.items || [];
      } catch (e) {
        this.error = e.message;
      } finally {
        this.loading = false;
      }
    },

    get filtered() {
      if (!this.search) return this.counties;
      const q = this.search.toLowerCase();
      return this.counties.filter(c => c.name.toLowerCase().includes(q));
    },

    // Pull a named indicator out of the indicators array (may be absent)
    indicator(county, variable_id) {
      if (!county.indicators) return null;
      return county.indicators.find(i => i.variable_id === variable_id) || null;
    },

    fmt(n) {
      return (n != null && n !== '') ? Number(n).toLocaleString() : '—';
    },
    fmtPct(v) {
      return (v != null) ? Number(v).toFixed(1) + '%' : '—';
    },
    fmtDollars(v) {
      return (v != null) ? '$' + Math.round(Number(v)).toLocaleString() : '—';
    },

    navigate(geoid) {
      window.location.hash = '#/county/' + geoid;
    }
  }));
});

// Inject the template into the placeholder div once the DOM is ready.
// Alpine has not scanned this element yet, so we set x-data and innerHTML
// before Alpine processes it.  If Alpine has already initialised (the script
// tag runs synchronously during parse, before alpine:init fires on DOMContentLoaded
// in some configurations), we fall back to a MutationObserver guard.
(function injectCountyExplorer() {
  function inject() {
    const el = document.querySelector('[x-ref="countyExplorer"]');
    if (!el) return;

    el.setAttribute('x-data', 'countyExplorer()');

    el.innerHTML = `
      <!-- Page header -->
      <div style="margin-bottom:1.5rem;">
        <h1 style="font-size:1.75rem;font-weight:700;color:var(--navy);margin-bottom:0.25rem;">
          Wisconsin County Explorer
        </h1>
        <p style="color:var(--text-secondary);font-size:0.95rem;">
          Live county-level indicators for all 72 counties.
        </p>
      </div>

      <!-- Toolbar: count + search -->
      <div style="display:flex;align-items:center;justify-content:space-between;gap:1rem;margin-bottom:1.25rem;flex-wrap:wrap;">
        <span
          x-show="!loading && !error"
          class="badge badge-teal"
          style="font-size:0.85rem;padding:0.3rem 0.75rem;"
        >
          <span x-text="filtered.length"></span>
          <span x-text="filtered.length === 1 ? ' county' : ' counties'"></span>
          <template x-if="search">
            <span> &mdash; filtered from <span x-text="counties.length"></span></span>
          </template>
        </span>

        <input
          class="input"
          type="search"
          placeholder="Search counties…"
          x-model="search"
          x-show="!loading && !error"
          style="max-width:280px;"
          aria-label="Search counties by name"
        >
      </div>

      <!-- Loading state -->
      <div x-show="loading" class="loading">
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
          style="animation:spin 1s linear infinite;margin-right:0.5rem;">
          <circle cx="12" cy="12" r="10" stroke-opacity="0.25"/>
          <path d="M12 2a10 10 0 0 1 10 10" stroke-linecap="round"/>
        </svg>
        Loading counties…
      </div>

      <!-- Error state -->
      <div x-show="error && !loading" class="card" style="border-color:var(--red);color:var(--red);padding:1.25rem;">
        <strong>Failed to load counties:</strong>
        <span x-text="error"></span>
      </div>

      <!-- Empty search state -->
      <div
        x-show="!loading && !error && filtered.length === 0"
        class="empty"
      >
        No counties match "<span x-text="search"></span>".
      </div>

      <!-- County grid -->
      <div x-show="!loading && !error && filtered.length > 0" class="grid-3">
        <template x-for="county in filtered" :key="county.geoid">
          <div
            class="card"
            style="cursor:pointer;transition:box-shadow 0.15s,transform 0.15s;"
            onmouseover="this.style.boxShadow='var(--shadow-md)';this.style.transform='translateY(-2px)'"
            onmouseout="this.style.boxShadow='';this.style.transform=''"
            @click="navigate(county.geoid)"
            role="link"
            :aria-label="county.name + ' county profile'"
            tabindex="0"
            @keydown.enter="navigate(county.geoid)"
            @keydown.space.prevent="navigate(county.geoid)"
          >
            <!-- Card header -->
            <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:1rem;">
              <div>
                <div class="card-header" style="margin-bottom:0.15rem;" x-text="county.name"></div>
                <div style="font-size:0.75rem;color:var(--text-secondary);" x-text="'GEOID: ' + county.geoid"></div>
              </div>
              <span class="badge badge-teal" style="flex-shrink:0;margin-top:0.2rem;">WI</span>
            </div>

            <!-- Population stat -->
            <div style="margin-bottom:1rem;padding-bottom:0.875rem;border-bottom:1px solid var(--border);">
              <div class="stat-value" style="font-size:1.4rem;" x-text="fmt(county.population)"></div>
              <div class="stat-label">Population</div>
            </div>

            <!-- 3 key indicators -->
            <div style="display:flex;flex-direction:column;gap:0.5rem;">

              <!-- Poverty rate -->
              <div style="display:flex;justify-content:space-between;align-items:center;">
                <span style="font-size:0.85rem;color:var(--text-secondary);">Poverty rate</span>
                <span
                  style="font-size:0.9rem;font-weight:600;"
                  x-text="indicator(county, 'poverty_rate') ? fmtPct(indicator(county, 'poverty_rate').value) : '—'"
                ></span>
              </div>

              <!-- Median household income -->
              <div style="display:flex;justify-content:space-between;align-items:center;">
                <span style="font-size:0.85rem;color:var(--text-secondary);">Median income</span>
                <span
                  style="font-size:0.9rem;font-weight:600;"
                  x-text="indicator(county, 'median_household_income') ? fmtDollars(indicator(county, 'median_household_income').value) : '—'"
                ></span>
              </div>

              <!-- Uninsured rate -->
              <div style="display:flex;justify-content:space-between;align-items:center;">
                <span style="font-size:0.85rem;color:var(--text-secondary);">Uninsured</span>
                <span
                  style="font-size:0.9rem;font-weight:600;"
                  x-text="indicator(county, 'uninsured_rate') ? fmtPct(indicator(county, 'uninsured_rate').value) : '—'"
                ></span>
              </div>

            </div>

            <!-- "View profile" hint -->
            <div style="margin-top:1rem;padding-top:0.75rem;border-top:1px solid var(--border);display:flex;align-items:center;gap:0.25rem;color:var(--teal);font-size:0.82rem;font-weight:600;">
              View full profile
              <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
                <path d="M3 8h10M9 4l4 4-4 4"/>
              </svg>
            </div>

          </div>
        </template>
      </div>

      <!-- Inline spinner keyframes (only injected once) -->
      <style>
        @keyframes spin { to { transform: rotate(360deg); } }
      </style>
    `;
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', inject);
  } else {
    inject();
  }
})();
