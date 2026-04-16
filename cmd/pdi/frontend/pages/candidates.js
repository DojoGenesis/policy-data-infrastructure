// frontend/pages/candidates.js — Candidate Policy Tracker
// Loaded as a static script tag inside the Alpine.js `router()` component.
// Registers the `candidateTracker` Alpine component, then injects its template
// into the x-ref="candidateTracker" div that index.html provides.

const fallbackPolicies = [
  { id: 'FH-HSG-001', candidate: 'Francesca Hong', office: 'Governor', state: 'WI', category: 'Housing', title: 'Expand affordable housing', description: 'State-funded affordable housing programs; support community land trusts and ADUs', equity_dimension: 'housing_affordability' },
  { id: 'FH-HEALTH-001', candidate: 'Francesca Hong', office: 'Governor', state: 'WI', category: 'Healthcare', title: 'Expand BadgerCare', description: 'Expand Medicaid and create a public health insurance option to lower premiums and stabilize rural hospitals', equity_dimension: 'health_access' },
  { id: 'FH-EDU-001', candidate: 'Francesca Hong', office: 'Governor', state: 'WI', category: 'Schools', title: 'Free school meals', description: 'State-funded free healthy school meals for all children; supports WI farmers', equity_dimension: 'food_access' },
  { id: 'FH-ENV-001', candidate: 'Francesca Hong', office: 'Governor', state: 'WI', category: 'Environment', title: 'Climate Accountability Act', description: 'Cut greenhouse gas emissions by half by 2030; net-zero by 2050', equity_dimension: 'climate_equity' },
  { id: 'FH-LABOR-001', candidate: 'Francesca Hong', office: 'Governor', state: 'WI', category: 'Labor', title: 'Paid family leave', description: 'Create statewide insurance program covering all workers including the self-employed', equity_dimension: 'labor_rights' },
  { id: 'ZM-HSG-001', candidate: 'Zohran Mamdani', office: 'Mayor', state: 'NY', category: 'Housing', title: 'Rent freeze for stabilized tenants', description: 'Freeze rents for two million rent-stabilized tenants to prevent displacement', equity_dimension: 'housing_affordability' },
  { id: 'ZM-HSG-002', candidate: 'Zohran Mamdani', office: 'Mayor', state: 'NY', category: 'Housing', title: '200K affordable housing units', description: 'Build 200,000 affordable units through non-profit developers and community land trusts', equity_dimension: 'housing_supply' },
  { id: 'ZM-TRANSIT-001', candidate: 'Zohran Mamdani', office: 'Mayor', state: 'NY', category: 'Transit', title: 'Free city buses', description: 'Eliminate bus fares; speed up service to reduce commute times for low-income workers', equity_dimension: 'transit_access' },
  { id: 'ZM-HEALTH-001', candidate: 'Zohran Mamdani', office: 'Mayor', state: 'NY', category: 'Healthcare', title: 'Universal free childcare', description: 'Expand free and low-cost childcare citywide; goal of universal access', equity_dimension: 'childcare_access' },
  { id: 'ZM-EQUITY-001', candidate: 'Zohran Mamdani', office: 'Mayor', state: 'NY', category: 'Racial Equity', title: 'Citywide racial equity plan', description: 'True cost of living measure showing 62% cannot meet basic needs; address racial wealth gap', equity_dimension: 'civil_rights' },
  { id: 'ZM-ENV-001', candidate: 'Zohran Mamdani', office: 'Mayor', state: 'NY', category: 'Environment', title: 'All-Electric Buildings Act', description: 'Transition to all-electric buildings; oppose gas-fired power plant expansion', equity_dimension: 'environmental_health' },
  { id: 'ZM-LABOR-001', candidate: 'Zohran Mamdani', office: 'Mayor', state: 'NY', category: 'Labor', title: '$25 minimum wage', description: 'Living wage increase to match true cost of living in urban areas', equity_dimension: 'income_equity' }
];

document.addEventListener('alpine:init', () => {
  Alpine.data('candidateTracker', () => ({
    policies: [],
    loading: true,
    selectedCandidate: 'All',
    selectedCategory: 'All',

    async init() {
      try {
        const resp = await fetch('/v1/policy/policies?limit=200');
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        this.policies = data.policies || [];
      } catch (_) {
        // Degrade silently to hardcoded fallback
        this.policies = fallbackPolicies;
      } finally {
        this.loading = false;
      }
    },

    get candidates() {
      const seen = new Set();
      const result = [];
      for (const p of this.policies) {
        if (!seen.has(p.candidate)) {
          seen.add(p.candidate);
          result.push(p.candidate);
        }
      }
      return result;
    },

    get categories() {
      const seen = new Set();
      const result = [];
      for (const p of this.policies) {
        if (!seen.has(p.category)) {
          seen.add(p.category);
          result.push(p.category);
        }
      }
      return result.sort();
    },

    get candidateSummaries() {
      const map = {};
      for (const p of this.policies) {
        if (!map[p.candidate]) {
          map[p.candidate] = { candidate: p.candidate, office: p.office, state: p.state, count: 0 };
        }
        map[p.candidate].count++;
      }
      return Object.values(map);
    },

    get filtered() {
      return this.policies.filter(p => {
        const matchCandidate = this.selectedCandidate === 'All' || p.candidate === this.selectedCandidate;
        const matchCategory = this.selectedCategory === 'All' || p.category === this.selectedCategory;
        return matchCandidate && matchCategory;
      });
    },

    fmtDimension(dim) {
      if (!dim) return '';
      return dim.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    }
  }));
});

(function injectCandidateTracker() {
  function inject() {
    const el = document.querySelector('[x-ref="candidateTracker"]');
    if (!el) return;

    el.setAttribute('x-data', 'candidateTracker()');

    el.innerHTML = `
      <!-- Page header -->
      <div style="margin-bottom:1.5rem;">
        <h1 style="font-size:1.75rem;font-weight:700;color:var(--navy);margin-bottom:0.25rem;">
          Candidate Policy Tracker
        </h1>
        <p style="color:var(--text-secondary);font-size:0.95rem;" x-show="!loading">
          <span x-text="policies.length"></span> policies across
          <span x-text="candidates.length"></span>
          <span x-text="candidates.length === 1 ? 'candidate' : 'candidates'"></span>
        </p>
      </div>

      <!-- Loading state -->
      <div x-show="loading" class="loading">
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
          style="animation:spin 1s linear infinite;margin-right:0.5rem;">
          <circle cx="12" cy="12" r="10" stroke-opacity="0.25"/>
          <path d="M12 2a10 10 0 0 1 10 10" stroke-linecap="round"/>
        </svg>
        Loading policies…
      </div>

      <!-- Candidate summary cards -->
      <div x-show="!loading" style="display:flex;flex-wrap:wrap;gap:1rem;margin-bottom:1.75rem;">
        <template x-for="s in candidateSummaries" :key="s.candidate">
          <div
            class="card"
            style="flex:1;min-width:200px;max-width:320px;cursor:pointer;transition:box-shadow 0.15s,transform 0.15s;border:2px solid transparent;"
            :style="selectedCandidate === s.candidate ? 'border-color:var(--teal);' : ''"
            onmouseover="this.style.boxShadow='var(--shadow-md)';this.style.transform='translateY(-2px)'"
            onmouseout="this.style.boxShadow='';this.style.transform=''"
            @click="selectedCandidate = (selectedCandidate === s.candidate ? 'All' : s.candidate); selectedCategory = 'All'"
            role="button"
            tabindex="0"
            :aria-label="'Filter by ' + s.candidate"
            :aria-pressed="selectedCandidate === s.candidate"
            @keydown.enter="selectedCandidate = (selectedCandidate === s.candidate ? 'All' : s.candidate); selectedCategory = 'All'"
            @keydown.space.prevent="selectedCandidate = (selectedCandidate === s.candidate ? 'All' : s.candidate); selectedCategory = 'All'"
          >
            <div class="card-header" style="margin-bottom:0.35rem;" x-text="s.candidate"></div>
            <div style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap;">
              <span class="badge badge-teal" x-text="s.office + ' · ' + s.state"></span>
              <span style="font-size:0.82rem;color:var(--text-secondary);">
                <span x-text="s.count"></span> <span x-text="s.count === 1 ? 'policy' : 'policies'"></span>
              </span>
            </div>
          </div>
        </template>
      </div>

      <!-- Candidate filter row -->
      <div x-show="!loading" style="margin-bottom:0.75rem;">
        <div style="font-size:0.78rem;text-transform:uppercase;letter-spacing:0.06em;color:var(--text-secondary);margin-bottom:0.5rem;font-weight:600;">Candidate</div>
        <div style="display:flex;flex-wrap:wrap;gap:0.5rem;">
          <button
            class="badge"
            :class="selectedCandidate === 'All' ? 'badge-teal' : ''"
            style="cursor:pointer;border:none;font-size:0.82rem;padding:0.3rem 0.75rem;"
            :style="selectedCandidate !== 'All' ? 'background:var(--bg-secondary);color:var(--text-secondary);' : ''"
            @click="selectedCandidate = 'All'"
          >All</button>
          <template x-for="c in candidates" :key="c">
            <button
              class="badge"
              :class="selectedCandidate === c ? 'badge-teal' : ''"
              style="cursor:pointer;border:none;font-size:0.82rem;padding:0.3rem 0.75rem;"
              :style="selectedCandidate !== c ? 'background:var(--bg-secondary);color:var(--text-secondary);' : ''"
              @click="selectedCandidate = c"
              x-text="c"
            ></button>
          </template>
        </div>
      </div>

      <!-- Category filter row -->
      <div x-show="!loading" style="margin-bottom:1.5rem;">
        <div style="font-size:0.78rem;text-transform:uppercase;letter-spacing:0.06em;color:var(--text-secondary);margin-bottom:0.5rem;font-weight:600;">Category</div>
        <div style="display:flex;flex-wrap:wrap;gap:0.5rem;">
          <button
            class="badge"
            :class="selectedCategory === 'All' ? 'badge-amber' : ''"
            style="cursor:pointer;border:none;font-size:0.82rem;padding:0.3rem 0.75rem;"
            :style="selectedCategory !== 'All' ? 'background:var(--bg-secondary);color:var(--text-secondary);' : ''"
            @click="selectedCategory = 'All'"
          >All</button>
          <template x-for="cat in categories" :key="cat">
            <button
              class="badge"
              :class="selectedCategory === cat ? 'badge-amber' : ''"
              style="cursor:pointer;border:none;font-size:0.82rem;padding:0.3rem 0.75rem;"
              :style="selectedCategory !== cat ? 'background:var(--bg-secondary);color:var(--text-secondary);' : ''"
              @click="selectedCategory = cat"
              x-text="cat"
            ></button>
          </template>
        </div>
      </div>

      <!-- Results count -->
      <div x-show="!loading" style="margin-bottom:1rem;">
        <span class="badge badge-teal" style="font-size:0.85rem;padding:0.3rem 0.75rem;">
          <span x-text="filtered.length"></span>
          <span x-text="filtered.length === 1 ? ' policy' : ' policies'"></span>
          <template x-if="selectedCandidate !== 'All' || selectedCategory !== 'All'">
            <span> &mdash; filtered from <span x-text="policies.length"></span></span>
          </template>
        </span>
      </div>

      <!-- Empty state -->
      <div
        x-show="!loading && filtered.length === 0"
        class="empty"
      >
        No policies match the selected filters.
      </div>

      <!-- Policy cards grid -->
      <div x-show="!loading && filtered.length > 0" class="grid-3">
        <template x-for="policy in filtered" :key="policy.id">
          <div
            class="card"
            style="display:flex;flex-direction:column;gap:0.75rem;"
          >
            <!-- Candidate + office badge -->
            <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:0.5rem;flex-wrap:wrap;">
              <div style="font-size:0.9rem;font-weight:600;color:var(--navy);" x-text="policy.candidate"></div>
              <span class="badge badge-teal" style="flex-shrink:0;font-size:0.75rem;" x-text="policy.office + ' · ' + policy.state"></span>
            </div>

            <!-- Policy title -->
            <div style="font-size:1rem;font-weight:700;color:var(--text-primary);line-height:1.35;" x-text="policy.title"></div>

            <!-- Description -->
            <div style="font-size:0.875rem;color:var(--text-secondary);line-height:1.5;flex:1;" x-text="policy.description"></div>

            <!-- Badges row -->
            <div style="display:flex;flex-wrap:wrap;gap:0.4rem;padding-top:0.5rem;border-top:1px solid var(--border);">
              <span class="badge badge-teal" style="font-size:0.74rem;" x-text="fmtDimension(policy.equity_dimension)"></span>
              <span class="badge badge-amber" style="font-size:0.74rem;" x-text="policy.category"></span>
            </div>
          </div>
        </template>
      </div>

      <!-- Inline spinner keyframes -->
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
