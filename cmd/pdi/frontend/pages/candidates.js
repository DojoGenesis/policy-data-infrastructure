// pages/candidates.js — Policy tracker with dual candidate + category filter.
document.addEventListener('alpine:init', () => {
  Alpine.data('candidateTracker', () => ({
    policies: [],
    activeCandidate: 'all',
    activeCat: 'all',
    loading: false,
    error: null,

    // Hardcoded fallback used if API is unavailable.
    _fallback: [
      { id: 'hk-001', candidate: 'Hong', candidate_full: 'Sen. Melissa Agard (placeholder)', equity_dimension: 'housing_affordability', category: 'Housing', title: 'Expand Renter Protections', summary: 'Establish just-cause eviction standards and limit rent increases to CPI + 5% statewide.', geoid: null },
      { id: 'hk-002', candidate: 'Hong', candidate_full: 'Sen. Melissa Agard (placeholder)', equity_dimension: 'health_access', category: 'Health', title: 'Universal Primary Care Access', summary: 'Expand Medicaid eligibility to 200% FPL and fund rural FQHC expansion.', geoid: null },
      { id: 'hk-003', candidate: 'Hong', candidate_full: 'Sen. Melissa Agard (placeholder)', equity_dimension: 'food_access', category: 'Food Access', title: 'Double SNAP Benefit Match at Wisconsin Farmers Markets', summary: 'Fund double-match program at 200+ certified markets statewide.', geoid: null },
      { id: 'mm-001', candidate: 'Mamdani', candidate_full: 'Zohran Mamdani (placeholder)', equity_dimension: 'economic_equity', category: 'Economic', title: 'Worker Ownership Fund', summary: 'Create a $50M revolving loan fund for employee buyouts of retiring business owners in distressed counties.', geoid: null },
      { id: 'mm-002', candidate: 'Mamdani', candidate_full: 'Zohran Mamdani (placeholder)', equity_dimension: 'income_equity', category: 'Economic', title: 'Raise Minimum Wage to $20', summary: 'Phase in $20/hr minimum wage over 3 years with annual CPI adjustments.', geoid: null },
      { id: 'mm-003', candidate: 'Mamdani', candidate_full: 'Zohran Mamdani (placeholder)', equity_dimension: 'environmental_health', category: 'Environment', title: 'Clean Air Fund for Disadvantaged Tracts', summary: 'Direct $25M annually to census tracts in top EPA EJScreen percentile for air quality.', geoid: null }
    ],

    // Unique candidate list derived from loaded policies.
    _candidates: [],

    // Category list derived from loaded policies.
    _categories: [],

    async init() {
      this.loading = true;
      try {
        const p = await PDI.policies();
        this.policies = p.length > 0 ? p : this._fallback;
      } catch (_) {
        this.policies = this._fallback;
      } finally {
        this.loading = false;
      }

      // Build unique candidate and category lists.
      const candSet = new Set();
      const catSet = new Set();
      for (const p of this.policies) {
        if (p.candidate) candSet.add(p.candidate);
        if (p.category) catSet.add(p.category);
      }
      this._candidates = Array.from(candSet).sort();
      this._categories = Array.from(catSet).sort();
    },

    get candidateList() {
      return [{ id: 'all', label: 'All Candidates' }, ...this._candidates.map(c => ({ id: c, label: c }))];
    },
    // HTML template uses candidateNames (string array)
    get candidateNames() { return this._candidates; },

    get categoryList() {
      return [{ id: 'all', label: 'All Categories' }, ...this._categories.map(c => ({ id: c, label: c }))];
    },
    // HTML template uses categories (string array)
    get categories() { return this._categories; },

    get filtered() {
      return this.policies.filter(p => {
        const matchCand = this.activeCandidate === 'all' || p.candidate === this.activeCandidate;
        const matchCat  = this.activeCat === 'all'       || p.category === this.activeCat;
        return matchCand && matchCat;
      });
    },

    setCandidate(c) { this.activeCandidate = c; },
    setCategory(c)  { this.activeCat = c; },

    fmtDim(dim) { return Domain.fmtDimension(dim); },

    // Link to counties filtered by this policy's equity dimension — currently navigates
    // to counties page; a future enhancement can pass a filter param via hash.
    countyLinkForPolicy(policy) {
      if (policy.geoid) return `#/county/${policy.geoid}`;
      return '#/counties';
    }
  }));
});
