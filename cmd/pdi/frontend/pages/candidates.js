// pages/candidates.js — Policy tracker with related county data per policy.
document.addEventListener('alpine:init', () => {
  Alpine.data('candidateTracker', () => ({
    policies: [],
    _counties: [],  // All counties with indicators for cross-referencing
    activeCandidate: 'all',
    activeCat: 'all',
    loading: false,
    error: null,
    expandedPolicy: null,  // ID of policy with expanded county list

    _fallback: [
      { id: 'fb-001', candidate: 'Francesca Hong', office: 'Governor', state: 'WI', equity_dimension: 'housing_affordability', category: 'Housing', title: 'Expand Renter Protections', description: 'Establish just-cause eviction standards and limit rent increases statewide.' },
      { id: 'fb-002', candidate: 'Francesca Hong', office: 'Governor', state: 'WI', equity_dimension: 'health_access', category: 'Health', title: 'Expand BadgerCare', description: 'Expand Medicaid eligibility and fund rural FQHC expansion.' },
      { id: 'fb-003', candidate: 'Zohran Mamdani', office: 'Mayor', state: 'NY', equity_dimension: 'income_equity', category: 'Economic', title: '$25 Minimum Wage', description: 'Phase in $25/hr minimum wage to match true cost of living.' }
    ],

    _candidates: [],
    _categories: [],

    async init() {
      this.loading = true;
      try {
        const [policyResult, countyResult] = await Promise.allSettled([
          PDI.policies(),
          PDI.counties()
        ]);

        if (policyResult.status === 'fulfilled') {
          const p = policyResult.value;
          this.policies = (Array.isArray(p) ? p : p.policies || p.items || []);
          if (this.policies.length === 0) this.policies = this._fallback;
        } else {
          this.policies = this._fallback;
        }

        if (countyResult.status === 'fulfilled') {
          this._counties = countyResult.value.items || [];
        }
      } catch (_) {
        this.policies = this._fallback;
      } finally {
        this.loading = false;
      }

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
    get candidateNames() { return this._candidates; },

    get categoryList() {
      return [{ id: 'all', label: 'All Categories' }, ...this._categories.map(c => ({ id: c, label: c }))];
    },
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

    toggleExpand(policyId) {
      this.expandedPolicy = this.expandedPolicy === policyId ? null : policyId;
    },

    // Find the 5 counties most affected by this policy's equity dimension
    relatedCounties(policy) {
      if (!policy.equity_dimension || this._counties.length === 0) return [];

      const dim = policy.equity_dimension;
      // Map equity dimensions to the indicator we sort by (worst = highest need)
      const dimToIndicator = {
        'housing_affordability': 'poverty_rate',
        'housing_stability': 'poverty_rate',
        'housing_supply': 'poverty_rate',
        'health_access': 'uninsured_rate',
        'health_equity': 'uninsured_rate',
        'food_access': 'poverty_rate',
        'income_equity': 'poverty_rate',
        'economic_equity': 'poverty_rate',
        'economic_opportunity': 'poverty_rate',
        'education_funding': 'poverty_rate',
        'education_equity': 'poverty_rate',
        'education_access': 'poverty_rate',
        'environmental_health': 'poverty_rate',
        'environmental_justice': 'poverty_rate',
        'climate_equity': 'poverty_rate',
        'transit_access': 'poverty_rate',
        'rural_equity': 'poverty_rate',
        'labor_rights': 'poverty_rate',
        'labor_mobility': 'poverty_rate',
        'childcare_access': 'poverty_rate',
        'care_access': 'uninsured_rate',
        'civil_rights': 'poverty_rate',
        'criminal_justice': 'poverty_rate',
        'fiscal_equity': 'poverty_rate',
        'community_safety': 'poverty_rate',
        'immigration_rights': 'poverty_rate',
        'reproductive_rights': 'uninsured_rate',
        'land_preservation': 'poverty_rate'
      };

      const sortVar = dimToIndicator[dim] || 'poverty_rate';

      return this._counties
        .map(c => ({
          name: c.name,
          geoid: c.geoid,
          value: Domain.indValue(c, sortVar),
          income: Domain.indValue(c, 'median_household_income'),
          indicator: sortVar
        }))
        .filter(c => c.value != null)
        .sort((a, b) => b.value - a.value)  // Highest need first
        .slice(0, 5);
    },

    fmtCountyVal(county) {
      if (county.indicator === 'uninsured_rate') return county.value.toFixed(1) + '% uninsured';
      return county.value.toFixed(1) + '% poverty';
    }
  }));
});
