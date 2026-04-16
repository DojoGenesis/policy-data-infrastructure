// pages/county.js — Full county profile with grouped indicators and wiki links.
document.addEventListener('alpine:init', () => {
  Alpine.data('countyProfile', () => ({
    county: null,
    indicators: [],
    tractCount: 0,
    tractScores: [],
    allPolicies: [],
    loading: false,
    error: null,
    _geoid: null,

    init() {
      // Read geoid from hash on load and on every hash change
      this._loadFromHash();
      window.addEventListener('hashchange', () => this._loadFromHash());
    },

    _loadFromHash() {
      const h = window.location.hash || '';
      if (h.startsWith('#/county/') && !h.includes('/tracts')) {
        const geoid = h.replace('#/county/', '');
        if (geoid && geoid !== this._geoid) {
          this.load(geoid);
        }
      }
    },

    async load(geoid) {
      this._geoid = geoid;
      this.loading = true;
      this.error = null;
      this.county = null;
      this.indicators = [];
      this.tractScores = [];

      try {
        const [geo, childResult, policies] = await Promise.allSettled([
          PDI.geography(geoid),
          PDI.children(geoid),
          PDI.policies()
        ]);

        if (geo.status === 'fulfilled') {
          this.county = geo.value;
          this.indicators = geo.value.indicators ?? [];
        } else {
          this.error = 'County not found';
        }

        if (childResult.status === 'fulfilled') {
          this.tractCount = childResult.value.total ?? (childResult.value.items?.length || 0);
        }

        if (policies.status === 'fulfilled') {
          this.allPolicies = policies.value || [];
        }
      } catch (err) {
        this.error = err.message;
      } finally {
        this.loading = false;
      }
    },

    get grouped() {
      return Domain.groupIndicators(this.indicators);
    },

    get relatedPolicies() {
      return Domain.findRelatedPolicies(this.indicators, this.allPolicies).slice(0, 6);
    },

    barWidth(ind) {
      return Domain.barWidth(ind, this.indicators);
    },

    barColor(ind) {
      return Domain.barColor(ind);
    },

    fmtVal(ind) {
      if (!ind || ind.value == null) return '\u2014';
      return Domain.fmtValue(ind.value, ind.unit);
    },

    fmtValue(ind) { return this.fmtVal(ind); },

    tierBadge(tier) {
      if (!tier) return 'badge-teal';
      const t = (tier + '').toLowerCase();
      if (t === 'very_high' || t === 'critical') return 'badge-red';
      if (t === 'high' || t === 'warning') return 'badge-amber';
      return 'badge-green';
    },

    fmtDim(dim) { return Domain.fmtDimension(dim); },
    tractsUrl() { return '#/county/' + this._geoid + '/tracts'; }
  }));
});
