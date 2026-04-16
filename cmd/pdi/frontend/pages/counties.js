// pages/counties.js — Searchable county card grid with key indicators.
document.addEventListener('alpine:init', () => {
  Alpine.data('countyExplorer', () => ({
    counties: [],
    search: '',
    loading: false,
    error: null,

    async init() {
      this.loading = true;
      this.error = null;
      try {
        const result = await PDI.counties();
        this.counties = result.items || result.geographies || [];
      } catch (err) {
        this.error = err.message;
        this.counties = [];
      } finally {
        this.loading = false;
      }
    },

    get filtered() {
      if (!this.search.trim()) return this.counties;
      const q = this.search.toLowerCase();
      return this.counties.filter(c =>
        (c.name || '').toLowerCase().includes(q) ||
        (c.geoid || '').includes(q)
      );
    },

    // Generic indicator formatter — called from template as fmtInd(county, varId, format)
    fmtInd(county, varId, format) {
      return Domain.fmtInd(county, varId, format);
    },

    fmtPoverty(county) { return Domain.fmtInd(county, 'poverty_rate', 'pct'); },
    fmtIncome(county) { return Domain.fmtInd(county, 'median_household_income', 'dollar'); },
    fmtUninsured(county) { return Domain.fmtInd(county, 'uninsured_rate', 'pct'); },

    // Tier class driven by poverty_rate percentile if available, else raw value.
    tierClass(county) {
      const pov = Domain.indValue(county, 'poverty_rate');
      if (pov == null) return '';
      // Simple threshold: >15% poverty = critical, >10% = warning
      if (pov >= 15) return 'critical';
      if (pov >= 10) return 'warning';
      return '';
    },

    countyUrl(county) {
      return `#/county/${county.geoid}`;
    }
  }));
});
