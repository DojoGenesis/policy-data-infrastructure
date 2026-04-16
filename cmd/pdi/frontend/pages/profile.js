document.addEventListener('alpine:init', () => {
  Alpine.data('countyProfile', () => ({
    county: null, indicators: [], loading: true, error: null,
    init() {
      this.$watch('$root.geoid', v => { if (v && this.$root.page === 'county') this.load(v); });
      if (this.$root.geoid && this.$root.page === 'county') this.load(this.$root.geoid);
    },
    async load(geoid) {
      this.loading = true; this.error = null;
      try {
        const r = await fetch('/v1/policy/geographies/' + geoid);
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const d = await r.json();
        this.county = d;
        this.indicators = d.indicators || [];
      } catch (e) { this.error = e.message; }
      finally { this.loading = false; }
    },
    get grouped() {
      const g = {};
      for (const ind of this.indicators) {
        const k = this.sourceGroup(ind.variable_id);
        if (!g[k]) g[k] = { name: k, items: [] };
        g[k].items.push(ind);
      }
      return Object.values(g);
    },
    sourceGroup(v) {
      if (v.startsWith('cdc_')) return 'Health (CDC PLACES)';
      if (v.startsWith('usda_')) return 'Food Access (USDA)';
      if (v.startsWith('owner_') || v.startsWith('renter_') || v.startsWith('housing_')) return 'Housing (ACS)';
      return 'Demographics (Census ACS)';
    },
    barWidth(ind) {
      if (!ind.value) return '0%';
      if (ind.unit === 'percent') return Math.min(ind.value, 100) + '%';
      const max = Math.max(...this.indicators.filter(i => i.unit === ind.unit).map(i => i.value || 0));
      return max > 0 ? Math.min((ind.value / max) * 100, 100) + '%' : '0%';
    },
    fmtVal(ind) {
      if (ind.value == null) return '\u2014';
      if (ind.unit === 'percent') return ind.value.toFixed(1) + '%';
      if (ind.unit === 'dollars') return '$' + Math.round(ind.value).toLocaleString();
      return Math.round(ind.value).toLocaleString();
    }
  }));
});
