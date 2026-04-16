document.addEventListener('alpine:init', () => {
  Alpine.data('countyExplorer', () => ({
    counties: [], search: '', loading: true, error: null,
    async init() {
      try {
        const r = await fetch('/v1/policy/geographies?level=county&state_fips=55&limit=100');
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const d = await r.json();
        this.counties = d.items || [];
      } catch (e) { this.error = e.message; }
      finally { this.loading = false; }
    },
    get filtered() {
      if (!this.search) return this.counties;
      const q = this.search.toLowerCase();
      return this.counties.filter(c => c.name.toLowerCase().includes(q));
    },
    fmtInd(county, varId, fmt) {
      const ind = (county.indicators || []).find(i => i.variable_id === varId);
      if (!ind || ind.value == null) return '\u2014';
      if (fmt === 'pct') return ind.value.toFixed(1) + '%';
      if (fmt === 'dollar') return '$' + Math.round(ind.value).toLocaleString();
      return Math.round(ind.value).toLocaleString();
    }
  }));
});
