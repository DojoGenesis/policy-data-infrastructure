document.addEventListener('alpine:init', () => {
  Alpine.data('compareTool', () => ({
    counties: [], geoid1: '', geoid2: '', result: null, comparing: false,
    async init() {
      try {
        const r = await fetch('/v1/policy/geographies?level=county&state_fips=55&limit=100');
        if (r.ok) { const d = await r.json(); this.counties = d.items || []; }
      } catch(e) { /* silent */ }
    },
    async compare() {
      if (!this.geoid1 || !this.geoid2) return;
      this.comparing = true; this.result = null;
      try {
        const r = await fetch('/v1/policy/compare', {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ geoid1: this.geoid1, geoid2: this.geoid2 })
        });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        this.result = await r.json();
      } catch(e) { alert('Compare failed: ' + e.message); }
      finally { this.comparing = false; }
    }
  }));
});
