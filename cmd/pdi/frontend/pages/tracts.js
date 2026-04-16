// pages/tracts.js — Tract-level explorer within a county.
document.addEventListener('alpine:init', () => {
  Alpine.data('tractExplorer', () => ({
    geoid: null,
    countyName: '',
    tracts: [],
    search: '',
    loading: false,
    error: null,

    // Key indicators to pull for each tract.
    _varIds: [
      'poverty_rate',
      'cdc_obesity',
      'cdc_diabetes',
      'cdc_mhlth',
      'usda_lila',
      'uninsured_rate'
    ],

    async init() {
      this.$watch('$root.geoid', async (val) => {
        if (val && this.$root.page === 'tracts') await this.load(val);
      });
      if (this.$root.geoid) await this.load(this.$root.geoid);
    },

    async load(geoid) {
      if (!geoid || geoid === this.geoid) return;
      this.geoid = geoid;
      this.loading = true;
      this.error = null;
      this.tracts = [];

      try {
        // Load county name and tract indicators in parallel.
        const [geoResult, tractResult] = await Promise.allSettled([
          PDI.geography(geoid),
          PDI.tractIndicators(geoid, this._varIds)
        ]);

        if (geoResult.status === 'fulfilled') {
          this.countyName = geoResult.value?.name || geoid;
        }

        if (tractResult.status === 'fulfilled') {
          this.tracts = tractResult.value.items || [];
        } else {
          // Fallback: just list tract geoids without indicators.
          const children = await PDI.children(geoid, 500);
          this.tracts = (children.items || []).map(t => ({ ...t, indicators: [] }));
        }
      } catch (err) {
        this.error = err.message;
      } finally {
        this.loading = false;
      }
    },

    get filtered() {
      if (!this.search.trim()) return this.tracts;
      const q = this.search.toLowerCase();
      return this.tracts.filter(t =>
        (t.name || '').toLowerCase().includes(q) ||
        (t.geoid || '').includes(q)
      );
    },

    fmtInd(tract, varId, format) {
      return Domain.fmtInd(tract, varId, format);
    },

    tierClass(tract) {
      const pov = Domain.indValue(tract, 'poverty_rate');
      if (pov == null) return '';
      if (pov >= 20) return 'critical';
      if (pov >= 12) return 'warning';
      return '';
    },

    tractLabel(tract) {
      return Domain.fmtGeoid(tract.geoid) || tract.name || tract.geoid;
    },

    countyUrl() {
      return `#/county/${this.geoid}`;
    }
  }));
});
