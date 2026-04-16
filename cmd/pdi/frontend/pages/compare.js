// pages/compare.js — Side-by-side county comparison tool.
document.addEventListener('alpine:init', () => {
  Alpine.data('compareTool', () => ({
    countyList: [],
    geoid1: '',
    geoid2: '',
    result: null,
    nameMap: {},   // variable_id → display name built from response
    loading: false,
    error: null,
    compared: false,

    async init() {
      try {
        const r = await PDI.counties();
        this.countyList = (r.items || r.geographies || [])
          .sort((a, b) => (a.name || '').localeCompare(b.name || ''));
      } catch (_) {
        this.countyList = [];
      }
    },

    async compare() {
      if (!this.geoid1 || !this.geoid2) return;
      if (this.geoid1 === this.geoid2) {
        this.error = 'Please select two different counties.';
        return;
      }
      this.loading = true;
      this.error = null;
      this.result = null;
      this.compared = false;

      try {
        const r = await PDI.compare(this.geoid1, this.geoid2);
        this.result = r;
        this.compared = true;

        // Build nameMap from geography1.indicators so diff rows have human names.
        this.nameMap = {};
        for (const ind of (r.geography1?.indicators ?? [])) {
          if (ind.variable_id && ind.name) {
            this.nameMap[ind.variable_id] = ind.name;
          }
        }
      } catch (err) {
        this.error = err.message;
      } finally {
        this.loading = false;
      }
    },

    get differences() {
      return this.result?.differences || [];
    },

    get name1() {
      return this.result?.geography1?.name || this.geoid1;
    },

    get name2() {
      return this.result?.geography2?.name || this.geoid2;
    },

    // Display name for a variable_id — prefers nameMap, then prettifies the id.
    displayName(varId) {
      if (this.nameMap[varId]) return this.nameMap[varId];
      return varId.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    },

    fmtVal1(diff) {
      const ind = (this.result?.geography1?.indicators ?? [])
        .find(i => i.variable_id === diff.variable_id);
      return Domain.fmtValue(ind?.value, ind?.unit || diff.unit);
    },

    fmtVal2(diff) {
      const ind = (this.result?.geography2?.indicators ?? [])
        .find(i => i.variable_id === diff.variable_id);
      return Domain.fmtValue(ind?.value, ind?.unit || diff.unit);
    },

    fmtDiff(diff) {
      if (diff.difference == null) return '\u2014';
      const sign = diff.difference > 0 ? '+' : '';
      const unit = diff.unit || '';
      if (unit === 'percent') return sign + diff.difference.toFixed(1) + '%';
      if (unit === 'dollars') return sign + '$' + Math.round(Math.abs(diff.difference)).toLocaleString();
      return sign + Math.round(diff.difference).toLocaleString();
    },

    diffClass(diff) {
      return Domain.diffClass(diff.difference, diff.direction);
    },

    // Alias for HTML template: fmtDiffVal(d, value)
    fmtDiffVal(diff, value) {
      if (value == null) return '\u2014';
      const ind = (this.result?.geography1?.indicators ?? [])
        .find(i => i.variable_id === diff.variable_id);
      return Domain.fmtValue(value, ind?.unit || diff.unit);
    },

    county1Url() { return this.geoid1 ? `#/county/${this.geoid1}` : '#/counties'; },
    county2Url() { return this.geoid2 ? `#/county/${this.geoid2}` : '#/counties'; }
  }));
});
