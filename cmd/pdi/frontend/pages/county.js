// pages/county.js — Full county profile with grouped indicators, tracts, and wiki links.
document.addEventListener('alpine:init', () => {
  Alpine.data('countyProfile', () => ({
    geoid: null,
    county: null,
    children: [],
    tractCount: 0,
    relatedAnalyses: [],
    allPolicies: [],
    loading: false,
    error: null,

    async init() {
      // $root is the appRouter component — watch its geoid.
      this.$watch('$root.geoid', async (val) => {
        if (val && this.$root.page === 'county') await this.load(val);
      });
      // Initial load if geoid is already set.
      if (this.$root.geoid) await this.load(this.$root.geoid);
    },

    async load(geoid) {
      if (!geoid || geoid === this.geoid) return;
      this.geoid = geoid;
      this.loading = true;
      this.error = null;
      this.county = null;
      this.children = [];
      this.relatedAnalyses = [];

      try {
        const [geo, childResult, policies, analyses] = await Promise.allSettled([
          PDI.geography(geoid),
          PDI.children(geoid),
          PDI.policies(),
          PDI.analyses()
        ]);

        if (geo.status === 'fulfilled') {
          this.county = geo.value;
        } else {
          this.error = geo.reason?.message || 'Failed to load county data.';
        }

        if (childResult.status === 'fulfilled') {
          this.children = childResult.value.items || [];
          this.tractCount = childResult.value.total ?? this.children.length;
        }

        if (policies.status === 'fulfilled') {
          this.allPolicies = policies.value;
        }

        if (analyses.status === 'fulfilled') {
          const all = analyses.value.analyses || [];
          // Keep analyses scoped to this county or state-wide.
          this.relatedAnalyses = all.filter(a =>
            !a.scope_geoid || a.scope_geoid === geoid || a.scope_geoid === '55'
          ).slice(0, 3);
        }
      } catch (err) {
        this.error = err.message;
      } finally {
        this.loading = false;
      }
    },

    // ── Computed getters ───────────────────────────────────────────────────

    get grouped() {
      if (!this.county) return [];
      return Domain.groupIndicators(this.county.indicators ?? []);
    },

    get relatedPolicies() {
      if (!this.county) return [];
      return Domain.findRelatedPolicies(this.county.indicators ?? [], this.allPolicies).slice(0, 6);
    },

    // ── Display helpers ────────────────────────────────────────────────────

    barWidth(ind) {
      const indicators = this.county?.indicators ?? [];
      return Domain.barWidth(ind, indicators);
    },

    barColor(ind) {
      return Domain.barColor(ind);
    },

    fmtValue(ind) {
      if (!ind || ind.value == null) return '\u2014';
      return Domain.fmtValue(ind.value, ind.unit);
    },

    tierClass(ind) {
      return Domain.tierClass(ind?.percentile);
    },

    tierLabel(ind) {
      return Domain.tierLabel(ind?.percentile);
    },

    fmtDim(dim) {
      return Domain.fmtDimension(dim);
    },

    tractsUrl() {
      return `#/county/${this.geoid}/tracts`;
    }
  }));
});
