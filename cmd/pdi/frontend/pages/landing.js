// pages/landing.js — Hero landing page with aggregate stats.
document.addEventListener('alpine:init', () => {
  Alpine.data('landingPage', () => ({
    // Stats — populated from API if available, otherwise fall back to known values.
    countyCount: 72,
    tractCount: 1652,
    variableCount: 42,
    policyCount: 85,
    loading: false,
    error: null,

    // HTML template uses stats.counties, stats.tracts, etc.
    get stats() {
      return {
        counties: this.countyCount.toLocaleString(),
        tracts: this.tractCount.toLocaleString(),
        variables: this.variableCount.toLocaleString(),
        policies: this.policyCount.toLocaleString()
      };
    },

    async init() {
      this.loading = true;
      try {
        // Attempt to pull live aggregate counts; fall through gracefully on error.
        const [counties, analyses] = await Promise.allSettled([
          PDI.counties(),
          PDI.analyses()
        ]);

        if (counties.status === 'fulfilled') {
          const total = counties.value?.total ?? counties.value?.count;
          if (total) this.countyCount = total;
        }
        if (analyses.status === 'fulfilled') {
          // analyses count is visible in the about page; here we keep policy count.
        }
      } catch (_) {
        // Non-fatal — static fallback values remain.
      } finally {
        this.loading = false;
      }
    }
  }));
});
