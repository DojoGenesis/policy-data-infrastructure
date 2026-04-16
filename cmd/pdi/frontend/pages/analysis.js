// pages/analysis.js — Analysis detail page (composite, correlation, OLS, tipping point).
document.addEventListener('alpine:init', () => {
  Alpine.data('analysisDetail', () => ({
    analysisId: null,
    analysis: null,
    scores: [],
    topTracts: [],
    bottomTracts: [],
    loading: false,
    error: null,

    async init() {
      this.$watch('$root.analysisId', async (val) => {
        if (val && this.$root.page === 'analysis') await this.load(val);
      });
      if (this.$root.analysisId) await this.load(this.$root.analysisId);
    },

    async load(id) {
      if (!id || id === this.analysisId) return;
      this.analysisId = id;
      this.loading = true;
      this.error = null;
      this.analysis = null;
      this.scores = [];

      try {
        const [aResult, sResult] = await Promise.allSettled([
          PDI.analysis(id),
          PDI.analysisScores(id)
        ]);

        if (aResult.status === 'fulfilled') {
          this.analysis = aResult.value;
        } else {
          this.error = aResult.reason?.message || 'Failed to load analysis.';
        }

        if (sResult.status === 'fulfilled') {
          this.scores = sResult.value.scores || [];
          const { top, bottom } = Domain.topBottomScores(this.scores, 5);
          this.topTracts = top;
          this.bottomTracts = bottom;
        }
      } catch (err) {
        this.error = err.message;
      } finally {
        this.loading = false;
      }
    },

    // ── Type helpers ───────────────────────────────────────────────────────

    get type() { return this.analysis?.type || ''; },
    get isComposite()    { return this.type === 'composite_index'; },
    get isCorrelation()  { return this.type === 'correlation_matrix'; },
    get isOLS()          { return this.type === 'ols_regression'; },
    get isTipping()      { return this.type === 'tipping_point'; },

    get typeLabel() { return Domain.fmtAnalysisType(this.type); },

    // ── Results accessors ──────────────────────────────────────────────────

    // OLS: returns coefficient rows sorted by absolute beta.
    get olsCoefficients() {
      const betas = this.analysis?.results?.betas || {};
      return Object.entries(betas)
        .map(([name, value]) => ({ name, value }))
        .sort((a, b) => Math.abs(b.value) - Math.abs(a.value));
    },

    get olsRSquared() {
      const r2 = this.analysis?.results?.r_squared;
      return r2 != null ? (r2 * 100).toFixed(1) + '%' : '\u2014';
    },

    get olsPValue() {
      const p = this.analysis?.results?.p_value;
      return p != null ? p.toFixed(4) : '\u2014';
    },

    // Correlation matrix: returns variable list + matrix entries.
    get corrVariables() {
      return this.analysis?.results?.variables || [];
    },

    get corrMatrix() {
      return this.analysis?.results?.matrix || [];
    },

    // For a correlation matrix cell, return background color intensity.
    corrColor(value) {
      if (value == null) return '';
      const abs = Math.abs(value);
      if (abs >= 0.8) return value > 0 ? 'corr-strong-pos' : 'corr-strong-neg';
      if (abs >= 0.5) return value > 0 ? 'corr-mod-pos'    : 'corr-mod-neg';
      if (abs >= 0.3) return value > 0 ? 'corr-weak-pos'   : 'corr-weak-neg';
      return '';
    },

    fmtCorr(value) {
      return value != null ? value.toFixed(2) : '\u2014';
    },

    // Tipping point results.
    get tippingThreshold() {
      const t = this.analysis?.results?.threshold;
      return t != null ? t.toFixed(2) : '\u2014';
    },

    get tippingSlope1() {
      const s = this.analysis?.results?.slope_before;
      return s != null ? s.toFixed(3) : '\u2014';
    },

    get tippingSlope2() {
      const s = this.analysis?.results?.slope_after;
      return s != null ? s.toFixed(3) : '\u2014';
    },

    // Score helpers.
    fmtScore(s) {
      return s?.score != null ? s.score.toFixed(2) : '\u2014';
    },

    tractLabel(s) {
      return s?.name || Domain.fmtGeoid(s?.geoid) || s?.geoid || '\u2014';
    },

    scoreBarWidth(s) {
      if (!s?.score || this.scores.length === 0) return '0%';
      const max = Math.max(...this.scores.map(x => x.score ?? 0));
      return max > 0 ? Math.min((s.score / max) * 100, 100) + '%' : '0%';
    }
  }));
});
