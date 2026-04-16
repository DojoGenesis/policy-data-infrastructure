// lib/domain.js — Formatting, tier logic, wiki-link resolution. No fetch() calls.
const Domain = {

  // ── Value Formatting ───────────────────────────────────────────────────────

  fmtValue(value, unit) {
    if (value == null) return '\u2014';
    if (unit === 'percent') return value.toFixed(1) + '%';
    if (unit === 'dollars') return '$' + Math.round(value).toLocaleString();
    return Math.round(value).toLocaleString();
  },

  // Format an indicator by variable_id from a geography object.
  // format: 'pct' | 'dollar' | 'count'
  fmtInd(geo, varId, format) {
    const ind = (geo.indicators ?? []).find(i => i.variable_id === varId);
    if (!ind || ind.value == null) return '\u2014';
    if (format === 'pct') return ind.value.toFixed(1) + '%';
    if (format === 'dollar') return '$' + Math.round(ind.value).toLocaleString();
    return Math.round(ind.value).toLocaleString();
  },

  // Get raw indicator value from a geography (null if missing).
  indValue(geo, varId) {
    const ind = (geo.indicators ?? []).find(i => i.variable_id === varId);
    return ind ? ind.value : null;
  },

  // ── Tier Logic ─────────────────────────────────────────────────────────────

  tierClass(percentile) {
    if (percentile == null) return '';
    if (percentile >= 80) return 'critical';
    if (percentile >= 60) return 'warning';
    return '';
  },

  tierLabel(percentile) {
    if (percentile == null) return 'Unknown';
    if (percentile >= 80) return 'Critical';
    if (percentile >= 60) return 'Warning';
    if (percentile >= 40) return 'Moderate';
    return 'Low';
  },

  tierBadgeClass(percentile) {
    if (percentile == null) return 'badge-neutral';
    if (percentile >= 80) return 'badge-critical';
    if (percentile >= 60) return 'badge-warning';
    return 'badge-ok';
  },

  // ── Bar Chart Helpers ──────────────────────────────────────────────────────

  barWidth(ind, indicators) {
    if (!ind || ind.value == null) return '0%';
    if (ind.unit === 'percent') return Math.min(ind.value, 100) + '%';
    const sameUnit = indicators.filter(i => i.unit === ind.unit && i.value != null);
    const max = Math.max(...sameUnit.map(i => i.value));
    return max > 0 ? Math.min((ind.value / max) * 100, 100) + '%' : '0%';
  },

  barColor(ind) {
    if (!ind) return 'bar-fill-teal';
    if (ind.direction === 'lower_better') return 'bar-fill-amber';
    if (ind.direction === 'higher_better') return 'bar-fill-teal';
    return 'bar-fill-teal';
  },

  // ── Source Grouping ────────────────────────────────────────────────────────

  sourceGroup(varId) {
    if (!varId) return 'Other';
    if (varId.startsWith('cdc_')) return 'Health Outcomes (CDC PLACES)';
    if (varId.startsWith('usda_')) return 'Food Access (USDA)';
    if (varId.startsWith('owner_') || varId.startsWith('renter_') || varId.startsWith('housing_'))
      return 'Housing (ACS)';
    if (varId.startsWith('bls_')) return 'Employment (BLS LAUS)';
    return 'Demographics (Census ACS)';
  },

  // Groups an indicators array by source category.
  // Returns array of { name, items } objects.
  groupIndicators(indicators) {
    const g = {};
    for (const ind of (indicators || [])) {
      const k = this.sourceGroup(ind.variable_id);
      if (!g[k]) g[k] = { name: k, items: [] };
      g[k].items.push(ind);
    }
    return Object.values(g);
  },

  // ── Wiki Link Resolution ───────────────────────────────────────────────────

  // Maps variable_ids to equity dimensions for policy linkage.
  _dimMap: {
    poverty_rate:              ['income_equity', 'economic_equity', 'economic_opportunity'],
    median_household_income:   ['income_equity', 'economic_opportunity', 'fiscal_equity'],
    uninsured_rate:            ['health_access', 'health_equity'],
    cdc_obesity:               ['health_equity', 'environmental_health'],
    cdc_mhlth:                 ['health_access', 'care_access'],
    cdc_diabetes:              ['health_equity', 'food_access'],
    housing_units_cost_burden: ['housing_affordability', 'housing_stability'],
    usda_lila:                 ['food_access'],
    cdc_bphigh:                ['health_equity', 'environmental_health'],
    cdc_casthma:               ['environmental_health', 'health_equity'],
    cdc_cancer:                ['health_equity'],
    cdc_chd:                   ['health_equity', 'care_access'],
    cdc_stroke:                ['health_equity', 'care_access'],
    cdc_depression:            ['health_access', 'care_access'],
    cdc_sleep:                 ['health_equity'],
    usda_lalowi:               ['food_access', 'economic_equity'],
    usda_lahunv:               ['food_access', 'transportation_equity']
  },

  // Returns policies that relate to any of the given geography's indicators.
  findRelatedPolicies(indicators, policies) {
    const activeDims = new Set();
    for (const ind of (indicators ?? [])) {
      const dims = this._dimMap[ind.variable_id] || [];
      dims.forEach(d => activeDims.add(d));
    }
    if (activeDims.size === 0) return [];
    return (policies || []).filter(p => activeDims.has(p.equity_dimension));
  },

  // ── Display Helpers ────────────────────────────────────────────────────────

  fmtDimension(dim) {
    return (dim || '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  },

  fmtGeoid(geoid) {
    if (!geoid) return '';
    if (geoid.length === 11) return `Tract ${geoid.slice(5)}`; // census tract
    return geoid;
  },

  // Format an analysis type label for display.
  fmtAnalysisType(type) {
    const map = {
      composite_index:   'Composite Index',
      correlation_matrix: 'Correlation Matrix',
      ols_regression:    'OLS Regression',
      tipping_point:     'Tipping Point Analysis'
    };
    return map[type] || (type || '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  },

  // Return CSS class for a diff value (positive/negative).
  diffClass(diff, direction) {
    if (diff == null) return '';
    if (direction === 'lower_better') {
      return diff < 0 ? 'diff-better' : diff > 0 ? 'diff-worse' : '';
    }
    return diff > 0 ? 'diff-better' : diff < 0 ? 'diff-worse' : '';
  },

  // ── Score Helpers ─────────────────────────────────────────────────────────

  // Sort scores and return top N and bottom N.
  topBottomScores(scores, n = 5) {
    if (!scores || scores.length === 0) return { top: [], bottom: [] };
    const sorted = [...scores].sort((a, b) => (b.score ?? 0) - (a.score ?? 0));
    return {
      top:    sorted.slice(0, n),
      bottom: sorted.slice(-n).reverse()
    };
  }
};
