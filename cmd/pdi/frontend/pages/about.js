// pages/about.js — Platform story, methodology, data sources, and open source.
document.addEventListener('alpine:init', () => {
  Alpine.data('aboutPage', () => ({
    sources: [],
    loading: false,

    _fallbackSources: [
      { name: 'Census American Community Survey', type: 'Demographic', vintage: '2023 5-Year', coverage: 'State, County, Tract', variables: '15+ indicators', description: 'Poverty rate, median household income, housing cost burden, educational attainment, race/ethnicity, insurance coverage.' },
      { name: 'CDC PLACES', type: 'Health', vintage: '2022', coverage: 'County, Tract', variables: '8 outcomes', description: 'Obesity, diabetes, mental health distress, high blood pressure, asthma, smoking, physical health, insurance gaps.' },
      { name: 'BLS Local Area Unemployment', type: 'Economic', vintage: '2023', coverage: 'County', variables: '4 measures', description: 'Unemployment rate, unemployed count, employed count, civilian labor force.' },
      { name: 'USDA Food Access Atlas', type: 'Food', vintage: '2019', coverage: 'Tract', variables: '6 indicators', description: 'Food desert classification, low-access populations (1-mile urban, 10-mile rural), SNAP access, vehicle access.' },
      { name: 'EPA EJScreen', type: 'Environment', vintage: '2023', coverage: 'Tract, Block Group', variables: '10+ indices', description: 'PM2.5, ozone, cancer risk, traffic proximity, lead paint, hazardous waste proximity, wastewater discharge.' },
      { name: 'HUD CHAS', type: 'Housing', vintage: '2016-2020', coverage: 'Tract', variables: '5 indicators', description: 'Cost burden (30% and 50% thresholds), housing problems, extremely low-income renters, overcrowding.' },
      { name: 'HMDA', type: 'Lending', vintage: '2023', coverage: 'Tract', variables: '5 indicators', description: 'Mortgage originations, denial rates, minority denial rates, loan-to-value ratios, median loan amounts.' },
      { name: 'HRSA', type: 'Healthcare', vintage: '2023', coverage: 'County', variables: '5 indicators', description: 'Health professional shortage area scores (primary care, dental, mental health), FQHC counts.' },
      { name: 'GTFS', type: 'Transit', vintage: '2023', coverage: 'Tract', variables: '3 indicators', description: 'Transit stop counts, daily trips, trips per hour (Madison Metro Transit).' },
      { name: 'WI DPI', type: 'Education', vintage: '2022-23', coverage: 'District', variables: '7 indicators', description: 'Enrollment, chronic absence rate, attendance rate, race-stratified absence (Black, Hispanic, White, econ disadvantaged).' },
      { name: 'EPA TRI', type: 'Toxics', vintage: '2022', coverage: 'County', variables: '4 indicators', description: 'TRI facility counts, total releases, air emissions, carcinogen-handling facility counts.' },
      { name: 'HUD PIT', type: 'Homelessness', vintage: '2023', coverage: 'CoC/County', variables: '6+ measures', description: 'Point-in-time counts: total homeless, sheltered, unsheltered, chronic, veterans, youth.' },
      { name: 'Census TIGER', type: 'Geography', vintage: '2023', coverage: 'All levels', variables: 'Boundaries', description: 'County and census tract boundary shapefiles for PostGIS spatial analysis.' }
    ],

    methodologySteps: [
      { step: '1', title: 'Raw Indicators First', body: 'All data is stored as raw indicator values with source vintage and unit metadata. NULL means missing or suppressed — never a sentinel value. Composites are derived views layered on top of validated raws, never stored truth.' },
      { step: '2', title: '30% Null Gate', body: 'The pipeline validation stage (ValidateStage in the Go pipeline) rejects any data load where more than 30% of values for the primary indicator are null. This matches the Census Bureau\'s own suppression threshold and catches government APIs that silently drop data.' },
      { step: '3', title: 'ICE Scoring', body: 'The Index of Concentration at the Extremes (Krieger et al., 2016) measures segregation per tract: ICE = (privileged − deprived) / total. Range [-1, +1]. Positive = affluence concentration, negative = poverty concentration. Used as the primary equity metric.' },
      { step: '4', title: 'Statistical Engine', body: '10 functions in Go: OLS regression (with p-values and confidence intervals), Pearson correlation matrices, tipping point detection via piecewise linear regression, Blinder-Oaxaca decomposition, bootstrap confidence intervals, composite weighted indices, percentile ranking, dissimilarity and isolation indices, coefficient of variation for reliability.' },
      { step: '5', title: 'Evidence Crosswalk', body: '38 equity dimensions map indicator variables to policy positions. Each candidate\'s policies are linked to the data that measures the problems they address. This enables the wiki-link architecture: every county page shows relevant candidates, every candidate page shows relevant data.' }
    ],

    platformStats: [
      { value: '13', label: 'Data Sources' },
      { value: '42', label: 'Indicator Variables' },
      { value: '72', label: 'Wisconsin Counties' },
      { value: '1,652', label: 'Census Tracts' },
      { value: '85', label: 'Policy Positions' },
      { value: '380+', label: 'Automated Tests' }
    ],

    async init() {
      this.loading = true;
      try {
        const r = await PDI.sources();
        const items = r.sources || r.items || [];
        this.sources = items.length > 0 ? items : this._fallbackSources;
      } catch (_) {
        this.sources = this._fallbackSources;
      } finally {
        this.loading = false;
      }
    }
  }));
});
