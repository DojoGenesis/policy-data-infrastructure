// pages/about.js — Data sources, methodology, and platform stats.
document.addEventListener('alpine:init', () => {
  Alpine.data('aboutPage', () => ({
    sources: [],
    loading: false,
    error: null,

    // Hardcoded fallback source list — used if API returns empty or errors.
    _fallbackSources: [
      { id: 'acs',        name: 'Census American Community Survey',  vintage: '2023 5-Year', url: 'https://data.census.gov', description: 'Poverty rate, household income, housing cost burden, educational attainment, and demographic composition at county and tract level.' },
      { id: 'cdc_places', name: 'CDC PLACES',                       vintage: '2022',        url: 'https://www.cdc.gov/places', description: '8 health outcome measures — obesity, diabetes, mental health distress, high blood pressure, asthma, cancer, CHD, stroke — at census tract level.' },
      { id: 'usda',       name: 'USDA Food Access Research Atlas',   vintage: '2019',        url: 'https://www.ers.usda.gov/data-products/food-access-research-atlas/', description: '6 food access indicators including LILA (low income + low access), LAHUNV (no vehicle access), and distance-to-store measures at tract level.' },
      { id: 'bls',        name: 'BLS Local Area Unemployment Stats', vintage: '2023',        url: 'https://www.bls.gov/lau/', description: 'Monthly and annual average unemployment and labor force data for all 72 Wisconsin counties.' },
      { id: 'tiger',      name: 'Census TIGER/Line Shapefiles',      vintage: '2023',        url: 'https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html', description: 'County and census tract geographic boundaries for Wisconsin (FIPS 55).' },
      { id: 'epa',        name: 'EPA EJScreen',                     vintage: '2023',        url: 'https://www.epa.gov/ejscreen', description: 'Environmental justice screening indices including particulate matter, ozone, hazardous waste, and demographic vulnerability scores.' },
      { id: 'wi_dpi',     name: 'Wisconsin DPI Attendance Data',     vintage: '2022–23',     url: 'https://dpi.wi.gov/wisedash', description: 'Chronic absenteeism and attendance rates by school district, linked to census tract geography.' }
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
    },

    // Methodology steps — static content.
    methodologySteps: [
      {
        step: '1',
        title: 'Raw Indicators First',
        body: 'All data is stored as raw indicator values with source vintage, null tracking, and unit metadata. No composite scores are computed at ingest time — composites are layered on top of validated raws.'
      },
      {
        step: '2',
        title: '30% Null Gate',
        body: 'Any indicator load with more than 30% null values for the primary variable is rejected before hitting the database. Government APIs frequently suppress small-geography data — the null gate catches silent drops.'
      },
      {
        step: '3',
        title: 'ICE Scoring',
        body: 'The Index of Concentration at the Extremes (ICE) is computed per tract using ACS income and race data. ICE ranges from −1 (extreme poverty concentration) to +1 (extreme affluence concentration).'
      },
      {
        step: '4',
        title: 'Statistical Engine',
        body: 'The Go statistical engine runs OLS regression, Pearson correlation matrices, tipping point detection, bootstrap confidence intervals, and composite index scoring across all 1,652 Wisconsin tracts.'
      },
      {
        step: '5',
        title: 'Equity Crosswalk',
        body: 'Each indicator is crosswalked to one or more equity dimensions (housing_affordability, health_access, food_access, etc.) enabling automated wiki-links between data and candidate policy positions.'
      }
    ]
  }));
});
