document.addEventListener('alpine:init', () => {
  Alpine.data('aboutPage', () => ({
    sources: [
      { name: 'Census ACS', type: 'Demographic', coverage: 'State, County, Tract' },
      { name: 'CDC PLACES', type: 'Health', coverage: 'County, Tract' },
      { name: 'BLS LAUS', type: 'Economic', coverage: 'County' },
      { name: 'EPA EJScreen', type: 'Environment', coverage: 'Tract, Block Group' },
      { name: 'USDA Food Access', type: 'Food', coverage: 'Tract' },
      { name: 'HUD CHAS', type: 'Housing', coverage: 'Tract' },
      { name: 'HMDA', type: 'Lending', coverage: 'Tract' },
      { name: 'HRSA', type: 'Healthcare', coverage: 'County' },
      { name: 'GTFS', type: 'Transit', coverage: 'Tract' },
      { name: 'WI DPI', type: 'Education', coverage: 'District' },
      { name: 'EPA TRI', type: 'Toxics', coverage: 'County' },
      { name: 'HUD PIT', type: 'Homelessness', coverage: 'CoC/County' }
    ]
  }));
});
