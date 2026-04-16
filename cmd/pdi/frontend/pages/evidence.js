// pages/evidence.js — Evidence card gallery with category filters.
document.addEventListener('alpine:init', () => {
  Alpine.data('evidenceGallery', () => ({
    activeCategory: 'all',

    // Hardcoded evidence cards — future: from /v1/policy/evidence API.
    cards: [
      {
        id: 'ev-001',
        title: 'Dane County Food Desert Burden',
        category: 'food_access',
        categoryLabel: 'Food Access',
        summary: 'Over 18% of Dane County tracts qualify as low-income, low-access food areas per USDA criteria. Rural tracts show LILA rates 2.4× the county average.',
        source: 'USDA Food Access Research Atlas 2019',
        geoid: '55025',
        countyName: 'Dane County'
      },
      {
        id: 'ev-002',
        title: 'Milwaukee Mental Health Access Gap',
        category: 'health',
        categoryLabel: 'Health',
        summary: 'CDC PLACES data shows Milwaukee County tracts in the top poverty quartile report mental health distress rates 34% above state average. Uninsured rate remains 11.2% countywide.',
        source: 'CDC PLACES 2022, ACS 2023 5-Year',
        geoid: '55079',
        countyName: 'Milwaukee County'
      },
      {
        id: 'ev-003',
        title: 'Housing Cost Burden Across Wisconsin',
        category: 'housing',
        categoryLabel: 'Housing',
        summary: 'Statewide, 31% of Wisconsin renter households are cost-burdened (>30% income on housing). Dane and Waukesha counties lead in absolute counts; Menominee County has the highest rate at 44%.',
        source: 'ACS 2023 5-Year Estimates, Table B25070',
        geoid: '55',
        countyName: 'Wisconsin'
      },
      {
        id: 'ev-004',
        title: 'Rural Diabetes Concentration — Northern WI',
        category: 'health',
        categoryLabel: 'Health',
        summary: 'CDC PLACES estimates show Burnett, Iron, and Ashland counties have adult diabetes prevalence 28–33%, compared to 10% in urban Dane County tracts. Rural-urban gap is widening.',
        source: 'CDC PLACES 2022 County-Level',
        geoid: null,
        countyName: null
      },
      {
        id: 'ev-005',
        title: 'ICE Index: Income Concentration + Extremes',
        category: 'economic',
        categoryLabel: 'Economic',
        summary: 'The Index of Concentration at the Extremes for Wisconsin tracts ranges from −0.62 (high poverty concentration) to +0.44 (high affluence). The most disadvantaged tracts cluster in Milwaukee\'s north side and rural reservation communities.',
        source: 'ACS 2023 5-Year, computed via PDI statistical engine',
        geoid: null,
        countyName: null
      },
      {
        id: 'ev-006',
        title: 'USDA Vehicle Access + Food Insecurity',
        category: 'food_access',
        categoryLabel: 'Food Access',
        summary: 'Tracts with both low income and no vehicle access (LAHUNV) show food insecurity rates 2.1× higher than low-income tracts with vehicle access. Northern Wisconsin has the highest LAHUNV concentration.',
        source: 'USDA Food Access Research Atlas 2019',
        geoid: null,
        countyName: null
      },
      {
        id: 'ev-007',
        title: 'BLS Employment Gap: Northern vs. Southern WI',
        category: 'economic',
        categoryLabel: 'Economic',
        summary: 'Annual average unemployment in Wisconsin\'s 20 northernmost counties averaged 5.8% (2023) vs 3.1% in the 20 southernmost counties. Vilas, Forest, and Menominee counties exceeded 7.5%.',
        source: 'BLS Local Area Unemployment Statistics 2023',
        geoid: null,
        countyName: null
      },
      {
        id: 'ev-008',
        title: 'Composite Disadvantage Index — Tract Distribution',
        category: 'methodology',
        categoryLabel: 'Methodology',
        summary: 'PDI\'s composite disadvantage index combines 8 indicators across health, housing, and economic domains. Score distribution is right-skewed: 15% of tracts score above 0.70, representing concentrated disadvantage in Milwaukee, Racine, and reservation communities.',
        source: 'PDI Statistical Engine, composite_index analysis run',
        geoid: null,
        countyName: null
      }
    ],

    categories: [
      { id: 'all',         label: 'All Evidence' },
      { id: 'health',      label: 'Health' },
      { id: 'food_access', label: 'Food Access' },
      { id: 'housing',     label: 'Housing' },
      { id: 'economic',    label: 'Economic' },
      { id: 'methodology', label: 'Methodology' }
    ],

    get filtered() {
      if (this.activeCategory === 'all') return this.cards;
      return this.cards.filter(c => c.category === this.activeCategory);
    },

    setCategory(cat) {
      this.activeCategory = cat;
    },

    countyUrl(card) {
      return card.geoid && card.geoid !== '55' ? `#/county/${card.geoid}` : null;
    }
  }));
});
