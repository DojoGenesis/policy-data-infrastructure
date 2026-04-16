document.addEventListener('alpine:init', () => {
  Alpine.data('evidenceGallery', () => ({
    cards: [
      { id: 'FH-HSG-001', title: 'Expand affordable housing', category: 'Housing', finding: 'In Dane County, 42.3% of renter households are cost-burdened, ranking 3rd highest among WI counties.', quality: 'strong' },
      { id: 'FH-HSG-004', title: 'Protecting renters', category: 'Housing', finding: 'Milwaukee County has the highest severe cost burden rate at 28.1% of households paying >50% of income on housing.', quality: 'strong' },
      { id: 'FH-EDU-001', title: 'Public school funding equity', category: 'Schools', finding: 'Counties with median household income below $55,000 have chronic absence rates 1.8x higher than affluent counties.', quality: 'moderate' },
      { id: 'FH-HEALTH-001', title: 'Expand BadgerCare', category: 'Healthcare', finding: 'Menominee County has the highest uninsured rate at 12.8%, nearly triple the state median of 4.7%.', quality: 'strong' },
      { id: 'FH-ENV-001', title: 'Clean water standards', category: 'Environment', finding: 'CDC PLACES data shows 15.2% of adults in high-poverty WI tracts report poor physical health, vs 10.1% in low-poverty tracts.', quality: 'moderate' },
      { id: 'FH-LABOR-001', title: 'Raise the minimum wage', category: 'Labor', finding: 'The gap between highest and lowest county median incomes in Wisconsin is $43,000 (Waukesha: $95K vs Menominee: $52K).', quality: 'strong' },
      { id: 'FH-FOOD-001', title: 'Address food deserts', category: 'Agriculture', finding: '23% of Wisconsin census tracts are classified as food deserts, affecting an estimated 340,000 residents.', quality: 'strong' },
      { id: 'ZM-TRANSIT-001', title: 'Free city buses', category: 'Transit', finding: 'Low-income workers disproportionately rely on buses; fare costs consume 5-10% of minimum wage income.', quality: 'moderate' }
    ],
    activeCategory: null,
    get categories() { return [...new Set(this.cards.map(c => c.category))].sort(); },
    get filtered() { return this.activeCategory ? this.cards.filter(c => c.category === this.activeCategory) : this.cards; }
  }));
});
