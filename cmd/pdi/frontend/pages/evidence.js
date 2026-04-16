// frontend/pages/evidence.js — Evidence Card Gallery

(function () {
  const evidenceCards = [
    { id: 'FH-HSG-001', title: 'Expand affordable housing', category: 'Housing', dimension: 'housing_affordability', finding: 'In Dane County, 42.3% of renter households are cost-burdened (paying >30% of income on housing), ranking 3rd highest among WI counties.', quality: 'strong' },
    { id: 'FH-HSG-004', title: 'Protecting renters', category: 'Housing', dimension: 'housing_stability', finding: 'Milwaukee County has the highest severe cost burden rate at 28.1% of households paying >50% of income on housing costs.', quality: 'strong' },
    { id: 'FH-EDU-001', title: 'Public school funding equity', category: 'Schools', dimension: 'education_funding', finding: 'Counties with median household income below $55,000 have chronic absence rates 1.8x higher than affluent counties.', quality: 'moderate' },
    { id: 'FH-HEALTH-001', title: 'Expand BadgerCare', category: 'Healthcare', dimension: 'health_access', finding: 'Menominee County has the highest uninsured rate in Wisconsin at 12.8%, nearly triple the state median of 4.7%.', quality: 'strong' },
    { id: 'FH-ENV-001', title: 'Clean water standards', category: 'Environment', dimension: 'environmental_health', finding: 'CDC PLACES data shows 15.2% of adults in high-poverty WI tracts report poor physical health, vs 10.1% in low-poverty tracts.', quality: 'moderate' },
    { id: 'FH-LABOR-001', title: 'Raise the minimum wage', category: 'Labor', dimension: 'income_equity', finding: 'The gap between the highest and lowest county median household incomes in Wisconsin is $43,000 (Waukesha: $95K vs Menominee: $52K).', quality: 'strong' },
    { id: 'FH-FOOD-001', title: 'Address food deserts', category: 'Agriculture', dimension: 'food_access', finding: '23% of Wisconsin census tracts are classified as food deserts (low-income + low-access), affecting an estimated 340,000 residents.', quality: 'strong' },
    { id: 'FH-TRANSIT-001', title: 'Expand rural transit', category: 'Cities Towns Transit', dimension: 'transit_access', finding: 'Rural counties with poverty rates above 15% have effectively zero public transit options outside of county seat areas.', quality: 'moderate' },
  ];

  const allCategories = ['All', ...Array.from(new Set(evidenceCards.map(c => c.category)))];

  Alpine.data('evidenceGallery', function () {
    return {
      cards: evidenceCards,
      categories: allCategories,
      activeCategory: 'All',

      get filtered() {
        if (this.activeCategory === 'All') return this.cards;
        return this.cards.filter(c => c.category === this.activeCategory);
      },

      qualityClass(quality) {
        return quality === 'strong' ? 'badge-green' : 'badge-amber';
      },

      qualityLabel(quality) {
        return quality === 'strong' ? 'Strong evidence' : 'Moderate evidence';
      },
    };
  });

  const target = document.querySelector('[x-ref="evidenceCards"]');
  if (target) {
    target.innerHTML = `
      <div x-data="evidenceGallery">
        <div style="margin-bottom:1.5rem;">
          <h1 style="font-size:1.75rem;font-weight:700;color:var(--navy);margin-bottom:0.25rem;">Evidence Cards</h1>
          <p style="color:var(--text-secondary);">Policy-linked findings derived from federal and state data sources. Each card connects a policy position to measured county-level outcomes.</p>
        </div>

        <!-- Category filters -->
        <div style="display:flex;flex-wrap:wrap;gap:0.5rem;margin-bottom:1.25rem;align-items:center;">
          <template x-for="cat in categories" :key="cat">
            <button
              class="btn btn-sm"
              :class="activeCategory === cat ? 'btn-primary' : 'btn-outline'"
              @click="activeCategory = cat"
              x-text="cat"
            ></button>
          </template>
          <span style="margin-left:auto;font-size:0.85rem;color:var(--text-secondary);">
            <span x-text="filtered.length"></span> card<span x-show="filtered.length !== 1">s</span>
          </span>
        </div>

        <!-- Cards grid -->
        <div class="grid-3">
          <template x-for="card in filtered" :key="card.id">
            <div class="card" style="display:flex;flex-direction:column;gap:0.75rem;">
              <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:0.5rem;">
                <span class="badge badge-teal" x-text="card.category"></span>
                <span class="badge" :class="qualityClass(card.quality)" x-text="qualityLabel(card.quality)"></span>
              </div>
              <div>
                <div style="font-weight:700;font-size:1rem;color:var(--navy);margin-bottom:0.35rem;" x-text="card.title"></div>
                <p style="font-size:0.875rem;color:var(--text);line-height:1.5;" x-text="card.finding"></p>
              </div>
              <div style="margin-top:auto;padding-top:0.5rem;border-top:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;">
                <span style="font-size:0.75rem;color:var(--text-secondary);font-family:monospace;" x-text="card.id"></span>
                <span style="font-size:0.75rem;color:var(--text-secondary);text-transform:capitalize;" x-text="card.dimension.replace(/_/g,' ')"></span>
              </div>
            </div>
          </template>
        </div>

        <div x-show="filtered.length === 0" class="empty">
          No cards in this category.
        </div>
      </div>
    `;
    Alpine.initTree(target);
  }
})();
