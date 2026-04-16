document.addEventListener('alpine:init', () => {
  const fallback = [
    { id: 'FH-HSG-001', candidate: 'Francesca Hong', office: 'Governor', state: 'WI', category: 'Housing', title: 'Expand affordable housing', description: 'State-funded affordable housing; community land trusts and ADUs' },
    { id: 'FH-HEALTH-001', candidate: 'Francesca Hong', office: 'Governor', state: 'WI', category: 'Healthcare', title: 'Expand BadgerCare', description: 'Expand Medicaid and create a public health insurance option' },
    { id: 'FH-EDU-001', candidate: 'Francesca Hong', office: 'Governor', state: 'WI', category: 'Schools', title: 'Free school meals', description: 'State-funded free healthy school meals for all children' },
    { id: 'FH-LABOR-001', candidate: 'Francesca Hong', office: 'Governor', state: 'WI', category: 'Labor', title: 'Paid family leave', description: 'Statewide insurance program covering all workers' },
    { id: 'ZM-HSG-001', candidate: 'Zohran Mamdani', office: 'Mayor', state: 'NY', category: 'Housing', title: 'Rent freeze', description: 'Freeze rents for two million rent-stabilized tenants' },
    { id: 'ZM-HSG-002', candidate: 'Zohran Mamdani', office: 'Mayor', state: 'NY', category: 'Housing', title: '200K affordable units', description: 'Build 200,000 affordable units through non-profit developers' },
    { id: 'ZM-TRANSIT-001', candidate: 'Zohran Mamdani', office: 'Mayor', state: 'NY', category: 'Transit', title: 'Free city buses', description: 'Eliminate bus fares; speed up service for low-income workers' },
    { id: 'ZM-HEALTH-001', candidate: 'Zohran Mamdani', office: 'Mayor', state: 'NY', category: 'Healthcare', title: 'Universal free childcare', description: 'Expand free childcare citywide; goal of universal access' },
    { id: 'ZM-EQUITY-001', candidate: 'Zohran Mamdani', office: 'Mayor', state: 'NY', category: 'Racial Equity', title: 'Racial equity plan', description: '62% cannot meet basic needs; address racial wealth gap' },
    { id: 'ZM-LABOR-001', candidate: 'Zohran Mamdani', office: 'Mayor', state: 'NY', category: 'Labor', title: '$25 minimum wage', description: 'Living wage increase to match true cost of living' }
  ];

  Alpine.data('candidateTracker', () => ({
    policies: [], activeCandidate: null, activeCat: null,
    async init() {
      try {
        const r = await fetch('/v1/policy/policies?limit=200');
        if (r.ok) { const d = await r.json(); this.policies = d.policies || fallback; }
        else this.policies = fallback;
      } catch(e) { this.policies = fallback; }
    },
    get candidateNames() { return [...new Set(this.policies.map(p => p.candidate))]; },
    get categories() { return [...new Set(this.policies.map(p => p.category))].sort(); },
    get filtered() {
      return this.policies.filter(p =>
        (!this.activeCandidate || p.candidate === this.activeCandidate) &&
        (!this.activeCat || p.category === this.activeCat)
      );
    }
  }));
});
