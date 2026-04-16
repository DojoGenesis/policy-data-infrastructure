// lib/router.js — Hash-based SPA router
// Registered as BOTH a global function (for x-data="appRouter()") and
// an Alpine.data component (for x-data="appRouter").
function appRouter() {
  return {
    page: 'landing',
    geoid: null,
    analysisId: null,

    init() {
      this.route();
      window.addEventListener('hashchange', () => this.route());
    },

    route() {
      const h = window.location.hash || '#/';

      if (h === '#/' || h === '') {
        this.page = 'landing';
        this.geoid = null;
        this.analysisId = null;
      } else if (h === '#/counties') {
        this.page = 'counties';
        this.geoid = null;
      } else if (h.startsWith('#/county/') && h.includes('/tracts')) {
        this.page = 'tracts';
        this.geoid = h.split('/')[2];
      } else if (h.startsWith('#/county/')) {
        this.page = 'county';
        this.geoid = h.replace('#/county/', '');
      } else if (h === '#/compare') {
        this.page = 'compare';
        this.geoid = null;
      } else if (h.startsWith('#/analysis/')) {
        this.page = 'analysis';
        this.analysisId = h.replace('#/analysis/', '');
      } else if (h === '#/evidence') {
        this.page = 'evidence';
        this.geoid = null;
      } else if (h === '#/candidates') {
        this.page = 'candidates';
        this.geoid = null;
      } else if (h === '#/chat') {
        this.page = 'chat';
        this.geoid = null;
      } else if (h === '#/about') {
        this.page = 'about';
        this.geoid = null;
      } else {
        this.page = 'landing';
        this.geoid = null;
        this.analysisId = null;
      }
    },

    navigate(hash) {
      window.location.hash = hash;
    }
  };
}
