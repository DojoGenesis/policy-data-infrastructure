// frontend/pages/compare.js — Compare Tool page

(function () {
  Alpine.data('compareTool', function () {
    return {
      counties: [],
      geoid1: '',
      geoid2: '',
      loading: false,
      result: null,
      error: null,

      async init() {
        try {
          const res = await fetch('/v1/policy/geographies?level=county&state_fips=55&limit=100');
          if (!res.ok) throw new Error('Failed to load counties');
          const data = await res.json();
          this.counties = (data.geographies || data || []).sort((a, b) =>
            (a.name || a.display_name || '').localeCompare(b.name || b.display_name || '')
          );
        } catch (e) {
          this.error = 'Could not load county list: ' + e.message;
        }
      },

      countyName(geoid) {
        const c = this.counties.find(c => c.geoid === geoid);
        return c ? (c.name || c.display_name || geoid) : geoid;
      },

      async compare() {
        if (!this.geoid1 || !this.geoid2) {
          this.error = 'Please select both counties before comparing.';
          return;
        }
        if (this.geoid1 === this.geoid2) {
          this.error = 'Please select two different counties.';
          return;
        }
        this.loading = true;
        this.error = null;
        this.result = null;
        try {
          const res = await fetch('/v1/policy/compare', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ geoid1: this.geoid1, geoid2: this.geoid2 }),
          });
          if (!res.ok) throw new Error('Compare request failed (' + res.status + ')');
          this.result = await res.json();
        } catch (e) {
          this.error = 'Comparison failed: ' + e.message;
        } finally {
          this.loading = false;
        }
      },

      diffClass(row) {
        if (row.diff == null) return '';
        const better = row.direction === 'lower' ? row.diff < 0 : row.diff > 0;
        return better ? 'badge-green' : 'badge-red';
      },

      barWidthA(row) {
        const v1 = row.value1 ?? 0;
        const v2 = row.value2 ?? 0;
        const max = Math.max(Math.abs(v1), Math.abs(v2), 0.001);
        return Math.round((Math.abs(v1) / max) * 100);
      },

      barWidthB(row) {
        const v1 = row.value1 ?? 0;
        const v2 = row.value2 ?? 0;
        const max = Math.max(Math.abs(v1), Math.abs(v2), 0.001);
        return Math.round((Math.abs(v2) / max) * 100);
      },

      barColorA(row) {
        const v1 = row.value1 ?? 0;
        const v2 = row.value2 ?? 0;
        const aWins = row.direction === 'lower' ? v1 <= v2 : v1 >= v2;
        return aWins ? 'bar-fill-green' : 'bar-fill-amber';
      },

      barColorB(row) {
        const v1 = row.value1 ?? 0;
        const v2 = row.value2 ?? 0;
        const bWins = row.direction === 'lower' ? v2 <= v1 : v2 >= v1;
        return bWins ? 'bar-fill-green' : 'bar-fill-amber';
      },

      fmt(val) {
        if (val == null) return '—';
        const n = parseFloat(val);
        if (isNaN(n)) return val;
        return n % 1 === 0 ? n.toLocaleString() : n.toLocaleString(undefined, { maximumFractionDigits: 2 });
      },

      fmtPct(val) {
        if (val == null) return '';
        const n = parseFloat(val);
        if (isNaN(n)) return '';
        const sign = n > 0 ? '+' : '';
        return sign + n.toFixed(1) + '%';
      },
    };
  });

  const target = document.querySelector('[x-ref="compareTool"]');
  if (target) {
    target.innerHTML = `
      <div x-data="compareTool" x-init="init()">
        <div style="margin-bottom:1.5rem;">
          <h1 style="font-size:1.75rem;font-weight:700;color:var(--navy);margin-bottom:0.25rem;">County Comparison Tool</h1>
          <p style="color:var(--text-secondary);">Select two Wisconsin counties to compare key equity indicators side by side.</p>
        </div>

        <!-- Selector Row -->
        <div class="card" style="margin-bottom:1.5rem;">
          <div class="grid-2" style="gap:1rem;align-items:flex-end;">
            <div>
              <label style="display:block;font-size:0.85rem;font-weight:600;margin-bottom:0.4rem;color:var(--text-secondary);">COUNTY A</label>
              <select class="input" x-model="geoid1">
                <option value="">Select a county…</option>
                <template x-for="c in counties" :key="c.geoid">
                  <option :value="c.geoid" x-text="c.name || c.display_name || c.geoid"></option>
                </template>
              </select>
            </div>
            <div>
              <label style="display:block;font-size:0.85rem;font-weight:600;margin-bottom:0.4rem;color:var(--text-secondary);">COUNTY B</label>
              <select class="input" x-model="geoid2">
                <option value="">Select a county…</option>
                <template x-for="c in counties" :key="c.geoid">
                  <option :value="c.geoid" x-text="c.name || c.display_name || c.geoid"></option>
                </template>
              </select>
            </div>
          </div>
          <div style="margin-top:1rem;">
            <button class="btn btn-primary" @click="compare()" :disabled="loading">
              <span x-show="!loading">Compare Counties</span>
              <span x-show="loading">Loading…</span>
            </button>
          </div>
        </div>

        <!-- Error -->
        <div x-show="error" class="card" style="border-color:var(--red);margin-bottom:1rem;">
          <p style="color:var(--red);" x-text="error"></p>
        </div>

        <!-- Loading spinner -->
        <div x-show="loading" class="loading">
          <span>Fetching comparison data…</span>
        </div>

        <!-- Results -->
        <div x-show="result && !loading">
          <div class="card">
            <div class="card-header" x-text="'Comparing ' + countyName(geoid1) + ' vs. ' + countyName(geoid2)"></div>
            <div x-show="!result?.differences?.length" class="empty">No indicator data available for this comparison.</div>
            <div x-show="result?.differences?.length">
              <table class="table">
                <thead>
                  <tr>
                    <th>Indicator</th>
                    <th x-text="countyName(geoid1)"></th>
                    <th x-text="countyName(geoid2)"></th>
                    <th>Difference</th>
                    <th>Visual</th>
                  </tr>
                </thead>
                <tbody>
                  <template x-for="row in (result?.differences || [])" :key="row.variable_id">
                    <tr>
                      <td style="font-weight:600;" x-text="row.label || row.variable_id"></td>
                      <td x-text="fmt(row.value1)"></td>
                      <td x-text="fmt(row.value2)"></td>
                      <td>
                        <span class="badge" :class="diffClass(row)" x-text="fmtPct(row.pct_diff)"></span>
                      </td>
                      <td style="width:180px;">
                        <div style="display:flex;flex-direction:column;gap:3px;">
                          <div class="bar-track" style="height:16px;">
                            <div class="bar-fill" :class="barColorA(row)" :style="'width:'+barWidthA(row)+'%'" :title="countyName(geoid1)"></div>
                          </div>
                          <div class="bar-track" style="height:16px;">
                            <div class="bar-fill" :class="barColorB(row)" :style="'width:'+barWidthB(row)+'%'" :title="countyName(geoid2)"></div>
                          </div>
                        </div>
                      </td>
                    </tr>
                  </template>
                </tbody>
              </table>
              <p style="margin-top:0.75rem;font-size:0.78rem;color:var(--text-secondary);">
                Green bar = better outcome for that county. Color depends on indicator direction (lower or higher is better).
              </p>
            </div>
          </div>
        </div>
      </div>
    `;
    Alpine.initTree(target);
  }
})();
