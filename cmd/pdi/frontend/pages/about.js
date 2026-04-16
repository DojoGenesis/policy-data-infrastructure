// frontend/pages/about.js — About / Methodology page

(function () {
  Alpine.data('aboutPage', function () {
    return {
      sources: [
        { name: 'Census ACS',       type: 'Demographic',  description: 'American Community Survey 5-year estimates — population, housing, income, poverty' },
        { name: 'CDC PLACES',       type: 'Health',       description: 'Small-area estimates for 30+ chronic disease and health risk measures' },
        { name: 'BLS LAUS',         type: 'Economic',     description: 'Local Area Unemployment Statistics — monthly labor force, employment, unemployment' },
        { name: 'EPA EJScreen',     type: 'Environment',  description: 'Environmental justice screening — pollution burden, demographic vulnerability' },
        { name: 'USDA Food Access', type: 'Food',         description: 'Food access research atlas — low-income/low-access census tracts, food desert classification' },
        { name: 'HUD CHAS',         type: 'Housing',      description: 'Comprehensive Housing Affordability Strategy — cost burden, severe cost burden by tenure' },
        { name: 'HMDA',             type: 'Lending',      description: 'Home Mortgage Disclosure Act — loan originations, denials, demographics by census tract' },
        { name: 'HRSA',             type: 'Healthcare',   description: 'Health Resources and Services Administration — health professional shortage areas, FQHC locations' },
        { name: 'GTFS',             type: 'Transit',      description: 'General Transit Feed Specification — transit routes, stops, frequency for WI agencies' },
        { name: 'WI DPI',           type: 'Education',    description: 'Wisconsin Department of Public Instruction — chronic absenteeism, graduation rates by district' },
        { name: 'EPA TRI',          type: 'Toxics',       description: 'Toxic Release Inventory — industrial chemical releases by facility and census tract' },
        { name: 'HUD PIT',          type: 'Homelessness', description: 'Point-in-Time Count — annual sheltered and unsheltered homeless population by CoC' },
      ],
    };
  });

  const target = document.querySelector('[x-ref="aboutPage"]');
  if (target) {
    target.innerHTML = `
      <div x-data="aboutPage">
        <div style="margin-bottom:2rem;">
          <h1 style="font-size:1.75rem;font-weight:700;color:var(--navy);margin-bottom:0.25rem;">About &amp; Methodology</h1>
          <p style="color:var(--text-secondary);">How this platform works and where the data comes from.</p>
        </div>

        <!-- What is this? -->
        <div class="card" style="margin-bottom:1.25rem;">
          <div class="card-header">What is this?</div>
          <p style="color:var(--text);line-height:1.7;">
            Policy Data Infrastructure is an open-source national policy data platform. It ingests data from 13 federal and state sources, stores indicators at multiple geographic levels in PostGIS, runs statistical analyses, and generates evidence-based policy narratives.
          </p>
          <p style="margin-top:0.75rem;color:var(--text);line-height:1.7;">
            The platform is designed for policy researchers, advocates, and government staff who need fast, reliable access to equity-relevant county and tract-level data without building their own pipelines.
          </p>
        </div>

        <!-- Data Sources -->
        <div class="card" style="margin-bottom:1.25rem;">
          <div class="card-header">Data Sources</div>
          <table class="table" style="margin-top:0.5rem;">
            <thead>
              <tr>
                <th>Source</th>
                <th>Type</th>
                <th>Coverage</th>
              </tr>
            </thead>
            <tbody>
              <template x-for="s in sources" :key="s.name">
                <tr>
                  <td style="font-weight:600;white-space:nowrap;" x-text="s.name"></td>
                  <td>
                    <span class="badge badge-teal" x-text="s.type"></span>
                  </td>
                  <td style="color:var(--text-secondary);font-size:0.875rem;" x-text="s.description"></td>
                </tr>
              </template>
            </tbody>
          </table>
        </div>

        <!-- Methodology -->
        <div class="card" style="margin-bottom:1.25rem;">
          <div class="card-header">Methodology</div>
          <p style="color:var(--text);line-height:1.7;">
            Indicators are stored as raw values with <code style="font-size:0.85em;background:#f1f5f9;padding:0.1em 0.35em;border-radius:4px;">nil</code> for missing or suppressed data. Analysis uses percentile ranking within geographic scope. The pipeline validation gate rejects data loads with &gt;30% null rate, matching federal data quality standards.
          </p>
          <ul style="margin-top:0.75rem;padding-left:1.25rem;color:var(--text);line-height:1.9;font-size:0.9rem;">
            <li>All GEOIDs are zero-padded FIPS strings — never parsed as integers.</li>
            <li>Census null sentinel (<code style="font-size:0.85em;background:#f1f5f9;padding:0.1em 0.35em;border-radius:4px;">-666666666</code>) is converted to <code style="font-size:0.85em;background:#f1f5f9;padding:0.1em 0.35em;border-radius:4px;">null</code> before storage.</li>
            <li>BLS LAUS annual averages are computed client-side from monthly M01–M12 values to avoid silent API data drops.</li>
            <li>Evidence cards link each finding to a specific policy position and geographic scope.</li>
            <li>All data is refreshed on a per-source schedule; vintage year is stored with each indicator.</li>
          </ul>
        </div>

        <!-- Open Source -->
        <div class="card">
          <div class="card-header">Open Source</div>
          <p style="color:var(--text);line-height:1.7;">
            Policy Data Infrastructure is fully open source under the MIT license. Contributions, issue reports, and forks are welcome.
          </p>
          <div style="margin-top:1rem;">
            <a
              href="https://github.com/DojoGenesis/policy-data-infrastructure"
              target="_blank"
              rel="noopener noreferrer"
              class="btn btn-outline"
            >
              View on GitHub
            </a>
          </div>
        </div>
      </div>
    `;
    Alpine.initTree(target);
  }
})();
