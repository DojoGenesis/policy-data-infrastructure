// ═══════════════════════════════════════════════════════════════════
// PDI ACS Snapshot — embedded county table + derived rankings
// ──────────────────────────────────────────────────────────────────
// GENERATED FILE — do not hand-edit the COUNTIES table below.
// Regenerate from cmd/pdi/frontend/explorer-data.js (the same embedded
// ACS snapshot /explorer renders) with scripts/gen-acs-table.py.
//
// WHY THIS EXISTS. On 2026-07-30 a spike hand-transcribed county
// figures into the homepage and got them wrong — including naming the
// wrong county the state leader on both poverty and uninsured rate. The
// fix is not "transcribe more carefully": it is to make transcription
// impossible. Callers ask for a ranking and receive {county, value};
// there is no code path here that lets a caller assert a leader.
//
// The table is geometry-free on purpose. explorer-data.js is ~120KB
// dominated by polygon coordinates the homepage has no use for; this is
// the same 72 counties x 10 fields at ~22KB, with no <script> dependency
// on the map payload.
//
// THESE TEN FIELDS ARE THE ONLY ONES THAT EXIST. There is no transit
// access, life expectancy, unemployment, graduation rate, broadband, or
// food insecurity in any PDI source — every one of those was a spike
// fabrication. Do not add a field here that is not in the snapshot.
//
// Usage:
//   <script src="/static/lib/acs.js"></script>
//   PDIAcs.extreme('poverty_rate', 'max')  -> {geoid, name, value}
//   PDIAcs.topN('uninsured_rate', 5)       -> [{geoid, name, value}, ...]
//   PDIAcs.spread('uninsured_rate')        -> {max, min, ratio}
//   PDIAcs.format('median_hh_income', v)   -> "$106,076"
//   PDIAcs.label('poverty_rate', 'es')     -> "Tasa de pobreza"
// ═══════════════════════════════════════════════════════════════════

(function () {
  'use strict';

  /* Field order of the v[] tuple in COUNTIES. */
  var ORDER = [
    'median_hh_income', 'poverty_rate', 'pct_cost_burdened',
    'pct_severely_cost_burdened', 'uninsured_rate', 'pct_bachelors_or_higher',
    'pct_poc', 'pct_hispanic', 'pct_non_hispanic_black', 'total_population'
  ];

  /* direction: 'worse' = a higher value is a worse outcome, 'better' = a
     higher value is a better outcome, 'neutral' = descriptive only.
     Neutral fields MUST NEVER be phrased as better or worse — pct_poc,
     pct_hispanic and pct_non_hispanic_black describe who lives somewhere,
     not how well they are doing. */
  var FIELDS = {
    median_hh_income:           {en: 'Median household income',   es: 'Ingreso familiar medio',            fmt: 'dollar',  dir: 'better'},
    poverty_rate:               {en: 'Poverty rate',              es: 'Tasa de pobreza',                   fmt: 'percent', dir: 'worse'},
    pct_cost_burdened:          {en: 'Cost-burdened households',  es: 'Hogares con carga de costos',       fmt: 'percent', dir: 'worse'},
    pct_severely_cost_burdened: {en: 'Severely cost-burdened',    es: 'Carga de costos severa',            fmt: 'percent', dir: 'worse'},
    uninsured_rate:             {en: 'Uninsured rate',            es: 'Tasa de personas sin seguro',       fmt: 'percent', dir: 'worse'},
    pct_bachelors_or_higher:    {en: "Bachelor's degree or higher", es: 'Licenciatura o superior',         fmt: 'percent', dir: 'better'},
    pct_poc:                    {en: 'People of color',           es: 'Personas de color',                 fmt: 'percent', dir: 'neutral'},
    pct_hispanic:               {en: 'Hispanic or Latino',        es: 'Hispano o latino',                  fmt: 'percent', dir: 'neutral'},
    pct_non_hispanic_black:     {en: 'Black, non-Hispanic',       es: 'Negro, no hispano',                 fmt: 'percent', dir: 'neutral'},
    total_population:           {en: 'Total population',          es: 'Poblacion total',                   fmt: 'count',   dir: 'neutral'}
  };

  var COUNTIES = [
    {g:"55001",n:"Adams",v:[59442,14.1,26.8,12.1,4.6,17.5,12.4,4.4,2.0,21123]},
    {g:"55003",n:"Ashland",v:[62462,13.4,23.9,9.7,6.4,24.5,18.8,3.0,0.5,16080]},
    {g:"55005",n:"Barron",v:[65933,11.0,24.8,9.8,6.2,23.2,9.2,3.2,1.3,46797]},
    {g:"55007",n:"Bayfield",v:[71151,9.8,22.7,9.2,5.3,35.0,16.3,2.1,0.7,16575]},
    {g:"55009",n:"Brown",v:[79649,9.1,24.0,10.0,5.1,33.5,22.7,10.3,2.4,270892]},
    {g:"55011",n:"Buffalo",v:[70479,8.4,27.0,10.3,4.2,22.1,6.4,2.9,0.4,13393]},
    {g:"55013",n:"Burnett",v:[62819,12.8,24.4,11.3,6.0,22.4,10.4,2.0,0.9,16916]},
    {g:"55015",n:"Calumet",v:[88810,5.9,20.2,8.1,2.6,33.0,12.5,5.7,0.9,52942]},
    {g:"55017",n:"Chippewa",v:[74680,10.2,23.7,9.7,4.4,24.2,7.6,2.0,1.4,66799]},
    {g:"55019",n:"Clark",v:[67400,12.1,20.1,8.6,22.5,13.0,9.5,6.4,0.3,34739]},
    {g:"55021",n:"Columbia",v:[85351,7.8,22.1,9.0,5.0,26.0,9.7,4.1,1.6,58272]},
    {g:"55023",n:"Crawford",v:[64094,13.8,23.1,9.3,7.0,18.3,7.3,2.0,1.6,16042]},
    {g:"55025",n:"Dane",v:[89975,10.6,31.3,13.5,3.6,55.3,24.2,7.8,5.1,572674]},
    {g:"55027",n:"Dodge",v:[75929,8.5,24.2,9.7,4.1,20.3,12.7,6.7,2.5,88742]},
    {g:"55029",n:"Door",v:[74795,7.7,27.2,12.7,3.5,37.4,8.5,4.2,0.0,30445]},
    {g:"55031",n:"Douglas",v:[75099,10.8,24.4,11.0,4.0,28.4,10.3,1.9,0.8,44229]},
    {g:"55033",n:"Dunn",v:[74824,10.7,24.8,10.9,4.9,29.6,9.5,2.5,0.9,45527]},
    {g:"55035",n:"Eau Claire",v:[74546,11.8,24.7,10.7,6.2,36.8,12.8,3.2,1.2,107116]},
    {g:"55037",n:"Florence",v:[61086,7.3,20.3,11.2,5.8,25.0,5.1,0.5,0.3,4646]},
    {g:"55039",n:"Fond du Lac",v:[74275,9.2,23.3,8.9,4.1,22.3,14.0,6.8,1.7,104137]},
    {g:"55041",n:"Forest",v:[61071,15.2,21.2,10.3,7.7,16.1,22.3,2.2,0.4,9369]},
    {g:"55043",n:"Grant",v:[66858,13.0,23.9,10.5,8.0,25.6,7.0,2.6,1.5,51770]},
    {g:"55045",n:"Green",v:[82852,7.7,21.4,8.2,3.8,27.7,8.0,4.2,0.8,37017]},
    {g:"55047",n:"Green Lake",v:[68196,13.8,24.7,10.9,9.0,22.8,9.5,5.6,0.5,19263]},
    {g:"55049",n:"Iowa",v:[82182,7.6,23.6,9.3,3.4,28.4,7.6,2.3,0.8,23867]},
    {g:"55051",n:"Iron",v:[60625,10.5,21.6,8.4,6.1,26.0,7.1,1.6,0.0,6210]},
    {g:"55053",n:"Jackson",v:[68110,12.4,27.6,12.1,6.8,15.9,14.7,3.7,1.9,20981]},
    {g:"55055",n:"Jefferson",v:[83750,8.1,23.9,9.9,4.8,28.5,13.0,8.5,0.9,86003]},
    {g:"55057",n:"Juneau",v:[67270,13.0,25.4,10.8,5.4,16.9,12.6,3.0,2.3,26689]},
    {g:"55059",n:"Kenosha",v:[81239,10.5,28.2,13.1,5.7,31.2,28.5,15.2,5.9,168438]},
    {g:"55061",n:"Kewaunee",v:[82725,7.6,20.2,7.5,3.9,21.9,8.0,4.1,0.4,20664]},
    {g:"55063",n:"La Crosse",v:[73013,12.4,27.6,11.9,4.4,36.5,12.6,2.7,1.5,120488]},
    {g:"55065",n:"Lafayette",v:[76462,11.9,21.9,9.7,10.5,22.6,10.3,7.1,0.4,16942]},
    {g:"55067",n:"Langlade",v:[55878,13.5,22.5,9.8,5.1,17.0,8.5,2.5,0.4,19490]},
    {g:"55069",n:"Lincoln",v:[68164,10.6,23.4,8.9,4.7,19.6,7.0,2.3,0.5,28426]},
    {g:"55071",n:"Manitowoc",v:[69148,9.6,19.0,8.7,4.1,22.6,12.7,5.5,0.8,81406]},
    {g:"55073",n:"Marathon",v:[77884,8.7,21.4,8.7,5.8,28.1,13.8,3.5,0.6,138403]},
    {g:"55075",n:"Marinette",v:[63809,9.8,19.4,7.3,4.2,19.8,6.6,2.9,0.5,42046]},
    {g:"55077",n:"Marquette",v:[65681,13.1,24.1,9.7,5.5,16.2,7.7,3.7,0.5,15746]},
    {g:"55078",n:"Menominee",v:[62108,21.7,26.8,13.3,20.5,18.1,87.0,5.1,0.8,4252]},
    {g:"55079",n:"Milwaukee",v:[64435,17.2,35.3,17.0,7.2,34.7,51.9,17.0,25.4,926331]},
    {g:"55081",n:"Monroe",v:[69467,11.7,23.0,9.8,8.9,21.3,11.9,5.8,1.5,46208]},
    {g:"55083",n:"Oconto",v:[75049,8.2,21.1,7.8,3.5,18.9,6.7,2.3,0.1,39589]},
    {g:"55085",n:"Oneida",v:[69371,8.5,22.3,10.2,4.2,28.7,6.6,1.7,0.5,38167]},
    {g:"55087",n:"Outagamie",v:[85069,6.3,21.2,7.8,4.0,32.7,15.1,5.3,1.5,192826]},
    {g:"55089",n:"Ozaukee",v:[96996,5.0,26.6,10.3,2.5,51.2,10.8,3.7,1.5,92966]},
    {g:"55091",n:"Pepin",v:[75256,10.2,22.0,7.7,9.1,23.0,5.9,2.7,0.4,7431]},
    {g:"55093",n:"Pierce",v:[92109,9.8,22.7,9.1,3.1,33.5,8.7,3.0,0.6,42584]},
    {g:"55095",n:"Polk",v:[77219,8.2,24.0,9.4,6.2,24.3,7.0,2.2,0.2,45555]},
    {g:"55097",n:"Portage",v:[76070,10.1,21.7,9.2,4.8,34.0,11.0,4.2,0.8,70832]},
    {g:"55099",n:"Price",v:[60546,11.9,21.4,9.0,5.8,17.9,6.9,1.8,0.1,14092]},
    {g:"55101",n:"Racine",v:[78096,10.3,27.6,12.0,5.7,28.4,31.4,15.1,10.3,197532]},
    {g:"55103",n:"Richland",v:[66420,14.5,24.6,10.0,7.0,21.2,7.9,3.3,0.3,17205]},
    {g:"55105",n:"Rock",v:[75673,9.3,25.4,10.2,5.5,25.8,20.4,10.1,4.6,164350]},
    {g:"55107",n:"Rusk",v:[59944,13.1,22.7,11.7,5.7,16.9,6.9,2.1,1.2,14179]},
    {g:"55111",n:"Sauk",v:[79541,8.1,23.5,10.4,6.0,27.3,13.7,6.8,0.8,66009]},
    {g:"55113",n:"Sawyer",v:[60801,15.6,25.0,11.3,6.5,27.4,23.1,2.4,0.6,18476]},
    {g:"55115",n:"Shawano",v:[66479,11.1,23.1,9.5,6.1,19.1,15.4,3.3,0.6,41039]},
    {g:"55117",n:"Sheboygan",v:[73094,8.5,20.7,8.1,4.0,27.5,19.2,7.8,2.1,117991]},
    {g:"55109",n:"St. Croix",v:[103046,6.6,22.3,9.6,4.1,39.5,8.6,3.1,0.7,96033]},
    {g:"55119",n:"Taylor",v:[69350,9.6,20.9,7.6,7.4,17.0,6.2,3.0,0.3,20024]},
    {g:"55121",n:"Trempealeau",v:[76313,9.9,23.3,9.3,8.7,20.9,16.1,13.0,0.3,30839]},
    {g:"55123",n:"Vernon",v:[71893,13.1,22.9,9.1,15.0,24.9,5.7,1.8,0.3,31049]},
    {g:"55125",n:"Vilas",v:[68431,11.1,23.5,10.4,5.6,30.9,15.2,2.3,0.3,23647]},
    {g:"55127",n:"Walworth",v:[80520,9.4,26.0,12.3,5.7,32.4,16.9,12.1,0.7,105657]},
    {g:"55129",n:"Washburn",v:[63441,11.5,24.6,8.8,5.3,25.5,7.6,2.1,0.2,16854]},
    {g:"55131",n:"Washington",v:[96359,5.8,23.5,9.5,2.8,35.2,9.8,3.8,1.3,137879]},
    {g:"55133",n:"Waukesha",v:[106076,5.2,24.0,10.6,3.1,48.9,15.0,5.7,1.5,411762]},
    {g:"55135",n:"Waupaca",v:[72830,10.4,25.9,11.1,5.9,20.5,7.6,3.9,0.5,51569]},
    {g:"55137",n:"Waushara",v:[67107,10.9,24.1,11.1,9.1,17.7,12.8,7.0,1.8,24868]},
    {g:"55139",n:"Winnebago",v:[74925,11.3,24.2,10.1,4.5,31.1,15.1,5.1,2.7,171769]},
    {g:"55141",n:"Wood",v:[67989,10.8,21.8,9.4,4.5,21.4,9.8,3.7,0.6,74004]}
  ];

  function idx(field) { return ORDER.indexOf(field); }

  function valueOf(row, field) {
    var i = idx(field);
    if (i < 0) { return null; }
    var v = row.v[i];
    return (v === null || v === undefined || isNaN(v)) ? null : v;
  }

  /* Every county as {geoid, name, value} for one field, missing dropped.
     Ties are broken by county name so repeated calls are stable — an
     unstable sort would let the page name a different "highest" county
     between two renders of the same data. */
  function series(field) {
    var out = [];
    for (var i = 0; i < COUNTIES.length; i++) {
      var v = valueOf(COUNTIES[i], field);
      if (v === null) { continue; }
      out.push({geoid: COUNTIES[i].g, name: COUNTIES[i].n, value: v});
    }
    return out;
  }

  function rankBy(field, descending) {
    var s = series(field);
    var desc = descending !== false;
    s.sort(function (a, b) {
      if (a.value !== b.value) { return desc ? b.value - a.value : a.value - b.value; }
      return a.name < b.name ? -1 : (a.name > b.name ? 1 : 0);
    });
    return s;
  }

  function topN(field, n, descending) {
    return rankBy(field, descending).slice(0, n > 0 ? n : 0);
  }

  /* which: 'max' | 'min'. Returns null rather than guessing when the
     field is unknown or every value is missing — absent beats wrong. */
  function extreme(field, which) {
    var s = rankBy(field, which !== 'min');
    return s.length ? s[0] : null;
  }

  function spread(field) {
    var s = rankBy(field, true);
    if (s.length < 2) { return null; }
    var hi = s[0], lo = s[s.length - 1];
    return {
      max: hi,
      min: lo,
      ratio: (lo.value ? hi.value / lo.value : null)
    };
  }

  function get(geoid) {
    for (var i = 0; i < COUNTIES.length; i++) {
      if (COUNTIES[i].g === geoid) {
        var o = {geoid: COUNTIES[i].g, name: COUNTIES[i].n};
        for (var j = 0; j < ORDER.length; j++) { o[ORDER[j]] = valueOf(COUNTIES[i], ORDER[j]); }
        return o;
      }
    }
    return null;
  }

  function format(field, v) {
    if (v === null || v === undefined || isNaN(v)) { return '\u2014'; }
    var f = FIELDS[field];
    var kind = f ? f.fmt : 'count';
    if (kind === 'dollar')  { return '$' + Math.round(v).toLocaleString('en-US'); }
    /* Always one decimal. Math.round(v*10)/10 renders 5.0 as "5" and 13.0
       as "13", which reads as a different precision than the "21.7%" sitting
       next to it in the same column. */
    if (kind === 'percent') { return v.toFixed(1) + '%'; }
    return Math.round(v).toLocaleString('en-US');
  }

  function label(field, lang) {
    var f = FIELDS[field];
    if (!f) { return field; }
    return (lang === 'es' && f.es) ? f.es : f.en;
  }

  function direction(field) {
    var f = FIELDS[field];
    return f ? f.dir : 'neutral';
  }

  var PDIAcs = {
    /* Provenance string every consumer should surface verbatim. These
       numbers are an embedded ACS snapshot, not a live API read, and a
       reader is entitled to know which. */
    PROVENANCE: 'ACS snapshot',
    fields: FIELDS,
    order: ORDER,
    count: COUNTIES.length,
    get: get,
    series: series,
    rankBy: rankBy,
    topN: topN,
    extreme: extreme,
    spread: spread,
    format: format,
    label: label,
    direction: direction
  };

  window.PDIAcs = PDIAcs;
})();
