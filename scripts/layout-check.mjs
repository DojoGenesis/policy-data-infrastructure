#!/usr/bin/env node
/* PDI layout regression check — horizontal overflow, in the LOADED state.
 *
 * WHY THIS EXISTS
 * ---------------
 * The same defect shipped three times on three pages, each time presenting
 * as unexplained sideways scroll on a phone:
 *
 *   county.html      118px over at 375px
 *   composites.html  193px over at 375px
 *   compare.html   1,216px over at 375px
 *
 * Every prior sweep missed compare.html for one reason: it loaded /compare
 * in its DEFAULT EMPTY STATE — two blank search boxes and "Select two
 * counties above to compare." — and never selected counties. With almost no
 * content there is almost no overflow, so the page passed while being
 * unusable the moment anyone actually used it.
 *
 * So the rule this script encodes: a page whose primary content appears only
 * after user input must be measured AFTER that input. Checking the empty
 * state is checking the one state nobody stays in.
 *
 * WHAT IT REPORTS
 * ---------------
 * Not just "the page overflows" — that was already easy to see and still took
 * three rounds to diagnose. It names the element that actually LEAKS: one
 * that extends past the viewport while no ancestor clips it horizontally.
 * An element inside an overflow-x:auto scroller is contained by design and is
 * not a defect, which is exactly the red herring that misdirected the first
 * two investigations (nav links inside the deliberate .site-nav scroll rail).
 *
 * USAGE
 *   node scripts/layout-check.mjs [baseUrl]
 *   node scripts/layout-check.mjs https://api.policydatainfrastructure.com
 *
 * Requires playwright (npm i playwright). Exits non-zero on any leak.
 */
import { chromium } from 'playwright';

const BASE = process.argv[2] || 'http://localhost:8340';
const WIDTHS = [320, 375, 414, 768];
const LANGS = ['en', 'es'];   // Spanish runs ~15-20% longer and breaks layouts English survives

/* `setup` drives a page into the state a real user reaches. A page with a
   null setup is already meaningful on load. Add an entry here whenever a
   page grows an interaction that reveals content. */
const PAGES = [
  { path: '/', label: 'landing', setup: null },
  { path: '/county?geoid=55025', label: 'county profile', setup: null },
  /* BOTH compare states. The loaded one is where the 1,216px overflow lived,
     but bare /compare is no longer an empty prompt — it renders statewide
     distribution strips and derived entry points on load. Same shape of page
     this script was written for (real content behind zero interaction, just
     fetch-triggered rather than click-triggered), so it needs its own row or
     the checker recreates the exact blind spot it exists to close. */
  { path: '/compare', label: 'compare (empty -> aggregate)', setup: null },
  { path: '/compare?geoid1=55025&geoid2=55079', label: 'compare (RESULTS)', setup: null },
  { path: '/evidence', label: 'evidence', setup: null },
  { path: '/candidates', label: 'candidates', setup: null },
  { path: '/map', label: 'map (TRACT CLICKED)', setup: 'clickTract' },
  { path: '/narrative', label: 'narrative', setup: null },
  { path: '/about', label: 'about', setup: null },
  { path: '/composite', label: 'composites (COMPUTED)', setup: 'compute' },
  { path: '/chat', label: 'chat', setup: null },
];

const SETUPS = {
  async compute(pg) {
    await pg.evaluate(() => {
      const b = [...document.querySelectorAll('button')]
        .find(x => /compute|calcular/i.test(x.textContent) && !x.disabled);
      if (b) b.click();
    });
    await pg.waitForTimeout(4000);
  },
  async clickTract(pg) {
    await pg.evaluate(() => {
      const paths = [...document.querySelectorAll('.leaflet-overlay-pane svg path')];
      const c = paths.map(p => ({ p, b: p.getBoundingClientRect() }))
        .filter(o => o.b.width > 8 && o.b.height > 8)
        .sort((a, b) => (b.b.width * b.b.height) - (a.b.width * a.b.height))[0];
      if (c) c.p.dispatchEvent(new MouseEvent('click', {
        bubbles: true, clientX: c.b.x + c.b.width / 2, clientY: c.b.y + c.b.height / 2,
      }));
    });
    await pg.waitForTimeout(2000);
  },
};

/* An element is a LEAK only if nothing above it clips horizontally.
   Inside a scroller it is contained by design — reporting those is what
   sent the first two investigations after the wrong element. */
function probe() {
  const de = document.documentElement;
  const clips = el => ['auto', 'scroll', 'hidden'].includes(getComputedStyle(el).overflowX);
  const leaks = [];
  for (const el of document.querySelectorAll('body *')) {
    const b = el.getBoundingClientRect();
    if (b.width < 1 || b.right <= de.clientWidth + 1) continue;
    let n = el.parentElement, contained = false;
    while (n && n !== document.body) { if (clips(n)) { contained = true; break; } n = n.parentElement; }
    if (contained) continue;
    leaks.push({
      tag: el.tagName.toLowerCase(),
      cls: (el.className || '').toString().slice(0, 46),
      width: Math.round(b.width),
      right: Math.round(b.right),
    });
  }
  return { overflow: de.scrollWidth - de.clientWidth, leaks: leaks.slice(0, 5) };
}

const browser = await chromium.launch();
let failures = 0, checks = 0;

for (const page of PAGES) {
  const bad = [];
  for (const width of WIDTHS) {
    for (const lang of LANGS) {
      const ctx = await browser.newContext({ viewport: { width, height: 900 } });
      const pg = await ctx.newPage();
      await pg.addInitScript(l => { try { localStorage.setItem('tp-lang', l); } catch (e) {} }, lang);
      try {
        await pg.goto(BASE + page.path, { waitUntil: 'networkidle', timeout: 120000 });
        await pg.waitForTimeout(3000);
        if (page.setup) await SETUPS[page.setup](pg);
        await pg.evaluate(async () => {
          const H = document.documentElement.scrollHeight;
          for (let y = 0; y < H; y += 450) { window.scrollTo(0, y); await new Promise(r => setTimeout(r, 90)); }
        });
        await pg.waitForTimeout(700);
        const r = await pg.evaluate(probe);
        checks++;
        if (r.overflow > 0 || r.leaks.length) bad.push({ width, lang, ...r });
      } catch (e) {
        checks++;
        bad.push({ width, lang, overflow: -1, leaks: [], error: String(e).slice(0, 90) });
      }
      await ctx.close();
    }
  }
  if (bad.length) {
    failures += bad.length;
    console.log(`FAIL  ${page.label}`);
    for (const b of bad) {
      console.log(`        ${b.width}px ${b.lang}: overflow=${b.overflow}px${b.error ? ' ' + b.error : ''}`);
      for (const l of b.leaks) console.log(`          leaks: <${l.tag} class="${l.cls}"> width=${l.width} right=${l.right}`);
    }
  } else {
    console.log(`ok    ${page.label}`);
  }
}

await browser.close();
console.log(`\n${checks} checks across ${PAGES.length} pages x ${WIDTHS.length} widths x ${LANGS.length} languages`);
if (failures) { console.log(`${failures} FAILED`); process.exit(1); }
console.log('no horizontal overflow, no uncontained elements');
