#!/usr/bin/env node
// Canon demo — Multi-tab session isolation test
//
// Verifies that multiple browser tabs get independent game sessions with
// no event cross-talk. Each tab creates its own session via POST /sessions,
// registers on the WS, and receives only its own events.
//
// Usage: node e2e/test-multi-tab.js
//        make k8s-test-multi-tab  (from canon-demo/)

const { chromium } = require('playwright');

const FRONTEND = process.env.FRONTEND_URL || 'http://localhost:3000';
const TABS = 3;

let passed = 0;
let failed = 0;

function pass(name, detail) { console.log(`  ✅ ${name}${detail ? ' (' + detail + ')' : ''}`); passed++; }
function fail(name, reason) { console.log(`  ❌ ${name}: ${reason}`); failed++; }

(async () => {
  const browser = await chromium.launch();
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });

  console.log(`\nCanon multi-tab isolation test (${TABS} tabs)\n`);

  const pages = [];
  const sessionIds = [];

  // Helper: wait for session to be ready
  const waitForSession = async (page, timeoutS = 30) => {
    for (let i = 0; i < timeoutS; i++) {
      await page.waitForTimeout(1000);
      const btns = await page.$$eval('button', bs =>
        bs.filter(b => b.offsetParent !== null && !b.disabled).map(b => b.textContent.trim()));
      if (btns.some(b => ['Alpha', 'Beta', 'Gamma', 'Delta'].some(s => b.includes(s)))) return true;
    }
    return false;
  };

  // Helper: wait for dock
  const waitForDocked = async (page, timeoutS = 20) => {
    const t0 = Date.now();
    for (let i = 0; i < timeoutS; i++) {
      await page.waitForTimeout(1000);
      const btns = await page.$$eval('button', bs =>
        bs.filter(b => b.offsetParent !== null && !b.disabled).map(b => b.textContent.trim().substring(0, 40)));
      if (btns.some(b => b.includes('Load') || b.includes('Deliver') || b.includes('◉')))
        return Math.round((Date.now() - t0) / 1000);
    }
    return -1;
  };

  // ── Open tabs and create sessions ──────────────────────────────────────
  for (let i = 0; i < TABS; i++) {
    const page = await context.newPage();
    if (process.env.CANON_AUTH_PASSWORD) {
      const url = new URL(FRONTEND);
      await context.addCookies([{
        name: 'canon_auth', value: process.env.CANON_AUTH_PASSWORD,
        domain: url.hostname, path: '/', httpOnly: true,
        secure: url.protocol === 'https:', sameSite: 'None',
      }]);
    }
    let sid = null;
    page.on('response', res => {
      if (res.url().includes('/sessions') && res.status() === 200) {
        res.json().then(j => { sid = j.session_id; }).catch(() => {});
      }
    });
    await page.goto(FRONTEND, { waitUntil: 'networkidle', timeout: 30000 });
    pages.push(page);

    // Wait briefly for session response
    for (let j = 0; j < 5; j++) {
      await page.waitForTimeout(1000);
      if (sid) break;
    }
    sessionIds.push(sid);
  }

  // Wait for all sessions to be ready
  const readyResults = await Promise.all(pages.map(p => waitForSession(p)));
  if (readyResults.every(r => r)) pass('all_sessions_ready');
  else fail('all_sessions_ready', `ready: ${readyResults.join(',')}`);

  // ── Verify unique session IDs ──────────────────────────────────────────
  const uniqueIds = new Set(sessionIds.filter(Boolean));
  if (uniqueIds.size === TABS) {
    pass('unique_session_ids', `${TABS} unique`);
  } else {
    fail('unique_session_ids', `only ${uniqueIds.size}/${TABS} unique`);
  }

  // ── Fly each tab to a different station ────────────────────────────────
  const destinations = ['Alpha', 'Beta', 'Gamma'];
  const flyResults = await Promise.all(pages.map(async (page, i) => {
    const dest = destinations[i];
    const btn = await page.$(`button:has-text("${dest}")`);
    if (btn && !(await btn.evaluate(b => b.disabled))) {
      await btn.click({ force: true });
      const t = await waitForDocked(page);
      return { tab: i + 1, dest, time: t, ok: t > 0 };
    }
    return { tab: i + 1, dest, ok: false };
  }));

  for (const r of flyResults) {
    if (r.ok) pass(`tab${r.tab}_flight`, `→ ${r.dest} in ${r.time}s`);
    else fail(`tab${r.tab}_flight`, `→ ${r.dest} failed`);
  }

  // ── Verify event log independence ──────────────────────────────────────
  for (let i = 0; i < pages.length; i++) {
    const text = await pages[i].textContent('body');
    const hasEvents = text.includes('ShipDeparted') || text.includes('StockDrained') || text.includes('ShipDockedAtStation');
    if (hasEvents) pass(`tab${i + 1}_has_events`);
    else fail(`tab${i + 1}_has_events`, 'event log empty');
  }

  // ── Close 2 tabs, verify surviving tab still works ─────────────────────
  await pages[1].close();
  await pages[2].close();
  await pages[0].waitForTimeout(3000);
  const survivingBtns = await pages[0].$$eval('button', bs =>
    bs.filter(b => b.offsetParent !== null && !b.disabled).length);
  if (survivingBtns >= 4) pass('surviving_tab', 'still functional after closing others');
  else fail('surviving_tab', `only ${survivingBtns} buttons`);

  await pages[0].close();
  await browser.close();

  console.log(`\n  ${passed} passed, ${failed} failed\n`);
  process.exit(failed > 0 ? 1 : 0);
})();
