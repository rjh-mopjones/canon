#!/usr/bin/env node
// Canon demo — Stress test (multiple tabs × multiple rounds)
//
// Opens N tabs simultaneously, each creating their own session, then
// flies ships and verifies the game works under concurrent load.
// Tabs are closed between rounds to verify session cleanup and that
// new sessions work after old ones expire.
//
// Usage: node e2e/test-stress.js
//        TABS=5 ROUNDS=3 node e2e/test-stress.js

const { chromium } = require('playwright');

const FRONTEND = process.env.FRONTEND_URL || 'http://localhost:3000';
const TABS = parseInt(process.env.TABS || '3');
const ROUNDS = parseInt(process.env.ROUNDS || '2');

let passed = 0;
let failed = 0;

function pass(name, detail) { console.log(`  ✅ ${name}${detail ? ' (' + detail + ')' : ''}`); passed++; }
function fail(name, reason) { console.log(`  ❌ ${name}: ${reason}`); failed++; }

(async () => {
  const browser = await chromium.launch();

  console.log(`\nCanon stress test: ${TABS} tabs × ${ROUNDS} rounds\n`);

  const waitForSession = async (page, timeoutS = 40) => {
    for (let i = 0; i < timeoutS; i++) {
      await page.waitForTimeout(1000);
      const btns = await page.$$eval('button', bs =>
        bs.filter(b => b.offsetParent !== null && !b.disabled).map(b => b.textContent.trim()));
      if (btns.some(b => ['Alpha', 'Beta', 'Gamma', 'Delta'].some(s => b.includes(s)))) return true;
    }
    return false;
  };

  const waitForDocked = async (page, timeoutS = 20) => {
    const t0 = Date.now();
    for (let i = 0; i < timeoutS; i++) {
      await page.waitForTimeout(1000);
      const btns = await page.$$eval('button', bs =>
        bs.filter(b => b.offsetParent !== null && !b.disabled).map(b => b.textContent.trim().substring(0, 40)));
      if (btns.some(b => b.includes('Load') || b.includes('Deliver')))
        return Math.round((Date.now() - t0) / 1000);
      // ◉ button is disabled, so check all visible buttons
      const allVisible = await page.$$eval('button', bs =>
        bs.filter(b => b.offsetParent !== null).map(b => b.textContent.trim().substring(0, 40)));
      if (allVisible.some(b => b.includes('◉')))
        return Math.round((Date.now() - t0) / 1000);
      if (btns.includes('Restart')) return -1;
    }
    return -2;
  };

  for (let round = 1; round <= ROUNDS; round++) {
    console.log(`\n  ── Round ${round} ──`);
    const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
    const pages = [];
    const sessionIds = [];

    // Open tabs
    for (let t = 0; t < TABS; t++) {
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
      await page.goto(FRONTEND, { waitUntil: 'domcontentloaded', timeout: 30000 });
      pages.push(page);
      // Brief wait for session response
      for (let j = 0; j < 5; j++) { await page.waitForTimeout(1000); if (sid) break; }
      sessionIds.push(sid);
    }

    // Wait for all sessions
    const ready = await Promise.all(pages.map(p => waitForSession(p)));
    const readyCount = ready.filter(Boolean).length;
    if (readyCount === TABS) pass(`r${round}_all_sessions`, `${TABS}/${TABS}`);
    else fail(`r${round}_all_sessions`, `${readyCount}/${TABS}`);

    // Unique sessions
    const unique = new Set(sessionIds.filter(Boolean)).size;
    if (unique === TABS) pass(`r${round}_unique_ids`);
    else fail(`r${round}_unique_ids`, `${unique}/${TABS}`);

    // Fly each tab simultaneously — pick available destinations dynamically
    // Wait for initial dock AND all 4 station buttons before flying
    await Promise.all(pages.map(p => waitForDocked(p)));

    const allStations = ['Alpha', 'Beta', 'Gamma', 'Delta'];

    // Wait for all 4 stations to appear in each tab — Kafka event ordering
    // means some stations may bootstrap slower than others under concurrent load
    await Promise.all(pages.map(async (page) => {
      for (let i = 0; i < 30; i++) {
        const btns = await page.$$eval('button', bs =>
          bs.filter(b => b.offsetParent !== null && !b.disabled).map(b => b.textContent.trim()));
        if (allStations.every(s => btns.some(b => b.includes(s)))) return;
        await page.waitForTimeout(1000);
      }
    }));

    // Stagger flights 500ms apart to avoid simultaneous POST contention
    const flights = [];
    for (let i = 0; i < pages.length; i++) {
      const page = pages[i];
      const btns = await page.$$eval('button', bs =>
        bs.filter(b => b.offsetParent !== null && !b.disabled).map(b => b.textContent.trim().substring(0, 30)));
      const candidates = [...allStations.slice(i), ...allStations.slice(0, i)];
      const dest = candidates.find(d => btns.some(b => b.includes(d)));
      if (!dest) { flights.push({ tab: i + 1, dest: '?', ok: false }); continue; }
      try {
        await page.dispatchEvent(`button.dest-tab:has-text("${dest}")`, 'click');
        await page.waitForTimeout(500);
      } catch {
        flights.push({ tab: i + 1, dest, ok: false }); continue;
      }
      flights.push({ tab: i + 1, dest, pending: true });
    }
    for (const f of flights) {
      if (f.pending) {
        const t = await waitForDocked(pages[f.tab - 1]);
        f.time = t; f.ok = t > 0; delete f.pending;
      }
    }

    for (const f of flights) {
      if (f.ok) pass(`r${round}_t${f.tab}_fly`, `→ ${f.dest} ${f.time}s`);
      else fail(`r${round}_t${f.tab}_fly`, `→ ${f.dest}`);
    }

    // Supply chain leg in each tab — wait for dock state to settle first
    await Promise.all(pages.map(p => waitForDocked(p)));

    const legs = await Promise.all(pages.map(async (page, i) => {
      try {
        await page.click('button:has-text("Load"):not([disabled])', { timeout: 5000 });
      } catch {}
      await page.waitForTimeout(3000);
      const text = await page.textContent('body');
      const m = text.match(/for\s+(Alpha|Beta|Gamma|Delta)/);
      if (m) {
        try {
          await page.click(`button.dest-tab:has-text("${m[1]}")`, { force: true, timeout: 5000 });
          const t = await waitForDocked(page);
          return { tab: i + 1, dest: m[1], time: t, ok: t > 0 };
        } catch {
          return { tab: i + 1, dest: m[1], ok: false };
        }
      }
      return { tab: i + 1, ok: false };
    }));

    for (const l of legs) {
      if (l.ok) pass(`r${round}_t${l.tab}_leg`, `→ ${l.dest} ${l.time}s`);
      else fail(`r${round}_t${l.tab}_leg`, 'failed');
    }

    // Close all tabs
    await context.close();
  }

  await browser.close();

  console.log(`\n  ${passed} passed, ${failed} failed\n`);
  process.exit(failed > 0 ? 1 : 0);
})();
