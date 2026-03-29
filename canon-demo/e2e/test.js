#!/usr/bin/env node
// Canon demo — Playwright e2e smoke tests
//
// Requires: npm install playwright
//           npx playwright install chromium
//
// Usage:    node e2e/test.js
//           make k8s-test-e2e   (from canon-demo/)
//
// Expects frontend on http://localhost:3000 and gateway on http://localhost:8080.
// All tests share a single page/session to avoid pipeline saturation.

const { chromium } = require('playwright');

const FRONTEND = process.env.FRONTEND_URL || 'http://localhost:3000';
const TIMEOUT = 30_000;

let passed = 0;
let failed = 0;

function pass(name, detail) { console.log(`  ✅ ${name}${detail ? ' (' + detail + ')' : ''}`); passed++; }
function fail(name, reason) { console.log(`  ❌ ${name}: ${reason}`); failed++; }

(async () => {
  const browser = await chromium.launch();
  const context = await browser.newContext({ viewport: { width: 1280, height: 720 } });
  const page = await context.newPage();

  // If CANON_AUTH_PASSWORD is set, add auth cookie to the browser context.
  // Don't use setExtraHTTPHeaders — it sends headers to ALL origins (breaks font CORS).
  if (process.env.CANON_AUTH_PASSWORD) {
    const url = new URL(FRONTEND);
    await context.addCookies([{
      name: 'canon_auth',
      value: process.env.CANON_AUTH_PASSWORD,
      domain: url.hostname,
      path: '/',
      httpOnly: true,
      secure: url.protocol === 'https:',
      sameSite: 'None',
    }]);
  }

  // Collect console errors across all tests
  const consoleErrors = [];
  page.on('pageerror', e => consoleErrors.push(e.message));
  page.on('console', msg => {
    if (msg.type() === 'error') consoleErrors.push(msg.text());
  });

  console.log('\nCanon e2e smoke tests\n');

  // Load page once, wait for session creation + hydration
  await page.goto(FRONTEND, { waitUntil: 'domcontentloaded', timeout: TIMEOUT });

  // Wait for station cards to appear (session created + hydrated)
  try {
    await page.waitForSelector('.stn-card-pct', { timeout: 20_000 });
  } catch {
    fail('session_setup', 'station cards never appeared (session bootstrap failed)');
    await browser.close();
    console.log(`\n  ${passed} passed, ${failed} failed\n`);
    process.exit(1);
  }

  // ── Test 1: Stations have initial stock ────────────────────────────────
  // With the polling projection model, stock starts at 0% and fills from
  // bootstrap events within 2-5s. Wait for stocks to populate.
  {
    let pcts = [];
    for (let i = 0; i < 15; i++) {
      pcts = await page.$$eval('.stn-card-pct', els => els.map(e => e.textContent.trim()));
      if (pcts.length >= 4 && pcts.every(p => p !== '0%' && p !== 'NaN%')) break;
      await page.waitForTimeout(1000);
    }
    if (pcts.length < 4) {
      fail('stations_have_initial_stock', `only ${pcts.length} station cards`);
    } else {
      const allAboveZero = pcts.every(p => p !== '0%');
      if (allAboveZero) {
        pass('stations_have_initial_stock', pcts.join(', '));
      } else {
        fail('stations_have_initial_stock', `stock values: ${pcts.join(', ')}`);
      }
    }
  }

  // ── Test 2: Stock drains over time ─────────────────────────────────────
  {
    // Wait for stocks to be populated (bootstrap events take 2-5s to flow).
    // Stock starts at 0% with the projection model — events fill it.
    let before = [0, 0, 0, 0];
    for (let i = 0; i < 30; i++) {
      await page.waitForTimeout(1000);
      before = await page.$$eval('.stn-card-pct', els =>
        els.map(e => parseFloat(e.textContent))
      );
      if (before.every(v => v > 0)) break;
    }

    // Wait for drain events (drain starts ~5s after bootstrap, ticks every 2.5s).
    await page.waitForTimeout(30_000);

    const after = await page.$$eval('.stn-card-pct', els =>
      els.map(e => parseFloat(e.textContent))
    );

    const anyDecreased = before.some((v, i) => after[i] < v);
    if (anyDecreased) {
      pass('stock_drains_over_time');
    } else {
      fail('stock_drains_over_time', `before: ${before}, after: ${after}`);
    }
  }

  // ── Test 3: Ship popup appears on planet click ─────────────────────────
  {
    const canvas = await page.$('canvas');
    if (canvas) {
      const box = await canvas.boundingBox();
      // Click Alpha Depot (~18% x, ~26% y)
      await page.mouse.click(box.x + box.width * 0.18, box.y + box.height * 0.26);
      await page.waitForTimeout(1500);

      const popup = await page.$('.ship-popup, .popup, .cmd-popup, [class*="popup"]');
      const bodyText = await page.evaluate(() => document.body.innerText);
      const hasDestinations = bodyText.includes('Select destination') || bodyText.includes('Alpha Depot');

      if (popup || hasDestinations) {
        pass('ship_popup_on_planet_click');
      } else {
        fail('ship_popup_on_planet_click', 'no popup detected after click');
      }

      // Close popup if open
      const backdrop = await page.$('.popup-backdrop');
      if (backdrop) await backdrop.click();
      await page.waitForTimeout(500);
    } else {
      fail('ship_popup_on_planet_click', 'no canvas element');
    }
  }

  // ── Test 4: Event log receives events ──────────────────────────────────
  {
    // By now drain has been running for ~20s, events should be in the log
    let hasEvents = false;
    for (let i = 0; i < 5; i++) {
      const logBody = await page.$('.log-body');
      if (logBody) {
        const children = await logBody.$$('*');
        if (children.length > 0) {
          hasEvents = true;
          break;
        }
      }
      await page.waitForTimeout(2000);
    }

    if (hasEvents) {
      pass('event_log_receives_events');
    } else {
      fail('event_log_receives_events', 'no events in log');
    }
  }

  // ── Test 5: Scenarios page renders ─────────────────────────────────────
  {
    const tab = await page.$('text=Scenarios');
    if (!tab) {
      fail('scenarios_page_renders', 'no Scenarios tab found');
    } else {
      await tab.click();
      await page.waitForTimeout(2000);
      const text = await page.evaluate(() => document.body.innerText);
      const missionKeywords = ['Stranded Cargo', 'Ghost Ship', 'Resupply Crisis', 'Cassandra Incident', 'Duplicate Signal'];
      const found = missionKeywords.filter(k => text.includes(k));
      if (found.length >= 5) {
        pass('scenarios_page_renders', `${found.length} missions`);
      } else {
        fail('scenarios_page_renders', `only found ${found.length}/5 missions`);
      }

      // Go back to Live Fleet
      const lf = await page.$('text=Live Fleet');
      if (lf) await lf.click();
      await page.waitForTimeout(500);
    }
  }

  // ── Test 6: No real console errors ─────────────────────────────────────
  {
    const real = consoleErrors.filter(e =>
      !e.includes('__trunk_address__') &&
      !e.includes('reactive') &&
      !e.includes('HMR') &&
      !e.includes('unreachable') &&
      !e.includes('integrity')
    );

    if (real.length === 0) {
      pass('no_console_errors');
    } else {
      fail('no_console_errors', `${real.length} errors: ${real[0].substring(0, 100)}`);
    }
  }

  await page.close();
  await browser.close();

  // ── Summary ────────────────────────────────────────────────────────────
  console.log(`\n  ${passed} passed, ${failed} failed\n`);
  process.exit(failed > 0 ? 1 : 0);
})();
