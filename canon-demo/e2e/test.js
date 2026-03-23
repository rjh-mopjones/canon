#!/usr/bin/env node
// Canon demo — Playwright e2e smoke tests
//
// Requires: npm install -g playwright @playwright/test
//           npx playwright install chromium
//
// Usage:    node e2e/test.js
//           make k8s-test-e2e   (from canon-demo/)
//
// Expects frontend on http://localhost:3000 and gateway on http://localhost:8080.

const { chromium } = require('playwright');

const FRONTEND = process.env.FRONTEND_URL || 'http://localhost:3000';
const TIMEOUT = 30_000;

let passed = 0;
let failed = 0;

function pass(name) { console.log(`  ✅ ${name}`); passed++; }
function fail(name, reason) { console.log(`  ❌ ${name}: ${reason}`); failed++; }

(async () => {
  const browser = await chromium.launch();
  const context = await browser.newContext({ viewport: { width: 1280, height: 720 } });
  const errors = [];

  console.log('\nCanon e2e smoke tests\n');

  // ── Test 1: Stations have initial stock ────────────────────────────────
  {
    const page = await context.newPage();
    try {
      await page.goto(FRONTEND, { waitUntil: 'networkidle', timeout: TIMEOUT });
      // Wait for station cards to render with stock data
      await page.waitForSelector('.stn-card-pct', { timeout: 10_000 });
      const pcts = await page.$$eval('.stn-card-pct', els => els.map(e => e.textContent.trim()));

      if (pcts.length < 4) {
        fail('stations_have_initial_stock', `only ${pcts.length} station cards`);
      } else {
        const allAboveZero = pcts.every(p => p !== '0%');
        if (allAboveZero) {
          pass(`stations_have_initial_stock (${pcts.join(', ')})`);
        } else {
          fail('stations_have_initial_stock', `stock values: ${pcts.join(', ')}`);
        }
      }
    } catch (e) {
      fail('stations_have_initial_stock', e.message);
    }
    await page.close();
  }

  // ── Test 2: Stock drains over time ─────────────────────────────────────
  {
    const page = await context.newPage();
    try {
      await page.goto(FRONTEND, { waitUntil: 'networkidle', timeout: TIMEOUT });
      await page.waitForSelector('.stn-card-pct', { timeout: 10_000 });
      const before = await page.$$eval('.stn-card-pct', els =>
        els.map(e => parseFloat(e.textContent))
      );

      await page.waitForTimeout(12_000); // wait for ~4 drain ticks

      const after = await page.$$eval('.stn-card-pct', els =>
        els.map(e => parseFloat(e.textContent))
      );

      const anyDecreased = before.some((v, i) => after[i] < v);
      if (anyDecreased) {
        pass('stock_drains_over_time');
      } else {
        fail('stock_drains_over_time', `before: ${before}, after: ${after}`);
      }
    } catch (e) {
      fail('stock_drains_over_time', e.message);
    }
    await page.close();
  }

  // ── Test 3: Ship popup appears on planet click ─────────────────────────
  {
    const page = await context.newPage();
    try {
      await page.goto(FRONTEND, { waitUntil: 'networkidle', timeout: TIMEOUT });
      const canvas = await page.waitForSelector('canvas', { timeout: 10_000 });
      const box = await canvas.boundingBox();

      // Click Alpha Depot (~18% x, ~26% y)
      await page.mouse.click(box.x + box.width * 0.18, box.y + box.height * 0.26);
      await page.waitForTimeout(1500);

      // Check if ship popup appeared (contains destination list or ship name)
      const popup = await page.$('.ship-popup, .popup, [class*="popup"]');
      const bodyText = await page.evaluate(() => document.body.innerText);
      const hasDestinations = bodyText.includes('Select destination') || bodyText.includes('Alpha Depot');

      if (popup || hasDestinations) {
        pass('ship_popup_on_planet_click');
      } else {
        fail('ship_popup_on_planet_click', 'no popup detected after click');
      }
    } catch (e) {
      fail('ship_popup_on_planet_click', e.message);
    }
    await page.close();
  }

  // ── Test 4: Event log receives events ──────────────────────────────────
  {
    const page = await context.newPage();
    try {
      await page.goto(FRONTEND, { waitUntil: 'networkidle', timeout: TIMEOUT });
      // Wait for drain events to appear in the log (up to 20s)
      let hasEvents = false;
      for (let i = 0; i < 10; i++) {
        await page.waitForTimeout(2000);
        const logBody = await page.$('.log-body');
        if (logBody) {
          const children = await logBody.$$('*');
          if (children.length > 0) {
            hasEvents = true;
            break;
          }
        }
      }

      if (hasEvents) {
        pass('event_log_receives_events');
      } else {
        fail('event_log_receives_events', 'no events in log after 20s');
      }
    } catch (e) {
      fail('event_log_receives_events', e.message);
    }
    await page.close();
  }

  // ── Test 5: Scenarios page renders ─────────────────────────────────────
  {
    const page = await context.newPage();
    try {
      await page.goto(FRONTEND, { waitUntil: 'networkidle', timeout: TIMEOUT });
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
          pass(`scenarios_page_renders (${found.length} missions)`);
        } else {
          fail('scenarios_page_renders', `only found ${found.length}/5 missions: ${found.join(', ')}`);
        }
      }
    } catch (e) {
      fail('scenarios_page_renders', e.message);
    }
    await page.close();
  }

  // ── Test 6: No real console errors ─────────────────────────────────────
  {
    const page = await context.newPage();
    const consoleErrors = [];
    page.on('pageerror', e => consoleErrors.push(e.message));
    page.on('console', msg => {
      if (msg.type() === 'error') consoleErrors.push(msg.text());
    });

    try {
      await page.goto(FRONTEND, { waitUntil: 'networkidle', timeout: TIMEOUT });
      await page.waitForTimeout(5000);

      // Filter known harmless errors
      const real = consoleErrors.filter(e =>
        !e.includes('__trunk_address__') &&
        !e.includes('reactive') &&
        !e.includes('HMR') &&
        !e.includes('unreachable')
      );

      if (real.length === 0) {
        pass('no_console_errors');
      } else {
        fail('no_console_errors', `${real.length} errors: ${real[0].substring(0, 100)}`);
      }
    } catch (e) {
      fail('no_console_errors', e.message);
    }
    await page.close();
  }

  await browser.close();

  // ── Summary ────────────────────────────────────────────────────────────
  console.log(`\n  ${passed} passed, ${failed} failed\n`);
  process.exit(failed > 0 ? 1 : 0);
})();
