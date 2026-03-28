#!/usr/bin/env node
// Canon demo — Regression tests for polling transport (#345)
//
// These tests catch bugs specific to the in-memory projection + polling
// architecture: double-counted stock, missing cargo flow, skipped transit.
//
// Usage: node e2e/test-regression.js
//        FRONTEND_URL=https://canon-staging.mopjones.com node e2e/test-regression.js

const { chromium } = require('playwright');

const FRONTEND = process.env.FRONTEND_URL || 'http://localhost:3000';

let passed = 0;
let failed = 0;

function pass(name, detail) { console.log(`  \u2705 ${name}${detail ? ' (' + detail + ')' : ''}`); passed++; }
function fail(name, reason) { console.log(`  \u274c ${name}: ${reason}`); failed++; }

(async () => {
  const browser = await chromium.launch();
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  const page = await context.newPage();

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

  console.log('\nCanon regression tests (polling transport #345)\n');

  await page.goto(FRONTEND, { waitUntil: 'networkidle', timeout: 30000 });

  // Wait for session hydration
  try {
    await page.waitForSelector('.stn-card-pct', { timeout: 20_000 });
  } catch {
    fail('session_setup', 'station cards never appeared');
    await browser.close();
    console.log(`\n  ${passed} passed, ${failed} failed\n`);
    process.exit(1);
  }

  // Helper: get all station stock percentages as numbers
  const stationPcts = async () => page.$$eval('.stn-card-pct', els =>
    els.map(e => parseFloat(e.textContent)));

  // Helper: get enabled button texts
  const enabledBtns = async () => page.$$eval('button', bs =>
    bs.filter(b => b.offsetParent !== null && !b.disabled).map(b => b.textContent.trim().substring(0, 60)));

  // Helper: wait for docked state (destination buttons or Load/Deliver appear)
  const waitForDocked = async (timeoutS = 30) => {
    for (let i = 0; i < timeoutS * 2; i++) {
      await page.waitForTimeout(500);
      const btns = await enabledBtns();
      if (btns.some(b => b.includes('Load') || b.includes('Deliver'))) return true;
      const allVisible = await page.$$eval('button', bs =>
        bs.filter(b => b.offsetParent !== null).map(b => b.textContent.trim().substring(0, 40)));
      if (allVisible.some(b => b.includes('\u25c9'))) return true;
      if (btns.includes('Restart')) return false;
    }
    return false;
  };

  // ── Test 1: Initial stock matches bootstrap values ────────────────────
  // Bootstrap: Alpha 85%, Beta 60%, Gamma 40%, Delta 75%
  // Allow a tolerance band for drain ticks that may have fired.
  // Stock should NEVER be above the bootstrap values (double-counting bug).
  {
    // Wait a bit for initial hydration to stabilize
    await page.waitForTimeout(3000);
    const pcts = await stationPcts();

    const expected = [85, 60, 40, 75];
    const tolerance = 15; // drain may have reduced stock by up to 15%
    let allOk = true;
    const details = [];

    for (let i = 0; i < 4; i++) {
      const pct = pcts[i];
      const exp = expected[i];

      // Stock should be at or below bootstrap value (with small margin for
      // rounding), never significantly above it
      if (pct > exp + 5) {
        details.push(`station ${i}: ${pct.toFixed(1)}% > ${exp}% (double-counted)`);
        allOk = false;
      } else if (pct < exp - tolerance) {
        details.push(`station ${i}: ${pct.toFixed(1)}% < ${exp - tolerance}% (too low)`);
        allOk = false;
      } else {
        details.push(`${pct.toFixed(1)}%`);
      }
    }

    if (allOk) {
      pass('initial_stock_correct', details.join(', '));
    } else {
      fail('initial_stock_correct', details.join('; '));
    }
  }

  // ── Test 2: Stock doesn't spike above bootstrap on fresh session ──────
  // Monitor stock for 10s — values should only decrease (drain), never
  // increase above bootstrap. A spike indicates double-counting from
  // CargoReceived events applied on top of seeded values.
  {
    const samples = [];
    for (let i = 0; i < 10; i++) {
      await page.waitForTimeout(1000);
      samples.push(await stationPcts());
    }

    const maxSeen = [0, 0, 0, 0];
    for (const sample of samples) {
      for (let i = 0; i < 4; i++) {
        if (sample[i] > maxSeen[i]) maxSeen[i] = sample[i];
      }
    }

    const expected = [85, 60, 40, 75];
    const spikes = [];
    for (let i = 0; i < 4; i++) {
      if (maxSeen[i] > expected[i] + 5) {
        spikes.push(`station ${i}: peaked at ${maxSeen[i].toFixed(1)}% (expected max ~${expected[i]}%)`);
      }
    }

    if (spikes.length === 0) {
      pass('no_stock_spike', `max seen: ${maxSeen.map(v => v.toFixed(1) + '%').join(', ')}`);
    } else {
      fail('no_stock_spike', spikes.join('; '));
    }
  }

  // ── Test 3: Ship shows transit state during flight ────────────────────
  // After clicking a destination, the ship status should change to "transit"
  // (visible via "En route" text or transit indicator) before arriving.
  // With polling, transit state must persist long enough to be observed.
  {
    // First fly to a station if not already docked
    const btns = await enabledBtns();
    const destName = ['Alpha', 'Beta', 'Gamma', 'Delta'].find(d =>
      btns.some(b => b.includes(d) && !b.includes('\u25c9')));

    if (!destName) {
      fail('transit_visible', 'no destination button available');
    } else {
      const destBtn = await page.$(`button:has-text("${destName}")`);
      if (!destBtn) {
        fail('transit_visible', `${destName} button not found`);
      } else {
        await destBtn.click({ force: true });

        // Poll rapidly (100ms) to catch the transit state
        let sawTransit = false;
        for (let i = 0; i < 100; i++) {
          await page.waitForTimeout(100);
          const bodyText = await page.evaluate(() => document.body.innerText);
          // Check for transit indicators: "En route", "In Transit", ship moving
          if (bodyText.includes('En route') || bodyText.includes('transit') ||
              bodyText.includes('In Transit') || bodyText.includes('Departing')) {
            sawTransit = true;
            break;
          }
          // Also check if we see arrival already (too fast)
          const arrived = (await enabledBtns()).some(b =>
            b.includes('Load') || b.includes('Deliver') || b.includes('\u25c9'));
          if (arrived) break;
        }

        if (sawTransit) {
          pass('transit_visible', `"En route" seen during flight to ${destName}`);
        } else {
          fail('transit_visible', `ship flew to ${destName} without showing transit state — animation skipped`);
        }

        // Wait for arrival before continuing
        await waitForDocked();
      }
    }
  }

  // ── Test 4: Load cargo works ──────────────────────────────────────────
  // After docking, clicking "Load" should show cargo state in the UI.
  {
    await page.waitForTimeout(2000); // let pending state clear

    const loadBtn = await page.$('button:has-text("Load")');
    if (!loadBtn || await loadBtn.evaluate(b => b.disabled)) {
      // Try flying to a station first
      const btns2 = await enabledBtns();
      const dest2 = btns2.find(b => ['Alpha', 'Beta', 'Gamma', 'Delta'].some(s => b.includes(s)) && !b.includes('\u25c9'));
      if (dest2) {
        const btn2 = await page.$(`button:has-text("${dest2.substring(0, 12)}")`);
        if (btn2) {
          await btn2.click({ force: true });
          await waitForDocked();
          await page.waitForTimeout(2000);
        }
      }
    }

    const loadBtn2 = await page.$('button:has-text("Load")');
    if (!loadBtn2 || await loadBtn2.evaluate(b => b.disabled)) {
      fail('load_cargo', 'Load button not available after docking');
    } else {
      await loadBtn2.click();
      await page.waitForTimeout(5000); // wait for pipeline + next poll

      // Check that the UI shows cargo state: "for <station>" text,
      // or the action bar shows "Deliver", or cargo indicator visible
      const bodyText = await page.evaluate(() => document.body.innerText);
      const btns = await enabledBtns();
      const hasCargo = bodyText.includes('for ') ||
        btns.some(b => b.includes('Deliver')) ||
        bodyText.includes('Cargo') || bodyText.includes('cargo');

      if (hasCargo) {
        pass('load_cargo', 'cargo loaded — Deliver or cargo indicator visible');
      } else {
        fail('load_cargo', 'clicked Load but no cargo state appeared in UI');
      }
    }
  }

  // ── Test 5: Deliver cargo works ───────────────────────────────────────
  // Fly to the destination and deliver. Station stock should increase.
  {
    const btns = await enabledBtns();
    const deliverBtn = btns.find(b => b.includes('Deliver'));

    if (deliverBtn) {
      // Already at delivery destination — deliver directly
      const btn = await page.$('button:has-text("Deliver")');
      const beforePcts = await stationPcts();
      await btn.click();
      await page.waitForTimeout(5000);
      const afterPcts = await stationPcts();

      // At least one station should have increased (the delivery target)
      const anyIncrease = beforePcts.some((v, i) => afterPcts[i] > v + 1);
      if (anyIncrease) {
        pass('deliver_cargo', 'station stock increased after delivery');
      } else {
        fail('deliver_cargo', `stock before: ${beforePcts.map(v => v.toFixed(1)).join(', ')} / after: ${afterPcts.map(v => v.toFixed(1)).join(', ')}`);
      }
    } else {
      // Need to fly to destination first
      const dest = ['Alpha', 'Beta', 'Gamma', 'Delta'].find(d =>
        btns.some(b => b.includes(d) && !b.includes('\u25c9')));
      if (dest) {
        const btn = await page.$(`button:has-text("${dest}")`);
        if (btn) {
          await btn.click({ force: true });
          await waitForDocked();
          await page.waitForTimeout(2000);

          const deliverBtn2 = await page.$('button:has-text("Deliver")');
          if (deliverBtn2 && !(await deliverBtn2.evaluate(b => b.disabled))) {
            const beforePcts = await stationPcts();
            await deliverBtn2.click();
            await page.waitForTimeout(5000);
            const afterPcts = await stationPcts();

            const anyIncrease = beforePcts.some((v, i) => afterPcts[i] > v + 1);
            if (anyIncrease) {
              pass('deliver_cargo', 'station stock increased after delivery');
            } else {
              fail('deliver_cargo', `stock before: ${beforePcts.map(v => v.toFixed(1)).join(', ')} / after: ${afterPcts.map(v => v.toFixed(1)).join(', ')}`);
            }
          } else {
            fail('deliver_cargo', 'Deliver button not available after flying to destination');
          }
        } else {
          fail('deliver_cargo', 'could not find destination button');
        }
      } else {
        fail('deliver_cargo', 'no destination available and no Deliver button');
      }
    }
  }

  await page.close();
  await browser.close();

  console.log(`\n  ${passed} passed, ${failed} failed\n`);
  process.exit(failed > 0 ? 1 : 0);
})();
