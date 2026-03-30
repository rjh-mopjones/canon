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

  await page.goto(FRONTEND, { waitUntil: 'domcontentloaded', timeout: 30000 });

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

  // Helper: wait for docked state. Must NOT be in transit (enforced transit
  // holds "En route" for 5s even after arrival event). Waits for Load/Deliver
  // buttons or ◉ marker AND no "En route" text.
  const waitForDocked = async (timeoutS = 30) => {
    for (let i = 0; i < timeoutS * 2; i++) {
      await page.waitForTimeout(500);
      const bodyText = await page.evaluate(() => document.body.innerText);
      const inTransit = bodyText.includes('En route') || bodyText.includes('In Transit');
      if (inTransit) continue; // still in enforced transit period

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
      const destLoc = page.locator(`button.dest-tab:has-text("${destName}"):not([disabled])`);
      if (await destLoc.count() === 0) {
        fail('transit_visible', `${destName} button not found`);
      } else {
        await destLoc.click({ force: true, timeout: 5000 });

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

  // ── Test 4: Cargo is available (user-loaded or auto-loaded by cascade) ──
  // After docking, either Load button is available or cargo was auto-loaded
  // by the cross-service cascade (ManifestCreated on arrival).
  {
    await page.waitForTimeout(5000); // let pending clear + pipeline settle

    const bodyText = await page.evaluate(() => document.body.innerText);
    const loadLoc = page.locator('button:has-text("Load"):not([disabled])');
    const hasLoad = await loadLoc.count() > 0;
    const hasCargo = bodyText.match(/for\s+(Alpha|Beta|Gamma|Delta)/);

    if (hasLoad) {
      await loadLoc.click({ timeout: 5000 });
      await page.waitForTimeout(5000);
      const afterText = await page.evaluate(() => document.body.innerText);
      const cargoShown = afterText.match(/for\s+(Alpha|Beta|Gamma|Delta)/);
      if (cargoShown) {
        pass('load_cargo', `cargo loaded for ${cargoShown[1]}`);
      } else {
        fail('load_cargo', 'clicked Load but no cargo destination appeared');
      }
    } else if (hasCargo) {
      pass('load_cargo', `cargo auto-loaded by pipeline for ${hasCargo[1]}`);
    } else {
      fail('load_cargo', 'no Load button and no cargo destination visible');
    }
  }

  // ── Test 5: Deliver cargo works ───────────────────────────────────────
  // Fly to the cargo's destination and deliver. The cross-service cascade
  // may auto-process the delivery (ManifestClosed + CargoReceived), so
  // check for either: Deliver button available, OR delivery already happened.
  {
    // Read the cargo destination — might be "for X", "supplies for X", etc.
    const bodyText = await page.evaluate(() => document.body.innerText);
    const destMatch = bodyText.match(/for\s+(Alpha|Beta|Gamma|Delta)/i)
      || bodyText.match(/(Alpha|Beta|Gamma|Delta).*deliver/i);
    const dest = destMatch ? destMatch[1] : null;

    if (!dest) {
      // Cargo might have been auto-delivered already — check event log
      const logText = await page.$eval('.log-body', el => el.textContent).catch(() => '');
      if (logText.includes('CargoReceived') || logText.includes('ManifestClosed')) {
        pass('deliver_cargo', 'cargo auto-delivered by pipeline cascade (no manual delivery needed)');
      } else {
        fail('deliver_cargo', 'no cargo destination found in UI text');
      }
    } else {
      const btns = await enabledBtns();
      const alreadyAtDest = btns.some(b => b.includes('Deliver'));

      if (alreadyAtDest) {
        await page.locator('button:has-text("Deliver"):not([disabled])').click({ timeout: 5000 });
        await page.waitForTimeout(5000);
        pass('deliver_cargo', `delivered at ${dest}`);
      } else {
        // Fly to destination — use .dest-tab class to avoid matching Load button
        const destLoc = page.locator(`button.dest-tab:has-text("${dest}"):not([disabled])`);
        if (await destLoc.count() === 0) {
          fail('deliver_cargo', `${dest} button not available`);
        } else {
          const beforePcts = await stationPcts();
          await destLoc.click({ force: true, timeout: 5000 });
          await waitForDocked();
          await page.waitForTimeout(5000);

          // Check: Deliver button available, OR cargo already delivered
          // (cross-service cascade auto-delivers on arrival)
          let delivered = false;

          // Try clicking Deliver if available — use locator for stability
          for (let retry = 0; retry < 5; retry++) {
            const deliverLoc = page.locator('button:has-text("Deliver"):not([disabled])');
            if (await deliverLoc.count() > 0) {
              await deliverLoc.click({ timeout: 5000 });
              await page.waitForTimeout(5000);
              delivered = true;
              break;
            }
            await page.waitForTimeout(1000);
          }

          if (!delivered) {
            // Check if cargo was auto-delivered by the pipeline cascade
            const logText = await page.$eval('.log-body', el => el.textContent).catch(() => '');
            const afterPcts = await stationPcts();
            const autoDelivered = logText.includes('CargoReceived') ||
              logText.includes('ManifestClosed') ||
              beforePcts.some((v, i) => afterPcts[i] > v + 1);

            if (autoDelivered) {
              delivered = true;
            }
          }

          if (delivered) {
            pass('deliver_cargo', `cargo delivered to ${dest} (pipeline cascade)`);
          } else {
            fail('deliver_cargo', `Deliver button not available at ${dest} and no auto-delivery detected`);
          }
        }
      }
    }
  }

  await page.close();
  await browser.close();

  console.log(`\n  ${passed} passed, ${failed} failed\n`);
  process.exit(failed > 0 ? 1 : 0);
})();
