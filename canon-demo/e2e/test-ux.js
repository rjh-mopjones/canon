#!/usr/bin/env node
// Canon demo — UX timeline tests
//
// Unlike functional tests that check "did the right thing eventually happen",
// these tests sample the DOM at high frequency (200ms) and assert that the
// user NEVER sees a broken state at ANY point during the journey.
//
// Usage: node e2e/test-ux.js
//        FRONTEND_URL=https://canon-staging.mopjones.com node e2e/test-ux.js

const { chromium } = require('playwright');
const path = require('path');

const FRONTEND = process.env.FRONTEND_URL || 'http://localhost:3000';
const SCREENSHOT_DIR = process.env.SCREENSHOT_DIR || '/tmp/canon-ux';

let passed = 0;
let failed = 0;

function pass(name, detail) { console.log(`  \u2705 ${name}${detail ? ' (' + detail + ')' : ''}`); passed++; }
function fail(name, reason) { console.log(`  \u274c ${name}: ${reason}`); failed++; }

// ── UX Timeline Sampler ────────────────────────────────────────────────────
// Samples DOM state every intervalMs for durationMs. Returns an array of
// timestamped snapshots. Use this to assert invariants across the entire
// observation window — "at no point did X happen."

async function sampleTimeline(page, durationMs, intervalMs = 200) {
  const samples = [];
  const t0 = Date.now();

  for (let elapsed = 0; elapsed < durationMs; elapsed += intervalMs) {
    await page.waitForTimeout(intervalMs);

    const sample = await page.evaluate(() => {
      const pcts = [...document.querySelectorAll('.stn-card-pct')]
        .map(el => parseFloat(el.textContent) || 0);
      const stationNames = [...document.querySelectorAll('.stn-card-name, .stn-name')]
        .map(el => el.textContent.trim());
      const bodyText = document.body.innerText;
      const btns = [...document.querySelectorAll('button')]
        .filter(b => b.offsetParent !== null)
        .map(b => ({ text: b.textContent.trim().substring(0, 40), disabled: b.disabled }));

      return {
        stocks: pcts,
        stationNames,
        hasShip: bodyText.includes('VSS') || bodyText.includes('Meridian'),
        gameOver: bodyText.includes('collapsed') || bodyText.includes('Restart'),
        enRoute: bodyText.includes('En route') || bodyText.includes('In Transit'),
        hasLoad: btns.some(b => b.text.includes('Load') && !b.disabled),
        hasDeliver: btns.some(b => b.text.includes('Deliver') && !b.disabled),
        hasDest: btns.some(b =>
          ['Alpha', 'Beta', 'Gamma', 'Delta'].some(s => b.text.includes(s)) && !b.disabled),
        pending: bodyText.includes('waiting for pipeline'),
        bodySnippet: bodyText.substring(0, 200),
        buttonTexts: btns.map(b => b.text + (b.disabled ? '[D]' : '')),
      };
    });

    sample.elapsed = Date.now() - t0;
    samples.push(sample);
  }

  return samples;
}

// ── Screenshot helper ──────────────────────────────────────────────────────

const fs = require('fs');
async function screenshot(page, name) {
  fs.mkdirSync(SCREENSHOT_DIR, { recursive: true });
  await page.screenshot({ path: path.join(SCREENSHOT_DIR, `${name}.png`), fullPage: true });
}

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

  console.log('\nCanon UX timeline tests\n');

  // ════════════════════════════════════════════════════════════════════════
  // JOURNEY 1: Fresh session load
  // The user opens the page for the first time. At NO point should they see
  // game over, missing ship, empty station names, or 0% stock.
  // ════════════════════════════════════════════════════════════════════════

  await page.goto(FRONTEND, { waitUntil: 'networkidle', timeout: 30000 });
  await screenshot(page, '01-initial-load');

  // Sample the first 15 seconds at 200ms intervals
  const loadSamples = await sampleTimeline(page, 15000, 200);
  await screenshot(page, '02-after-15s');

  // ── Test 1: Ship must be visible within 3s and never disappear ────────
  {
    const firstShipSeen = loadSamples.findIndex(s => s.hasShip);
    const shipDisappeared = loadSamples.some((s, i) => i > firstShipSeen && firstShipSeen >= 0 && !s.hasShip);

    if (firstShipSeen < 0) {
      fail('ship_always_visible', 'ship never appeared in 15s');
    } else if (firstShipSeen > 15) { // 15 samples * 200ms = 3s
      fail('ship_always_visible', `ship took ${(loadSamples[firstShipSeen].elapsed/1000).toFixed(1)}s to appear`);
    } else if (shipDisappeared) {
      fail('ship_always_visible', 'ship appeared then disappeared (flicker)');
    } else {
      pass('ship_always_visible', `appeared at ${(loadSamples[firstShipSeen].elapsed/1000).toFixed(1)}s`);
    }
  }

  // ── Test 2: Game over must NEVER trigger in first 15s ─────────────────
  {
    const gameOverSample = loadSamples.find(s => s.gameOver);
    if (gameOverSample) {
      fail('no_early_game_over', `"Supply chain collapsed" visible at ${(gameOverSample.elapsed/1000).toFixed(1)}s`);
      await screenshot(page, '02-FAIL-game-over');
    } else {
      pass('no_early_game_over', 'no game over in first 15s');
    }
  }

  // ── Test 3: Stocks must never show 0% after initial hydration ─────────
  // Allow up to 3s for hydration, then stocks must always be > 0.
  {
    const after3s = loadSamples.filter(s => s.elapsed > 3000);
    const zeroStock = after3s.find(s => s.stocks.length >= 4 && s.stocks.some(v => v <= 0));
    if (zeroStock) {
      fail('no_zero_stock', `station at 0% at ${(zeroStock.elapsed/1000).toFixed(1)}s: [${zeroStock.stocks.map(v => v.toFixed(0) + '%').join(', ')}]`);
    } else if (after3s.length === 0) {
      fail('no_zero_stock', 'no samples after 3s');
    } else {
      const lastStocks = after3s[after3s.length - 1].stocks;
      pass('no_zero_stock', `stocks stable: [${lastStocks.map(v => v.toFixed(0) + '%').join(', ')}]`);
    }
  }

  // ── Test 4: Stocks never spike above bootstrap values ─────────────────
  {
    const expected = [85, 60, 40, 75];
    const spike = loadSamples.find(s =>
      s.stocks.length >= 4 && s.stocks.some((v, i) => v > expected[i] + 5));
    if (spike) {
      fail('no_stock_spike', `spike at ${(spike.elapsed/1000).toFixed(1)}s: [${spike.stocks.map(v => v.toFixed(0) + '%').join(', ')}]`);
    } else {
      pass('no_stock_spike');
    }
  }

  // ── Test 5: Station names never empty after 2s ────────────────────────
  {
    const after2s = loadSamples.filter(s => s.elapsed > 2000);
    // Check station card text for actual names
    const emptyNames = after2s.find(s => {
      // Station names might be in different elements, check body text
      const hasAllNames = ['Alpha', 'Beta', 'Gamma', 'Delta'].every(
        name => s.bodySnippet.includes(name) || s.buttonTexts.some(b => b.includes(name)));
      return !hasAllNames;
    });
    if (emptyNames) {
      fail('station_names_present', `missing station names at ${(emptyNames.elapsed/1000).toFixed(1)}s`);
    } else {
      pass('station_names_present');
    }
  }

  // ════════════════════════════════════════════════════════════════════════
  // JOURNEY 2: Ship flight
  // User clicks a destination. They should see: buttons disable → "En route"
  // text appears → ship animates across canvas → arrives at station →
  // action buttons reappear. At NO point should the ship disappear or
  // the UI flash to game over.
  // ════════════════════════════════════════════════════════════════════════

  // Wait for session to be fully ready
  for (let i = 0; i < 20; i++) {
    await page.waitForTimeout(500);
    const ready = await page.evaluate(() => {
      const btns = [...document.querySelectorAll('button')]
        .filter(b => b.offsetParent !== null && !b.disabled)
        .map(b => b.textContent);
      return btns.some(b => ['Alpha', 'Beta', 'Gamma', 'Delta'].some(s => b.includes(s)));
    });
    if (ready) break;
  }

  await screenshot(page, '03-before-flight');

  // Click Beta Relay
  const destBtn = await page.$('button:has-text("Beta")');
  if (!destBtn || await destBtn.evaluate(b => b.disabled)) {
    // Try Alpha if Beta is current
    const altBtn = await page.$('button:has-text("Alpha")');
    if (altBtn && !(await altBtn.evaluate(b => b.disabled))) {
      await altBtn.click({ force: true });
    }
  } else {
    await destBtn.click({ force: true });
  }

  // Sample the flight at high frequency
  const flightSamples = await sampleTimeline(page, 12000, 200);
  await screenshot(page, '04-after-flight');

  // ── Test 6: "En route" must be visible for at least 1.5s during flight ─
  {
    const transitSamples = flightSamples.filter(s => s.enRoute);
    const transitDurationMs = transitSamples.length * 200;

    if (transitSamples.length === 0) {
      fail('transit_duration', 'ship never showed "En route" — animation skipped entirely');
    } else if (transitDurationMs < 1500) {
      fail('transit_duration', `"En route" only visible for ${transitDurationMs}ms (need >= 1500ms)`);
    } else {
      pass('transit_duration', `"En route" visible for ${(transitDurationMs/1000).toFixed(1)}s`);
    }
  }

  // ── Test 7: Ship never disappears during flight ───────────────────────
  {
    const shipGone = flightSamples.find(s => !s.hasShip);
    if (shipGone) {
      fail('ship_visible_during_flight', `ship disappeared at ${(shipGone.elapsed/1000).toFixed(1)}s into flight`);
    } else {
      pass('ship_visible_during_flight');
    }
  }

  // ── Test 8: No game over during flight ────────────────────────────────
  {
    const gameOver = flightSamples.find(s => s.gameOver);
    if (gameOver) {
      fail('no_game_over_during_flight', `game over at ${(gameOver.elapsed/1000).toFixed(1)}s into flight`);
    } else {
      pass('no_game_over_during_flight');
    }
  }

  // ── Test 9: Action buttons reappear after flight ──────────────────────
  {
    const afterTransit = flightSamples.filter(s => !s.enRoute && !s.pending);
    const hasActions = afterTransit.find(s => s.hasLoad || s.hasDeliver || s.hasDest);
    if (hasActions) {
      pass('actions_after_flight', 'buttons reappeared after landing');
    } else {
      fail('actions_after_flight', 'no Load/Deliver/Destination buttons after flight ended');
    }
  }

  // ════════════════════════════════════════════════════════════════════════
  // JOURNEY 3: Load and deliver cargo
  // User loads cargo at current station, flies to destination, delivers.
  // The cargo destination shown in UI must match where they actually need
  // to go, and the Deliver button must work when they get there.
  // ════════════════════════════════════════════════════════════════════════

  // Wait for docked state
  for (let i = 0; i < 20; i++) {
    await page.waitForTimeout(500);
    const docked = await page.evaluate(() => {
      const text = document.body.innerText;
      return !text.includes('En route') && !text.includes('waiting for pipeline');
    });
    if (docked) break;
  }
  await page.waitForTimeout(2000);

  // Load cargo
  const loadBtn = await page.$('button:has-text("Load")');
  let cargoTestsRun = false;

  if (loadBtn && !(await loadBtn.evaluate(b => b.disabled))) {
    await loadBtn.click();
    await page.waitForTimeout(5000);
    await screenshot(page, '05-after-load');

    // Read the cargo destination from UI
    const cargoInfo = await page.evaluate(() => {
      const text = document.body.innerText;
      const destMatch = text.match(/for\s+(Alpha|Beta|Gamma|Delta)/);
      // Also find current station
      const currentMatch = text.match(/Docked at\s+(Alpha|Beta|Gamma|Delta)/);
      const currentFromBtn = [...document.querySelectorAll('button')]
        .find(b => b.textContent.includes('\u25c9'));
      const currentStation = currentMatch ? currentMatch[1] :
        (currentFromBtn ? currentFromBtn.textContent.match(/(Alpha|Beta|Gamma|Delta)/)?.[1] : null);
      return {
        destination: destMatch ? destMatch[1] : null,
        currentStation,
      };
    });

    // ── Test 10: Cargo destination matches supply loop ───────────────
    {
      const supplyLoop = { 'Alpha': 'Beta', 'Beta': 'Gamma', 'Gamma': 'Delta', 'Delta': 'Alpha' };
      const expected = cargoInfo.currentStation ? supplyLoop[cargoInfo.currentStation] : null;

      if (!cargoInfo.destination) {
        fail('cargo_destination_correct', 'no cargo destination shown after loading');
      } else if (!expected) {
        fail('cargo_destination_correct', `could not determine current station (got: ${cargoInfo.currentStation})`);
      } else if (cargoInfo.destination !== expected) {
        fail('cargo_destination_correct', `at ${cargoInfo.currentStation}, destination should be ${expected} but got ${cargoInfo.destination}`);
      } else {
        pass('cargo_destination_correct', `at ${cargoInfo.currentStation} → ${cargoInfo.destination}`);
      }

      // ── Test 11: Cargo destination stays stable over 10s ──────────
      // Sample every second — destination must never change
      if (cargoInfo.destination) {
        let destChanged = false;
        let changedTo = null;
        for (let i = 0; i < 10; i++) {
          await page.waitForTimeout(1000);
          const currentDest = await page.evaluate(() => {
            const match = document.body.innerText.match(/for\s+(Alpha|Beta|Gamma|Delta)/);
            return match ? match[1] : null;
          });
          if (currentDest && currentDest !== cargoInfo.destination) {
            destChanged = true;
            changedTo = currentDest;
            break;
          }
        }
        if (destChanged) {
          fail('cargo_destination_stable', `destination changed from ${cargoInfo.destination} to ${changedTo} while docked`);
        } else {
          pass('cargo_destination_stable', `${cargoInfo.destination} stayed stable for 10s`);
        }
      } else {
        fail('cargo_destination_stable', 'no destination to monitor');
      }
    }

    cargoTestsRun = true;
  }

  if (!cargoTestsRun) {
    fail('cargo_destination_correct', 'Load button not available — could not test cargo flow');
    fail('cargo_destination_stable', 'skipped (no cargo loaded)');
  }

  // ════════════════════════════════════════════════════════════════════════
  // JOURNEY 4: Stock drain consistency
  // Over 20s, stocks should only decrease (or stay same). No sudden jumps
  // up (double-counting) or down to 0 (state reset).
  // ════════════════════════════════════════════════════════════════════════

  const drainSamples = await sampleTimeline(page, 20000, 500);

  // ── Test 12: No sudden stock drops > 15% in a single sample ───────────
  {
    let bigDrop = null;
    for (let i = 1; i < drainSamples.length; i++) {
      const prev = drainSamples[i - 1].stocks;
      const curr = drainSamples[i].stocks;
      if (prev.length < 4 || curr.length < 4) continue;
      for (let s = 0; s < 4; s++) {
        const drop = prev[s] - curr[s];
        if (drop > 15 && prev[s] > 0) {
          bigDrop = { station: s, from: prev[s], to: curr[s], elapsed: drainSamples[i].elapsed };
          break;
        }
      }
      if (bigDrop) break;
    }
    if (bigDrop) {
      fail('no_sudden_stock_drop', `station ${bigDrop.station} dropped ${bigDrop.from.toFixed(0)}% → ${bigDrop.to.toFixed(0)}% at ${(bigDrop.elapsed/1000).toFixed(1)}s`);
    } else {
      pass('no_sudden_stock_drop');
    }
  }

  // ── Test 13: No stock increases > 5% without user action ──────────────
  // (Delivery adds ~35%, but we're not delivering here)
  {
    let bigJump = null;
    for (let i = 1; i < drainSamples.length; i++) {
      const prev = drainSamples[i - 1].stocks;
      const curr = drainSamples[i].stocks;
      if (prev.length < 4 || curr.length < 4) continue;
      for (let s = 0; s < 4; s++) {
        const jump = curr[s] - prev[s];
        if (jump > 5) {
          bigJump = { station: s, from: prev[s], to: curr[s], elapsed: drainSamples[i].elapsed };
          break;
        }
      }
      if (bigJump) break;
    }
    if (bigJump) {
      fail('no_stock_jump', `station ${bigJump.station} jumped ${bigJump.from.toFixed(0)}% → ${bigJump.to.toFixed(0)}% at ${(bigJump.elapsed/1000).toFixed(1)}s (double-counting?)`);
    } else {
      pass('no_stock_jump');
    }
  }

  await screenshot(page, '06-final');
  await page.close();
  await browser.close();

  console.log(`\n  ${passed} passed, ${failed} failed`);
  console.log(`  Screenshots saved to ${SCREENSHOT_DIR}\n`);
  process.exit(failed > 0 ? 1 : 0);
})();
