#!/usr/bin/env node
// Canon demo — Full supply chain loop test
//
// Tests the complete game loop: dock → load → fly → deliver × 4 legs,
// verifying that the Canon event sourcing pipeline processes each command
// within a reasonable time.
//
// Usage: node e2e/test-supply-chain.js
//        make k8s-test-supply-chain  (from canon-demo/)

const { chromium } = require('playwright');

const FRONTEND = process.env.FRONTEND_URL || 'http://localhost:3000';
const MAX_FLIGHT_TIME = 30; // seconds — GKE preemptible nodes can be slow under load

let passed = 0;
let failed = 0;

function pass(name, detail) { console.log(`  ✅ ${name}${detail ? ' (' + detail + ')' : ''}`); passed++; }
function fail(name, reason) { console.log(`  ❌ ${name}: ${reason}`); failed++; }

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });

  // If CANON_AUTH_PASSWORD is set, authenticate via cookie (not setExtraHTTPHeaders — breaks font CORS)
  if (process.env.CANON_AUTH_PASSWORD) {
    const url = new URL(FRONTEND);
    await page.context().addCookies([{
      name: 'canon_auth',
      value: process.env.CANON_AUTH_PASSWORD,
      domain: url.hostname,
      path: '/',
      httpOnly: true,
      secure: url.protocol === 'https:',
      sameSite: 'None',
    }]);
  }

  console.log('\nCanon supply chain loop test\n');

  await page.goto(FRONTEND, { waitUntil: 'networkidle', timeout: 30000 });

  // Helper: get enabled button texts
  const enabledBtns = async () => page.$$eval('button', bs =>
    bs.filter(b => b.offsetParent !== null && !b.disabled).map(b => b.textContent.trim().substring(0, 60)));

  // Helper: wait until docked. Checks both enabled buttons (Load/Deliver)
  // and ALL visible buttons for the ◉ marker (which is always disabled).
  const waitForDocked = async (timeoutS = 45) => {
    const t0 = Date.now();
    for (let i = 0; i < timeoutS; i++) {
      await page.waitForTimeout(1000);
      const btns = await enabledBtns();
      if (btns.some(b => b.includes('Load') || b.includes('Deliver')))
        return Math.round((Date.now() - t0) / 1000);
      // ◉ button is disabled, so check ALL visible buttons
      const allVisible = await page.$$eval('button', bs =>
        bs.filter(b => b.offsetParent !== null).map(b => b.textContent.trim().substring(0, 40)));
      if (allVisible.some(b => b.includes('◉')))
        return Math.round((Date.now() - t0) / 1000);
      if (btns.includes('Restart')) return -1; // game over
    }
    return -2; // timeout
  };

  // Wait for session to be ready
  let sessionReady = false;
  for (let i = 0; i < 30; i++) {
    await page.waitForTimeout(1000);
    const btns = await enabledBtns();
    if (btns.some(b => ['Alpha', 'Beta', 'Gamma', 'Delta'].some(s => b.includes(s)))) {
      sessionReady = true;
      break;
    }
  }
  if (!sessionReady) {
    fail('session_setup', 'destination buttons never appeared');
    await browser.close();
    console.log(`\n  ${passed} passed, ${failed} failed\n`);
    process.exit(1);
  }
  pass('session_setup', 'session created + buttons visible');

  // Initial dock (ship starts undocked in center)
  const b0 = await enabledBtns();
  if (!b0.some(b => b.includes('Load'))) {
    const dest = b0.find(b => ['Alpha', 'Beta', 'Gamma', 'Delta'].some(s => b.includes(s)));
    if (dest) {
      const btn = await page.$(`button:has-text("${dest.substring(0, 12)}")`);
      await btn.click({ force: true });
      const t = await waitForDocked();
      if (t > 0) pass('initial_dock', `${t}s`);
      else fail('initial_dock', t === -1 ? 'game over' : 'timeout');
    }
  } else {
    pass('initial_dock', 'already docked');
  }

  // 4-leg supply chain: Alpha → Beta → Gamma → Delta → Alpha
  for (let leg = 1; leg <= 4; leg++) {
    // Deliver if at correct station
    const deliverBtn = await page.$('button:has-text("Deliver")');
    if (deliverBtn && !(await deliverBtn.evaluate(b => b.disabled))) {
      await deliverBtn.click();
      await page.waitForTimeout(2000);
    }

    // Load
    const loadBtn = await page.$('button:has-text("Load")');
    if (loadBtn && !(await loadBtn.evaluate(b => b.disabled))) {
      await loadBtn.click();
      await page.waitForTimeout(3000);
    }

    // Wait for destination buttons to stabilize (pending state clears)
    let destName = null;
    let alreadyAtDest = false;
    for (let wait = 0; wait < 15; wait++) {
      await page.waitForTimeout(1000);
      const text = await page.textContent('body');
      const forMatch = text.match(/for\s+(Alpha|Beta|Gamma|Delta)/);
      const btns = await enabledBtns();

      let candidate = forMatch ? forMatch[1] : null;
      if (!candidate) {
        const d = btns.find(b => !b.includes('◉') && !b.includes('Load') && !b.includes('Deliver') &&
          ['Alpha', 'Beta', 'Gamma', 'Delta'].some(s => b.includes(s)));
        if (d) candidate = d.match(/(Alpha|Beta|Gamma|Delta)/)?.[1] || null;
      }

      if (candidate) {
        // Check if ship is already at this destination
        if (btns.some(b => b.includes('◉') && b.includes(candidate))) {
          destName = candidate;
          alreadyAtDest = true;
          break;
        }
        // Check if the destination button is enabled and clickable
        const btn = await page.$(`button:has-text("${candidate}")`);
        if (btn && !(await btn.evaluate(b => b.disabled))) {
          destName = candidate;
          break;
        }
      }
    }

    if (alreadyAtDest) {
      pass(`leg_${leg}`, `→ ${destName} (already there)`);
      continue;
    }

    if (!destName) {
      fail(`leg_${leg}`, 'no destination found or button stayed disabled');
      continue;
    }

    const destBtn = await page.$(`button:has-text("${destName}")`);
    if (!destBtn || await destBtn.evaluate(b => b.disabled)) {
      fail(`leg_${leg}`, `${destName} button disabled after wait`);
      continue;
    }

    await destBtn.click({ force: true });
    const t = await waitForDocked();

    if (t > 0 && t <= MAX_FLIGHT_TIME) {
      pass(`leg_${leg}`, `→ ${destName} in ${t}s`);
    } else if (t > MAX_FLIGHT_TIME) {
      fail(`leg_${leg}`, `→ ${destName} took ${t}s (max ${MAX_FLIGHT_TIME}s)`);
    } else if (t === -1) {
      fail(`leg_${leg}`, 'game over during transit');
      break;
    } else {
      fail(`leg_${leg}`, `→ ${destName} timed out`);
      break;
    }
  }

  await page.close();
  await browser.close();

  console.log(`\n  ${passed} passed, ${failed} failed\n`);
  process.exit(failed > 0 ? 1 : 0);
})();
