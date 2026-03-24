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
const MAX_FLIGHT_TIME = 15; // seconds — fail if any leg exceeds this

let passed = 0;
let failed = 0;

function pass(name, detail) { console.log(`  ✅ ${name}${detail ? ' (' + detail + ')' : ''}`); passed++; }
function fail(name, reason) { console.log(`  ❌ ${name}: ${reason}`); failed++; }

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });

  console.log('\nCanon supply chain loop test\n');

  await page.goto(FRONTEND, { waitUntil: 'networkidle', timeout: 30000 });

  // Helper: get enabled button texts
  const enabledBtns = async () => page.$$eval('button', bs =>
    bs.filter(b => b.offsetParent !== null && !b.disabled).map(b => b.textContent.trim().substring(0, 60)));

  // Helper: wait until docked (Load/Deliver/◉ button visible)
  const waitForDocked = async (timeoutS = 20) => {
    const t0 = Date.now();
    for (let i = 0; i < timeoutS; i++) {
      await page.waitForTimeout(1000);
      const btns = await enabledBtns();
      if (btns.some(b => b.includes('Load') || b.includes('Deliver') || b.includes('◉')))
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

    // Find supply chain destination
    const text = await page.textContent('body');
    const forMatch = text.match(/for\s+(Alpha|Beta|Gamma|Delta)/);
    const btns = await enabledBtns();
    let destName = forMatch ? forMatch[1] : null;
    if (!destName) {
      const d = btns.find(b => !b.includes('◉') && !b.includes('Load') && !b.includes('Deliver') &&
        ['Alpha', 'Beta', 'Gamma', 'Delta'].some(s => b.includes(s)));
      if (d) destName = d.substring(0, 5);
    }

    if (!destName) {
      fail(`leg_${leg}`, 'no destination found');
      continue;
    }

    const destBtn = await page.$(`button:has-text("${destName}")`);
    if (!destBtn || await destBtn.evaluate(b => b.disabled)) {
      fail(`leg_${leg}`, `${destName} button disabled`);
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
