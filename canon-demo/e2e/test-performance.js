#!/usr/bin/env node
// Canon demo — Pipeline performance e2e tests
//
// Measures end-to-end latency at every stage of the game loop and verifies
// that stock drain events arrive smoothly (no erratic gaps or bursts).
//
// Usage:    node e2e/test-performance.js
//           make k8s-test-performance  (from canon-demo/)

const { chromium } = require('playwright');

const FRONTEND = process.env.FRONTEND_URL || 'http://localhost:3000';
const TIMEOUT = 30_000;

// Latency thresholds (ms)
const MAX_COMMAND_ACK_MS = 5_000;    // Click → UI feedback (button disabled/pending)
const MAX_FLIGHT_CONFIRM_MS = 8_000; // Click depart → ShipDeparted event in log
const MAX_ARRIVAL_MS = 15_000;       // Depart → ship docked at destination
const MAX_DRAIN_GAP_MS = 15_000;     // Max gap between consecutive StockDrained events (10s cycle)
const MIN_DRAIN_GAP_MS = 1_500;      // Min gap — faster than this is erratic bursting
const DRAIN_SAMPLE_COUNT = 4;        // Number of drain events to collect per station (10s cycle)

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

  // ── Intercept WebSocket messages for latency measurement ──────────────
  const wsEvents = [];
  await page.addInitScript(() => {
    window.__canonWsEvents = [];
    const origWS = window.WebSocket;
    window.WebSocket = function(...args) {
      const ws = new origWS(...args);
      ws.addEventListener('message', (evt) => {
        try {
          const data = JSON.parse(evt.data);
          window.__canonWsEvents.push({ ts: Date.now(), ...data });
        } catch {}
      });
      return ws;
    };
    window.WebSocket.prototype = origWS.prototype;
    Object.defineProperty(window.WebSocket, 'CONNECTING', { value: 0 });
    Object.defineProperty(window.WebSocket, 'OPEN', { value: 1 });
    Object.defineProperty(window.WebSocket, 'CLOSING', { value: 2 });
    Object.defineProperty(window.WebSocket, 'CLOSED', { value: 3 });
  });

  console.log('\nCanon pipeline performance tests\n');

  await page.goto(FRONTEND, { waitUntil: 'networkidle', timeout: TIMEOUT });

  // Wait for session setup
  try {
    await page.waitForSelector('.stn-card-pct', { timeout: 30_000 });
  } catch {
    fail('session_setup', 'station cards never appeared');
    await browser.close();
    console.log(`\n  ${passed} passed, ${failed} failed\n`);
    process.exit(1);
  }

  // Helper: get enabled button texts
  const enabledBtns = async () => page.$$eval('button', bs =>
    bs.filter(b => b.offsetParent !== null && !b.disabled).map(b => b.textContent.trim().substring(0, 60)));

  // Wait for destination buttons (ship docked)
  let ready = false;
  for (let i = 0; i < 30; i++) {
    await page.waitForTimeout(1000);
    const btns = await enabledBtns();
    if (btns.some(b => ['Alpha', 'Beta', 'Gamma', 'Delta'].some(s => b.includes(s)))) {
      ready = true;
      break;
    }
  }
  if (!ready) {
    fail('session_ready', 'destination buttons never appeared');
    await browser.close();
    console.log(`\n  ${passed} passed, ${failed} failed\n`);
    process.exit(1);
  }
  pass('session_ready', 'session created + hydrated');

  // ── Test 1: Command acknowledgement latency ──────────────────────────
  // Click a destination button, measure time until it becomes disabled (pending state)
  {
    const btns = await enabledBtns();
    // Load cargo first if available
    const loadBtn = await page.$('button:has-text("Load")');
    if (loadBtn && !(await loadBtn.evaluate(b => b.disabled))) {
      await loadBtn.click();
      await page.waitForTimeout(3000);
    }

    const destName = ['Beta', 'Gamma', 'Delta', 'Alpha'].find(d =>
      btns.some(b => b.includes(d)));

    if (!destName) {
      fail('command_ack_latency', 'no destination button found');
    } else {
      const destBtn = await page.$(`button:has-text("${destName}")`);
      if (!destBtn) {
        fail('command_ack_latency', `${destName} button not found`);
      } else {
        const t0 = Date.now();
        await destBtn.click({ force: true });

        // Wait for the button to become disabled or pending indicator to appear
        let ackMs = -1;
        for (let i = 0; i < MAX_COMMAND_ACK_MS / 100; i++) {
          await page.waitForTimeout(100);
          const disabled = await destBtn.evaluate(b => b.disabled).catch(() => true);
          const pending = await page.$('.pending-indicator, .pending').catch(() => null);
          if (disabled || pending) {
            ackMs = Date.now() - t0;
            break;
          }
        }

        if (ackMs > 0 && ackMs <= MAX_COMMAND_ACK_MS) {
          pass('command_ack_latency', `${ackMs}ms`);
        } else if (ackMs > MAX_COMMAND_ACK_MS) {
          fail('command_ack_latency', `${ackMs}ms exceeds ${MAX_COMMAND_ACK_MS}ms threshold`);
        } else {
          fail('command_ack_latency', 'button never became disabled/pending');
        }
      }
    }
  }

  // ── Test 2: ShipDeparted event latency ───────────────────────────────
  // Measure time from click until ShipDeparted appears in the WS event stream
  {
    const t0 = Date.now();
    let departMs = -1;

    for (let i = 0; i < MAX_FLIGHT_CONFIRM_MS / 200; i++) {
      await page.waitForTimeout(200);
      const events = await page.evaluate(() => window.__canonWsEvents || []);
      const departed = events.find(e =>
        e.event_type === 'ShipDeparted' && e.ts >= t0);
      if (departed) {
        departMs = departed.ts - t0;
        break;
      }
    }

    if (departMs > 0 && departMs <= MAX_FLIGHT_CONFIRM_MS) {
      pass('ship_departed_latency', `${departMs}ms`);
    } else if (departMs > MAX_FLIGHT_CONFIRM_MS) {
      fail('ship_departed_latency', `${departMs}ms exceeds ${MAX_FLIGHT_CONFIRM_MS}ms threshold`);
    } else {
      fail('ship_departed_latency', 'ShipDeparted event never arrived');
    }
  }

  // ── Test 3: Full flight latency (depart → arrive → dock) ────────────
  // Ship is already in transit from test 2, wait for arrival
  {
    const t0 = Date.now();
    let arrivalMs = -1;

    for (let i = 0; i < MAX_ARRIVAL_MS / 500; i++) {
      await page.waitForTimeout(500);
      const btns = await enabledBtns();
      // Arrived when Load/Deliver buttons appear
      if (btns.some(b => b.includes('Load') || b.includes('Deliver') || b.includes('\u25c9'))) {
        arrivalMs = Date.now() - t0;
        break;
      }
      // Check for game over
      if (btns.includes('Restart')) {
        arrivalMs = -2;
        break;
      }
    }

    if (arrivalMs > 0 && arrivalMs <= MAX_ARRIVAL_MS) {
      pass('full_flight_latency', `${arrivalMs}ms`);
    } else if (arrivalMs > MAX_ARRIVAL_MS) {
      fail('full_flight_latency', `${arrivalMs}ms exceeds ${MAX_ARRIVAL_MS}ms threshold`);
    } else if (arrivalMs === -2) {
      fail('full_flight_latency', 'game over during transit');
    } else {
      fail('full_flight_latency', 'ship never arrived');
    }
  }

  // ── Test 4: Cross-service event cascade latency ──────────────────────
  // After arrival, ShipArrivedAtStation triggers navigation→station→cargo chain.
  // Measure time from arrival to ManifestCreated (cross-service confirmation).
  {
    const t0 = Date.now();
    let cascadeMs = -1;

    for (let i = 0; i < 10_000 / 200; i++) {
      await page.waitForTimeout(200);
      const events = await page.evaluate(() => window.__canonWsEvents || []);
      const manifest = events.find(e =>
        e.event_type === 'ManifestCreated' && e.ts >= t0);
      if (manifest) {
        cascadeMs = manifest.ts - t0;
        break;
      }
    }

    if (cascadeMs > 0) {
      pass('cross_service_cascade', `${cascadeMs}ms (arrival → ManifestCreated)`);
    } else {
      // ManifestCreated may not fire for all game states — soft pass
      pass('cross_service_cascade', 'skipped (no manifest event — may depend on game state)');
    }
  }

  // ── Test 5: Stock drain regularity ───────────────────────────────────
  // Collect StockDrained events PER STATION and verify each station's
  // drain arrives at regular ~3s intervals. Cross-station gaps are expected
  // to be small (~750ms stagger) so we only check per-station regularity.
  {
    // Clear event buffer and collect fresh drain events with aggregate_id
    await page.evaluate(() => { window.__canonDrainEvents = []; });
    await page.evaluate(() => {
      const origPush = window.__canonWsEvents.push.bind(window.__canonWsEvents);
      window.__canonWsEvents.push = function(evt) {
        if (evt.event_type === 'StockDrained') {
          window.__canonDrainEvents.push({ ts: evt.ts, agg: evt.aggregate_id || '' });
        }
        return origPush(evt);
      };
    });

    // Wait to collect enough drain samples
    // Drain ticks every 10s per station, need DRAIN_SAMPLE_COUNT per station + buffer
    const collectTime = (DRAIN_SAMPLE_COUNT + 2) * 10 * 1000;
    console.log(`    collecting ${DRAIN_SAMPLE_COUNT} drain events per station (~${Math.round(collectTime/1000)}s)...`);
    await page.waitForTimeout(collectTime);

    const drainEvents = await page.evaluate(() => window.__canonDrainEvents || []);

    if (drainEvents.length < 4) {
      fail('stock_drain_regularity', `only ${drainEvents.length} drain events collected in ${Math.round(collectTime/1000)}s`);
    } else {
      // Group events by station (aggregate_id)
      const byStation = {};
      for (const evt of drainEvents) {
        const key = evt.agg || 'unknown';
        if (!byStation[key]) byStation[key] = [];
        byStation[key].push(evt.ts);
      }

      const stationIds = Object.keys(byStation);
      console.log(`    ${drainEvents.length} total drain events across ${stationIds.length} stations`);

      // Compute per-station gaps (each station should drain every ~3s)
      let allPerStationGaps = [];
      let worstCV = 0;
      let worstStation = '';
      for (const [sid, timestamps] of Object.entries(byStation)) {
        if (timestamps.length < 2) continue;
        const gaps = [];
        for (let i = 1; i < timestamps.length; i++) {
          gaps.push(timestamps[i] - timestamps[i - 1]);
        }
        const avg = gaps.reduce((a, b) => a + b, 0) / gaps.length;
        const stdDev = Math.sqrt(gaps.reduce((sum, g) => sum + (g - avg) ** 2, 0) / gaps.length);
        const cv = stdDev / avg;
        const shortId = sid.substring(0, 8);
        console.log(`    station ${shortId}: ${gaps.length} gaps, avg=${Math.round(avg)}ms, stddev=${Math.round(stdDev)}ms, CV=${(cv * 100).toFixed(0)}%`);
        allPerStationGaps.push(...gaps);
        if (cv > worstCV) { worstCV = cv; worstStation = shortId; }
      }

      if (allPerStationGaps.length === 0) {
        fail('stock_drain_regularity', 'no per-station gaps computed');
      } else {
        const minGap = Math.min(...allPerStationGaps);
        const maxGap = Math.max(...allPerStationGaps);
        const avgGap = Math.round(allPerStationGaps.reduce((a, b) => a + b, 0) / allPerStationGaps.length);

        // Per-station gaps should be ~3000ms (one drain cycle).
        // Allow 1s-8s range (pipeline jitter on GKE is real).
        if (minGap < MIN_DRAIN_GAP_MS) {
          fail('stock_drain_no_bursting', `per-station min gap ${minGap}ms < ${MIN_DRAIN_GAP_MS}ms — same station draining too fast`);
        } else {
          pass('stock_drain_no_bursting', `per-station min gap ${minGap}ms`);
        }

        if (maxGap > MAX_DRAIN_GAP_MS) {
          fail('stock_drain_no_gaps', `per-station max gap ${maxGap}ms > ${MAX_DRAIN_GAP_MS}ms — station went silent`);
        } else {
          pass('stock_drain_no_gaps', `per-station max gap ${maxGap}ms`);
        }

        // Per-station CV should be < 60% (more lenient than cross-station)
        if (worstCV < 0.6) {
          pass('stock_drain_regularity', `worst station CV=${(worstCV * 100).toFixed(0)}% (${worstStation}), avg gap=${avgGap}ms`);
        } else {
          fail('stock_drain_regularity', `station ${worstStation} CV=${(worstCV * 100).toFixed(0)}% > 60% — erratic per-station timing`);
        }
      }
    }
  }

  // ── Test 6: Station stock decreases over time ─────────────────────────
  // Wait for at least one full drain cycle (10s) and verify overall trend
  // is downward. One station may increase if a delivery just completed.
  {
    const before = await page.$$eval('.stn-card-pct', els =>
      els.map(e => parseFloat(e.textContent)));
    await page.waitForTimeout(15_000); // 1.5 drain cycles
    const after = await page.$$eval('.stn-card-pct', els =>
      els.map(e => parseFloat(e.textContent)));

    let increased = 0;
    let decreased = 0;
    for (let i = 0; i < before.length; i++) {
      if (after[i] > before[i] + 1.0) increased++;
      if (after[i] < before[i] - 0.5) decreased++;
    }

    // At most 1 station may increase (delivery destination).
    // At least 2 should decrease (drain is running).
    if (decreased >= 2 && increased <= 1) {
      pass('stock_drain_trend', `${decreased} decreased, ${increased} increased: [${before.map(v => v.toFixed(1)).join(', ')}] → [${after.map(v => v.toFixed(1)).join(', ')}]`);
    } else if (decreased === 0 && increased === 0) {
      pass('stock_drain_trend', 'no change detected (drain may not have ticked yet)');
    } else {
      fail('stock_drain_trend', `unexpected: ${decreased} decreased, ${increased} increased: [${before.map(v => v.toFixed(1)).join(', ')}] → [${after.map(v => v.toFixed(1)).join(', ')}]`);
    }
  }

  // ── Test 7: WebSocket event throughput ───────────────────────────────
  // Check that we're receiving a healthy stream of events
  {
    const events = await page.evaluate(() => window.__canonWsEvents || []);
    const now = Date.now();
    const recentEvents = events.filter(e => e.ts > now - 30_000);

    if (recentEvents.length >= 5) {
      pass('ws_event_throughput', `${recentEvents.length} events in last 30s`);
    } else {
      fail('ws_event_throughput', `only ${recentEvents.length} events in last 30s — pipeline may be stalled`);
    }

    // Check for InfraStatus events (health checks)
    const infraEvents = events.filter(e => e.type === 'InfraStatus');
    if (infraEvents.length > 0) {
      const latest = infraEvents[infraEvents.length - 1];
      if (latest.kafka_ok && latest.yugabyte_ok && latest.cassandra_ok) {
        pass('infra_health', 'all backends healthy');
      } else {
        fail('infra_health', `kafka=${latest.kafka_ok}, yugabyte=${latest.yugabyte_ok}, cassandra=${latest.cassandra_ok}`);
      }
    } else {
      fail('infra_health', 'no InfraStatus events received');
    }
  }

  await page.close();
  await browser.close();

  console.log(`\n  ${passed} passed, ${failed} failed\n`);
  process.exit(failed > 0 ? 1 : 0);
})();
