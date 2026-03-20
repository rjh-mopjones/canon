# test-demo — Canon demo end-to-end browser test

You are the Canon demo QA agent. Your job is to build the frontend, run automated
checks against the acceptance criteria from CLAUDE.md, serve the app, open it in
the browser, and systematically verify every interactive feature.

Read `CLAUDE.md` before doing anything else:
```bash
cat CLAUDE.md
```

---

## Phase 0 — Pre-flight checks

Verify the frontend source is in a compilable state before anything else.

### 0a. Check for source code violations

Scan for `unwrap()` and `expect()` outside tests (acceptance criterion):
```bash
cd canon-demo/frontend
grep -rn 'unwrap()' src/ | grep -v '_test\|#\[test\]' | head -30
grep -rn 'expect(' src/ | grep -v '_test\|#\[test\]' | head -30
```

If any are found, list them as **FAIL** items and continue (do not fix — report only).

### 0b. Check for hardcoded colours

Scan for hardcoded hex colours in Leptos components (not in CSS files):
```bash
grep -rn '#[0-9a-fA-F]\{3,8\}' src/ | grep -v '//\|///\|#\[' | head -20
```

If any are found in `.rs` files (not comments, not attributes), flag as **FAIL**.

### 0c. Verify all scenario components exist

```bash
for f in oversight.rs snapshot.rs resupply.rs dead_letter.rs idempotency.rs; do
  if [ -f "src/scenarios/$f" ]; then
    echo "OK: $f"
  else
    echo "MISSING: $f"
  fi
done
```

### 0d. Verify mockup reference exists

```bash
ls -la reference/mockup.html
```

---

## Phase 1 — Build the frontend

Build with Trunk in release mode to catch all compile errors:

```bash
cd canon-demo/frontend
trunk build --release 2>&1
```

Check the exit code. If the build fails:
- Print the full error output
- Categorise the failure (missing dependency, type error, borrow checker, etc.)
- Report as **BUILD FAIL** and STOP — no point continuing if it doesn't compile

If the build succeeds, verify the output:
```bash
ls -la dist/
# Expect: index.html, .wasm file, .js file, main.css
```

Report: **BUILD PASS** with the WASM bundle size.

---

## Phase 2 — Start the dev server

Start Trunk's dev server in the background:

```bash
cd canon-demo/frontend
trunk serve --port 8765 &
TRUNK_PID=$!
echo "Trunk dev server started (PID: $TRUNK_PID) on http://localhost:8765"
```

Wait for it to be ready:
```bash
for i in $(seq 1 30); do
  if curl -s -o /dev/null -w "%{http_code}" http://localhost:8765 | grep -q "200"; then
    echo "Server ready after ${i}s"
    break
  fi
  sleep 1
done
```

Verify the page returns valid HTML:
```bash
curl -s http://localhost:8765 | head -30
```

Check that the HTML contains expected elements:
```bash
curl -s http://localhost:8765 | grep -c 'id="app"'
curl -s http://localhost:8765 | grep -c 'main.css'
curl -s http://localhost:8765 | grep -c 'Share Tech Mono'
```

If any of these return 0, report as **SERVE FAIL**.

---

## Phase 3 — Open in browser and take screenshots

Open the app in the default browser:
```bash
open http://localhost:8765
```

**STOP HERE and ask the user:** "The demo is running at http://localhost:8765 and should have opened in your browser. I'll now walk through each acceptance criterion. Ready to proceed?"

Wait for the user to confirm before continuing.

---

## Phase 4 — Systematic acceptance criteria walkthrough

Work through each criterion from CLAUDE.md one by one. For each, tell the user what to
check in the browser and ask them to confirm pass/fail. Track results.

### Test 1: Visual match at 1440px

Ask the user:
> **Test 1 — Visual match**: Please resize your browser to 1440px wide. Open the mockup
> reference file (`canon-demo/frontend/reference/mockup.html`) in a separate tab.
> Compare side by side. Do they match visually (layout, colours, fonts, spacing)?

Also open the mockup for comparison:
```bash
open canon-demo/frontend/reference/mockup.html
```

Record their response.

### Test 2: Ships fly autonomously

Ask the user:
> **Test 2 — Autonomous flight**: On the Live Fleet page, do ships start flying
> automatically from page load? Are they cycling between stations continuously?
> (Expect 4 live ships — Meridian, Argo, Eclipse, Kronos — departing staggered ~1.8s apart.)

### Test 3: Ship popup

Ask the user:
> **Test 3 — Ship popup**: Click on any flying ship. Does a popup appear showing:
> - Ship name (Rajdhani font)
> - Status line
> - Destination buttons (4 stations, current station disabled)
> - Fuel percentage
> - Aggregate version
> - Events-since-snapshot progress bar (amber marker at origin, cyan fill)?

### Test 4: Ship departure via popup

Ask the user:
> **Test 4 — Manual departure**: In the ship popup, click a destination button.
> Does the ship depart? Does the full event chain fire in the sidebar log?
> (Expect: ShipDeparted, RouteAssigned, PositionUpdated..., ShipArrivedAtStation, etc.)

### Test 5: Oversight strip

Ask the user:
> **Test 5 — Oversight strip**: During a voyage, does an oversight strip appear at the
> bottom of the map? Does it show:
> - Handler ID and gate title
> - Two requirement rows (checkmark green if met, circle dim if pending)
> - Status badge (Not Ready = amber, Ready = green)?
> Does it disappear ~1s after both conditions are met?

### Test 6: Correlation highlighting

Ask the user:
> **Test 6 — Correlation highlighting**: In the sidebar event log, click any event entry.
> Do all entries sharing the same correlation ID get highlighted (lit border + background)?
> Click again to deselect. Does the footer hint text work?

### Test 7: Light/dark theme toggle

Ask the user:
> **Test 7 — Theme toggle**: Click the theme toggle in the header.
> - Does the entire UI switch to light mode (light backgrounds, adjusted colours)?
> - Does the starfield (background dots) fade out in light mode?
> - Toggle back — does it return to dark mode correctly?

### Test 8: Scenarios page — card grid

Ask the user:
> **Test 8 — Scenarios page**: Click the "Scenarios" tab. Do you see:
> - Hero section with title "Canon Feature Scenarios"
> - Grid of 5 mission cards
> - Each card has: mission number, name, ship line, description, tags, "Launch Mission" link
> - Hover raises cards with box-shadow?

### Test 9: Mission 01 — Oversight Gates

Ask the user:
> **Test 9 — Mission 01 "The Stranded Cargo"**: Launch Mission 01. In the runner:
> - Is there a step progress bar at the top?
> - Do you see the gate card with two requirement rows?
> - Row 1 (ShipArrivedAtStation) should be ticked green
> - Row 2 (ManifestCreated) should show empty circle, pulsing amber
> - Status badge shows "Not Ready" in amber
> - Click "File Cargo Manifest" — does row 2 animate to green checkmark?
> - Does badge flip to "Ready" in green with a glow pulse?
> - Does the gate card border transition from amber to green?
> - Do downstream events fire in the scenario event log?

### Test 10: Mission 02 — Snapshotting

Ask the user:
> **Test 10 — Mission 02 "The Ghost Ship"**: Launch Mission 02.
> - Do you see a hydration counter counting 0→247 rapidly (~40ms ticks)?
> - Does it show "replaying event vN..." status text?
> - At 247 does it pause and show elapsed time in amber ("~640ms")?
> - After snapshot write, does the second counter jump immediately to 247?
> - Do you see the side-by-side bar chart comparison?
> - Left bar: full width, red tint, "Without snapshot — 640ms — 247 events"
> - Right bar: narrow (~4%), green tint, "With snapshot — 28ms — 0 events"
> - Bars animate in with CSS width transition?
> - Speedup multiplier "23x faster hydration" in green below?

### Test 11: Mission 03 — Cross-service cascade

Ask the user:
> **Test 11 — Mission 03 "The Resupply Crisis"**: Launch Mission 03.
> - Do you see a vertical pipeline of 5 service nodes?
> - (station -> supply -> fleet -> nav -> cargo)
> - Are nodes connected by animated arrows?
> - Do nodes light up in sequence as the cascade fires?
> - Does each node pulse when its event arrives?
> - Does a travelling dot animate along each arrow?
> - Does the full pipeline animate top-to-bottom over ~6 seconds?
> - Does "10 events across 5 services" summary appear at the bottom?

### Test 12: Mission 04 — Dead letters

Ask the user:
> **Test 12 — Mission 04 "The Cassandra Incident"**: Launch Mission 04.
> - Do you see 3 stacked dead-letter cards?
> - Each shows: event name (red), attempt count, error string (mono)
> - Two buttons per card: "Requeue" and "Discard"
> - Click "Requeue" on one — does border transition red→green, opacity drop, "requeued" text?
> - Click "Discard" on another — does card fade out?
> - When all 3 are handled, does a success state appear?

### Test 13: Mission 05 — Idempotency

Ask the user:
> **Test 13 — Mission 05 "The Duplicate Signal"**: Launch Mission 05.
> - Two command envelope cards side by side?
> - Both show identical content with same message_id in cyan?
> - Card 1: "Command 1" green, "ACCEPTED" badge
> - Card 2: "Command 2 (duplicate)", "PENDING" amber badge
> - Click trigger — does red X sweep across card 2?
> - Badge changes to "DEDUPLICATED" dim red, opacity drops to 0.4?
> - Note appears: "INSERT ... ON CONFLICT DO NOTHING"?
> - Ship departs exactly once in the log?

### Test 14: WebSocket connection

Ask the user:
> **Test 14 — WebSocket**: Open browser DevTools (Network tab, filter WS).
> Is there a WebSocket connection attempt to `/events`?
> (It may show as failed/reconnecting if the gateway isn't running — that's OK.
> The important thing is the connection attempt exists.)

### Test 15: Initial hydration

Ask the user:
> **Test 15 — Initial hydration**: In browser DevTools (Network tab, filter XHR/Fetch),
> do you see fetch requests to these endpoints on page load?
> - GET /ships
> - GET /stations
> - GET /admin/oversight/windows
> - GET /admin/deadletters
> (They may 404 if the gateway isn't running — that's OK. The requests should exist.)

---

## Phase 5 — Compile results

After all tests, compile the results:

```
══════════════════════════════════════════════════════════
  CANON DEMO E2E TEST RESULTS
══════════════════════════════════════════════════════════

  Automated checks:
    [ ] No unwrap()/expect() outside tests
    [ ] No hardcoded colours in components
    [ ] All 5 scenario components exist
    [ ] Mockup reference exists
    [ ] trunk build --release passes
    [ ] Dev server responds with valid HTML

  Browser tests:
    [ ] Visual match to mockup at 1440px
    [ ] Ships fly autonomously from page load
    [ ] Ship popup with correct data
    [ ] Manual departure fires event chain
    [ ] Oversight strip with live requirement state
    [ ] Correlation highlighting in event log
    [ ] Light/dark theme toggle + starfield
    [ ] Scenarios page card grid
    [ ] Mission 01 — Oversight Gates animations
    [ ] Mission 02 — Snapshotting visualisation
    [ ] Mission 03 — Cross-service cascade pipeline
    [ ] Mission 04 — Dead letter recovery
    [ ] Mission 05 — Idempotency deduplication
    [ ] WebSocket connection attempt
    [ ] Initial hydration fetch requests

  PASS: N/15    FAIL: N/15    SKIP: N/15

══════════════════════════════════════════════════════════
```

Mark each as PASS, FAIL, or SKIP (if the user skipped or the feature isn't implemented yet).

For any FAIL items, note the specific failure and what needs to be fixed.

---

## Phase 6 — Cleanup

Kill the trunk dev server:
```bash
kill $TRUNK_PID 2>/dev/null
echo "Trunk server stopped."
```

If there are FAIL items, ask the user:
> "Would you like me to create GitHub issues for the failures, or fix them directly?"

---

## Rules

- **Do not fix anything automatically** — this is a test command, not a fix command.
- **Report everything** — even minor visual differences matter for the demo.
- **Be specific** — "the button is misaligned" is not enough; say "the 'Launch Mission' link is 4px lower than in the mockup at 1440px".
- **Wait for user input** between browser tests — do not rush through.
- **Track all results** — every test must have a PASS, FAIL, or SKIP recorded.
- **Kill the server** when done — do not leave orphaned processes.
