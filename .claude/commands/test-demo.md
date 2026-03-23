# test-demo — Canon demo end-to-end pipeline test

You are verifying that the Canon demo ("canon-demo") works end-to-end, including the full event sourcing pipeline. This command is **iterative**: when you find issues, you FIX them immediately and re-test until everything passes. At the end, you raise a single GitHub issue + PR with all fixes.

**Philosophy**: Don't just report — fix, verify, repeat.

## Phase 1 — Infrastructure

1. **Cross-compile and build all images**:
   ```bash
   cd canon-demo && make k8s-build
   ```
   This cross-compiles all 6 backend services locally (`cargo build --release --target aarch64-unknown-linux-musl`), builds slim alpine Docker images (COPY binary), builds the frontend WASM image, and loads everything into minikube via `minikube image load`. Should complete in under 3 minutes. Stale images are the #1 cause of service crashes — always rebuild.

   **Prerequisites** (one-time):
   ```bash
   rustup target add aarch64-unknown-linux-musl
   brew install filosottile/musl-cross/musl-cross
   ```

2. **Deploy the full stack**:
   ```bash
   cd canon-demo && make k8s-deploy
   ```
   This applies the Kustomize minikube overlay (`kubectl apply -k k8s/overlays/minikube/`).

   Or run both steps at once:
   ```bash
   cd canon-demo && make k8s-up
   ```

3. Wait for infrastructure pods to be ready (max 180s):
   ```bash
   kubectl wait --for=condition=ready pod -l tier=infra -n canon --timeout=180s
   ```
   Infrastructure: YugabyteDB (5433), Cassandra (9042), Zookeeper (2181), Kafka (9092).
   There is NO PostgreSQL and NO RabbitMQ.

4. Wait for init jobs to complete:
   ```bash
   kubectl wait --for=condition=complete job/init-schema -n canon --timeout=120s
   kubectl wait --for=condition=complete job/init-kafka-topics -n canon --timeout=120s
   ```
   If either failed, print its logs and stop:
   ```bash
   kubectl logs job/init-schema -n canon
   kubectl logs job/init-kafka-topics -n canon
   ```

5. Verify connectivity:
   - YugabyteDB: `kubectl exec statefulset/yugabytedb -n canon -- bash -c 'PGPASSWORD=canon bin/ysqlsh -h $(hostname -i) -U canon -d canon -c "SELECT 1"'`
   - Cassandra: `kubectl exec statefulset/cassandra -n canon -- cqlsh -e "DESCRIBE KEYSPACES"` (retry up to 60s)
   - Kafka: `kubectl exec statefulset/kafka -n canon -- kafka-topics --bootstrap-server localhost:9092 --list`

If any infra check fails, check pod logs (`kubectl logs <pod> -n canon`) and stop.

## Phase 2 — Services & Gateway

6. Check that all application pods are running:
   ```bash
   kubectl get pods -l tier=app -n canon
   ```
   All 5 services + gateway + frontend should show "Running".

7. **If any service pod is in CrashLoopBackOff**, check its logs:
   ```bash
   kubectl logs deployment/<service-name> -n canon
   ```
   Common failure modes:
   - `Keyspace 'canon' does not exist` → stale image, rebuild with `make k8s-build`
   - `Connection refused` → infrastructure pod may have died, check `kubectl get pods -l tier=infra -n canon`

8. Port-forward the gateway for API testing:
   ```bash
   kubectl port-forward svc/gateway 8080:8080 -n canon &
   ```

9. Wait for the gateway: poll `curl -sf http://localhost:8080/health` (retry up to 60s).

10. Port-forward the frontend:
    ```bash
    kubectl port-forward svc/frontend 3000:80 -n canon &
    ```
    Wait for it: poll `curl -sf http://localhost:3000` (retry up to 30s).

## Phase 3 — API Pipeline Verification

This tests the full Canon event pipeline by playing the game through API calls.

### Step 1: Register stations
```bash
for station in "Alpha Depot" "Beta Relay" "Gamma Outpost" "Delta Prime"; do
  STATION_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
  curl -s -X POST "http://localhost:8080/stations/$STATION_ID/register" \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"$station\", \"capacity_kg\": 5000.0}"
done
```
**Verify**: `GET /stations` returns 4 stations (may need 5-10s for projection). If it returns 0 after 15s, the projection consumer isn't populating the read model — **investigate and fix**.

### Step 2: Register a ship
```bash
curl -s -X POST http://localhost:8080/fleet/ships \
  -H "Content-Type: application/json" \
  -d '{"name": "VSS Meridian", "capacity_kg": 5000.0}'
```
**Verify**: `GET /ships` returns the ship. Extract `aggregate_id` as SHIP_ID.

### Step 3: Open a WebSocket listener
Use `websocat` or Node.js with the `ws` module (use `NODE_PATH=$(npm root -g)` if ws is globally installed). Capture events to `/tmp/canon-ws-events.jsonl` for 60 seconds in the background.

### Step 4: Depart ship
```bash
curl -s -X POST "http://localhost:8080/fleet/ships/$SHIP_ID/depart" \
  -H "Content-Type: application/json" \
  -d "{\"destination\": \"$FIRST_STATION_ID\"}"
```
**Verify**: Returns 200 with `command_id`.

### Step 5: Verify event pipeline (wait 5-10s)

a. **Event store**: `GET /ships/$SHIP_ID/history` should show ShipRegistered + ShipDeparted (2+ events)
b. **Kafka**: `kubectl exec statefulset/kafka -n canon -- kafka-console-consumer --bootstrap-server localhost:9092 --topic canon.fleet.events --from-beginning --timeout-ms 10000` should show events
c. **Cross-service**: Check navigation-service logs for "received ShipDeparted" and "PlanRoute" processing:
   `kubectl logs deployment/navigation-service -n canon | grep -i "ShipDeparted\|PlanRoute"`
d. **Outbox**: Query `canon_fleet.outbox` — all rows should have `delivered_at` set

If any pipeline check fails, **investigate the logs, find the root cause, and fix it**. Common issues:
- Events not in Cassandra → event store consumer not running or Kafka consumer group offset issue
- Outbox not draining → outbox processor not started
- Cross-service not firing → navigation-service not subscribed to fleet topic

### Step 6: Cargo pipeline
```bash
curl -s -X POST http://localhost:8080/cargo/manifests \
  -H "Content-Type: application/json" \
  -d "{\"ship_id\": \"$SHIP_ID\", \"voyage_id\": \"$(uuidgen | tr '[:upper:]' '[:lower:]')\"}"
```
Then load cargo on the manifest. Verify events reach the cargo event store.

If cargo-service shows `commands=0 events=0`, the command handler registrations don't match — check that `#[command(Aggregate, ...)]` names match `#[command_handler(Aggregate, ...)]` names. **Fix and rebuild**.

### Step 7: Check admin endpoints
- `GET /admin/oversight/windows` → should return 200
- `GET /admin/deadletters` → should return 200

### Step 8: Verify WebSocket events
Kill the WS listener, check `/tmp/canon-ws-events.jsonl`. Should contain at least `Event` and/or `InfraStatus` messages. If 0 events, the gateway Kafka consumer or WS broadcast is broken.

## Phase 4 — UI Interaction Testing (Playwright)

This phase tests the game **as a real user would**, by clicking through the UI.

11. Install Playwright if needed: `npx playwright install chromium`

12. **Take screenshots** of the frontend and the mockup reference:
    ```
    npx playwright screenshot http://localhost:3000 /tmp/canon-landing.png --wait-for-timeout=5000
    npx playwright screenshot file://$(pwd)/canon-demo/frontend/reference/mockup.html /tmp/canon-mockup.png --wait-for-timeout=3000
    ```

13. **Compare visually** — look at both screenshots yourself. Check:
    - Layout matches mockup (header, canvas map, station cards, action bar, event log)
    - Fonts: Inter + JetBrains Mono (not Share Tech Mono or Rajdhani)
    - Colors via CSS custom properties
    - 4 station planets at correct positions
    - Ship rendered correctly (no duplicate labels like "VSS VSS MERIDIAN")
    - If visual issues found, **fix and re-screenshot**

14. **Interactive UI test** with Playwright script. Write a Node.js script that:

    a. Navigates to `http://localhost:3000`
    b. Waits for the canvas map to render (look for `<canvas>` element)
    c. Verifies 4 station cards are visible below the map
    d. Clicks on a station planet on the canvas (use approximate pixel coordinates based on station positions: Alpha Depot ~18% 26%, Beta Relay ~68% 14%, Gamma Outpost ~76% 68%, Delta Prime ~24% 74%)
    e. Verifies the ship action bar changes (e.g. shows a "Fly" or "Depart" button, or the ship starts moving)
    f. Waits for events to appear in the event log strip at the bottom
    g. Captures a screenshot showing the ship in transit
    h. Switches to the "Scenarios" tab and verifies the 5 mission cards render
    i. Captures console errors — no WASM panics, no WebSocket connection failures (ignore Trunk HMR `{{__trunk_address__}}` errors — that's a known cosmetic build issue)

    The test should output PASS/FAIL for each interaction step.

15. **Check browser console** for real errors:
    - WebSocket connection to `/events` must succeed (not 502)
    - No WASM panics or uncaught JS errors
    - Ignore Trunk HMR template errors (`{{__trunk_address__}}`)
    - Ignore Leptos reactive tracking warnings (cosmetic)

## Phase 4b — Automated Playwright Smoke Tests

Run the automated e2e smoke tests:
```bash
cd canon-demo && make k8s-test-e2e
```

This runs 6 tests from `canon-demo/e2e/test.js`:
1. `stations_have_initial_stock` — all 4 stations show > 0% stock
2. `stock_drains_over_time` — stock decreases after 12s (drain pipeline works)
3. `ship_popup_on_planet_click` — clicking a planet shows the ship popup
4. `event_log_receives_events` — WS events appear in the event log
5. `scenarios_page_renders` — all 5 mission cards present
6. `no_console_errors` — no real browser errors

All 6 must pass. If any fail, investigate and fix before proceeding.

## Phase 5 — Fix Loop

**This is the key difference from a regular test.** If any check failed:

16. For each failure:
    a. Investigate root cause (read logs, source code, check schemas)
    b. Fix the issue in the codebase
    c. Rebuild affected images: `cd canon-demo && make k8s-build` (cross-compiles locally, rebuilds slim images, loads into minikube)
    d. Restart the affected deployment: `kubectl rollout restart deployment/<name> -n canon`
    e. Re-run the specific check that failed

17. Keep iterating until ALL checks pass. Track what you fixed.

18. If a fix requires only frontend changes:
    ```bash
    docker build -t canon-demo/frontend -f canon-demo/frontend/Dockerfile .
    minikube image load canon-demo/frontend
    kubectl rollout restart deployment/frontend -n canon
    ```

## Phase 6 — Final Verdict

19. Once all checks pass, summarise in a table:

    | Check | Status | Notes |
    |-------|--------|-------|
    | Infrastructure | pass/fail | |
    | Services running | pass/fail | |
    | Ship registration | pass/fail | |
    | Station registration | pass/fail | |
    | Stations projection | pass/fail | Does GET /stations return registered stations? |
    | Ship departure | pass/fail | |
    | Event pipeline (Cassandra) | pass/fail | |
    | Kafka publishing | pass/fail | |
    | Cross-service flow | pass/fail | |
    | Cargo pipeline | pass/fail | |
    | Oversight endpoint | pass/fail | |
    | Dead letter endpoint | pass/fail | |
    | WebSocket events | pass/fail | |
    | Frontend renders | pass/fail | |
    | Mockup match | pass/fail | |
    | UI click-through | pass/fail | |
    | Browser console clean | pass/fail | |
    | Playwright smoke tests | pass/fail | `make k8s-test-e2e` — all 6 tests |

    Overall verdict must be **ALL PASS** before proceeding.

## Phase 7 — Issue & PR

20. If you made any fixes during the test, create a **single GitHub issue** summarising all problems found and fixed:
    ```
    gh issue create \
      --title "fix(demo): e2e test failures — <brief summary>" \
      --body "<all issues found, root causes, and fixes applied>"
    ```

21. Create a **single PR** referencing the issue with all fixes:
    - Branch: `fix/demo-e2e-<date>` or similar
    - Title: references the issue number
    - Body: lists all fixes with file paths

22. If no fixes were needed (everything passed first time), congratulate and skip issue/PR creation.

## Phase 8 — Cleanup

23. Always run cleanup:
    - Kill any port-forward processes
    - `cd canon-demo && make k8s-down`
    - Remove temp files: `/tmp/canon-ws-events.jsonl`, `/tmp/canon-*.png`
    - Print "Cleanup complete."

## Important Notes

- `Makefile` lives in `canon-demo/` — always `cd canon-demo` first.
- Infrastructure: YugabyteDB (5433), Cassandra (9042), Zookeeper (2181), Kafka (9092).
- Gateway: port 8080 (via port-forward). Frontend: port 3000 (via port-forward from 80).
- Package names: `gateway`, `fleet-service`, `cargo-service`, `navigation-service`, `station-service`, `supply-service`.
- Read any file in the repo to figure out correct commands/ports/endpoints. Do not guess.
- **Always rebuild images** (`make k8s-build`) before testing if you suspect stale binaries. This cross-compiles locally — no Docker Rust compilation.
- **Prerequisites**: `rustup target add aarch64-unknown-linux-musl` + `brew install filosottile/musl-cross/musl-cross`
- Use `kubectl logs deployment/<name> -n canon` to debug service issues.
- Use `make k8s-status` for a quick overview of all pods, jobs, and services.
