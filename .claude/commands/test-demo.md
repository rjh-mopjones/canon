# test-demo — Canon demo end-to-end pipeline test

You are verifying that the Canon demo ("canon-demo") works end-to-end, including the full event sourcing pipeline. This command is **iterative**: when you find issues, you FIX them immediately and re-test until everything passes. At the end, you raise a single GitHub issue + PR with all fixes.

**Philosophy**: Don't just report — fix, verify, repeat.

## Phase 1 — Infrastructure

1. **Rebuild the Docker builder image** first — stale builder images are the #1 cause of service crashes:
   ```
   cd /path/to/canon && docker build --no-cache -t canon-builder:latest -f canon-demo/Dockerfile.builder .
   ```
   Then rebuild all service images:
   ```
   cd canon-demo && docker compose build --no-cache gateway fleet-service cargo-service navigation-service station-service supply-service
   ```
   This ensures Docker images contain the latest compiled code.

2. Start all infrastructure via Docker Compose:
   ```
   cd canon-demo && docker compose up -d
   ```

3. Wait for all containers to report healthy (max 120s, poll every 5s). Infrastructure:
   - YugabyteDB (port 5433), Cassandra (port 9042), Zookeeper (port 2181), Kafka (9092 internal / 9093 external)
   - There is NO PostgreSQL and NO RabbitMQ.

4. Wait for init containers to complete:
   - `init-schema` — must show `exited (0)`
   - `init-kafka-topics` — must show `exited (0)`
   If either failed, print its logs and stop.

5. Verify connectivity:
   - YugabyteDB: `docker compose exec yugabytedb bash -c 'PGPASSWORD=canon bin/ysqlsh -h $(hostname -i) -U canon -d canon -c "SELECT 1"'`
   - Cassandra: `docker compose exec cassandra cqlsh -e "DESCRIBE KEYSPACES"` (retry up to 60s)
   - Kafka: `docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list`

If any infra check fails, print docker logs and stop.

## Phase 2 — Services & Gateway

6. Check that all service containers are running (`docker compose ps`). All 5 services + gateway + frontend should show "Up".

7. **If any service exited**, check its logs. Common failure modes:
   - `Keyspace 'canon' does not exist` → stale Docker image, rebuild the builder (Phase 1 step 1)
   - `SSLRequest` or `sslmode` errors → when running natively, add `?sslmode=disable` to YUGABYTE_URL
   - `Connection refused` → infrastructure container may have died, restart it

8. **If the gateway Docker container fails**, fall back to running natively:
   ```
   docker compose stop gateway
   YUGABYTE_URL="postgres://canon:canon@localhost:5433/canon?sslmode=disable" \
   CASSANDRA_NODES=localhost:9042 \
   KAFKA_BROKERS=localhost:9093 \
   CORS_ORIGIN=http://localhost:3000 \
   LISTEN_ADDR=0.0.0.0:8080 \
   RUST_LOG=info \
   cargo run --release -p gateway > /tmp/gateway.log 2>&1 &
   ```
   Note: use `?sslmode=disable` and `localhost:9093` for Kafka when running natively. Store the PID for cleanup.

9. Wait for the gateway: poll `curl -sf http://localhost:8080/ships` (retry up to 60s). If running natively after a fresh compile, allow up to 10 minutes for the first build.

10. Wait for the frontend: poll `curl -sf http://localhost:3000` (retry up to 30s).

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
b. **Kafka**: `kafka-console-consumer --topic canon.fleet.events --from-beginning --timeout-ms 10000` should show events
c. **Cross-service**: Check navigation-service logs for "received ShipDeparted" and "PlanRoute" processing
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

    If the WebSocket shows 502, the gateway isn't reachable from the frontend nginx. Check:
    - Is the gateway Docker container running? (`docker compose ps gateway`)
    - Can the frontend container reach it? (`docker compose exec frontend curl -sf http://gateway:8080/ships`)
    - If gateway runs natively, it can't be reached via Docker hostname — you must run the gateway in Docker for the frontend WS to work.

## Phase 5 — Fix Loop

**This is the key difference from a regular test.** If any check failed:

16. For each failure:
    a. Investigate root cause (read logs, source code, check schemas)
    b. Fix the issue in the codebase
    c. Rebuild affected services (if Docker: rebuild builder + service images; if native: `cargo build --release`)
    d. Restart the affected service
    e. Re-run the specific check that failed

17. Keep iterating until ALL checks pass. Track what you fixed.

18. If a fix requires frontend changes, rebuild the frontend Docker image:
    ```
    cd canon-demo && docker compose build frontend && docker compose up -d frontend
    ```
    Note: if the frontend is a Trunk/WASM build, you need `trunk build --release` first, then rebuild the Docker image.

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
    - Kill any native PIDs (gateway, services, WebSocket listener)
    - `cd canon-demo && docker compose down`
    - Remove temp files: `/tmp/canon-ws-events.jsonl`, `/tmp/gateway.log`, `/tmp/canon-*.png`
    - Print "Cleanup complete."

## Important Notes

- `docker-compose.yml` lives in `canon-demo/` — always `cd canon-demo` first.
- Infrastructure: YugabyteDB (5433), Cassandra (9042), Zookeeper (2181), Kafka (9092/9093).
- Gateway: port 8080. Frontend: port 3000.
- When running natively: `KAFKA_BROKERS=localhost:9093`, `YUGABYTE_URL=...?sslmode=disable`.
- Package names: `gateway`, `fleet-service`, `cargo-service`, `navigation-service`, `station-service`, `supply-service`.
- Read any file in the repo to figure out correct commands/ports/endpoints. Do not guess.
- **Always rebuild the Docker builder image** before testing if you suspect stale binaries.
- **For frontend WS to work**, the gateway must run in Docker (nginx proxies to `gateway:8080`).

## Kubernetes / minikube alternative

If the user requests `--k8s` or the test should run against minikube:

1. Replace Phase 1 with:
   ```bash
   cd canon-demo && make k8s-up
   ```
   This starts minikube, builds all images into minikube's Docker daemon, and applies the Kustomize overlay.

2. Wait for infrastructure pods to be ready:
   ```bash
   kubectl wait --for=condition=ready pod -l tier=infra -n canon --timeout=180s
   ```

3. Wait for init jobs to complete:
   ```bash
   kubectl wait --for=condition=complete job/init-schema -n canon --timeout=120s
   kubectl wait --for=condition=complete job/init-kafka-topics -n canon --timeout=120s
   ```

4. Wait for application pods:
   ```bash
   kubectl wait --for=condition=ready pod -l tier=app -n canon --timeout=120s
   ```

5. Port-forward the gateway for API testing:
   ```bash
   kubectl port-forward svc/gateway 8080:8080 -n canon &
   ```

6. Access frontend via `minikube tunnel` or `kubectl port-forward svc/frontend 3000:80 -n canon &`.

7. All Phase 3–6 API checks remain the same (they hit localhost:8080).

8. Cleanup: `make k8s-down` instead of `docker compose down`.
