# test-demo — Canon demo end-to-end pipeline test

You are verifying that the Canon demo ("canon-demo") works end-to-end, including the full event sourcing pipeline. Follow every step below precisely. Do NOT skip steps or assume anything is healthy without checking.

## Phase 1 — Infrastructure

1. Start the backing infrastructure via Docker Compose from the `canon-demo/` directory:
   ```
   cd canon-demo && docker compose up -d
   ```
2. Wait for all containers to report healthy. Run `docker compose ps` in a loop (max 120s, poll every 5s) until **YugabyteDB**, **Cassandra**, **Zookeeper**, and **Kafka** all show "healthy" or "running". Note: there is no PostgreSQL or RabbitMQ — Canon uses YugabyteDB (YSQL on port 5433), Cassandra (port 9042), Zookeeper (port 2181), and Kafka (ports 9092 internal / 9093 external). If any container is stuck or restarting after 120s, report the failure and stop.

3. Wait for the init containers to complete:
   - `init-schema` — creates YugabyteDB tables and Cassandra keyspace/tables. Must show `exited (0)`.
   - `init-kafka-topics` — creates the 5 `canon.*.events` topics. Must show `exited (0)`.
   If either init container failed, print its logs and stop.

4. Verify connectivity from the host:
   - YugabyteDB: `docker compose exec yugabytedb bin/ysqlsh -U canon -d canon -c 'SELECT 1'` (retry up to 30s)
   - Cassandra: `docker compose exec cassandra cqlsh -e "DESCRIBE KEYSPACES"` (retry up to 60s — Cassandra is slow to boot)
   - Kafka: `docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list` — should show the 5 `canon.*.events` topics

If any infra check fails, print the docker logs for that container and stop.

## Phase 2 — Build & Launch Services

5. The docker-compose file builds and launches all 5 domain services (fleet-service, cargo-service, navigation-service, station-service, supply-service), the **gateway** (port 8080), and the **frontend** (nginx on port 3000, mapped from container port 80). Check that all service containers are running:
   ```
   docker compose ps
   ```
   All service containers should show "running" or "Up". If any service container has exited, print its logs and stop.

6. If the gateway Docker container fails (a known issue: GLIBC version mismatch on musl-based images), fall back to running the gateway natively:
   ```
   docker compose stop gateway
   YUGABYTE_URL=postgres://canon:canon@localhost:5433/canon \
   CASSANDRA_NODES=localhost:9042 \
   KAFKA_BROKERS=localhost:9093 \
   CORS_ORIGIN=http://localhost:3000 \
   LISTEN_ADDR=0.0.0.0:8080 \
   RUST_LOG=info \
   cargo run --release -p canon-demo-gateway > /tmp/gateway.log 2>&1 &
   ```
   Store the PID for cleanup. Note: use `localhost:9093` for Kafka (the external listener) when running natively on the host.

7. Wait for the gateway to be listening:
   - Poll `curl -sf http://localhost:8080/ships` — retry up to 30s. Expect a 200 response (even if the body is an empty array `[]`).
   - If the gateway is not responding after 30s, cat `/tmp/gateway.log` (if running natively) or `docker compose logs gateway`, report the failure, and skip to cleanup.

8. Wait for the frontend to be serving:
   - Poll `curl -sf http://localhost:3000` — retry up to 30s. Expect an HTML response.
   - If not responding, check `docker compose logs frontend` and report.

## Phase 3 — Play the Supply Chain Game via API

This is the core phase. It tests the full Canon event pipeline by playing the game through API calls and verifying events flow through the pipeline (command → outbox → Kafka → event store → projection → WebSocket).

### Step 1: Register stations

Register the 4 stations:
```bash
for station in "Alpha Depot" "Beta Relay" "Gamma Outpost" "Delta Prime"; do
  STATION_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
  RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "http://localhost:8080/stations/$STATION_ID/register" \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"$station\", \"capacity_kg\": 5000.0}")
  HTTP_CODE=$(echo "$RESPONSE" | tail -1)
  echo "Registered $station ($STATION_ID): HTTP $HTTP_CODE"
done
```
**Verify**: `GET http://localhost:8080/stations` returns 4 stations. If it returns fewer, print the response and any service logs, but continue.

### Step 2: Register a ship

```bash
SHIP_RESPONSE=$(curl -s -X POST http://localhost:8080/fleet/ships \
  -H "Content-Type: application/json" \
  -d '{"name": "VSS Meridian", "capacity_kg": 5000.0}')
SHIP_ID=$(echo "$SHIP_RESPONSE" | jq -r '.aggregate_id')
echo "Ship registered: $SHIP_ID"
```
**Verify**: `GET http://localhost:8080/ships` returns 1 ship with the correct ID. If `SHIP_ID` is null or empty, the command failed — print the response body and gateway logs, then skip to cleanup.

### Step 3: Open a WebSocket listener in the background

Capture live events for later verification:
```bash
# Use websocat if available, otherwise try a node one-liner
if command -v websocat &>/dev/null; then
  timeout 30 websocat -t ws://localhost:8080/events > /tmp/canon-ws-events.jsonl 2>/dev/null &
  WS_PID=$!
else
  # Node.js fallback using ws module
  node -e "
  const WebSocket = require('ws');
  const fs = require('fs');
  const ws = new WebSocket('ws://localhost:8080/events');
  const stream = fs.createWriteStream('/tmp/canon-ws-events.jsonl');
  ws.on('message', d => stream.write(d.toString() + '\n'));
  setTimeout(() => { stream.end(); ws.close(); process.exit(0); }, 30000);
  " > /tmp/ws-listener.log 2>&1 &
  WS_PID=$!
fi
echo "WebSocket listener PID: $WS_PID"
```
If neither `websocat` nor `node` with `ws` module is available, note this as a skip and continue without WebSocket capture.

### Step 4: Depart ship to a station

Test the `DepartForStation` command → `ShipDeparted` event flow:
```bash
# Get a station ID from the registered stations
DEST_STATION=$(curl -s http://localhost:8080/stations | jq -r '.[0].station_id // .[0].aggregate_id // empty')

if [ -z "$DEST_STATION" ]; then
  echo "FAIL: No stations found — cannot depart"
else
  DEPART_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "http://localhost:8080/fleet/ships/$SHIP_ID/depart" \
    -H "Content-Type: application/json" \
    -d "{\"destination\": \"$DEST_STATION\"}")
  HTTP_CODE=$(echo "$DEPART_RESPONSE" | tail -1)
  BODY=$(echo "$DEPART_RESPONSE" | head -n -1)
  echo "Depart response (HTTP $HTTP_CODE): $BODY"
fi
```
**Verify**: Response should be 200/202 with a `command_id` and/or `correlation_id`. If it fails, print the gateway and fleet-service logs.

### Step 5: Wait for event propagation and verify the event store

Sleep 5 seconds to allow events to flow through the pipeline (command → outbox → Kafka → Cassandra event store → projections).

```bash
sleep 5
```

Then verify events reached the stores:

**a. Check ship event history** (reads from Cassandra event store):
```bash
HISTORY=$(curl -s "http://localhost:8080/ships/$SHIP_ID/history")
echo "Ship history: $HISTORY"
EVENT_COUNT=$(echo "$HISTORY" | jq 'length // 0')
echo "Events in history: $EVENT_COUNT"
```
**Verify**: Should contain at least `ShipRegistered` + `ShipDeparted` events (2+ events). If 0 events, the event store pipeline is broken.

**b. Check debug endpoints** (if they exist — these are optional, do not fail if 404):
```bash
# These endpoints may or may not exist — check gracefully
curl -s "http://localhost:8080/debug/events?aggregateId=$SHIP_ID" | jq '.' 2>/dev/null || echo "Debug events endpoint not available"
curl -s "http://localhost:8080/debug/commands?aggregateId=$SHIP_ID" | jq '.' 2>/dev/null || echo "Debug commands endpoint not available"
```

### Step 6: Check cross-service event propagation via Kafka

Verify that `ShipDeparted` was published to the fleet events topic and is available for other services:
```bash
docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic canon.fleet.events \
  --from-beginning \
  --timeout-ms 10000 2>/dev/null | head -10
```
**Verify**: Output should contain at least one message (the `ShipDeparted` event). If the topic is empty or the command times out with no output, Kafka publishing is broken.

Also check if the navigation service received the event (ShipDeparted triggers cross-service flow to Navigation):
```bash
docker compose logs --tail=50 navigation-service 2>/dev/null | grep -i "departed\|route\|received" || echo "No navigation processing logs found"
```

### Step 7: Create a manifest and load cargo

Test the cargo service pipeline:
```bash
VOYAGE_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
MANIFEST_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST http://localhost:8080/cargo/manifests \
  -H "Content-Type: application/json" \
  -d "{\"ship_id\": \"$SHIP_ID\", \"voyage_id\": \"$VOYAGE_ID\"}")
HTTP_CODE=$(echo "$MANIFEST_RESPONSE" | tail -1)
MANIFEST_BODY=$(echo "$MANIFEST_RESPONSE" | head -n -1)
MANIFEST_ID=$(echo "$MANIFEST_BODY" | jq -r '.aggregate_id // empty')
echo "Manifest created (HTTP $HTTP_CODE): $MANIFEST_ID"

if [ -n "$MANIFEST_ID" ]; then
  ITEM_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
  LOAD_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "http://localhost:8080/cargo/manifests/$MANIFEST_ID/load" \
    -H "Content-Type: application/json" \
    -d "{\"item_id\": \"$ITEM_ID\", \"weight_kg\": 500.0, \"description\": \"Supply crates\"}")
  HTTP_CODE=$(echo "$LOAD_RESPONSE" | tail -1)
  echo "Cargo loaded (HTTP $HTTP_CODE)"
fi
```
**Verify**: Both POST calls return 200/202. If `MANIFEST_ID` is empty, the cargo service command handler failed.

Then check the manifest state:
```bash
if [ -n "$MANIFEST_ID" ]; then
  curl -s "http://localhost:8080/cargo/manifests/$MANIFEST_ID" | jq '.'
fi
```
**Verify**: Should show `ManifestCreated` + `CargoLoaded` events or a state reflecting loaded cargo.

### Step 8: Check oversight windows

Test the inbox/oversight pipeline:
```bash
OVERSIGHT=$(curl -s "http://localhost:8080/admin/oversight/windows")
echo "Oversight windows: $OVERSIGHT"
```
**Verify**: Endpoint returns 200 (even if the array is empty). If windows are present, they should show `handler_id`, `correlation_key`, and `status` (pending/ready). Active windows prove the inbox+oversight pipeline is working.

### Step 9: Check dead letter handling

```bash
DEADLETTERS=$(curl -s "http://localhost:8080/admin/deadletters")
echo "Dead letters: $DEADLETTERS"
```
**Verify**: Endpoint returns 200. An empty array is fine (no failures = good). If dead letters exist, note the error messages — they may indicate pipeline issues worth investigating.

### Step 10: Stop the WebSocket listener and check captured events

```bash
if [ -n "$WS_PID" ]; then
  kill $WS_PID 2>/dev/null
  wait $WS_PID 2>/dev/null

  if [ -f /tmp/canon-ws-events.jsonl ]; then
    WS_EVENT_COUNT=$(wc -l < /tmp/canon-ws-events.jsonl | tr -d ' ')
    echo "WebSocket events captured: $WS_EVENT_COUNT"

    if [ "$WS_EVENT_COUNT" -gt 0 ]; then
      echo "Event types received:"
      cat /tmp/canon-ws-events.jsonl | jq -r '.type // "unknown"' 2>/dev/null | sort | uniq -c | sort -rn
      echo ""
      echo "First 3 events:"
      head -3 /tmp/canon-ws-events.jsonl | jq '.' 2>/dev/null
    fi
  else
    echo "No WebSocket events file found"
  fi
fi
```
**Verify**: At least 1 event captured. Expected types include `Event`, `ShipUpdate`, `StationUpdate`, `InfraStatus`. If 0 events, the WebSocket broadcast pipeline is broken.

## Phase 4 — Visual Browser Verification & Mockup Comparison

9. Take a **screenshot** of the frontend landing page and the mockup:
    ```
    npx playwright screenshot http://localhost:3000 /tmp/canon-landing.png --wait-for-timeout=5000
    ```
    (Install playwright first if needed: `npx playwright install chromium`)

    Also open the mockup reference for comparison:
    ```
    npx playwright screenshot file://$(pwd)/canon-demo/frontend/reference/mockup.html /tmp/canon-mockup.png --wait-for-timeout=3000
    ```

10. **Look at both screenshots yourself.** Compare the landing page against the mockup. Verify:
    - The page renders (not blank, not an error page, not a build spinner).
    - **Fonts**: The app uses `Inter` for body text and `JetBrains Mono` for monospace readouts — NOT `Share Tech Mono`, `Rajdhani`, or other fonts. Check heading, label, and badge fonts.
    - **Text casing**: No `text-transform: uppercase` applied globally. Text should appear in sentence case or as specified in the mockup.
    - **Layout**: The page structure matches the mockup — header with nav tabs, canvas map area, station cards, ship action bar, event log strip at bottom.
    - **Colours**: Uses the CSS custom properties (cyan, green, amber, red) from the design system, not hardcoded hex values.
    - **Canvas map**: Shows 4 station planets at approximately the right positions, with the ship rendered.
    - **Station cards**: 4 cards below the map with stock level bars.
    - No giant red error banners or stack traces visible.

11. If the frontend looks substantially different from the mockup (wrong fonts, wrong layout, missing sections, wrong colours), note specific differences. This is a **FAIL** for mockup match.

12. Check the browser console for errors:
    ```
    npx playwright evaluate http://localhost:3000 "JSON.stringify(window.__console_errors || [])" --wait-for-timeout=3000
    ```
    Or use Playwright scripting to capture console errors. Look specifically for:
    - WebSocket connection errors (failed to connect to `ws://localhost:8080/events`)
    - WASM panics or JS errors
    - Failed fetch calls to the gateway

## Phase 5 — Verdict

13. Summarise your findings in a table:

    | Check | Status | Notes |
    |-------|--------|-------|
    | Infrastructure (YugabyteDB, Cassandra, Kafka, Zookeeper) | pass/fail | per component |
    | Init containers (schema + topics) | pass/fail | |
    | Services running (5 services + gateway + frontend) | pass/fail | per service |
    | Ship registration | pass/fail | POST /fleet/ships |
    | Station registration | pass/fail | POST /stations/:id/register |
    | Ship departure command | pass/fail | POST /fleet/ships/:id/depart |
    | **Event pipeline** | pass/fail | Did ShipRegistered+ShipDeparted reach event store (GET /ships/:id/history)? |
    | **Kafka publishing** | pass/fail | Did events appear on canon.fleet.events topic? |
    | **Cross-service flow** | pass/fail | Did navigation-service receive ShipDeparted? |
    | Cargo manifest + load | pass/fail | POST /cargo/manifests + /load |
    | **Oversight/inbox** | pass/fail | Did GET /admin/oversight/windows respond? Any windows active? |
    | Dead letter endpoint | pass/fail | Did GET /admin/deadletters respond? |
    | **WebSocket events** | pass/fail | Did the WS listener capture real events? |
    | Frontend renders | pass/fail | Does the page load without errors? |
    | **Mockup match** | pass/fail | Does the frontend match reference/mockup.html? (fonts, layout, colours) |
    | Browser console clean | pass/fail | No WebSocket errors, no WASM panics |

    Give an overall **PASS** or **FAIL** with a clear explanation of what broke. The pipeline checks (event pipeline, Kafka publishing, cross-service flow, WebSocket events) are the most important — a demo that renders but doesn't flow events through Canon is a FAIL.

## Phase 6 — Issue Filing

14. If the overall verdict is **FAIL**, or if any individual check failed:

    a. Compile a list of **distinct issues** found. Each issue should have:
       - A clear, concise **title** (e.g. "Event pipeline broken: ShipDeparted never reaches Cassandra event store")
       - The **phase** where it was detected
       - **Reproduction steps** (the exact command that failed and what happened)
       - **Relevant logs** (truncated to the key error — not the full log dump)
       - A suggested **label** (e.g. `bug`, `infra`, `frontend`, `pipeline`, `dx`)

    b. Present the full list of issues to the user in a numbered summary. For example:

       ```
       Found 3 issues:

       1. [bug][pipeline] ShipDeparted event never reaches Cassandra — GET /ships/:id/history returns empty
          -> Outbox processor may not be draining, or Kafka consumer is not writing to event store

       2. [bug][pipeline] WebSocket broadcasts no events — 0 events captured in 30s
          -> Gateway may not be subscribed to outbound Kafka topics

       3. [bug][frontend] Frontend uses Share Tech Mono instead of Inter — does not match mockup
          -> index.html loads wrong Google Fonts, main.css uses wrong font-family
       ```

    c. **Ask the user**: "Would you like me to create GitHub issues for any or all of these? (yes all / pick numbers / no)"

    d. If the user says **yes all** or picks specific numbers:
       - For each selected issue, run:
         ```
         gh issue create \
           --title "<issue title>" \
           --body "<formatted body with reproduction steps, logs, phase, and environment context>" \
           --label "<label>"
         ```
       - The issue body should follow this template:
         ```
         ## Summary
         <one-line description>

         ## Detected During
         E2E verification — Phase <N> (<phase name>)

         ## Steps to Reproduce
         1. `cd canon-demo && docker compose up -d`
         2. Wait for services to start
         3. <specific command or action that failed>

         ## Observed Behaviour
         <what happened, with key log lines>

         ## Expected Behaviour
         <what should have happened>

         ## Environment
         - OS: <detect via uname>
         - Rust: <detect via rustc --version>
         - Docker: <detect via docker --version>

         ## Logs
         <truncated relevant logs, max ~50 lines>
         ```
       - After creating each issue, print the issue URL so the user can see it.
       - If `gh` CLI is not authenticated or not installed, tell the user and offer to print the issue bodies so they can file them manually.

    e. If the user says **no**, acknowledge and move on to cleanup.

15. If the overall verdict is **PASS**, congratulate the user and skip issue filing.

## Phase 7 — Cleanup

16. Always run cleanup, even if earlier phases failed, and regardless of issue-filing outcome:
    - Kill any PIDs you stored (native gateway, WebSocket listener).
    - `cd canon-demo && docker compose down` to tear down all containers (infra + services + gateway + frontend).
    - Remove temp files: `/tmp/canon-ws-events.jsonl`, `/tmp/gateway.log`, `/tmp/ws-listener.log`.
    - Print "Cleanup complete."

## Important Notes

- The `docker-compose.yml` lives in `canon-demo/` — always `cd canon-demo` before running `docker compose` commands.
- Infrastructure: YugabyteDB (port 5433), Cassandra (port 9042), Zookeeper (port 2181), Kafka (port 9092 internal / 9093 external). There is NO PostgreSQL and NO RabbitMQ.
- Gateway listens on port 8080 (`LISTEN_ADDR=0.0.0.0:8080`). Frontend (nginx) serves on port 3000 (mapped from container port 80).
- When running services natively on the host, use `KAFKA_BROKERS=localhost:9093` (the external Kafka listener), not `localhost:9092`.
- Adapt binary names and endpoints to whatever you find in the actual code. Read Cargo.toml workspace members, gateway router code, and docker-compose.yml to get the real values.
- If `npx playwright` isn't available, install Node.js tooling as needed. Prefer Playwright for screenshots because it handles WASM apps well.
- You have full autonomy to read any file in the repo to figure out the correct commands, ports, and endpoints. Do not guess when you can grep.
