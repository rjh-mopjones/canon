# test-staging — Deploy current branch to GKE staging and run all tests

Deploy the current branch to `canon-staging.mopjones.com`, run exhaustive tests,
and report results. This is the pre-merge gate — run on a PR branch before merging.

**Philosophy**: Build, deploy, test exhaustively, fix, repeat. Don't merge until
everything passes.

---

## Prerequisites

Verify before starting:
```bash
gcloud config get-value project   # must be canon-demo-prod
kubectl config current-context    # must be gke_canon-demo-prod_*
docker info                       # Docker must be running
```

If any fail, stop and tell the user what to fix.

---

## Phase 1 — Build and Push

### 1a. Cross-compile backend services for x86_64 (GKE)

```bash
CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=x86_64-linux-musl-gcc \
  cargo build --release --target x86_64-unknown-linux-musl \
    -p fleet-service -p cargo-service -p navigation-service \
    -p supply-service -p station-service -p gateway
```

### 1b. Build and push all Docker images

Tag with `staging` so we don't pollute the `latest` tag used by prod.

```bash
REGISTRY=europe-west2-docker.pkg.dev/canon-demo-prod/canon

# Backend services
for svc in fleet-service cargo-service navigation-service supply-service station-service gateway; do
  docker build --no-cache --platform linux/amd64 \
    -t $REGISTRY/$svc:staging \
    --build-arg BINARY_PATH=target/x86_64-unknown-linux-musl/release/$svc \
    -f canon-demo/$svc/Dockerfile .
  docker push $REGISTRY/$svc:staging
done

# Frontend (multi-stage: trunk build inside Docker)
docker build --no-cache --platform linux/amd64 \
  -t $REGISTRY/frontend:staging \
  -f canon-demo/frontend/Dockerfile .
docker push $REGISTRY/frontend:staging
```

### 1c. Update staging image tags

Temporarily patch the staging kustomization to use `:staging` tags:

```bash
cd canon-demo/k8s/overlays/gke/shared
sed 's/newTag: latest/newTag: staging/g' kustomization.yaml > /tmp/kustomization-staging.yaml
cp /tmp/kustomization-staging.yaml kustomization.yaml
```

**Important**: Revert this after deploy — don't commit it.

---

## Phase 2 — Deploy to Staging

### 2a. Apply staging overlay

```bash
kubectl apply -k canon-demo/k8s/overlays/gke/staging/
```

If this is the first staging deploy, also apply infra policies:
```bash
kubectl apply -k canon-demo/k8s/overlays/gke/infra-policies/ || true
```

### 2b. Wait for rollout

```bash
for deploy in fleet-service cargo-service navigation-service supply-service station-service gateway frontend; do
  echo "Waiting for $deploy..."
  kubectl rollout status deployment/$deploy -n canon-staging --timeout=300s
done
```

### 2c. Verify all pods running

```bash
kubectl get pods -n canon-staging
```

All 7 should be Running. If any are CrashLoopBackOff, check logs:
```bash
kubectl logs deployment/<name> -n canon-staging
```

### 2d. Revert image tag change

```bash
cd canon-demo/k8s/overlays/gke/shared
git checkout kustomization.yaml
```

### 2e. Health check

Poll until staging responds:
```bash
curl --retry 10 --retry-delay 10 --retry-all-errors -sf https://canon-staging.mopjones.com/health
```

Note: staging requires auth if `CANON_AUTH_PASSWORD` is set. Check:
```bash
kubectl get deployment gateway -n canon-staging -o jsonpath='{.spec.template.spec.containers[0].env}' | grep -i AUTH
```

If auth is set, use the debug key for all curl requests:
```bash
DEBUG_KEY=$(cat ~/.canon-debug-key)
curl -H "X-Canon-Debug: $DEBUG_KEY" https://canon-staging.mopjones.com/health
```

---

## Phase 3 — Cargo Tests

Run the full Rust test suite (in-memory tier 1 tests):

```bash
cargo test --workspace 2>&1
```

All tests must pass. If any fail, investigate and fix before proceeding.

---

## Phase 4 — API Pipeline Verification

Test the full Canon event pipeline against staging. This verifies the entire path:
command → outbox → Kafka → event store → projections → cross-service → WebSocket.

```bash
STAGING=https://canon-staging.mopjones.com
# If auth is required, add to all curl commands: -H "X-Canon-Debug: $(cat ~/.canon-debug-key)"
```

### 4a. Create session and verify bootstrap

```bash
# Create a session — returns session_id, ship_id, stations
SESSION=$(curl -s -X POST "$STAGING/sessions" -H "Content-Type: application/json")
echo "$SESSION"
SESSION_ID=$(echo "$SESSION" | python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")
SHIP_ID=$(echo "$SESSION" | python3 -c "import sys,json; print(json.load(sys.stdin)['ship_id'])")
```

**Verify**: response contains `session_id`, `ship_id`, and 4 stations with stock.

### 4b. Verify stations are bootstrapped (projection read model)

```bash
curl -s "$STAGING/stations"
```

**Verify**: returns 4 stations with non-zero `current_stock_kg`. If 0 stations after 15s,
the projection consumer isn't populating the read model — check station-service logs.

### 4c. Verify ships

```bash
curl -s "$STAGING/ships"
```

**Verify**: returns at least one ship.

### 4d. Open a WebSocket listener

Capture WebSocket events in the background for 60s:

```bash
cd canon-demo/e2e && node -e "
const ws = new (require('ws'))('wss://canon-staging.mopjones.com/events');
const fs = require('fs');
const out = fs.createWriteStream('/tmp/canon-ws-events.jsonl');
ws.on('message', d => { out.write(d.toString() + '\n'); });
ws.on('open', () => console.log('WS connected'));
ws.on('error', e => console.error('WS error:', e.message));
setTimeout(() => { ws.close(); out.end(); process.exit(0); }, 60000);
" &
WS_PID=$!
```

If `ws` module isn't available, install it: `cd canon-demo/e2e && npm install ws`

### 4e. Depart ship — test the full cross-service pipeline

Extract the first station ID from the session response, then depart:

```bash
FIRST_STATION=$(echo "$SESSION" | python3 -c "import sys,json; s=json.load(sys.stdin)['stations']; print(s[1]['id'])")
curl -s -X POST "$STAGING/fleet/ships/$SHIP_ID/depart?session_id=$SESSION_ID" \
  -H "Content-Type: application/json" \
  -d "{\"destination\": \"$FIRST_STATION\"}"
```

**Verify**: returns 200 with `command_id`.

### 4f. Verify event pipeline (wait 10s for cross-service cascade)

```bash
sleep 10
```

a. **Event store**: check ship history for ShipDeparted event:
```bash
curl -s "$STAGING/ships/$SHIP_ID/history"
```
Should show ShipRegistered + ShipDockedAtStation + ShipDeparted (3+ events).

b. **Cross-service flow**: check service logs for the cascade:
```bash
kubectl logs deployment/fleet-service -n canon-staging --tail=20 | grep -i "DepartForStation\|ShipDeparted"
kubectl logs deployment/navigation-service -n canon-staging --tail=20 | grep -i "ShipDeparted\|PlanRoute\|RecordArrival"
kubectl logs deployment/station-service -n canon-staging --tail=20 | grep -i "ShipDocked\|RecordDocking"
```
Fleet → Navigation → Station should all show processed commands.

c. **Outbox drain**: verify outbox is draining:
```bash
kubectl exec -n canon-infra yb-tserver-0 -- /home/yugabyte/bin/ysqlsh \
  -h $(kubectl get pod yb-tserver-0 -n canon-infra -o jsonpath='{.status.podIP}') \
  -U yugabyte -d yugabyte \
  -c "SELECT COUNT(*) as pending FROM canon_staging_fleet.outbox WHERE delivered_at IS NULL;"
```
Should be 0 (all delivered). If > 0, outbox processor isn't running.

### 4g. Cargo pipeline

```bash
MANIFEST=$(curl -s -X POST "$STAGING/cargo/manifests?session_id=$SESSION_ID" \
  -H "Content-Type: application/json" \
  -d "{\"ship_id\": \"$SHIP_ID\", \"voyage_id\": \"$(uuidgen | tr '[:upper:]' '[:lower:]')\"}")
echo "$MANIFEST"
```

**Verify**: returns manifest with `aggregate_id`. Check cargo-service logs:
```bash
kubectl logs deployment/cargo-service -n canon-staging --tail=10 | grep -i "CreateManifest\|ManifestCreated"
```

### 4h. Admin endpoints

```bash
curl -s "$STAGING/admin/oversight/windows"   # should return 200 (may be empty array)
curl -s "$STAGING/admin/deadletters"          # should return 200 (may be empty array)
```

### 4i. WebSocket events check

```bash
kill $WS_PID 2>/dev/null
wc -l /tmp/canon-ws-events.jsonl
cat /tmp/canon-ws-events.jsonl | head -5
```

**Verify**: file has > 0 events. Should contain `Event`, `StationUpdate`, and/or
`InfraStatus` messages. If 0 events, the gateway WS broadcast is broken.

---

## Phase 5 — Playwright E2E Tests

### 5a. Smoke tests

```bash
cd canon-demo
FRONTEND_URL=https://canon-staging.mopjones.com node e2e/test.js
```

All 6 tests must pass:
1. `stations_have_initial_stock`
2. `stock_drains_over_time`
3. `ship_popup_on_planet_click`
4. `event_log_receives_events`
5. `scenarios_page_renders`
6. `no_console_errors`

### 5b. Supply chain loop test

```bash
FRONTEND_URL=https://canon-staging.mopjones.com node e2e/test-supply-chain.js
```

All 6 tests must pass:
1. `session_setup`
2. `initial_dock`
3. `leg_1` through `leg_4`

### 5c. Stress test (if it exists)

```bash
FRONTEND_URL=https://canon-staging.mopjones.com node e2e/test-stress.js
```

### 5d. Multi-tab test (if it exists)

```bash
FRONTEND_URL=https://canon-staging.mopjones.com node e2e/test-multi-tab.js
```

---

## Phase 6 — Visual Verification

### 6a. Take screenshots

```bash
npx playwright screenshot https://canon-staging.mopjones.com /tmp/canon-staging.png --wait-for-timeout=8000
npx playwright screenshot file://$(pwd)/canon-demo/frontend/reference/mockup.html /tmp/canon-mockup.png --wait-for-timeout=3000
```

### 6b. Compare visually

Read both screenshots and verify:
- **Layout**: header, canvas map, station cards, action bar, event log match mockup
- **Copper theme**: Josefin Sans font in header, copper/amber accent colours
- **Fonts**: Inter + JetBrains Mono (not Share Tech Mono or Rajdhani)
- **Colours**: via CSS custom properties, no hardcoded hex
- **Canvas map**: 4 station planets at correct positions, ship rendered
- **Station cards**: 4 cards with stock bars and percentage readouts
- **Event log**: horizontal strip at bottom with live events
- **Ship label**: correct format (no duplicate like "VSS VSS MERIDIAN")
- **Theme toggle**: light mode is default, dark mode toggle visible

If visual issues found, investigate and fix before proceeding.

---

## Phase 7 — Fix Loop

If any test failed:

1. Investigate root cause (read logs, source code)
2. Fix the issue in the codebase
3. Rebuild and push affected images (Phase 1)
4. Restart affected deployment: `kubectl rollout restart deployment/<name> -n canon-staging`
5. Re-run the failed test
6. Repeat until ALL tests pass

---

## Phase 8 — Report

Summarise results in a table:

| Check | Status | Notes |
|-------|--------|-------|
| Cross-compile (x86_64) | | |
| Docker build + push | | |
| Staging deploy | | |
| All pods running | | |
| Health endpoint | | |
| Cargo tests (`cargo test --workspace`) | | |
| Session creation + bootstrap | | |
| Stations projection (GET /stations) | | |
| Ship departure command | | |
| Event store (ship history) | | |
| Cross-service flow (fleet→nav→station logs) | | |
| Outbox drain (0 pending) | | |
| Cargo pipeline (create manifest) | | |
| Admin endpoints (oversight + deadletters) | | |
| WebSocket events captured | | |
| Playwright smoke (6 tests) | | |
| Supply chain loop (6 tests) | | |
| Stress test | | |
| Multi-tab test | | |
| Visual check (copper theme + mockup match) | | |

**Overall verdict: ALL PASS / FAILURES FOUND**

If all pass: "Staging validated. Safe to merge."
If failures remain after fix loop: list what's still broken and why.

---

## Important Notes

- **Staging uses the same infra** as prod (`canon-infra` namespace) but different schemas (`canon_staging_*` prefix) and Kafka topics (`canon.staging.*` prefix).
- **Never deploy to prod from this command.** This only touches `canon-staging`.
- **Image tag**: always use `:staging` to avoid polluting `:latest` (used by prod).
- **Auth**: staging may have `CANON_AUTH_PASSWORD` set. Use debug key header for API calls.
- **Revert kustomization.yaml**: always revert the image tag change after deploy. Don't commit it.
- **WebSocket over GKE**: all traffic routes through nginx (frontend pod) which proxies WS with upgrade headers. The GCE Ingress does NOT proxy WebSocket directly.
