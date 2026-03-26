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

Test the full Canon event pipeline against staging, same as `/test-demo` Phase 3
but targeting `https://canon-staging.mopjones.com` instead of `localhost:8080`.

```bash
STAGING=https://canon-staging.mopjones.com
# If auth is required, add: -H "X-Canon-Debug: $(cat ~/.canon-debug-key)"

# Create a session
curl -s -X POST "$STAGING/sessions" -H "Content-Type: application/json"

# Verify stations are bootstrapped
curl -s "$STAGING/stations"

# Verify ships
curl -s "$STAGING/ships"

# Check admin endpoints
curl -s "$STAGING/admin/oversight/windows"
curl -s "$STAGING/admin/deadletters"
```

Verify the full pipeline: commands → outbox → Kafka → event store → projections → WebSocket.

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

Take a screenshot of the staging frontend and verify it matches the mockup:

```bash
cd canon-demo/e2e
npx playwright screenshot https://canon-staging.mopjones.com /tmp/canon-staging.png --wait-for-timeout=8000
```

Read the screenshot and verify:
- Copper theme is applied (Josefin Sans font, copper accent colours)
- 4 stations visible on the canvas map
- Ship rendered correctly
- Station cards show stock levels
- Event log strip visible at bottom

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
| Session creation | | |
| Station bootstrap | | |
| API pipeline (commands → events) | | |
| WebSocket connectivity | | |
| Playwright smoke (6 tests) | | |
| Supply chain loop (6 tests) | | |
| Stress test | | |
| Multi-tab test | | |
| Visual check (copper theme) | | |

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
