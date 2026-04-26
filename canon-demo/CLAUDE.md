# canon-demo — Claude Code guide

Demo system showcasing Canon. Framework rules: `../CLAUDE.md`. Frontend: `frontend/CLAUDE.md`.

---

## Domains

| Service | Aggregate | Commands | Events |
|---|---|---|---|
| fleet | Ship | RegisterShip, AssignRoute, DepartForStation, ScheduleResupply, DecommissionShip | ShipRegistered, RouteAssigned, ShipDeparted, ResupplyScheduled, ShipDecommissioned |
| cargo | Manifest | CreateManifest, LoadCargo, BeginUnloading, RecordUnloaded, CloseManifest | ManifestCreated, CargoLoaded, UnloadingStarted, CargoUnloaded, ManifestClosed |
| navigation | Route | PlanRoute, RecordDeparture, UpdatePosition, RecordArrival | RoutePlanned, ShipDeparted, PositionUpdated, ShipArrivedAtStation |
| supply | Inventory | RecordStock, RequestResupply, DispatchResupply, ConfirmDelivery | StockRecorded, ResupplyRequested, ResupplyDispatched, DeliveryConfirmed |
| station | Station | RegisterStation, RecordDocking, RecordCargoReceived, UpdateCapacity, DrainStock | StationRegistered, ShipDocked, CargoReceived, StationStockLow, CapacityUpdated, StockDrained |

**Cross-service flows**: `Fleet:ShipDeparted → Navigation` · `Navigation:ShipArrivedAtStation → Cargo, Station` · `Station:StationStockLow → Supply` · `Supply:ResupplyDispatched → Fleet`.

---

## Kafka topics (15, all explicit, no auto-create)

- Inbound: `canon.{service}.inbound`
- Outbound: `canon.{service}.outbound`
- Published events: `canon.{service}.events`

---

## Gateway (axum)

- POST: `/fleet/ships`, `/fleet/ships/:id/route`, `/fleet/ships/:id/depart`, `/cargo/manifests`, `/cargo/manifests/:id/load`, `/navigation/routes`, `/supply/resupply`, `/stations/:id/register`
- GET: `/stations/:id/inventory`, `/ships/:id/history`, `/cargo/manifests/:id`, `/replay/counterfactual`
- WS: `/events` — `DemoEvent` JSON broadcast

Per-service pools via `AppState::pool_for_service()`.

### Bootstrap (idempotent on every gateway start)

- Register 4 stations (Alpha 5000kg, Beta 3000kg, Gamma 2000kg, Delta 4000kg)
- Seed stock: Alpha 85%, Beta 60%, Gamma 40%, Delta 75%
- Register VSS Meridian (5000kg)
- Stock-drain task starts after 15s.

---

## Deployment

Backend: cross-compile macOS → linux/musl, slim alpine `COPY` images. **Never compile Rust inside Docker.** Frontend: WASM/Trunk inside Docker.

```bash
rustup target add aarch64-unknown-linux-musl
brew install filosottile/musl-cross/musl-cross
make k8s-up        # minikube
make k8s-test-e2e  # Playwright smoke
```

Targets: `k8s-{up,down,build,deploy,status,logs,tunnel,restart,clean,test-e2e}`. Layout: `k8s/base/`, `k8s/overlays/{minikube,gke}/`. All pods in `canon` namespace; infra as StatefulSets+PVCs; init Jobs create schemas/keyspaces/topics.

### GKE production — currently paused

Live demo at `https://canon.mopjones.com` is **paused to save cost**. The static landing page + docs are served from GitHub Pages (`.github/workflows/pages.yml`); `/demo/` shows an offline page (`canon-site/demo/index.html`).

Cluster `canon-demo` in `europe-west2-a`, 1 preemptible e2-standard-4. Registry `europe-west2-docker.pkg.dev/canon-demo-prod/canon/`. Auto-deploy disabled in `.github/workflows/deploy.yml` — uncomment the `push` trigger to re-enable.

```bash
make gke-cost-check   # show project billing
make gke-pause        # scale to 0 nodes (~$2/mo, keeps PVCs)
make gke-resume       # scale back to 1 node
make gke-teardown     # delete cluster + IPs (irreversible — manifests remain)
make gke-deploy       # manual deploy (when cluster exists)
```

**Auth gate** (optional, `CANON_AUTH_PASSWORD` env):
- `X-Canon-Auth: <password>` (sets cookie) or `X-Canon-Debug: <key>` (CLI bypass).
- Local secret files: `~/.canon-debug-key`, `~/.canon-auth-password` (never committed).

```bash
curl -H "X-Canon-Debug: $(cat ~/.canon-debug-key)" https://canon.mopjones.com/health
CANON_AUTH_PASSWORD=$(cat ~/.canon-auth-password) npx playwright test
kubectl set env deployment/gateway -n canon CANON_AUTH_PASSWORD=<pw>   # lock
kubectl set env deployment/gateway -n canon CANON_AUTH_PASSWORD-       # unlock
```

---

## Debugging — when the demo is broken

Triage in this order. Do NOT skip.

1. **API first.** `curl` and inspect:
   ```bash
   SID=$(curl -s -X POST https://canon.mopjones.com/sessions | python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")
   curl -s "https://canon.mopjones.com/game/$SID" | python3 -m json.tool
   ```
   `ship.status`, `ship.station_id`, `stations[].stock_pct`, `event_count` correct → bug is frontend. Stop touching infra.

2. **API wrong** → check service logs:
   ```bash
   kubectl logs deployment/{fleet,navigation,station}-service -n canon-prod --tail=20
   ```
   Look for `ERROR`, `command processing failed`, `OffsetOutOfRange`.

3. **`OffsetOutOfRange`** = Kafka lost data. Diagnose first. Test offset-clearing recovery in disposable env. **Never delete Kafka topics on prod.**

4. **Frontend** — see `frontend/CLAUDE.md`.

5. **Never nuke prod data.**

---

## Playwright DOM rules (Leptos re-renders detach elements)

- **Never** `page.$()` + `handle.click()`. Use `page.locator(sel).click()` / `page.click(sel)`.
- **Never** `page.$()` + `handle.evaluate()` for disabled checks. Use `page.locator(sel + ':not([disabled])').count()`.
- Use `.dest-tab` class for destination buttons, not `:has-text("Alpha")`.
- Wrap flight clicks in try/catch in stress tests.
