# Canon — Claude Code guide

Canon is a Rust event sourcing framework. This file is the authoritative reference. The design is settled — your job is implementation, not design.

---

## Pipeline

```
External world → canon-adaptor-kafka → canon-inbox-yugabyte → canon-inbound-queue-kafka
    → Dispatcher (command handler | internal event handlers | external event handlers)
    → YugabyteDB txn (commands table + outbox table)
    → Outbox processor → canon-outbound-queue-kafka
    → Event store consumer (Cassandra + snapshots)
    → Projection consumer (YugabyteDB read models)
    → canon-publisher-kafka → canon.{service}.events → other services
```

All Kafka topics partitioned by `aggregate_id`.

Every stage in this pipeline must be wired, tested end-to-end, and verified with
`/test-demo`. A component that compiles but is never called is not implemented.

---

## Non-negotiable rules

- **tokio only**. No async-std. `async_trait` throughout. No manual `Pin<Box<dyn Future>>`.
- **`thiserror`** in every crate. No `anyhow`. No god error enum. Each crate owns its errors.
- **`AggregateId(Uuid)`** newtype always. Never generic, never plain `Uuid`.
- **Proc-macros** live in the crate that owns the concept. No separate `canon-macros` crate.
- **Strict DAG**: impl crates depend on their trait crate + `canon-core` only. No cross-impl dependencies.
- **In-memory impls** of every trait in `canon-core` — the test harness.
- **Outbox processor**: drains outbox → outbound queue only. No Cassandra, no projections, no external publish.
- **No direct Cassandra writes** from command handler. Events go to outbox only.
- **Snapshots by event store consumer**: checks `version % N == 0` after confirmed Cassandra write.
- **Outbox pattern**: events + command written in single YugabyteDB ACID txn. Outbox is the commit point.
- **Idempotency**: all event handlers and projections must be safe to call twice.
- **Optimistic concurrency**: event store rejects version mismatches.
- **Macro-driven traits**: users never implement `Aggregate`, `CommandHandler`, `EventHandler`, or `Projection` directly — macros generate all impls.
- **Exhaustiveness**: every `#[command(X, version = N)]` needs `#[command_handler(X, version = N)]`. Every `#[event(X, version = N)]` needs `#[event_combiner(X, version = N)]`. Missing = compile error.
- **Event handlers are aggregate-agnostic**: `#[event_handler]` has no aggregate type parameter.
- **No casting**: no upcasting or downcasting. Version-matched routing reads `event_version`/`command_version` and dispatches to the handler at that exact version.
- **`window_ttl` requires `oversight`**: compile error without it.
- **Window key is `(handler_id, correlation_key)`**: resolved from handler's `correlate`
  fn or fallback to envelope `correlation_id`. Never `aggregate_id`.
- **Auto-registration via `inventory`**: macros emit static registrations. `ServiceBuilder` discovers everything automatically.
- **READMEs in every crate**: the root README and each crate's own README must be kept up to date. When a PR adds or changes a crate's public API, traits, or modules, update that crate's README to reflect the change.
- **No local simulation in the frontend**: the demo exists to showcase Canon's event sourcing pipeline. Every state change in the UI (ship movement, stock levels, oversight gates, event log entries) must be driven by real events flowing through the Canon pipeline (command → outbox → Kafka → event store → WebSocket). The frontend must never fake events with local timers, hardcoded event chains, or fire-and-forget POST fallbacks. If the gateway is down, show a connection error — do not mask the failure with a local simulation.
- **Per-service storage isolation**: each demo service MUST use its own YugabyteDB schema (`canon_fleet`, `canon_cargo`, etc.) and Cassandra keyspace. Services must never share outbox, commands, inbox, or event store tables. Use `canon_demo_shared::db::create_service_pool()` for YugabyteDB and `CassandraEventStore::new_with_keyspace()` for Cassandra. The gateway uses per-service pools via `AppState::pool_for_service()`.
- **`rskafka` only for Kafka**. No `rdkafka`. No C dependencies in Kafka crates. All Kafka crates must be pure Rust and cross-compilable. Consumer offset management is in-memory with restart-from-zero -- application-layer idempotency is the safety net.

---

## Workspace layout & dependency graph

```
canon-core                              ← traits, types, in-memory impls, proc-macros
    └── canon-core-macros               ← proc-macro = true subcrate, re-exported from canon-core
    ├── canon-event-store               → canon-event-store-cassandra
    ├── canon-command-store             → canon-command-store-yugabyte
    ├── canon-snapshot-store            → canon-snapshot-store-yugabyte
    ├── canon-inbox                     → canon-inbox-yugabyte
    ├── canon-inbound-queue             → canon-inbound-queue-kafka
    ├── canon-outbound-queue            → canon-outbound-queue-kafka
    ├── canon-projection-store          → canon-projection-store-yugabyte
    ├── canon-publisher                 → canon-publisher-kafka
    ├── canon-adaptor                   → canon-adaptor-kafka
    └── canon-deadletter                → canon-deadletter-yugabyte
canon-test                              ← integration tests, in-memory only
canon-demo/                             ← shared/, fleet-service/, cargo-service/,
                                          navigation-service/, supply-service/,
                                          station-service/, gateway/, frontend/
```

---

## Implementation phases

Work strictly in order. Each phase must compile and pass tests before the next begins.

1. **Workspace scaffolding** — `Cargo.toml` per crate, empty `lib.rs` files. `cargo check --workspace` passes.
2. **canon-core types** — `AggregateId`, `Version`, `EventEnvelope`, `CommandEnvelope`, `IncomingMessage`, `Oversight`, counterfactual types.
3. **canon-core traits** — `Aggregate` (no `handle`/`apply`), `CommandHandler`, `EventHandler`, `EventCombiner`, `Projection`, `ProjectionHandler`, `CounterfactualReplay`.
3b. **canon-core proc-macros** — all eight macros in `canon-core/canon-core-macros/` subcrate (re-exported from `canon-core`): `#[aggregate]` → `#[command]` + `#[event]` → `#[event_combiner]` → `#[command_handler]` → `#[event_handler]` → `#[projection]` → `#[projection_handler]`.
4. **In-memory implementations** — all 10 in `canon-core/src/memory/`, must work with macro-generated dispatch.
5. **canon-test** — `TestHarness` wiring all in-memory impls. Test modules: snapshotting, oversight, counterfactual replay, dead lettering, projection rebuild, inbox window expiry, idempotency, outbound fan-out.
6. **Trait crates** — thin crates, trait + associated types only, re-export from `canon-core`.
7. **Infrastructure crates** — in order: inbox-yugabyte, inbound-queue-kafka, outbound-queue-kafka, command-store-yugabyte, snapshot-store-yugabyte, event-store-cassandra, projection-store-yugabyte, deadletter-yugabyte, publisher-kafka, adaptor-kafka.
8. **canon-demo shared** — domain types, events, commands, topic constants. No logic.
9. **fleet-service** — reference implementation using all framework features.
10. **Remaining services** — navigation, cargo, station, supply.
11. **Gateway** — axum REST + WebSocket.
12. **Frontend** — Leptos WASM.

---

## Core types (do not modify)

```rust
pub struct AggregateId(uuid::Uuid);  // new(), from_uuid(), as_uuid()
pub struct Version(u64);              // initial() → 0, next() → +1, as_u64()

pub struct EventEnvelope {
    pub event_id: Uuid, pub aggregate_id: AggregateId, pub version: Version,
    pub event_type: String, pub event_version: u32, pub payload: Bytes,
    pub correlation_id: Uuid, pub causation_id: Uuid, pub timestamp: DateTime<Utc>,
}

pub struct CommandEnvelope {
    pub command_id: Uuid, pub aggregate_id: AggregateId,
    pub correlation_id: Uuid, pub causation_id: Uuid, pub timestamp: DateTime<Utc>,
    pub payload: Bytes, pub command_version: u32,
}

pub enum IncomingMessage { Command(CommandEnvelope), InternalEvent(EventEnvelope), ExternalEvent(EventEnvelope) }
pub enum Oversight { Ready, NotReady, Discard }

pub struct CounterfactualRequest { pub aggregate_id: AggregateId, pub branch_version: Version, pub substituted_command: CommandEnvelope }
pub struct CounterfactualResult { pub original_commands: Vec<CommandEnvelope>, pub counterfactual_commands: Vec<CommandEnvelope>, pub diff: CommandDiff }
pub struct CommandDiff { pub added: Vec<CommandEnvelope>, pub removed: Vec<CommandEnvelope>, pub unchanged: Vec<CommandEnvelope> }
```

---

## Core traits (do not modify)

```rust
// Aggregate — hydrate dispatches to version-matched #[event_combiner] impls
pub trait Aggregate: Sized + Send + Sync + 'static {
    type State: Default + Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;
    fn hydrate(state: &mut Self::State, events: impl Iterator<Item = EventEnvelope>) -> Result<(), Self::Error>;
}

// CommandHandler — one per command type per version
pub trait CommandHandler<A: Aggregate>: Send + Sync + 'static {
    type Command: Send + Sync;
    type Event: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;
    async fn handle(&self, state: &A::State, command: Self::Command) -> Result<Self::Event, Self::Error>;
}

// EventHandler — no aggregate parameter, optional oversight + correlate
pub trait EventHandler: Send + Sync + 'static {
    type Event: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;
    async fn handle(&self, events: Vec<Self::Event>) -> Result<Option<CommandEnvelope>, Self::Error>;
    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight { Oversight::Ready }
    fn correlate(&self, message: &IncomingMessage) -> Uuid { message.correlation_id() }
}

// Projection — read model, idempotent apply
pub trait Projection: Send + Sync + 'static {
    type Event: Send + Sync;
    type Store: ProjectionStore;
    type Error: std::error::Error + Send + Sync + 'static;
    async fn apply(&self, event: &Self::Event, store: &Self::Store) -> Result<(), Self::Error>;
    async fn rebuild(&self, events: impl Stream<Item = Self::Event> + Send, store: &Self::Store) -> Result<(), Self::Error>;
    fn projection_id(&self) -> &str;
}

// EventCombiner — synchronous state folding, one per event per version
// Generated by #[event_combiner]. Used internally by Aggregate::hydrate() dispatch.
pub trait EventCombiner<A>: Send + Sync + 'static {
    fn combine(&self, state: &mut A);
}

// ProjectionHandler — applies one event type to a projection's read model
// Generated by #[projection_handler]. Used internally by projection dispatch.
pub trait ProjectionHandler<P>: Send + Sync + 'static {
    type Event: Send + Sync;
    fn apply(&self, event: &Self::Event, store: &mut P);
}

// CounterfactualReplay
pub trait CounterfactualReplay: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn replay(&self, request: CounterfactualRequest) -> Result<CounterfactualResult, Self::Error>;
}
```

---

## Macro surface (do not modify)

Users never implement traits directly. Macros generate all impls and emit `inventory` registrations. Version-matched routing: framework reads `event_version`/`command_version` from stored envelopes, dispatches to handler registered at that exact version. No casting.

### `#[aggregate(snapshot_every = N)]`

```rust
#[aggregate(snapshot_every = 50)]
pub struct Ship { status: ShipStatus, fuel_level: f32 }
```
Generates: `impl Aggregate` with `type State = Self` (aggregate struct is its own state — opinionated), version-matched hydration dispatch, `Default`, serde derives, `inventory` registration.

### `#[command(Aggregate, version = N, produces = [Events...])]`

```rust
#[command(Ship, version = 1, produces = [ShipDeparted])]
pub struct DepartForStation { pub destination: StationId }
```
`version` defaults to 1. `produces` is declarative metadata only — no type is generated. It documents which event the handler returns and is used for macro wiring and schema registry. Must have matching `#[command_handler]`.

### `#[event(Aggregate, version = N)]`

```rust
#[event(Ship, version = 1)]
pub struct ShipDeparted { pub destination: StationId }

#[event(Ship, version = 2)]  // v2 coexists as a different type
pub struct ShipDeparted { pub destination: StationId, pub fuel_at_departure: f32 }
```
`version` defaults to 1. Must have matching `#[event_combiner]` at same version.

### `#[event_combiner(Aggregate, version = N)]`

Synchronous, pure state folding. One per event per version.

```rust
#[event_combiner(Ship, version = 1)]
impl ShipDeparted {
    fn combine(&self, state: &mut Ship) { state.status = ShipStatus::InFlight; }
}
```

### `#[command_handler(Aggregate, version = N)]`

Returns `Result<EventType, Error>` — the single event type declared in `produces`. A command produces exactly one event or returns `Err`. Rejection is `Err`, not a separate event type. During counterfactual replay, `command_version` routes to the matching handler.

```rust
#[command_handler(Ship, version = 1)]
impl DepartForStationHandler {
    type Error = FleetError;
    fn handle(&self, state: &Ship, cmd: DepartForStation) -> Result<ShipDeparted, FleetError> {
        if state.status != ShipStatus::Docked { return Err(FleetError::ShipNotDocked); }
        Ok(ShipDeparted { destination: cmd.destination })
    }
}
```

### `#[event_handler]`

No aggregate parameter. `#[handles]` declares event type + version. `window_ttl` requires `oversight` (compile error without).

```rust
#[event_handler(window_ttl = "30m")]
impl CargoUnloadingHandler {
    #[handles(CargoEvent, version = 1)]
    fn handle(&self, events: Vec<CargoEvent>) -> Option<CommandEnvelope> { ... }

    // Optional — extracts domain correlation key. Falls back to envelope correlation_id.
    fn correlate(&self, message: &IncomingMessage) -> Uuid { ... }

    // Required when window_ttl is set. Returns Ready/NotReady/Discard.
    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight { ... }
}
```

### `#[projection]` / `#[projection_handler(ProjectionName)]`

```rust
#[projection]
pub struct StationInventory { pub station_id: StationId, pub stock_levels: HashMap<CargoType, u32> }

#[projection_handler(StationInventory)]
impl CargoReceivedHandler {
    fn apply(&self, event: &CargoReceived, store: &mut StationInventory) {
        *store.stock_levels.entry(event.cargo_type).or_insert(0) += event.quantity;
    }
}
```

### Compile-time enforcement

- `#[command(X, v=N)]` → requires `#[command_handler(X, v=N)]` — compile error if missing
- `#[event(X, v=N)]` → requires `#[event_combiner(X, v=N)]` — compile error if missing
- `#[event_handler]`, `#[projection_handler]` — optional (warning for unhandled new event versions)
- `window_ttl` without `oversight` → compile error
- `correlate` is optional — no enforcement, fallback to envelope `correlation_id` is always safe
- `#[command_handler]` return type must be the single type named in `produces` — compile error if mismatched

Enforcement via marker traits. `ServiceBuilder` auto-discovers via `inventory`:

```rust
ServiceBuilder::new().for_aggregate::<Ship>().build()
```

---

## Operational details

### Command handler write path
Single YugabyteDB ACID txn: `INSERT INTO commands (...)` + `INSERT INTO outbox (...) x N`. Commands direct, events via outbox.

### Outbox processor
Background tokio task. `SELECT ... FOR UPDATE SKIP LOCKED` → publish to outbound queue → set `delivered_at`. Backpressure via bounded channel (default 1024).

### Service lifecycle
`ServiceBuilder::build()` creates a `Service`. Call `service.start()` to spawn all
background tasks: outbox processor, event store consumer, projection consumer, publisher
consumer. Each runs as a `tokio::spawn` task with graceful shutdown via watch channel.
The `ConsumerReceiver` trait provides the polling interface for consumers to receive
events from the outbound queue.

### Outbound queue consumers (3 independent groups)
- **Event store**: writes to Cassandra. Snapshot if `version % N == 0`. Retry up to 3 on conflict → dead letter.
- **Projection**: applies to read models. Updates `last_version`. Rebuilds via Kafka offset reset while `rebuilding = true`.
- **Publisher**: publishes to `canon.{service}.events` for other services.

All consumers restart from offset 0 and rely on downstream idempotency to skip already-processed events. No Kafka-side offset commit.

### Kafka crate patterns (rskafka)

All four Kafka crates (`canon-inbound-queue-kafka`, `canon-outbound-queue-kafka`,
`canon-publisher-kafka`, `canon-adaptor-kafka`) use `rskafka` with a consistent pattern:

- **Connection**: `ClientBuilder::new(broker_list).build().await` then
  `client.partition_client(topic, 0, UnknownTopicHandling::Retry)` to get a `PartitionClient`.
- **Produce**: `partition_client.produce(vec![record], Compression::NoCompression)` with
  `Record { key, value, headers: BTreeMap::new(), timestamp }`.
- **Consume**: `partition_client.fetch_records(next_offset, 1..1_048_576, timeout_ms)` in a
  polling loop. Offset tracked in-memory (`Mutex<i64>`, starts at 0).
- **Commit**: Always a no-op. Application-layer idempotency (inbox dedup, Cassandra PK,
  projection checkpoint) handles duplicates on restart.
- **No consumer groups**: rskafka has no consumer group abstraction. Each consumer polls
  partition 0 independently. `group_id` fields are kept for API compatibility but unused.
- **Errors**: each crate defines its own `thiserror` error type wrapping rskafka errors as strings.

### Inbox
- Idempotent intake via `handler_id + message_id` composite key
- Window key is `(handler_id, correlation_key)` — from handler's `correlate` fn or fallback to envelope `correlation_id`
- Each unique correlation key is an independent window — a handler may have many concurrent in-flight windows
- Oversight controls dispatch readiness (`Ready`/`NotReady`/`Discard`)
- `window_id` travels with batch → `processed_windows` table for batch idempotency
- Window expiry: TTL → `expired` status → dead letter with reason `window_expired`

### InboxPort
Local re-entry only. Event handlers submit `CommandEnvelope` to local inbox. Cross-service = REST only.

### Counterfactual replay
Operates on commands not events. Hydrates state to branch point (version-matched combiners), routes stored commands via `command_version` to matching handler, diffs at command level. `ReplayEventStore` port points at read replica.

### Projection rebuild
Set `rebuilding = true` → read endpoints fall back to read-through → reset consumer offset → Kafka replays → set `rebuilding = false`.

### Dead letter handling
Retry count in `retry_attempts` table (crash-safe). Max failures → dead letter store. Requeue via admin API re-enters inbox with fresh `expires_at`.

---

## Schemas

### YugabyteDB tables (see init-schema scripts for full DDL)

Each service uses its own schema (`canon_fleet`, `canon_cargo`, `canon_navigation`, `canon_supply`, `canon_station`). All tables below exist in each schema:

```sql
-- inbox: inbox_messages(handler_id, message_id), inbox_windows(handler_id, correlation_key), processed_windows(window_id)
-- commands: commands(command_id), idx: (aggregate_id, created_at)
-- outbox: outbox(id), sequence outbox_seq, idx: (sequence_number) WHERE delivered_at IS NULL
-- other: snapshots(aggregate_id), projection_checkpoints(projection_id), dead_letters(id), retry_attempts(message_id)
```

### Cassandra

Each service uses its own keyspace (`canon_fleet`, `canon_cargo`, `canon_navigation`, `canon_supply`, `canon_station`):

```cql
CREATE TABLE canon_fleet.events (aggregate_id UUID, version BIGINT, ..., PRIMARY KEY (aggregate_id, version)) WITH CLUSTERING ORDER BY (version ASC);
```

### Environment variables

```
CASSANDRA_NODES=cassandra:9042
YUGABYTE_URL=postgres://canon:canon@yugabytedb:5433/canon
KAFKA_BROKERS=kafka:9092
```

---

## Deployment

All demo infrastructure runs on Kubernetes via minikube (local) or GKE (production).

### Prerequisites (one-time setup for local development)

```bash
rustup target add aarch64-unknown-linux-musl
brew install filosottile/musl-cross/musl-cross   # provides aarch64-linux-musl-gcc
```

### Build pipeline

Backend services are cross-compiled locally from macOS to Linux (musl), producing
static binaries. Each service has a slim alpine Dockerfile that just COPYs the
pre-built binary. No Rust compilation happens inside Docker.

```
cargo build --release --target aarch64-unknown-linux-musl   # ~2 min
docker build (alpine + COPY binary)                          # ~2s each
minikube image load                                          # ~5s each
```

The frontend still builds inside Docker (WASM/Trunk). `init-schema` is unchanged.

```
canon-demo/k8s/
  base/                  # shared manifests (namespace, infra, jobs, services)
  overlays/
    minikube/            # imagePullPolicy: Never, local images
    gke/                 # placeholder for production (Ingress, HPA, Artifact Registry)
```

```bash
cd canon-demo && make k8s-up
# or manually:
minikube start --cpus=4 --memory=8g
make k8s-build    # cross-compile + docker build + minikube image load
kubectl apply -k k8s/overlays/minikube/
minikube tunnel   # access frontend at localhost:80
```

All pods run in a single `canon` namespace. Infrastructure (YugabyteDB, Cassandra,
Zookeeper, Kafka) run as StatefulSets with PVCs. Init Jobs create schemas, keyspaces,
and all 15 Kafka topics before services start. The 5 Canon services are background
processors with no exposed ports — only gateway (8080) and frontend (80) have Services.

See `canon-demo/Makefile` for all `k8s-*` targets (`k8s-up`, `k8s-down`, `k8s-build`,
`k8s-deploy`, `k8s-status`, `k8s-logs`, `k8s-tunnel`, `k8s-restart`, `k8s-clean`,
`k8s-test-e2e`).

### Game bootstrap

The gateway automatically bootstraps the demo game state on startup:
- Registers 4 stations (Alpha Depot 5000kg, Beta Relay 3000kg, Gamma Outpost
  2000kg, Delta Prime 4000kg) if not already registered
- Seeds initial stock via `RecordCargoReceived` (Alpha 85%, Beta 60%, Gamma 40%,
  Delta 75% of capacity)
- Registers VSS Meridian ship (5000kg capacity) if not already registered
- Idempotent — safe on every gateway restart (checks if data already exists before inserting)

The stock drain background task starts after a 15s delay to allow bootstrap
and pipeline processing to complete.

### GKE (production)

The demo runs on GKE at `https://canon.mopjones.com`.

**Access:**
- Frontend: `https://canon.mopjones.com`
- API: `https://canon.mopjones.com/health`, `/fleet/ships`, `/stations`, etc.
- WebSocket: `wss://canon.mopjones.com/events`

**Authentication (when site is locked down):**

The gateway has an optional auth gate (`CANON_AUTH_PASSWORD` env). When set, all
requests require authentication. Two methods:

1. Header: `X-Canon-Auth: <password>` (sets a cookie for subsequent browser requests)
2. Debug key: `X-Canon-Debug: <key>` (bypasses all auth, for CLI debugging)

Local files (never committed):
- `~/.canon-debug-key` — debug API key
- `~/.canon-auth-password` — auth gate password

**CLI usage:**
```bash
# Check health:
curl -H "X-Canon-Debug: $(cat ~/.canon-debug-key)" https://canon.mopjones.com/health

# Run Playwright against live site:
CANON_AUTH_PASSWORD=$(cat ~/.canon-auth-password) npx playwright test
```

**Toggle public/private:**
```bash
# Lock down (require password):
kubectl set env deployment/gateway -n canon CANON_AUTH_PASSWORD=<password>

# Go public (remove auth):
kubectl set env deployment/gateway -n canon CANON_AUTH_PASSWORD-
```

**Deploy:**
Merging to `main` automatically deploys via GitHub Actions.
Manual deploy: `cd canon-demo && make gke-deploy`

**GKE cluster:** `canon-demo` in `europe-west2-a`, 1 preemptible e2-standard-4 node.
**Image registry:** `europe-west2-docker.pkg.dev/canon-demo-prod/canon/`

---

## canon-demo domains

| Service | Aggregate | Commands | Events |
|---|---|---|---|
| fleet | Ship | RegisterShip, AssignRoute, DepartForStation, ScheduleResupply, DecommissionShip | ShipRegistered, RouteAssigned, ShipDeparted, ResupplyScheduled, ShipDecommissioned |
| cargo | Manifest | CreateManifest, LoadCargo, BeginUnloading, RecordUnloaded, CloseManifest | ManifestCreated, CargoLoaded, UnloadingStarted, CargoUnloaded, ManifestClosed |
| navigation | Route | PlanRoute, RecordDeparture, UpdatePosition, RecordArrival | RoutePlanned, ShipDeparted, PositionUpdated, ShipArrivedAtStation |
| supply | Inventory | RecordStock, RequestResupply, DispatchResupply, ConfirmDelivery | StockRecorded, ResupplyRequested, ResupplyDispatched, DeliveryConfirmed |
| station | Station | RegisterStation, RecordDocking, RecordCargoReceived, UpdateCapacity, DrainStock | StationRegistered, ShipDocked, CargoReceived, StationStockLow, CapacityUpdated, StockDrained |

### Cross-service flows
`Fleet:ShipDeparted → Navigation` · `Navigation:ShipArrivedAtStation → Cargo, Station` · `Station:StationStockLow → Supply` · `Supply:ResupplyDispatched → Fleet`

### Kafka topics (15 total — all explicitly created, no auto-create)
- **Inbound** (adaptor → inbox → dispatcher): `canon.fleet.inbound` · `canon.cargo.inbound` · `canon.navigation.inbound` · `canon.supply.inbound` · `canon.station.inbound`
- **Outbound** (outbox processor → 3 consumer groups): `canon.fleet.outbound` · `canon.cargo.outbound` · `canon.navigation.outbound` · `canon.supply.outbound` · `canon.station.outbound`
- **Published events** (cross-service): `canon.fleet.events` · `canon.cargo.events` · `canon.navigation.events` · `canon.supply.events` · `canon.station.events`

### Gateway (axum)
POST: `/fleet/ships`, `/fleet/ships/:id/route`, `/fleet/ships/:id/depart`, `/cargo/manifests`, `/cargo/manifests/:id/load`, `/navigation/routes`, `/supply/resupply`, `/stations/:id/register`
GET: `/stations/:id/inventory` (read-ready), `/ships/:id/history` (read-through), `/cargo/manifests/:id` (read-through), `/replay/counterfactual`
WS: `/events` — broadcast all DemoEvent as JSON

### Frontend (Leptos WASM)

The frontend is a Leptos 0.7 CSR WASM application built with Trunk at
`canon-demo/frontend/`. The **visual reference** is
`canon-demo/frontend/reference/mockup.html` — open it in a browser before writing
any frontend code. The mockup is the source of truth for behaviour, layout, and
interaction flows.

- **The mockup is correct**: if the Leptos app differs from the mockup, the app is wrong.
  Extract CSS variables, colours, fonts, spacing, layout, interactions, and text casing
  from the mockup.
- **Do not mimic** its DOM structure or inline JS. Use idiomatic reactive signals and
  composable components. Observable behaviour must be identical.
- **Do not "improve" away from the mockup**: do not add, remove, or rearrange UI
  elements, change text casing, fonts, colours, or spacing. Accessibility/responsive
  improvements are fine only if they don't change appearance or interaction flow.

---

#### Design system

Fonts loaded from Google Fonts in `index.html`:
- `Inter` (400/500/600/700) — headings, body text, panel titles, nav tabs, labels
- `JetBrains Mono` (400/600) — all monospace readouts, timestamps, badges, IDs, code

CSS custom properties defined in `style/main.css` (`:root` for dark, `body.light` for light).
Key variables: `--bg`, `--panel`, `--raised`, `--border`, `--borderhi`, `--cyan`, `--green`,
`--amber`, `--red`, `--purple`, `--txt`, `--txthi`, `--txtlo`, `--mono`, `--sans`.

All colours via CSS variables. No hardcoded hex. Light mode is the default.
Theme toggle adds/removes `light` class on `<body>`. Starfield fades to `opacity:0` in light.

---

#### Application structure

Two pages, switched via nav tabs **inside the header bar** (no separate nav row):

**Page 1 — Live Fleet** (`/` or default tab)
One ship (VSS Meridian), user-controlled. Ship only moves when the user commands it.
Station stock drains over time — the user must load/deliver supplies to keep stations
alive. Oversight strip appears bottom-of-map during voyages. Event log as a slim
horizontal strip at the bottom of the page.

**Page 2 — Scenarios** (`/scenarios`)
Grid of 5 mission cards. Each card opens a full-screen runner with step progress bar,
narrative text, interactive action area, and dedicated event log. Each mission demonstrates
one Canon feature with a beautiful, animated WASM visualisation.

---

#### Page 1 — Live Fleet layout

Full-width vertical column (no sidebar):

```
┌──────────────────────────────────────────────────────────┐
│ HEADER (56px): logo · nav tabs · fleet status dot · toggle│
├──────────────────────────────────────────────────────────┤
│ MAP BAR: ship status · destination buttons               │
├──────────────────────────────────────────────────────────┤
│                                                          │
│   CANVAS MAP (flex:1)                                    │
│                                                          │
│   Canvas-drawn planets (coloured circles)                │
│   Canvas-drawn ship (hull + thrust flame)                │
│   Canvas-drawn route lines, grid, stars                  │
│   Station labels + stock % below each planet             │
│                                                          │
│   [Oversight strip, position:absolute bottom]            │
│                                                          │
├──────────────────────────────────────────────────────────┤
│ STATION CARDS: 4 equal-width cards with stock bars       │
├──────────────────────────────────────────────────────────┤
│ SHIP ACTION BAR: contextual button (load/deliver/fly)    │
├──────────────────────────────────────────────────────────┤
│ EVENT LOG STRIP: horizontal, max-height 160px, scrollable│
└──────────────────────────────────────────────────────────┘
```

All visual details (header, popup, oversight strip, cards, action bar, event log) match
the mockup exactly. See `reference/mockup.html`.

**Station positions** (% of canvas):
- Alpha Depot: 18% 26% — green planet (radius 32)
- Beta Relay: 68% 14% — purple planet (radius 22) with ring
- Gamma Outpost: 76% 68% — coral/red planet (radius 28)
- Delta Prime: 24% 74% — blue planet (radius 20)

**Supply chain game constants**: Fixed supply loop Alpha→Beta→Gamma→Delta→Alpha.
Drain rates per 3s tick: Alpha 0.15 (starts 85%), Beta 0.20 (starts 60%), Gamma 0.25
(starts 40%), Delta 0.18 (starts 75%). Deliver at correct station → replenish by 35%.
Station hits 0% → game over.

---

#### Page 2 — Scenarios layout

Scenario page layout matches the mockup. Grid of 5 mission cards → full-screen runner
modal with step progress bar, narrative section, action area, and event log.

---

#### Scenario visualisations (WASM — must be beautiful and animated)

Each scenario has a central interactive visualisation. Must be polished, animated, and clear.

**Mission 01 — The Stranded Cargo (Oversight Gates)**
Gate card with two requirement rows. User clicks trigger → row animates ○→✓, badge flips amber→green. Downstream events fire.

**Mission 02 — The Ghost Ship (Snapshotting)**
Hydration counter 0→247 (fast ticking, ~40ms), shows elapsed time. Then snapshot hydration flashes to 247 instantly. Side-by-side bar chart comparison with speedup multiplier ("23x faster hydration").

**Mission 03 — The Resupply Crisis (Cross-service cascade)**
Vertical pipeline of 5 service nodes lighting up in sequence with animated arrows between them. Summary count at end.

**Mission 04 — The Cassandra Incident (Dead letters)**
Three dead-letter cards with Requeue/Discard buttons. Requeue transitions red→green, discard fades out.

**Mission 05 — The Duplicate Signal (Idempotency)**
Two identical command envelope cards side-by-side. Trigger deduplicates second card with red X animation. "ON CONFLICT DO NOTHING" note.

---

#### Data sources (when connected to real gateway)

Initial hydration on mount:
```
GET /ships          → Vec<ShipState>
GET /stations       → Vec<StationState>
GET /admin/oversight/windows  → Vec<OversightWindow>
GET /admin/deadletters        → Vec<DeadLetterEntry>
```

Live updates via `WS /events` — `WsMessage` tagged enum:
```rust
#[serde(tag = "type")]
pub enum WsMessage {
    Event(LiveEvent),
    ShipUpdate(ShipState),
    StationUpdate(StationState),
    OversightUpdate(OversightWindow),
    DeadLetter(DeadLetterEntry),
    InfraStatus(InfraStatusMsg),
}
```

WebSocket reconnects with 2s backoff. In-memory signals are the source of truth for rendering;
the WebSocket patches them incrementally. **Signals must only change in response to real
events arriving over the WebSocket or from initial hydration — never from local timers,
fake event chains, or fire-and-forget POST fallbacks.** If the gateway is unreachable, the
UI must show a connection error, not simulate events locally.

---

#### Build configuration

See `canon-demo/frontend/Cargo.toml` for dependencies. See `canon-demo/frontend/Trunk.toml`.

---

#### Acceptance criteria (all must pass before merge)

- [ ] `trunk build --release` produces a working WASM bundle, zero errors
- [ ] Visual language (colours, fonts, layout proportions) consistent with `reference/mockup.html`
- [ ] Fonts are Inter (400/500/600/700) + JetBrains Mono (400/600) — no other font families
- [ ] Ship only moves when the user commands it (click planet or destination button)
- [ ] Clicking a ship shows popup with correct version/snapshot data
- [ ] Selecting a destination POSTs a command to the gateway and the ship moves only when the real event arrives via WebSocket
- [ ] All UI state changes (ship position, stock levels, oversight gates, event log) are driven by real events from the Canon pipeline — zero local simulation
- [ ] If the gateway is unreachable, the UI shows a connection error — it does not fake events or fall back to local timers
- [ ] Oversight strip shows live requirement state during each voyage
- [ ] Correlation highlighting works in event log (using real correlation IDs from the pipeline)
- [ ] All 5 scenario missions complete without errors
- [ ] All 5 scenario visualisations are animated as specced
- [ ] Light/dark theme toggle works, starfield fades in light mode
- [ ] WebSocket connects to `WS /events` and patches signals on each message
- [ ] Initial hydration fetches all 4 endpoints on mount
- [ ] No `unwrap()` or `expect()` outside tests
- [ ] No hardcoded colours — all via CSS custom properties
- [ ] `make k8s-up` (or `kubectl apply -k canon-demo/k8s/overlays/minikube/`) deploys the full stack to minikube with zero manual intervention
- [ ] Gateway starts and responds on port 8080 inside Kubernetes
- [ ] `make k8s-test-e2e` Playwright smoke tests pass (stations have stock, ship can fly, events flow, scenarios render)

---

## Codebase exploration

Always use the LSP tool first when exploring the codebase — go-to-definition, find-references, hover for type info, and workspace symbol search. Fall back to Grep/Glob only when LSP doesn't cover what you need.

---
## What to do when stuck

1. Re-read the relevant section of this file.
2. Check the trait — the signature is the contract.
3. Check the dependency graph — wrong dependency = wrong approach.
4. Ask the user — do not invent solutions.

## What never to do

- Do not add unlisted dependencies without asking.
- Do not change trait signatures.
- Do not put business logic in infrastructure crates or infrastructure in `canon-core`.
- Do not use `unwrap()`/`expect()` in library code.
- Do not use `clone()` to dodge the borrow checker without flagging it.
- Do not write `// TODO` — implement it or ask.
- **Never checkout other branches in the main working directory.** Always use git worktrees (`isolation: "worktree"` in Agent tool, or `git worktree add`) for branch work. Checking out branches directly causes dist file conflicts, merge conflicts with agent worktrees, and lost work. The main working directory must always stay on `main`.
- **Never use `InMemory*` stores in demo service `main.rs`** — always wire real YugabyteDB/Cassandra/Kafka implementations. In-memory stores are for `canon-test` only.
- **Never use `#[ignore]` for pipeline tests** — use testcontainers instead. Ignored tests rot and silently break.
- **Never implement pipeline components in isolation without an e2e test** — every new component must be covered by an in-memory e2e test that exercises it as part of the full pipeline.
- **Never add C dependencies to Kafka crates** — `rdkafka`, `cmake-build`, `librdkafka-sys` are banned. Use `rskafka` only.
- **Never store Kafka offsets externally for correctness** — application-layer idempotency is the safety net. External offset storage is a performance optimization only, belongs in service wiring, not framework crates.
- **Never commit secrets, passwords, API keys, or credentials to the repo.** This includes:
  - GCP service account keys (`.json` key files)
  - `~/.canon-debug-key`, `~/.canon-auth-password`
  - Any `CANON_DEBUG_KEY`, `CANON_AUTH_PASSWORD` values
  - Database passwords (the `canon:canon` default is fine for local dev ConfigMaps, but
    production secrets must be in K8s Secrets, never in committed YAML)
  - GitHub tokens, `gcloud` credentials, kubeconfig files
  If you need to reference a secret value, use an environment variable or K8s Secret ref.
  If a file contains secrets, add it to `.gitignore`.

---

## Testing strategy

Two tiers of end-to-end tests, both run on every `cargo test` — no `#[ignore]`:

**Tier 1 — In-memory e2e (canon-test)**
Wire all `InMemory*` stores into a real `Service` via `ServiceBuilder`. Submit commands
through the dispatcher, step through outbox processor and all 3 consumers, assert events
reach the event store, projections, and publisher. Sub-second execution, no Docker.
Tests the logic of every component wired together.

**Tier 2 — Testcontainers e2e (canon-test or separate crate)**
Uses the `testcontainers` crate to spin up real YugabyteDB, Cassandra, and Kafka per
test module. Wires real `CassandraEventStore`, `YugabyteSnapshotStore`, `KafkaPublisher`,
etc. Tests actual SQL, CQL, and Kafka protocol. Catches serialisation bugs, schema
mismatches, and connection handling that in-memory can't catch. Containers managed
automatically — no manual Docker Compose.

**Tier 3 — Playwright e2e (canon-demo/e2e/)**
Browser-based smoke tests against a running cluster. Verify the full user experience:
stations have stock, stock drains over time, ship popup works, events appear in the
log, scenarios render. Run with `make k8s-test-e2e` after `make k8s-up`, or
automatically as part of `/test-demo`. Local only — not in CI.

**Never use `#[ignore]` for new pipeline tests.** If a test needs infrastructure, use
testcontainers. `#[ignore]` tests rot — they are never run and silently break.
Existing `#[ignore]` tests in infrastructure crates (e.g., `canon-command-store-yugabyte`,
`canon-deadletter-yugabyte`) will be migrated to testcontainers in #251.
