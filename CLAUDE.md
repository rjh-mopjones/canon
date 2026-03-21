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
    fn handle(&self, events: Vec<CargoEvent>) -> Option<CommandEnvelope> { todo!() }

    // Optional. Extracts domain correlation key to group messages into the same window.
    // When absent, falls back to envelope correlation_id.
    fn correlate(&self, message: &IncomingMessage) -> Uuid {
        match message {
            IncomingMessage::ExternalEvent(e) => e.correlation_id,
            IncomingMessage::InternalEvent(e) => e.correlation_id,
            IncomingMessage::Command(c) => c.correlation_id,
        }
    }

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        if accumulated.iter().any(|m| matches!(m,
            IncomingMessage::ExternalEvent(e) if e.event_type == "ShipDecommissioned"))
        {
            return Oversight::Discard;
        }
        let has_arrival = accumulated.iter().any(|m| matches!(m,
            IncomingMessage::ExternalEvent(e) if e.event_type == "ShipArrivedAtStation"));
        let has_manifest = accumulated.iter().any(|m| matches!(m,
            IncomingMessage::InternalEvent(e) if e.event_type == "ManifestReceived"));
        if has_arrival && has_manifest { Oversight::Ready } else { Oversight::NotReady }
    }
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

### Outbound queue consumers (3 independent groups)
- **Event store**: writes to Cassandra. Snapshot if `version % N == 0`. Retry up to 3 on conflict → dead letter.
- **Projection**: applies to read models. Updates `last_version`. Rebuilds via Kafka offset reset while `rebuilding = true`.
- **Publisher**: publishes to `canon.{service}.events` for other services.

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

### YugabyteDB

```sql
-- inbox
CREATE TABLE inbox_messages (handler_id TEXT, message_id UUID, aggregate_id UUID, message_type TEXT, payload BYTEA, received_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY (handler_id, message_id));
CREATE TABLE inbox_windows (handler_id TEXT, correlation_key UUID, window_id UUID DEFAULT gen_random_uuid(), messages JSONB DEFAULT '[]', status TEXT DEFAULT 'pending', expires_at TIMESTAMPTZ, created_at TIMESTAMPTZ DEFAULT now(), updated_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY (handler_id, correlation_key));
CREATE TABLE processed_windows (window_id UUID PRIMARY KEY, handler_id TEXT, processed_at TIMESTAMPTZ DEFAULT now());

-- command store
CREATE TABLE commands (command_id UUID PRIMARY KEY, aggregate_id UUID, command_type TEXT, command_version INT DEFAULT 1, payload BYTEA, correlation_id UUID, causation_id UUID, created_at TIMESTAMPTZ DEFAULT now());
CREATE INDEX commands_aggregate_idx ON commands (aggregate_id, created_at);

-- outbox
CREATE SEQUENCE outbox_seq;
CREATE TABLE outbox (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), sequence_number BIGINT DEFAULT nextval('outbox_seq'), aggregate_id UUID, payload BYTEA, created_at TIMESTAMPTZ DEFAULT now(), delivered_at TIMESTAMPTZ);
CREATE INDEX outbox_seq_idx ON outbox (sequence_number) WHERE delivered_at IS NULL;

-- snapshot, projection, dead letter, retry
CREATE TABLE snapshots (aggregate_id UUID PRIMARY KEY, version BIGINT, state BYTEA, taken_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE projection_checkpoints (projection_id TEXT PRIMARY KEY, last_version BIGINT DEFAULT 0, rebuilding BOOLEAN DEFAULT false, updated_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE dead_letters (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), message_id UUID, handler_id TEXT, aggregate_id UUID, payload BYTEA, error TEXT, attempts INT DEFAULT 1, created_at TIMESTAMPTZ DEFAULT now(), last_attempted TIMESTAMPTZ DEFAULT now());
CREATE TABLE retry_attempts (message_id UUID PRIMARY KEY, handler_id TEXT, attempts INT DEFAULT 0, last_attempted TIMESTAMPTZ DEFAULT now());
```

### Cassandra

```cql
CREATE KEYSPACE canon WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE canon.events (aggregate_id UUID, version BIGINT, event_id UUID, event_type TEXT, event_version INT, payload BLOB, correlation_id UUID, causation_id UUID, created_at TIMESTAMP, PRIMARY KEY (aggregate_id, version)) WITH CLUSTERING ORDER BY (version ASC);
```

### Environment variables

```
CASSANDRA_NODES=cassandra:9042
YUGABYTE_URL=yugabyte://canon:canon@yugabyte:5433/canon
KAFKA_BROKERS=kafka:9092
```

---

## canon-demo domains

| Service | Aggregate | Commands | Events |
|---|---|---|---|
| fleet | Ship | RegisterShip, AssignRoute, DepartForStation, ScheduleResupply, DecommissionShip | ShipRegistered, RouteAssigned, ShipDeparted, ResupplyScheduled, ShipDecommissioned |
| cargo | Manifest | CreateManifest, LoadCargo, BeginUnloading, RecordUnloaded, CloseManifest | ManifestCreated, CargoLoaded, UnloadingStarted, CargoUnloaded, ManifestClosed |
| navigation | Route | PlanRoute, RecordDeparture, UpdatePosition, RecordArrival | RoutePlanned, ShipDeparted, PositionUpdated, ShipArrivedAtStation |
| supply | Inventory | RecordStock, RequestResupply, DispatchResupply, ConfirmDelivery | StockRecorded, ResupplyRequested, ResupplyDispatched, DeliveryConfirmed |
| station | Station | RegisterStation, RecordDocking, RecordCargoReceived, UpdateCapacity | StationRegistered, ShipDocked, CargoReceived, StationStockLow, CapacityUpdated |

### Cross-service flows
`Fleet:ShipDeparted → Navigation` · `Navigation:ShipArrivedAtStation → Cargo, Station` · `Station:StationStockLow → Supply` · `Supply:ResupplyDispatched → Fleet`

### Fleet-service example

```rust
#[aggregate(snapshot_every = 50)]
pub struct Ship { status: ShipStatus, fuel_level: f32, assigned_route: Option<Uuid> }

#[command(Ship, version = 1, produces = [ShipDeparted])]
pub struct DepartForStation { pub destination: StationId }

#[event(Ship, version = 1)]
pub struct ShipDeparted { pub destination: StationId, pub fuel_at_departure: f32 }

#[event_combiner(Ship, version = 1)]
impl ShipDeparted {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::InFlight;
        state.fuel_level -= self.fuel_at_departure * 0.1;
    }
}

#[command_handler(Ship, version = 1)]
impl DepartForStationHandler {
    type Error = FleetError;
    fn handle(&self, state: &Ship, cmd: DepartForStation) -> Result<ShipDeparted, FleetError> {
        if state.status != ShipStatus::Docked { return Err(FleetError::ShipNotDocked); }
        Ok(ShipDeparted { destination: cmd.destination, fuel_at_departure: state.fuel_level })
    }
}

#[event_handler]
impl ResupplyHandler {
    #[handles(ResupplyDispatched, version = 1)]
    fn handle(&self, events: Vec<ResupplyDispatched>) -> Option<CommandEnvelope> { todo!() }
}

ServiceBuilder::new().for_aggregate::<Ship>().build()
```

### Cargo-service oversight (UnloadingHandler)

```rust
#[event_handler(window_ttl = "30m")]
impl UnloadingHandler {
    #[handles(CargoEvent, version = 1)]
    fn handle(&self, events: Vec<CargoEvent>) -> Option<CommandEnvelope> { todo!() }

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        if accumulated.iter().any(|m| matches!(m, IncomingMessage::ExternalEvent(e) if e.event_type == "ShipDecommissioned")) {
            return Oversight::Discard;
        }
        let has_arrival = accumulated.iter().any(|m| matches!(m, IncomingMessage::ExternalEvent(e) if e.event_type == "ShipArrivedAtStation"));
        let has_manifest = accumulated.iter().any(|m| matches!(m, IncomingMessage::InternalEvent(e) if e.event_type == "ManifestCreated"));
        if has_arrival && has_manifest { Oversight::Ready } else { Oversight::NotReady }
    }
}
```

### Kafka topics
`canon.fleet.events` · `canon.cargo.events` · `canon.navigation.events` · `canon.supply.events` · `canon.station.events`

### Gateway (axum)
POST: `/fleet/ships`, `/fleet/ships/:id/route`, `/fleet/ships/:id/depart`, `/cargo/manifests`, `/cargo/manifests/:id/load`, `/navigation/routes`, `/supply/resupply`, `/stations/:id/register`
GET: `/stations/:id/inventory` (read-ready), `/ships/:id/history` (read-through), `/cargo/manifests/:id` (read-through), `/replay/counterfactual`
WS: `/events` — broadcast all DemoEvent as JSON

### Frontend (Leptos WASM)

The frontend is a Leptos 0.7 CSR WASM application built with Trunk. It lives at
`canon-demo/frontend/`. The **visual reference** is
`canon-demo/frontend/reference/mockup.html` — open it in a browser.

The mockup is a static HTML prototype with faked data and `setTimeout` chains. Use it
as a **reference for design tokens and interaction flows**, not as a pixel-perfect
specification:

- **Extract from the mockup**: CSS variables, colour palette, fonts, spacing, layout
  proportions, interaction patterns (what happens when you click X), and scenario flow
  sequences.
- **Do not mimic**: its DOM structure, `setTimeout`-based animation timing, or inline JS
  patterns. The Leptos app should use idiomatic reactive signals, composable components,
  and CSS-driven animations.
- **Free to improve**: responsive behaviour, accessibility, animation polish, and
  handling of real data edge cases (empty states, reconnection, error feedback) may
  diverge from the mockup where the real app benefits.

---

#### Design system

Fonts loaded from Google Fonts in `index.html`:
- `Inter` (400/500/600/700) — headings, body text, panel titles, nav tabs, labels
- `JetBrains Mono` (400/600) — all monospace readouts, timestamps, badges, IDs, code

CSS custom properties (defined on `:root`, overridden on `body.light`):

```css
/* dark */
--bg:#070f1c; --panel:#0d1e33; --raised:#112540;
--border:rgba(0,160,230,.18); --borderhi:rgba(0,160,230,.45);
--cyan:#00b4ff; --cyandim:rgba(0,180,255,.5);
--green:#00e58a; --greendim:rgba(0,229,138,.55);
--amber:#f5a623; --amberdim:rgba(245,166,35,.55);
--red:#ff4069; --reddim:rgba(255,64,105,.55);
--purple:#a78bfa;
--txt:#9db8d2; --txthi:#daeaf8; --txtlo:#4e6a82;
--grid:rgba(0,160,230,.032);
--logbg:rgba(0,160,230,.05); --loglit:rgba(0,160,230,.09); --logorig:rgba(0,160,230,.15);
--divclr:rgba(0,160,230,.07); --shadow:rgba(0,0,0,.45);
--mono:'JetBrains Mono',monospace; --sans:'Inter',sans-serif;

/* light (body.light) — light mode is the default */
--bg:#eef3f9; --panel:#fff; --raised:#f4f8fd;
--border:rgba(0,120,180,.15); --borderhi:rgba(0,120,180,.4);
--cyan:#0086cc; --cyandim:rgba(0,134,204,.55);
--green:#00a86b; --greendim:rgba(0,168,107,.6);
--amber:#d4820a; --amberdim:rgba(212,130,10,.6);
--red:#d42e55; --reddim:rgba(212,46,85,.6);
--purple:#7c3aed;
--txt:#3d5a7a; --txthi:#0f2a45; --txtlo:#7a9ab8;
--grid:rgba(0,120,180,.06);
--logbg:rgba(0,120,180,.04); --loglit:rgba(0,120,180,.08); --logorig:rgba(0,120,180,.14);
--divclr:rgba(0,120,180,.1); --shadow:rgba(0,0,0,.12);
```

All colours via CSS variables. No hardcoded hex in Leptos components. **Light mode is the
default.** Theme toggle adds/removes `light` class on `<body>`. Starfield (`body::before`)
fades to `opacity:0` in light mode.

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

**Header**: "CANON / fleet ops demo" logo, nav tabs ("Live Fleet" / "Scenarios")
inline, fleet running status dot + light/dark toggle on right. No infrastructure
badges (Kafka/YugabyteDB/Cassandra) in the header.

**Canvas map**: `<canvas>` element, not DOM/SVG. Grid lines (70px), stars (dark mode
only), route lines (dashed while in transit), and all station/ship rendering done in
canvas draw calls. Background colour follows theme.

**Stations** (4 fixed positions as % of canvas):
- Alpha Depot: 18% 26% — green planet (radius 32)
- Beta Relay: 68% 14% — purple planet (radius 22) with ring
- Gamma Outpost: 76% 68% — coral/red planet (radius 28)
- Delta Prime: 24% 74% — blue planet (radius 20)

Each planet: solid colour fill, subtle white highlight sheen, name label below in
`Inter 600 13px`. Live stock % drawn below the label, colour-coded (green >50%,
amber 25–50%, red <25%). Pulsing amber warning ring around planets with critical
stock (<25%).

**Ship**: One ship — VSS Meridian. Renders as a directional hull with an animated
thrust flame while in transit. Smooth cubic-eased interpolation between planets (not
instant teleport). Ship sits above the planet when docked, moves to centre when
undocked. Clicking a planet sends the ship there. Clicking the ship opens the detail
popup.

**Ship click → popup**: Shows ship name (Inter 16px 700), status line, "Select
destination:" hint, destination button list (all 4 stations, current station disabled
with reduced opacity), plus info panel showing fuel %, aggregate version,
events-since-snapshot progress bar (cyan fill, amber snapshot marker).

**Oversight strip**: `position:absolute; bottom:0; left:0; right:0`. Shows handler ID,
gate title, two requirement rows (✓ green if met, ○ dim if pending), status badge
(Not Ready = amber, Ready = green). Appears when a voyage starts, disappears after
both conditions met.

**Station cards**: 4 equal-width cards below the map. Each shows: station name
(highlighted in cyan when ship is docked there), "Supplied from X" subtitle,
health bar (colour-coded: green >50%, amber 25–50%, red <25%), large % readout.
Clicking a card flies the ship to that station.

**Ship action bar**: Single contextual row that changes based on state:
- Flying (empty): "Click a planet to fly there"
- Flying (loaded): "Carrying supplies — fly to [X] to deliver"
- Docked (empty): "Docked at [X]" + "Load supplies for [Y]" button
- Docked (loaded, right station): "Docked at [X]" + "Deliver supplies here" button
- Docked (loaded, wrong station): "Supplies are for [Y] — fly there to deliver"
- Game over: "Supply chain collapsed" + Restart button

**Supply chain game**: Each station has one stock bar (0–100%) that drains over time.
Fixed supply routes form a loop: Alpha→Beta→Gamma→Delta→Alpha. Drain rates per 3s tick:
Alpha 0.15 (starts 85%), Beta 0.20 (starts 60%), Gamma 0.25 (starts 40%), Delta 0.18
(starts 75%). Load cargo at a station → picks up supplies for the next station in the
loop. Deliver at the correct station → replenishes stock by 35%. Station hits 0% →
game over. Canon events fire on load/unload/stock-low/game-over with correlation IDs.

**Event log strip**: Horizontal strip at the bottom (not a sidebar), max-height 160px,
scrollable. Each row: timestamp · service badge (coloured pill) · event name · aggregate
text. Flash animation on new events. Click-to-highlight correlation chains still works.

---

#### Page 2 — Scenarios layout

Hero section (padding 36px 40px): title "Canon Feature Scenarios", subtitle explaining the
purpose. Below: CSS grid of mission cards (`grid-template-columns: repeat(auto-fill, minmax(320px,1fr))`).

Each card: mission number (JetBrains Mono 11px dim), name (Inter 18px 700, sentence case),
ship/context line (JetBrains Mono 12px cyan), description (Inter 13px, line-height 1.6),
feature tags (small border pills), "Launch Mission →" link. Top accent line (3px cyan
gradient) appears on hover/active. Hover raises with box-shadow.

**Scenario runner** (full-screen modal, `position:fixed;inset:0;z-index:100`):
- Header bar: mission title + close button
- Body: two-column grid — stage left (flex-fill), event log right (360px)
- Stage: step progress bar top, narrative section, success banner (hidden until complete),
  action area (centred, contains the interactive visualisation)

Step progress bar: numbered circles (24px, border-radius 50%). Done = green fill + ✓.
Active = cyan fill + glow. Future = dim border. Connected by horizontal lines that turn
green when step completes.

---

#### Scenario visualisations (WASM — must be beautiful and animated)

Each scenario has a central interactive visualisation in the action area. These are the
heart of the demo. They must be polished, animated, and clear.

**Mission 01 — The Stranded Cargo (Oversight Gates)**
Visualisation: a gate card showing two requirement rows. Row 1 (ShipArrivedAtStation) already
ticked green with a checkmark animation. Row 2 (ManifestCreated) shows an empty circle, dim
text, pulsing amber. Status badge shows "Not Ready" in amber. User clicks "File Cargo Manifest"
— row 2 animates from ○ to ✓ (scale-pop animation), text brightens, badge flips to "Ready"
in green with a brief glow pulse. Gate card border transitions from amber to green. Then the
downstream events fire in the log automatically.

**Mission 02 — The Ghost Ship (Snapshotting)**
Has two sub-visualisations shown in sequence. First: hydration counter — large monospace
number (42px cyan) counting upward from 0 to 247 with a progress bar, showing "replaying
event vN…" status text. Counter ticks rapidly (every ~40ms) to feel visceral. When it
reaches 247 it pauses and shows the elapsed time in amber ("640ms"). Second: after the
snapshot is written (animated fill bar counting 0→247 in green), the second hydration counter
jumps immediately to 247 with a single flash — "28ms". Final state: side-by-side bar chart.
Left bar (full width, red tint): "Without snapshot — 640ms — 247 events". Right bar
(narrow, ~4% width, green tint): "With snapshot — 28ms — 0 events". Bars animate in with a
CSS width transition. Speedup multiplier displayed below: "23× faster hydration" in green
Inter 700.

**Mission 03 — The Resupply Crisis (Cross-service cascade)**
Visualisation: a vertical pipeline of 5 service nodes (station → supply → fleet → nav → cargo),
connected by animated arrows. Each node: service badge pill + event name. As the cascade fires,
nodes light up in sequence — each node pulses briefly when its event arrives, the connecting
arrow animates (travelling dot from one node to the next). Nodes that haven't fired yet are dim.
The whole pipeline animates from top to bottom over ~6 seconds. A "10 events across 5 services"
summary appears at the bottom when complete.

**Mission 04 — The Cassandra Incident (Dead letters)**
Visualisation: three dead-letter cards stacked vertically. Each shows event name (red), attempt
count ("3 attempts"), error string (mono, truncated). Two action buttons per card: "Requeue" and
"Discard". On Requeue: card border transitions from red to green, opacity drops, button replaced
by "✓ requeued" text in green. Each requeue fires events in the log. When all 3 are requeued a
success state appears. Discard removes the card with a fade-out animation.

**Mission 05 — The Duplicate Signal (Idempotency)**
Visualisation: two command "envelopes" rendered as bordered cards side by side. Both show
identical content — same command type, same message_id highlighted in cyan. First card:
"Command 1" label in green, "ACCEPTED" badge. Second card: "Command 2 (duplicate)" label,
initially shows a "PENDING" badge in amber. After user clicks the trigger button, the second
card animates — a red ✕ sweeps across it, badge changes to "DEDUPLICATED" in dim red, card
opacity drops to 0.4. A note appears below: "INSERT … ON CONFLICT DO NOTHING — row already
exists". The ship departs exactly once in the log.

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
the WebSocket patches them incrementally.

---

#### New gateway endpoints required

The frontend needs these endpoints added to the gateway (not currently specced):

```
GET  /ships                          → Vec<ShipState> (all ships + positions)
GET  /admin/oversight/windows        → Vec<OversightWindow> (pending inbox windows)
GET  /admin/deadletters              → Vec<DeadLetterEntry>
POST /admin/deadletters/:id/requeue  → requeue dead letter
DELETE /admin/deadletters/:id        → discard dead letter
```

---

#### Leptos Cargo.toml dependencies

```toml
[dependencies]
leptos = { version = "0.7", features = ["csr"] }
wasm-bindgen = "0.2"
web-sys = { version = "0.3", features = ["WebSocket","MessageEvent","CloseEvent","ErrorEvent","Performance"] }
gloo-net = { version = "0.6", features = ["http","websocket"] }
gloo-timers = { version = "0.3", features = ["callbacks"] }
js-sys = "0.3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["serde","js"] }
futures = "0.3"
canon-demo-shared = { path = "../shared" }
```

#### Trunk.toml

```toml
[build]
target = "index.html"
dist = "dist"
public_url = "/"
```

---

#### Acceptance criteria (all must pass before merge)

- [ ] `trunk build --release` produces a working WASM bundle, zero errors
- [ ] Visual language (colours, fonts, layout proportions) consistent with `reference/mockup.html`
- [ ] Ships fly autonomously from page load, loop indefinitely
- [ ] Clicking a ship shows popup with correct version/snapshot data
- [ ] Selecting a destination departs the ship and fires the full event chain
- [ ] Oversight strip shows live requirement state during each voyage
- [ ] Correlation highlighting works in event log
- [ ] All 5 scenario missions complete without errors
- [ ] All 5 scenario visualisations are animated as specced
- [ ] Light/dark theme toggle works, starfield fades in light mode
- [ ] WebSocket connects to `WS /events` and patches signals on each message
- [ ] Initial hydration fetches all 4 endpoints on mount
- [ ] No `unwrap()` or `expect()` outside tests
- [ ] No hardcoded colours — all via CSS custom properties

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
