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
- **Auto-registration via `inventory`**: macros emit static registrations. `ServiceBuilder` discovers everything automatically.

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
    async fn handle(&self, state: &A::State, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error>;
}

// EventHandler — no aggregate parameter, optional oversight
pub trait EventHandler: Send + Sync + 'static {
    type Event: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;
    async fn handle(&self, events: Vec<Self::Event>) -> Result<Option<CommandEnvelope>, Self::Error>;
    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight { Oversight::Ready }
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
// Generates: pub enum DepartForStationEvent { ShipDeparted(ShipDeparted) }
```
`version` defaults to 1. `produces` generates a per-command event enum. Must have matching `#[command_handler]`.

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

Returns `Result<Vec<PerCommandEventEnum>>`. During counterfactual replay, `command_version` routes to matching handler.

```rust
#[command_handler(Ship, version = 1)]
impl DepartForStationHandler {
    type Error = FleetError;
    fn handle(&self, state: &Ship, cmd: DepartForStation) -> Result<Vec<DepartForStationEvent>, FleetError> {
        if state.status != ShipStatus::Docked { return Err(FleetError::ShipNotDocked); }
        Ok(vec![DepartForStationEvent::ShipDeparted(ShipDeparted { destination: cmd.destination })])
    }
}
```

### `#[event_handler]`

No aggregate parameter. `#[handles]` declares event type + version. `window_ttl` requires `oversight` (compile error without).

```rust
#[event_handler(window_ttl = "30m")]
impl ShipArrivedHandler {
    #[handles(ShipArrivedAtStation, version = 1)]
    fn handle(&self, events: Vec<ShipArrivedAtStation>) -> Option<CommandEnvelope> { None }
    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight { Oversight::Ready }
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
- `#[command_handler]` return type constrained to `produces` list

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
CREATE TABLE inbox_windows (handler_id TEXT, aggregate_id UUID, window_id UUID DEFAULT gen_random_uuid(), messages JSONB DEFAULT '[]', status TEXT DEFAULT 'pending', expires_at TIMESTAMPTZ, created_at TIMESTAMPTZ DEFAULT now(), updated_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY (handler_id, aggregate_id));
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
    fn handle(&self, state: &Ship, cmd: DepartForStation) -> Result<Vec<DepartForStationEvent>, FleetError> {
        if state.status != ShipStatus::Docked { return Err(FleetError::ShipNotDocked); }
        Ok(vec![DepartForStationEvent::ShipDeparted(ShipDeparted {
            destination: cmd.destination, fuel_at_departure: state.fuel_level,
        })])
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
Fleet map · Station depots · Cargo tracker · Supply chain · Event log · Counterfactual explorer

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
