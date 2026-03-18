# Canon — Claude Code guide

Canon is a Rust event sourcing framework. This file is the authoritative reference for every Claude Code session. Read it in full before writing any code. The design is settled — your job is implementation, not design.

---

## What Canon is

A production-grade event sourcing framework built around a four-stage message processing pipeline:

```
External caller
      │
      ▼
   Inbox                    ← idempotent intake, event assembly, oversight evaluation
      │
      ▼
Internal queue              ← crash-safe delivery (RabbitMQ)
      │
      ▼
Command handler             ← load aggregate, validate, emit events
      │
      ├──▶ Command store     ← append-only audit trail + replay source
      │
      ▼
  Event store               ← append-only, versioned, one stream per aggregate
      │
      ├──▶ Event handlers    ← fan-out, produce zero or one command each
      ├──▶ Projections       ← build read models, idempotent
      └──▶ Publisher         ← outbound events to other services
```

---

## Non-negotiable rules

These are settled decisions. Do not propose alternatives. Do not deviate.

- **Async runtime**: tokio only. No async-std. No sync variants of any trait.
- **Async traits**: `async_trait` macro throughout. No manual `Pin<Box<dyn Future>>`.
- **Error handling**: `thiserror` in every crate. No `anyhow` in library code. No god error enum. Each crate owns its errors.
- **AggregateId**: always `AggregateId(Uuid)` newtype. Never generic. Never a plain `Uuid`.
- **Macros**: proc-macros live in the crate that owns the concept. No separate `canon-macros` crate.
- **Cross-crate dependencies**: implementation crates depend on their trait crate and `canon-core` only. No impl crate depends on another impl crate.
- **In-memory implementations**: every trait has an in-memory impl in `canon-core`. These are the test harness. Do not skip them.
- **No in-memory queues**: the internal queue is always RabbitMQ-backed. In-memory queue is for tests only, via the `canon-core` in-memory impl.
- **Outbox pattern**: events are written to the event store and outbox in the same transaction. Never write to the event store and publish directly.
- **Idempotency**: all event handlers and projections must be safe to call twice with the same input.
- **Optimistic concurrency**: the event store must reject writes where the expected version does not match the stored version.

---

## Workspace layout

```
canon/
├── CLAUDE.md                          ← you are here
├── Cargo.toml                         ← workspace root
├── canon-core/                        ← traits, types, in-memory impls, proc-macros
├── canon-event-store/                 ← EventStore trait
├── canon-event-store-cassandra/       ← Cassandra impl
├── canon-command-store/               ← CommandStore trait
├── canon-command-store-pg/            ← PostgreSQL impl
├── canon-snapshot-store/              ← SnapshotStore trait
├── canon-snapshot-store-pg/           ← PostgreSQL impl
├── canon-inbox/                       ← Inbox trait
├── canon-inbox-pg/                    ← PostgreSQL impl
├── canon-queue/                       ← MessageQueue trait
├── canon-queue-rabbitmq/              ← RabbitMQ impl
├── canon-projection-store/            ← ProjectionStore trait
├── canon-projection-store-pg/         ← PostgreSQL impl
├── canon-publisher/                   ← EventPublisher trait
├── canon-publisher-kafka/             ← Kafka impl
├── canon-adaptor/                     ← EventAdaptor trait (inbound from other services)
├── canon-adaptor-kafka/               ← Kafka impl
├── canon-deadletter/                  ← DeadLetterStore trait
├── canon-deadletter-pg/               ← PostgreSQL impl
└── canon-demo/
    ├── Cargo.toml                     ← demo workspace
    ├── docker-compose.yml
    ├── k8s/
    ├── shared/                        ← shared domain types, events, commands, topic names
    ├── fleet-service/
    ├── cargo-service/
    ├── navigation-service/
    ├── supply-service/
    ├── station-service/
    ├── gateway/                       ← axum, REST + WebSocket
    └── frontend/                      ← Leptos WASM
```

### Dependency graph (strict DAG — never violate this)

```
canon-core
    ├── canon-event-store
    │       └── canon-event-store-cassandra
    ├── canon-command-store
    │       └── canon-command-store-pg
    ├── canon-snapshot-store
    │       └── canon-snapshot-store-pg
    ├── canon-inbox
    │       └── canon-inbox-pg
    ├── canon-queue
    │       └── canon-queue-rabbitmq
    ├── canon-projection-store
    │       └── canon-projection-store-pg
    ├── canon-publisher
    │       └── canon-publisher-kafka
    ├── canon-adaptor
    │       └── canon-adaptor-kafka
    └── canon-deadletter
            └── canon-deadletter-pg
```

---

## Implementation phases

Work strictly in this order. Do not start a phase until the previous one compiles and its tests pass.

### Phase 1 — workspace scaffolding
Create the workspace `Cargo.toml`, all crate directories, empty `lib.rs` files, and `Cargo.toml` per crate with correct dependency declarations. No logic. Verify `cargo check --workspace` passes before moving on.

### Phase 2 — canon-core types
Implement all fundamental types in `canon-core/src/types.rs`:
- `AggregateId`, `Version`, `EventEnvelope`, `CommandEnvelope`
- `IncomingMessage`, `Oversight`
- `CounterfactualRequest`, `CounterfactualResult`, `CommandDiff`

### Phase 3 — canon-core traits
Implement all traits in `canon-core/src/traits/`. One file per trait:
- `aggregate.rs` — `Aggregate`
- `command_handler.rs` — `CommandHandler`
- `event_handler.rs` — `EventHandler`
- `projection.rs` — `Projection`
- `replay.rs` — `CounterfactualReplay`

### Phase 4 — canon-core in-memory implementations
Implement in-memory versions of every infrastructure trait in `canon-core/src/memory/`. These are used in tests. They must be functionally correct, not just stub implementations.
- `InMemoryEventStore`
- `InMemoryCommandStore`
- `InMemorySnapshotStore`
- `InMemoryInbox`
- `InMemoryQueue`
- `InMemoryProjectionStore`
- `InMemoryPublisher`
- `InMemoryAdaptor`
- `InMemoryDeadLetterStore`

### Phase 5 — canon-core integration tests
Write tests in `canon-core/tests/` that exercise the full pipeline using only in-memory implementations. A test should be able to submit a command and assert the resulting events, handler outputs, and projection state — all without any external infrastructure.

### Phase 6 — trait crates
Implement the thin trait crates. Each contains only the trait definition and associated types — no logic. They re-export the relevant types from `canon-core`.

### Phase 7 — infrastructure crates (one at a time)
Implement each infrastructure crate in this order. Verify each compiles and its integration tests pass before starting the next.

1. `canon-inbox-pg` — most complex, do this first
2. `canon-queue-rabbitmq`
3. `canon-command-store-pg`
4. `canon-snapshot-store-pg`
5. `canon-event-store-cassandra`
6. `canon-projection-store-pg`
7. `canon-deadletter-pg`
8. `canon-publisher-kafka`
9. `canon-adaptor-kafka`

### Phase 8 — canon-demo shared crate
Implement `canon-demo/shared` with all domain event and command enums, serialisation derives, and Kafka topic name constants. No logic.

### Phase 9 — fleet-service
Implement `fleet-service` as the reference service. It must use every canon framework feature end to end. All other services follow this pattern.

### Phase 10 — remaining demo services
Implement in this order: `navigation-service`, `cargo-service`, `station-service`, `supply-service`.

### Phase 11 — gateway
Implement the axum gateway with REST command endpoints, WebSocket event streaming, and read model query endpoints.

### Phase 12 — frontend
Implement the Leptos WASM frontend.

---

## Core types (canonical definitions — do not modify)

```rust
// canon-core/src/types.rs

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct AggregateId(uuid::Uuid);

impl AggregateId {
    pub fn new() -> Self { Self(uuid::Uuid::new_v4()) }
    pub fn from_uuid(id: uuid::Uuid) -> Self { Self(id) }
    pub fn as_uuid(&self) -> &uuid::Uuid { &self.0 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct Version(u64);

impl Version {
    pub fn initial() -> Self { Self(0) }
    pub fn next(self) -> Self { Self(self.0 + 1) }
    pub fn as_u64(&self) -> u64 { self.0 }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EventEnvelope {
    pub event_id: uuid::Uuid,
    pub aggregate_id: AggregateId,
    pub version: Version,
    pub event_type: String,
    pub event_version: u32,
    pub payload: bytes::Bytes,
    pub correlation_id: uuid::Uuid,
    pub causation_id: uuid::Uuid,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommandEnvelope {
    pub command_id: uuid::Uuid,
    pub aggregate_id: AggregateId,
    pub correlation_id: uuid::Uuid,
    pub causation_id: uuid::Uuid,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub payload: bytes::Bytes,
}

#[derive(Debug, Clone)]
pub enum IncomingMessage {
    Command(CommandEnvelope),
    InternalEvent(EventEnvelope),
    ExternalEvent(EventEnvelope),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Oversight {
    Ready,
    NotReady,
    Discard,
}

#[derive(Debug, Clone)]
pub struct CounterfactualRequest {
    pub aggregate_id: AggregateId,
    pub branch_version: Version,
    pub substituted_command: CommandEnvelope,
}

#[derive(Debug, Clone)]
pub struct CounterfactualResult {
    pub original_commands: Vec<CommandEnvelope>,
    pub counterfactual_commands: Vec<CommandEnvelope>,
    pub diff: CommandDiff,
}

#[derive(Debug, Clone)]
pub struct CommandDiff {
    pub added: Vec<CommandEnvelope>,
    pub removed: Vec<CommandEnvelope>,
    pub unchanged: Vec<CommandEnvelope>,
}
```

---

## Core traits (canonical definitions — do not modify)

```rust
// canon-core/src/traits/aggregate.rs
#[async_trait::async_trait]
pub trait Aggregate: Sized + Send + Sync + 'static {
    type State: Default + Send + Sync;
    type Command: Send + Sync;
    type Event: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    fn apply(state: &mut Self::State, event: &Self::Event);

    async fn handle(
        state: &Self::State,
        command: Self::Command,
    ) -> Result<Vec<Self::Event>, Self::Error>;

    fn upcast(raw: EventEnvelope) -> Result<Self::Event, Self::Error>;

    fn hydrate(state: &mut Self::State, events: impl Iterator<Item = Self::Event>) {
        for event in events {
            Self::apply(state, &event);
        }
    }
}

// canon-core/src/traits/command_handler.rs
#[async_trait::async_trait]
pub trait CommandHandler<A: Aggregate>: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle(
        &self,
        envelope: CommandEnvelope,
        state: &A::State,
    ) -> Result<Vec<A::Event>, Self::Error>;
}

// canon-core/src/traits/event_handler.rs
#[async_trait::async_trait]
pub trait EventHandler: Send + Sync + 'static {
    type Event: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle(
        &self,
        events: Vec<Self::Event>,
    ) -> Result<Option<CommandEnvelope>, Self::Error>;

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        Oversight::Ready
    }
}

// canon-core/src/traits/projection.rs
#[async_trait::async_trait]
pub trait Projection: Send + Sync + 'static {
    type Event: Send + Sync;
    type Store: ProjectionStore;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn apply(
        &self,
        event: &Self::Event,
        store: &Self::Store,
    ) -> Result<(), Self::Error>;

    async fn rebuild(
        &self,
        events: impl futures::Stream<Item = Self::Event> + Send,
        store: &Self::Store,
    ) -> Result<(), Self::Error>;

    fn projection_id(&self) -> &str;
}

// canon-core/src/traits/replay.rs
#[async_trait::async_trait]
pub trait CounterfactualReplay: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn replay(
        &self,
        request: CounterfactualRequest,
    ) -> Result<CounterfactualResult, Self::Error>;
}
```

---

## Handler macro usage

The `#[canon::handler]` macro is defined in `canon-core`. It:
- Requires a `handle` method — infers event type and dispatch mode from the parameter type
- Treats `oversight` as optional — defaults to `Oversight::Ready` if absent
- Generates the `EventHandler` trait impl and handler registration boilerplate

```rust
// Batch of same-type events — Vec<T> infers batch dispatch
#[canon::handler]
impl MyHandler {
    async fn handle(&self, events: Vec<ShipDeparted>) -> Result<Option<CommandEnvelope>, MyError> {
        todo!()
    }
}

// Enum dispatch — Vec<DomainEvent> dispatches on variant
#[canon::handler]
impl MyHandler {
    async fn handle(&self, events: Vec<FleetEvent>) -> Result<Option<CommandEnvelope>, MyError> {
        todo!()
    }

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        if accumulated.len() >= 2 { Oversight::Ready } else { Oversight::NotReady }
    }
}

// Correlated window — Window<T> groups by aggregate ID
#[canon::handler]
impl MyHandler {
    async fn handle(&self, events: Window<FleetEvent>) -> Result<Option<CommandEnvelope>, MyError> {
        todo!()
    }
}
```

---

## Storage backends

| Store | Backend | Notes |
|---|---|---|
| Event store | Cassandra | Append-only, one partition per aggregate ID |
| Command store | PostgreSQL | Queryable by aggregate ID + version range |
| Inbox | PostgreSQL | Composite unique key: handler_id + message_id |
| Snapshot store | PostgreSQL | One row per aggregate ID, latest version only |
| Projection store | PostgreSQL | Checkpoint per projection_id |
| Dead letter store | PostgreSQL | Inspectable, requeueable |
| Internal queue | RabbitMQ | Durable, manual ack/nack, dead-letter exchange configured |

### Environment variables (all services)

```
CASSANDRA_NODES=cassandra:9042
POSTGRES_URL=postgres://canon:canon@postgres:5432/canon
RABBITMQ_URL=amqp://canon:canon@rabbitmq:5672
KAFKA_BROKERS=kafka:9092
```

### PostgreSQL schemas

**inbox**
```sql
CREATE TABLE inbox_messages (
    handler_id      TEXT NOT NULL,
    message_id      UUID NOT NULL,
    aggregate_id    UUID NOT NULL,
    message_type    TEXT NOT NULL,
    payload         BYTEA NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (handler_id, message_id)
);

CREATE TABLE inbox_windows (
    handler_id      TEXT NOT NULL,
    aggregate_id    UUID NOT NULL,
    messages        JSONB NOT NULL DEFAULT '[]',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (handler_id, aggregate_id)
);
```

**command store**
```sql
CREATE TABLE commands (
    command_id      UUID PRIMARY KEY,
    aggregate_id    UUID NOT NULL,
    command_type    TEXT NOT NULL,
    payload         BYTEA NOT NULL,
    correlation_id  UUID NOT NULL,
    causation_id    UUID NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX commands_aggregate_idx ON commands (aggregate_id, created_at);
```

**snapshot store**
```sql
CREATE TABLE snapshots (
    aggregate_id    UUID PRIMARY KEY,
    version         BIGINT NOT NULL,
    state           BYTEA NOT NULL,
    taken_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**projection store**
```sql
CREATE TABLE projection_checkpoints (
    projection_id   TEXT PRIMARY KEY,
    last_version    BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**dead letter store**
```sql
CREATE TABLE dead_letters (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id      UUID NOT NULL,
    handler_id      TEXT NOT NULL,
    aggregate_id    UUID NOT NULL,
    payload         BYTEA NOT NULL,
    error           TEXT NOT NULL,
    attempts        INT NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_attempted  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Cassandra schema (event store)

```cql
CREATE KEYSPACE canon WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE canon.events (
    aggregate_id    UUID,
    version         BIGINT,
    event_id        UUID,
    event_type      TEXT,
    event_version   INT,
    payload         BLOB,
    correlation_id  UUID,
    causation_id    UUID,
    created_at      TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
) WITH CLUSTERING ORDER BY (version ASC);
```

---

## canon-demo domains

### Aggregates and their events

| Service | Aggregate | Commands | Events |
|---|---|---|---|
| fleet-service | Ship | `RegisterShip`, `AssignRoute`, `DepartForStation`, `ScheduleResupply`, `DecommissionShip` | `ShipRegistered`, `RouteAssigned`, `ShipDeparted`, `ResupplyScheduled`, `ShipDecommissioned` |
| cargo-service | Manifest | `CreateManifest`, `LoadCargo`, `BeginUnloading`, `RecordUnloaded`, `CloseManifest` | `ManifestCreated`, `CargoLoaded`, `UnloadingStarted`, `CargoUnloaded`, `ManifestClosed` |
| navigation-service | Route | `PlanRoute`, `RecordDeparture`, `UpdatePosition`, `RecordArrival` | `RoutePlanned`, `ShipDeparted`, `PositionUpdated`, `ShipArrivedAtStation` |
| supply-service | Inventory | `RecordStock`, `RequestResupply`, `DispatchResupply`, `ConfirmDelivery` | `StockRecorded`, `ResupplyRequested`, `ResupplyDispatched`, `DeliveryConfirmed` |
| station-service | Station | `RegisterStation`, `RecordDocking`, `RecordCargoReceived`, `UpdateCapacity` | `StationRegistered`, `ShipDocked`, `CargoReceived`, `StationStockLow`, `CapacityUpdated` |

### Cross-service event flows (via Kafka)

```
Fleet:ShipDeparted → Navigation (adaptor)
Navigation:ShipArrivedAtStation → Cargo (adaptor), Station (adaptor)
Station:StationStockLow → Supply (adaptor)
Supply:ResupplyDispatched → Fleet (adaptor)
```

### Key oversight example (cargo-service UnloadingHandler)

```rust
fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
    // Discard if ship was decommissioned
    let decommissioned = accumulated.iter().any(|m| matches!(
        m, IncomingMessage::ExternalEvent(e) if e.event_type == "ShipDecommissioned"
    ));
    if decommissioned { return Oversight::Discard; }

    // Ready only when both ShipArrivedAtStation and ManifestCreated are present
    let has_arrival = accumulated.iter().any(|m| matches!(
        m, IncomingMessage::ExternalEvent(e) if e.event_type == "ShipArrivedAtStation"
    ));
    let has_manifest = accumulated.iter().any(|m| matches!(
        m, IncomingMessage::InternalEvent(e) if e.event_type == "ManifestCreated"
    ));

    if has_arrival && has_manifest { Oversight::Ready } else { Oversight::NotReady }
}
```

### Snapshot strategy

Fleet service snapshots every 50 events per ship aggregate. Implement in the `Service` orchestrator — after appending events, check if `version % 50 == 0` and write a snapshot if so.

### Kafka topics

```rust
// canon-demo/shared/src/topics.rs
pub const FLEET_EVENTS: &str = "canon.fleet.events";
pub const CARGO_EVENTS: &str = "canon.cargo.events";
pub const NAVIGATION_EVENTS: &str = "canon.navigation.events";
pub const SUPPLY_EVENTS: &str = "canon.supply.events";
pub const STATION_EVENTS: &str = "canon.station.events";
```

### Gateway REST + WebSocket

HTTP framework: axum. The gateway is the only entry point for the Leptos frontend.

```
POST /fleet/ships                    → RegisterShip command to fleet-service inbox
POST /fleet/ships/:id/route          → AssignRoute
POST /fleet/ships/:id/depart         → DepartForStation
POST /cargo/manifests                → CreateManifest
POST /cargo/manifests/:id/load       → LoadCargo
POST /navigation/routes              → PlanRoute
POST /supply/resupply                → RequestResupply
POST /stations/:id/register          → RegisterStation
GET  /stations/:id/inventory         → station inventory projection (read-ready)
GET  /ships/:id/history              → ship event history (read-through)
GET  /cargo/manifests/:id            → manifest state (read-through)
GET  /replay/counterfactual          → CounterfactualReplay
WS   /events                         → broadcast all DemoEvent as JSON
```

### Frontend views (Leptos WASM)

- Fleet map — ships, status, routes, fuel
- Station depots — inventory, dockings, stock alerts
- Cargo tracker — manifests, unloading progress
- Supply chain — resupply requests and missions
- Event log — live WebSocket feed, correlation ID highlighting
- Counterfactual explorer — pick a ship + version, substitute a command, view command diff

---

## What to do when stuck

1. Re-read the relevant section of this file.
2. Check the trait definition — the trait signature is the contract. If your implementation doesn't match it, your implementation is wrong.
3. Check the dependency graph — if you're reaching for a crate that shouldn't be a dependency, you're solving the wrong problem.
4. If a design decision feels missing, ask the user — do not invent a solution. The design is complete and settled.

## What never to do

- Do not add dependencies not listed in this file without asking first.
- Do not change a trait signature. Traits are the public API contract.
- Do not implement business logic in infrastructure crates.
- Do not implement infrastructure concerns in `canon-core`.
- Do not use `unwrap()` or `expect()` in library code. Use `?` and proper error types.
- Do not use `clone()` to work around a borrow checker issue without flagging it.
- Do not write `// TODO` and move on. Either implement it or stop and ask.
