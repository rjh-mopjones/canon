# Canon — Claude Code guide

Canon is a Rust event sourcing framework. This file is the authoritative reference for every Claude Code session. Read it in full before writing any code. The design is settled — your job is implementation, not design.

---

## What Canon is

A production-grade event sourcing framework built around a multi-stage message processing pipeline:

```
External world
      │
      ▼
canon-adaptor-kafka          ← inbound events from other services
      │
      ▼
canon-inbox-yugabyte         ← idempotency, assembly, oversight
      │
      ▼
canon-inbound-queue-kafka    ← assembled batches to handlers (partitioned by aggregate_id)
      │
      ▼
Dispatcher
  ├──▶ Command handler
  ├──▶ Internal event handlers
  └──▶ External event handlers
      │
      ▼
YugabyteDB transaction
  ├── commands table          ← audit trail (direct write)
  └── outbox table            ← event staging (sequence_number ordered)
      │
      ▼
Outbox processor              ← single responsibility: drain outbox → publish to outbound queue
      │
      ▼
canon-outbound-queue-kafka   ← committed events fanning out (partitioned by aggregate_id)
      │
      ├──▶ Event store consumer     → Cassandra (+ snapshot writes)
      ├──▶ Projection consumer      → YugabyteDB read models
      └──▶ canon-publisher-kafka    → canon.{service}.events → other services
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
- **Outbox processor single responsibility**: the outbox processor drains the YugabyteDB outbox table and publishes to the outbound queue. It does NOT write to Cassandra, does NOT trigger projections, does NOT publish externally.
- **No direct Cassandra writes from command handler**: the command handler writes events to the outbox only. The event store consumer on the outbound queue writes to Cassandra.
- **Snapshot writes owned by event store consumer**: after a confirmed Cassandra write, the event store consumer checks `version % 50 == 0` and writes a snapshot to YugabyteDB. Snapshots are NEVER written by the command handler or outbox processor.
- **Outbox pattern**: events are written to the outbox within a YugabyteDB ACID transaction alongside the command write. The outbox is the durable commit point for all events.
- **Idempotency**: all event handlers and projections must be safe to call twice with the same input.
- **Optimistic concurrency**: the event store must reject writes where the expected version does not match the stored version.
- **Macro-driven traits**: users never implement `Aggregate`, `CommandHandler`, `EventHandler`, or `Projection` traits directly. The macros generate all trait impls.
- **Command/event exhaustiveness**: every `#[command]` must have a `#[command_handler]`. Every `#[event]` must have an `#[event_combiner]`. Missing either is a compile error.
- **Event handlers are aggregate-agnostic**: `#[event_handler]` has no aggregate type parameter — event handlers are not tied to a specific aggregate.

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
├── canon-command-store-yugabyte/      ← YugabyteDB impl
├── canon-snapshot-store/              ← SnapshotStore trait
├── canon-snapshot-store-yugabyte/     ← YugabyteDB impl
├── canon-inbox/                       ← Inbox trait
├── canon-inbox-yugabyte/              ← YugabyteDB impl
├── canon-inbound-queue/               ← InboundQueue trait
├── canon-inbound-queue-kafka/         ← Kafka impl
├── canon-outbound-queue/              ← OutboundQueue trait
├── canon-outbound-queue-kafka/        ← Kafka impl
├── canon-projection-store/            ← ProjectionStore trait
├── canon-projection-store-yugabyte/   ← YugabyteDB impl
├── canon-publisher/                   ← EventPublisher trait
├── canon-publisher-kafka/             ← Kafka impl
├── canon-adaptor/                     ← EventAdaptor trait (inbound from other services)
├── canon-adaptor-kafka/               ← Kafka impl
├── canon-deadletter/                  ← DeadLetterStore trait
├── canon-deadletter-yugabyte/         ← YugabyteDB impl
├── canon-test/                        ← integration test harness (in-memory only)
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
    │       └── canon-command-store-yugabyte
    ├── canon-snapshot-store
    │       └── canon-snapshot-store-yugabyte
    ├── canon-inbox
    │       └── canon-inbox-yugabyte
    ├── canon-inbound-queue
    │       └── canon-inbound-queue-kafka
    ├── canon-outbound-queue
    │       └── canon-outbound-queue-kafka
    ├── canon-projection-store
    │       └── canon-projection-store-yugabyte
    ├── canon-publisher
    │       └── canon-publisher-kafka
    ├── canon-adaptor
    │       └── canon-adaptor-kafka
    └── canon-deadletter
            └── canon-deadletter-yugabyte
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
- `aggregate.rs` — `Aggregate` (no `handle`, no `apply` — those are macro-generated)
- `command_handler.rs` — `CommandHandler`
- `event_handler.rs` — `EventHandler`
- `projection.rs` — `Projection`
- `replay.rs` — `CounterfactualReplay`

### Phase 3b — canon-core proc-macros
Implement all eight proc-macros in `canon-core`. Each macro must compile cleanly and generate correct trait impls before Phase 4 begins. Order: `#[aggregate]` → `#[command]` + `#[event]` → `#[event_combiner]` → `#[command_handler]` → `#[event_handler]` → `#[projection]` → `#[projection_handler]`.

### Phase 4 — canon-core in-memory implementations
Implement in-memory versions of every infrastructure trait in `canon-core/src/memory/`. These are used in tests. They must be functionally correct, not just stub implementations. In-memory impls must work with the macro-generated dispatch from Phase 3b.
- `InMemoryEventStore`
- `InMemoryCommandStore`
- `InMemorySnapshotStore`
- `InMemoryInbox`
- `InMemoryInboundQueue`
- `InMemoryOutboundQueue`
- `InMemoryProjectionStore`
- `InMemoryPublisher`
- `InMemoryAdaptor`
- `InMemoryDeadLetterStore`

### Phase 5 — canon-test integration test harness
Write tests in the `canon-test` crate using only in-memory implementations. The `TestHarness` wires all in-memory impls together. Tests exercise the full pipeline — submit a command and assert resulting events, handler outputs, projection state, and outbox contents — all without external infrastructure. Test modules per feature:
- Snapshotting — every N events via event store consumer
- Oversight — NotReady accumulation, Discard, Ready dispatch
- Counterfactual replay — command substitution and diff
- Dead lettering — max retries exceeded
- Projection rebuild — rebuilding flag, read-through fallback, offset reset
- Inbox window expiry — TTL exceeded → dead letter
- Idempotency — duplicate command, duplicate event, duplicate window
- Outbound queue fan-out — all three consumers receive event independently

### Phase 6 — trait crates
Implement the thin trait crates. Each contains only the trait definition and associated types — no logic. They re-export the relevant types from `canon-core`.

### Phase 7 — infrastructure crates (one at a time)
Implement each infrastructure crate in this order. Verify each compiles and its integration tests pass before starting the next.

1. `canon-inbox-yugabyte` — most complex, do this first
2. `canon-inbound-queue-kafka`
3. `canon-outbound-queue-kafka`
4. `canon-command-store-yugabyte`
5. `canon-snapshot-store-yugabyte`
6. `canon-event-store-cassandra`
7. `canon-projection-store-yugabyte`
8. `canon-deadletter-yugabyte`
9. `canon-publisher-kafka`
10. `canon-adaptor-kafka`

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
pub trait Aggregate: Sized + Send + Sync + 'static {
    type State: Default + Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    fn upcast(raw: EventEnvelope) -> Result<Box<dyn Any>, Self::Error>;

    fn hydrate(state: &mut Self::State, events: impl Iterator<Item = Box<dyn Any>>) {
        // generated by #[aggregate] macro — dispatches to registered event combiners
    }
}

// canon-core/src/traits/command_handler.rs
#[async_trait::async_trait]
pub trait CommandHandler<A: Aggregate>: Send + Sync + 'static {
    type Command: Send + Sync;
    type Event: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle(
        &self,
        state: &A::State,
        command: Self::Command,
    ) -> Result<Vec<Self::Event>, Self::Error>;
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

## Macro surface (canonical definitions — do not modify)

These are the only macros users interact with. Each is defined in `canon-core`. Users never implement `Aggregate`, `CommandHandler`, `EventHandler`, or `Projection` traits directly — the macros generate all trait impls.

### `#[aggregate(snapshot_every = N)]`

Registers a struct as an aggregate. `snapshot_every` is optional — omitting it disables snapshotting for this aggregate. The macro generates:
- `impl Aggregate` with hydration dispatch table across all registered `#[event_combiner]` impls for this aggregate
- `impl Default` for state if not already present
- Serialisation derives

```rust
#[aggregate(snapshot_every = 50)]
pub struct Ship {
    status: ShipStatus,
    fuel_level: f32,
}
```

### `#[command(AggregateName)]`

Registers a struct as a command belonging to the named aggregate. The macro generates serialisation derives and registers the command type. Every `#[command]` must have a corresponding `#[command_handler]` — missing one is a compile error enforced via generated trait bounds.

```rust
#[command(Ship)]
pub struct DepartForStation {
    pub destination: StationId,
}
```

### `#[event(AggregateName)]`

Registers a struct as an event belonging to the named aggregate. The macro generates serialisation derives and registers the event type. Every `#[event]` must have a corresponding `#[event_combiner]` — missing one is a compile error enforced via generated trait bounds.

```rust
#[event(Ship)]
pub struct ShipDeparted {
    pub destination: StationId,
    pub fuel_at_departure: f32,
}
```

### `#[event_combiner(AggregateName)]`

Defines how an event folds into aggregate state. One impl block per event type. The `combine` method takes `&self` (the event) and `&mut AggregateName` (the state). This is called during hydration — it is synchronous, pure, and has no side effects.

```rust
#[event_combiner(Ship)]
impl ShipDeparted {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::InFlight;
        state.fuel_level -= self.fuel_at_departure * 0.1;
    }
}
```

### `#[command_handler(AggregateName)]`

One impl block per command type. The `handle` method receives the current aggregate state and the command, and returns a `Result<Vec<EventType>>`. The error type must be declared as an associated item.

```rust
#[command_handler(Ship)]
impl DepartForStationHandler {
    type Error = FleetError;

    fn handle(&self, state: &Ship, cmd: DepartForStation) -> Result<Vec<ShipDeparted>, FleetError> {
        if state.status != ShipStatus::Docked {
            return Err(FleetError::ShipNotDocked);
        }
        Ok(vec![ShipDeparted {
            destination: cmd.destination,
            fuel_at_departure: state.fuel_level,
        }])
    }
}
```

### `#[event_handler]`

Downstream reactive logic. No aggregate type parameter — event handlers are not tied to a specific aggregate. Optional — not every event needs a handler. One impl block per event type. The `handle` method receives a batch of events and optionally produces a `CommandEnvelope` to submit back through the inbox via `InboxPort`.

Optionally defines `oversight` to control inbox window dispatch readiness. Supports `window_ttl` attribute for inbox window expiry — e.g. `#[event_handler(window_ttl = "30m")]`.

```rust
#[event_handler]
impl ShipArrivedAtStationHandler {
    fn handle(&self, events: Vec<ShipArrivedAtStation>) -> Option<CommandEnvelope> {
        // optionally produce a command
    }

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        Oversight::Ready
    }
}
```

### `#[projection]`

Registers a struct as a projection (read model). The macro generates `impl Projection` scaffolding and a `projection_id` derived from the struct name.

```rust
#[projection]
pub struct StationInventory {
    pub station_id: StationId,
    pub stock_levels: HashMap<CargoType, u32>,
}
```

### `#[projection_handler(ProjectionName)]`

Defines how an event updates a projection's read model. Optional — projections only need handlers for the event types they care about. The `apply` method receives the event and a mutable reference to the projection store.

```rust
#[projection_handler(StationInventory)]
impl CargoReceivedHandler {
    fn apply(&self, event: &CargoReceived, store: &mut StationInventory) {
        *store.stock_levels.entry(event.cargo_type).or_insert(0) += event.quantity;
    }
}
```

### Compile-time enforcement rules

- Every `#[command(X)]` → must have exactly one `#[command_handler(X)]` — compile error if missing
- Every `#[event(X)]` → must have exactly one `#[event_combiner(X)]` — compile error if missing
- `#[event_handler]` — optional, no exhaustiveness requirement
- `#[projection_handler]` — optional, no exhaustiveness requirement

Enforcement mechanism: `#[command(X)]` and `#[event(X)]` generate marker traits. `ServiceBuilder` is generic over a type that must implement all marker traits, making missing handlers a type error at the `ServiceBuilder` call site.

---

## Kafka topics

| Topic | Crate | Direction | Carries |
|---|---|---|---|
| `canon.{service}.inbound` | canon-inbound-queue-kafka | internal | `IncomingMessage` — assembled batches from inbox to handlers |
| `canon.{service}.outbound` | canon-outbound-queue-kafka | internal | `EventEnvelope` — committed events to event store, projections, publisher |
| `canon.{service}.events` | canon-publisher-kafka | external outbound | domain events to other services |
| (consumed from other services) | canon-adaptor-kafka | external inbound | domain events from other services |

All topics partitioned by `aggregate_id`.

---

## Storage backends

| Store | Backend | Notes |
|---|---|---|
| Event store | Cassandra | Append-only, one partition per aggregate ID |
| Command store | YugabyteDB | Queryable by aggregate ID + version range |
| Inbox | YugabyteDB | Composite unique key: handler_id + message_id |
| Snapshot store | YugabyteDB | One row per aggregate ID, latest version only |
| Projection store | YugabyteDB | Checkpoint per projection_id, rebuilding flag |
| Dead letter store | YugabyteDB | Inspectable, requeueable |
| Outbox | YugabyteDB | Sequence-numbered event staging, drained by outbox processor |
| Inbound queue | Kafka | Assembled batches from inbox to handlers, partitioned by aggregate_id |
| Outbound queue | Kafka | Committed events fanning out to event store, projections, publisher |

### Environment variables (all services)

```
CASSANDRA_NODES=cassandra:9042
YUGABYTE_URL=yugabyte://canon:canon@yugabyte:5433/canon
KAFKA_BROKERS=kafka:9092
```

### YugabyteDB schemas

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
    window_id       UUID NOT NULL DEFAULT gen_random_uuid(),
    messages        JSONB NOT NULL DEFAULT '[]',
    status          TEXT NOT NULL DEFAULT 'pending',
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (handler_id, aggregate_id)
);

CREATE TABLE processed_windows (
    window_id       UUID PRIMARY KEY,
    handler_id      TEXT NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
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

**outbox**
```sql
CREATE SEQUENCE outbox_seq;

CREATE TABLE outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sequence_number BIGINT NOT NULL DEFAULT nextval('outbox_seq'),
    aggregate_id    UUID NOT NULL,
    payload         BYTEA NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at    TIMESTAMPTZ
);
CREATE INDEX outbox_seq_idx ON outbox (sequence_number) WHERE delivered_at IS NULL;
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
    rebuilding      BOOLEAN NOT NULL DEFAULT false,
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

**retry attempts**
```sql
CREATE TABLE retry_attempts (
    message_id      UUID PRIMARY KEY,
    handler_id      TEXT NOT NULL,
    attempts        INT NOT NULL DEFAULT 0,
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

## Dispatcher

The dispatcher sits on the consumer side of the inbound queue and is part of the `canon-core` `Service` orchestrator — not part of `canon-inbound-queue-kafka`.

It routes `IncomingMessage` by type:
- `Command` → command handler
- `InternalEvent` → registered internal event handlers
- `ExternalEvent` → registered external event handlers

Handler registration happens at `ServiceBuilder` construction time.

---

## Command handler write path

After handling a command, within a single YugabyteDB ACID transaction:

```sql
BEGIN
  INSERT INTO commands (...)     -- audit trail, direct write, not outbox
  INSERT INTO outbox (...) x N   -- one row per event produced
COMMIT
```

Commands are written directly to the command store — not via outbox. Events are written to the outbox — never directly to Cassandra. The outbox is the durable commit point for all events.

---

## Outbox processor

Single responsibility: drain YugabyteDB outbox table and publish events to the outbound queue.

- Owned by `Service` orchestrator in `canon-core`, non-optional tokio background task spawned by `ServiceBuilder`
- Uses `SELECT ... FOR UPDATE SKIP LOCKED` to prevent double-processing across replicas:
```sql
SELECT id, sequence_number, aggregate_id, payload
FROM outbox
WHERE delivered_at IS NULL
ORDER BY sequence_number
LIMIT 100
FOR UPDATE SKIP LOCKED;
```
- Sets `delivered_at` after confirmed Kafka publish
- Does NOT write to Cassandra
- Does NOT trigger projections
- Does NOT publish to external Kafka topics
- Bounded tokio channel between command handler and outbox processor for backpressure — channel capacity configurable via `ServiceBuilder`, default 1024

---

## Outbound queue consumers

Three independent Kafka consumer groups consume from `canon.{service}.outbound`:

### Event store consumer
- Writes `EventEnvelope` to Cassandra event store
- After confirmed Cassandra write, checks `version % 50 == 0` — writes snapshot to YugabyteDB snapshot store if true
- Snapshot is NEVER written by the command handler or outbox processor
- On Cassandra version conflict: reload, retry up to configured max (default 3), persist retry count in `retry_attempts`, dead letter after max failures

### Projection consumer
- Applies events to YugabyteDB read models via registered `Projection` implementations
- Updates `projection_checkpoints.last_version` after each successful apply
- Each projection runs in its own tokio task
- While `projection_checkpoints.rebuilding == true`, read endpoints fall back to read-through against event store
- Projection rebuild: reset consumer group offset on `canon.{service}.outbound` to target checkpoint — Kafka replays in order, no custom rebuild logic against event store needed

### Publisher consumer
- Publishes events to `canon.{service}.events` external Kafka topic via `canon-publisher-kafka`
- Other services consume via `canon-adaptor-kafka` → their inbox

All three consumers fail and recover independently. All three registered via `ServiceBuilder`.

---

## InboxPort — local re-entry only

Event handlers that produce a `CommandEnvelope` submit via `InboxPort` trait:
- Local re-entry only — submits directly to the local inbox
- Cross-service commands do not exist as a framework concept — cross-service is REST only
- `InboxPort` defined in `canon-core`, injected into event handlers via `ServiceBuilder`

---

## Idempotency — window_id batch key

- `inbox_windows.window_id` assigned at window creation, travels with assembled batch onto inbound queue
- Inbound queue consumer inserts `window_id` into `processed_windows` before processing — `INSERT ... ON CONFLICT DO NOTHING`
- If insert is a no-op, skip batch and commit Kafka offset — already processed
- Closes the Kafka rebalance duplicate processing window

---

## Inbox window expiry

- `inbox_windows.expires_at` set at window creation with configurable TTL
- `inbox_windows.status` tracks lifecycle: `pending | dispatched | expired | dead_lettered`
- `Service` orchestrator spawns a cleanup background task via `ServiceBuilder`
- Cleanup task scans for expired windows — sets status `expired`, moves to dead letter store with reason `window_expired`
- TTL configurable per handler via `#[event_handler(window_ttl = "...")]` attribute

---

## Retry and dead letter handling

- Event store consumer retries on Cassandra version conflict — up to 3 times, retry count in `retry_attempts`
- After max failures: write to dead letter store, remove from retry_attempts
- Dead letter requeue is manual only — via gateway admin API
- Requeue re-inserts messages back into `inbox_windows` with fresh `expires_at` and status `pending` — oversight runs again from scratch, not bypassed
- Original `message_id` values preserved — inbox idempotency deduplicates naturally

---

## Counterfactual replay

The counterfactual replay engine operates on commands not events:
- Reads command history from command store for the aggregate
- Events used only to hydrate aggregate state up to the branch point
- Substituted command re-run through command handler chain forward from branch point
- Diff at command level — `CommandDiff` captures divergence in intent
- Dedicated `ReplayEventStore` port in `canon-core` — points at Cassandra read replica, separate from live `EventStore`
- `ReplayEventStore` injected via `ServiceBuilder` independently of live `EventStore`

---

## Projection rebuild strategy

- `projection_checkpoints.rebuilding` set to `true` at rebuild start
- While rebuilding, read endpoints fall back to read-through against event store
- Rebuild by resetting projection consumer group offset on `canon.{service}.outbound` to target checkpoint — Kafka replays in order
- Once rebuild completes, set `rebuilding = false`
- `rebuild_from` checkpoint — reset to last known good version, not necessarily beginning of topic

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

### Fleet-service macro usage example

```rust
// Aggregate definition
#[aggregate(snapshot_every = 50)]
pub struct Ship {
    status: ShipStatus,
    fuel_level: f32,
    assigned_route: Option<uuid::Uuid>,
}

// Commands
#[command(Ship)]
pub struct DepartForStation { pub destination: StationId }

// Events
#[event(Ship)]
pub struct ShipDeparted { pub destination: StationId, pub fuel_at_departure: f32 }

// Event combiner — state folding
#[event_combiner(Ship)]
impl ShipDeparted {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::InFlight;
        state.fuel_level -= self.fuel_at_departure * 0.1;
    }
}

// Command handler
#[command_handler(Ship)]
impl DepartForStationHandler {
    type Error = FleetError;

    fn handle(&self, state: &Ship, cmd: DepartForStation) -> Result<Vec<ShipDeparted>, FleetError> {
        if state.status != ShipStatus::Docked {
            return Err(FleetError::ShipNotDocked);
        }
        Ok(vec![ShipDeparted {
            destination: cmd.destination,
            fuel_at_departure: state.fuel_level,
        }])
    }
}

// Event handler — cross-service reactive logic (no aggregate parameter)
#[event_handler]
impl ResupplyHandler {
    fn handle(&self, events: Vec<ResupplyDispatched>) -> Option<CommandEnvelope> {
        // produce ScheduleResupply command
        todo!()
    }
}
```

### Key oversight example (cargo-service UnloadingHandler)

Uses `#[event_handler]` (no aggregate type parameter) with an `oversight` method:

```rust
#[event_handler]
impl UnloadingHandler {
    fn handle(&self, events: Vec<CargoEvent>) -> Option<CommandEnvelope> {
        // Find ManifestCreated in the batch, build BeginUnloading command
        todo!()
    }

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
}
```

### Snapshot strategy

Fleet service snapshots every 50 events per ship aggregate — configured via `#[aggregate(snapshot_every = 50)]`. Implemented by the **event store consumer** on the outbound queue — after a confirmed Cassandra write, the consumer checks if `version % 50 == 0` and writes a snapshot to YugabyteDB if so.

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
