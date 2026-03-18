# Canon — system design

Canon is a Rust event sourcing framework built around a multi-stage message processing pipeline. It provides opinionated, production-ready primitives for building event-sourced services with strong durability guarantees, pluggable infrastructure, and a clean hexagonal architecture.

Its an experiment to see how far I can take AI - can it generate an entire framework?

---

## Core principles

- **Hexagonal architecture** — every infrastructure concern is behind a trait. Swap the crate, keep the domain.
- **Append-only truth** — the event store is the source of truth. Everything else is derived.
- **Crash safety** — all durable state survives process death. The outbox is the commit point; the outbound queue is the delivery mechanism.
- **Testability** — in-memory implementations of every port ship in `canon-core`. A dedicated `canon-test` crate provides a `TestHarness` for framework integration tests with zero external infrastructure.
- **Macro-driven ergonomics** — proc-macros are distributed throughout the crates that own the concepts they augment. No separate macros crate.

---

## Message processing pipeline

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

A single command produces one or more events. Events are staged in the outbox within a YugabyteDB transaction, then drained to the outbound queue by the outbox processor. Three independent consumers handle event persistence, projection updates, and cross-service publishing. An event can have multiple handlers. Each handler produces at most one command. Projections produce nothing — they only write to read models.

---

## Aggregate

An aggregate rebuilds its state by replaying events via `apply()`. Version tracking enables optimistic concurrency — the event store rejects writes where the expected version does not match.

```rust
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

    fn hydrate(
        state: &mut Self::State,
        events: impl Iterator<Item = Self::Event>,
    ) {
        for event in events {
            Self::apply(state, &event);
        }
    }
}
```

Hydration strategy: load snapshot → replay events from snapshot version forward → current state. If no snapshot exists, replay from version zero.

### Upcasting

Event schemas evolve over time. `upcast()` is called transparently during hydration, before `apply()`, to transform raw stored events into the current domain type. The `event_version` field on `EventEnvelope` identifies which schema version a stored event uses.

---

## Event envelope

Every event in the store is wrapped in an envelope carrying full provenance metadata.

```rust
pub struct EventEnvelope {
    pub event_id: Uuid,
    pub aggregate_id: AggregateId,
    pub version: Version,
    pub event_type: String,
    pub event_version: u32,       // used by upcast()
    pub payload: Bytes,
    pub correlation_id: Uuid,     // trace a command through all downstream effects
    pub causation_id: Uuid,       // which command caused this event
    pub timestamp: DateTime<Utc>,
}
```

`correlation_id` threads through an entire causal chain — from the originating command to every downstream command it triggers. `causation_id` identifies the immediate cause.

---

## Command envelope

```rust
pub struct CommandEnvelope {
    pub command_id: Uuid,
    pub aggregate_id: AggregateId,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub payload: Bytes,
}
```

---

## Inbox and oversight

The inbox is the entry point for all incoming messages — commands, internal events, and external events arriving via `canon-adaptor`. It is responsible for:

1. **Idempotent intake** — deduplication via `handler_id + message_id` composite keys, stored in YugabyteDB.
2. **Event assembly** — accumulating messages for a handler until its oversight function signals readiness.
3. **Queue dispatch** — forwarding ready batches (with `window_id`) to the inbound Kafka queue.

Handler registrations are discovered at startup via `#[canon::handler]` macro scanning. The inbox is seeded with the handler manifest so it knows which windows to track.

### Incoming message type

```rust
pub enum IncomingMessage {
    Command(CommandEnvelope),
    InternalEvent(EventEnvelope),
    ExternalEvent(EventEnvelope),
}
```

### Oversight

Each handler declares an oversight function that inspects the accumulated messages for that handler and decides whether the batch is ready to be dispatched.

```rust
pub enum Oversight {
    Ready,
    NotReady,
    Discard,
}
```

`Ready` — dispatch the accumulated batch to the inbound queue immediately.
`NotReady` — wait for more messages.
`Discard` — abandon this accumulation window. Messages are not enqueued.

Oversight is defined as a method on the handler struct. If omitted, the default implementation returns `Ready` on every message.

```rust
#[canon::handler]
impl OrderHandler {
    async fn handle(events: Vec<OrderEvent>) -> Option<Command> {
        // process the ready batch
    }

    fn oversight(events: &[IncomingMessage]) -> Oversight {
        if events.iter().any(|e| matches!(e, IncomingMessage::Command(_))) {
            return Oversight::Discard;
        }
        if events.len() >= 3 {
            Oversight::Ready
        } else {
            Oversight::NotReady
        }
    }
}
```

The `#[canon::handler]` macro inspects the `handle` function signature to infer the event type and dispatch strategy, and generates the trait implementation and handler registration boilerplate. The `window_ttl` attribute configures inbox window expiry — e.g. `#[canon::handler(window_ttl = "30m")]`.

### Inbox window expiry

Windows that never reach `Ready` must not accumulate indefinitely:
- `inbox_windows.expires_at` set at window creation with configurable TTL per handler
- `inbox_windows.status` tracks lifecycle: `pending | dispatched | expired | dead_lettered`
- The `Service` orchestrator spawns a cleanup background task
- Expired windows are moved to the dead letter store with reason `window_expired`

### Batch idempotency — window_id

Each window is assigned a `window_id` at creation time. This ID travels with the batch onto the inbound queue. The consumer inserts the `window_id` into a `processed_windows` table before processing — `INSERT ... ON CONFLICT DO NOTHING`. If the insert is a no-op, the batch was already processed and is skipped. This closes the Kafka rebalance duplicate processing window.

---

## Dispatcher

The dispatcher sits on the consumer side of the inbound queue and is part of the `canon-core` `Service` orchestrator.

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

## Event handlers

Event handlers receive a batch of events and optionally produce a single command. An event can have multiple handlers — fan-out is achieved by registering multiple handlers against the same event type.

Event handlers that produce a `CommandEnvelope` submit via `InboxPort` trait — local re-entry only, submitting directly to the local inbox. Cross-service commands are not a framework concept — cross-service communication is REST only.

```rust
pub trait EventHandler: Send + Sync + 'static {
    type Event: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle(
        &self,
        events: Vec<Self::Event>,
    ) -> Result<Option<CommandEnvelope>, Self::Error>;

    fn oversight(
        &self,
        accumulated: &[IncomingMessage],
    ) -> Oversight {
        Oversight::Ready
    }
}
```

### Dispatch modes

The macro infers the dispatch mode from the handler's `handle` signature:

- `Vec<PaymentProcessed>` — batch of same-type events.
- `Vec<OrderEvent>` — enum dispatch over multiple event variants.
- `Window<OrderEvent>` — correlated window, events grouped by aggregate ID or transaction boundary.

---

## Projections

Projections are event handlers that write to read models. They produce no commands. Processing must be idempotent — applying the same event twice produces the same result.

```rust
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
        events: impl Stream<Item = Self::Event> + Send,
        store: &Self::Store,
    ) -> Result<(), Self::Error>;

    fn projection_id(&self) -> &str;
}
```

`projection_id` is used to track checkpoints in the projection store.

### Projection rebuild

Projections are updated by the **projection consumer** on the outbound queue, not by the command handler directly. Rebuild uses Kafka offset reset:

- `projection_checkpoints.rebuilding` set to `true` at rebuild start
- While `rebuilding == true`, read endpoints fall back to read-through against the event store — never serve stale materialised views
- Rebuild by resetting the projection consumer group offset on `canon.{service}.outbound` to the target checkpoint — Kafka replays in order, no custom rebuild logic against the event store needed
- Once rebuild completes, set `rebuilding = false`
- `rebuild_from` checkpoint — reset to last known good version, not necessarily beginning of topic

### Read-through vs read-ready

- **Read-through** — no persistent read model. State is computed on demand by replaying the event stream. Always consistent, expensive at scale.
- **Read-ready** — a materialised view maintained by `apply()`. Fast reads, eventually consistent. Requires checkpoint tracking and a rebuild path on restart.

Both modes use the same `Projection` trait. The difference is in the `ProjectionStore` implementation and whether `apply()` writes to a persistent store or an in-memory structure.

---

## Counterfactual replay

Counterfactual replay is a first-class feature in `canon-core`. It answers: "what downstream commands would have been produced if a given command had been different?"

The replay engine operates on **commands not events**:
- Reads command history from the command store for the aggregate
- Uses events only to hydrate aggregate state up to the branch point
- Substitutes the specified command, re-runs the command handler chain forward from the branch point
- Diffs at the command level — `CommandDiff` captures divergence in intent

A dedicated `ReplayEventStore` port in `canon-core` points at a Cassandra read replica, separate from the live `EventStore`. It is injected via `ServiceBuilder` independently.

```rust
pub struct CounterfactualRequest {
    pub aggregate_id: AggregateId,
    pub branch_version: Version,
    pub substituted_command: CommandEnvelope,
}

pub struct CounterfactualResult {
    pub original_commands: Vec<CommandEnvelope>,
    pub counterfactual_commands: Vec<CommandEnvelope>,
    pub diff: CommandDiff,
}

pub struct CommandDiff {
    pub added: Vec<CommandEnvelope>,
    pub removed: Vec<CommandEnvelope>,
    pub unchanged: Vec<CommandEnvelope>,
}
```

The diff is at the command level — it captures divergence in *intent* rather than raw event data. The event store only requires range reads by aggregate ID and version to support this; no stream forking or persistent branching is required.

---

## Snapshotting

Snapshotting is optional but important at scale. Without snapshots, hydrating an aggregate requires replaying its full event history on every load.

The hydration strategy with snapshots:

1. Load the most recent snapshot for the aggregate (if any).
2. Load events from the snapshot version forward.
3. Apply events via `apply()` to reach current state.

If no snapshot exists, all events are replayed from version zero. The snapshot store is a separate port from the event store, allowing independent scaling and retention policies.

Snapshots are written by the **event store consumer** on the outbound queue — after a confirmed Cassandra write, the consumer checks if `version % 50 == 0` and writes a snapshot to YugabyteDB if true. Snapshots are NEVER written by the command handler or outbox processor.

---

## Dual-write safety

Persisting events and notifying subscribers are two separate operations. Canon uses an outbox pattern where events are staged in a YugabyteDB outbox table within the same ACID transaction as the command write.

The **outbox processor** — a dedicated tokio background task spawned by `ServiceBuilder` — has a single responsibility: drain the outbox table and publish events to the outbound Kafka queue. It uses `SELECT ... FOR UPDATE SKIP LOCKED` to prevent double-processing across replicas, drains in `sequence_number` order, and sets `delivered_at` after confirmed Kafka publish.

The outbox processor does NOT write to Cassandra, does NOT trigger projections, and does NOT publish to external Kafka topics. Those responsibilities belong to the three independent consumers on the outbound queue.

Three independent consumer groups on the outbound queue handle downstream concerns:
- **Event store consumer** — writes to Cassandra, writes snapshots
- **Projection consumer** — updates YugabyteDB read models
- **Publisher consumer** — publishes to `canon.{service}.events` for other services

Each consumer fails and recovers independently. This provides at-least-once delivery guarantees. Consumers must be idempotent.

---

## Retry and dead letter handling

Retry count is persisted in a `retry_attempts` table (YugabyteDB) to survive process crashes.

- Event store consumer retries on Cassandra version conflict — up to configured max (default 3), retry count in `retry_attempts`
- After max failures: write to dead letter store, remove from retry_attempts
- Dead letter requeue is manual only — via gateway admin API
- Requeue re-inserts messages back into `inbox_windows` with fresh `expires_at` and status `pending` — oversight runs again from scratch, not bypassed
- Original `message_id` values preserved — inbox idempotency deduplicates naturally

The `canon-deadletter` trait provides a port for inspecting, requeueing, or discarding dead-lettered messages programmatically.

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

## Crate layout

### Foundation

| Crate | Contents |
|---|---|
| `canon-core` | Domain traits, `Service` orchestrator, dispatcher, outbox processor, replay engine, in-memory implementations, proc-macros |
| `canon-test` | Integration test harness using in-memory implementations, zero external infrastructure |

### Trait crates

| Crate | Port |
|---|---|
| `canon-event-store` | `EventStore` trait |
| `canon-command-store` | `CommandStore` trait |
| `canon-snapshot-store` | `SnapshotStore` trait |
| `canon-inbox` | `Inbox` trait |
| `canon-inbound-queue` | `InboundQueue` trait |
| `canon-outbound-queue` | `OutboundQueue` trait |
| `canon-projection-store` | `ProjectionStore` trait |
| `canon-publisher` | `EventPublisher` trait — outbound to other services |
| `canon-adaptor` | `EventAdaptor` trait — inbound from other services |
| `canon-deadletter` | `DeadLetterStore` trait |

### Implementations

| Crate | Implements |
|---|---|
| `canon-event-store-cassandra` | `EventStore` over Cassandra |
| `canon-command-store-yugabyte` | `CommandStore` over YugabyteDB |
| `canon-snapshot-store-yugabyte` | `SnapshotStore` over YugabyteDB |
| `canon-inbox-yugabyte` | `Inbox` over YugabyteDB |
| `canon-inbound-queue-kafka` | `InboundQueue` over Kafka |
| `canon-outbound-queue-kafka` | `OutboundQueue` over Kafka |
| `canon-projection-store-yugabyte` | `ProjectionStore` over YugabyteDB |
| `canon-publisher-kafka` | `EventPublisher` over Kafka |
| `canon-adaptor-kafka` | `EventAdaptor` over Kafka |
| `canon-deadletter-yugabyte` | `DeadLetterStore` over YugabyteDB |

### Dependency graph

All implementation crates depend on their trait crate and `canon-core`. No cross-dependencies exist between implementation crates. The graph is a strict DAG with `canon-core` at the root.

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

## Storage strategy

| Concern | Backend | Rationale |
|---|---|---|
| Event store | Cassandra | Append-optimised, high-volume, wide rows per aggregate stream |
| Command store | YugabyteDB | Transactional, queryable, replay by version range |
| Inbox | YugabyteDB | Idempotency requires strong consistency and composite key uniqueness |
| Snapshot store | YugabyteDB | Low-volume, transactional reads |
| Projection store | YugabyteDB | Queryable read models, checkpoint tracking, rebuilding flag |
| Dead letter store | YugabyteDB | Inspectable, requeueable, auditable |
| Outbox | YugabyteDB | Sequence-numbered event staging, drained by outbox processor |
| Retry attempts | YugabyteDB | Crash-safe retry count for event store consumer |
| Inbound queue | Kafka | Assembled batches from inbox to handlers, partitioned by aggregate_id, consumer groups per service |
| Outbound queue | Kafka | Committed events fanning out to event store, projections, publisher — three independent consumer groups |

---

## Runtime and async

Canon targets **tokio** exclusively. All traits are `async_trait` based. There is no sync variant.

---

## Error handling

`thiserror` is used throughout. Each crate defines and owns its own error types. There is no top-level `CanonError` god enum. Errors propagate upward via `Box<dyn std::error::Error>` at crate boundaries where needed.

---

## Monorepo layout

Canon and its demo live in a single Cargo workspace. The demo depends on canon crates via local path dependencies, making it the canonical integration test of the full framework.

```
canon/
├── Cargo.toml                        (workspace root)
├── canon-core/
├── canon-event-store/
├── canon-event-store-cassandra/
├── canon-command-store/
├── canon-command-store-yugabyte/
├── canon-snapshot-store/
├── canon-snapshot-store-yugabyte/
├── canon-inbox/
├── canon-inbox-yugabyte/
├── canon-inbound-queue/
├── canon-inbound-queue-kafka/
├── canon-outbound-queue/
├── canon-outbound-queue-kafka/
├── canon-projection-store/
├── canon-projection-store-yugabyte/
├── canon-publisher/
├── canon-publisher-kafka/
├── canon-adaptor/
├── canon-adaptor-kafka/
├── canon-deadletter/
├── canon-deadletter-yugabyte/
├── canon-test/
└── canon-demo/
    ├── Cargo.toml                    (demo workspace)
    ├── docker-compose.yml            (full local stack)
    ├── k8s/                          (Kubernetes manifests, one per service)
    ├── shared/                       (shared domain types, events, commands)
    ├── fleet-service/
    ├── cargo-service/
    ├── navigation-service/
    ├── supply-service/
    ├── station-service/
    ├── gateway/
    └── frontend/                     (Leptos WASM)
```

---

## canon-demo — spaceship logistics

`canon-demo` is a spaceship logistics service that demonstrates every Canon capability across five independent domain services communicating via Kafka. Each service is a separate binary deployable as its own Kubernetes pod.

### Purpose

The demo is the canonical integration test of the full framework. It exercises:

- Multiple aggregates with version-tracked state and snapshotting
- Cross-service event flows via `canon-adaptor-kafka` and `canon-publisher-kafka`
- Oversight-gated event assembly (cargo unloading requires both arrival and manifest events)
- Inbox window expiry with configurable TTL per handler
- Batch idempotency via `window_id` and `processed_windows` table
- Projections with read-through and read-ready modes, rebuild via Kafka offset reset
- Retry handling with crash-safe `retry_attempts` table
- Dead letter handling for failed processing with manual requeue via admin API
- Counterfactual replay on command history exposed via the gateway API
- Real-time event streaming to a Leptos WASM frontend over WebSocket

### Domains

#### Fleet service

Spaceships are the core aggregate. A ship has a status, assigned route, fuel level, and cargo capacity.

Commands: `RegisterShip`, `AssignRoute`, `DepartForStation`, `ScheduleResupply`, `DecommissionShip`

Events: `ShipRegistered`, `RouteAssigned`, `ShipDeparted`, `ResupplyScheduled`, `ShipDecommissioned`

Snapshot strategy: snapshot every 50 events per ship aggregate, written by the event store consumer on the outbound queue.

#### Cargo service

Cargo manifests are aggregates scoped to a ship + voyage. Tracks what is loaded, its weight, destination station, and unloading status.

Commands: `CreateManifest`, `LoadCargo`, `BeginUnloading`, `RecordUnloaded`, `CloseManifest`

Events: `ManifestCreated`, `CargoLoaded`, `UnloadingStarted`, `CargoUnloaded`, `ManifestClosed`

Oversight example: the `UnloadingHandler` uses oversight to wait for both a `ShipArrived` external event (from navigation service) and a `ManifestCreated` internal event before dispatching `BeginUnloading`. Until both arrive, oversight returns `NotReady`. If a `ShipDecommissioned` event arrives in the window, oversight returns `Discard`.

#### Navigation service

Routes and waypoints are aggregates. Tracks planned waypoints, current position, arrival and departure timestamps.

Commands: `PlanRoute`, `RecordDeparture`, `UpdatePosition`, `RecordArrival`

Events: `RoutePlanned`, `ShipDeparted`, `PositionUpdated`, `ShipArrivedAtStation`

`ShipArrivedAtStation` is published externally via `canon-publisher-kafka` and consumed by cargo service and station service via `canon-adaptor-kafka`. This is the primary cross-domain trigger.

#### Supply service

Fuel and parts inventory per depot. Tracks stock levels and schedules resupply missions.

Commands: `RecordStock`, `RequestResupply`, `DispatchResupply`, `ConfirmDelivery`

Events: `StockRecorded`, `ResupplyRequested`, `ResupplyDispatched`, `DeliveryConfirmed`

Consumes `StationStockLow` events from station service. Emits `ResupplyScheduled` commands back to fleet service via the internal command path.

#### Station service

Stations (depots) are aggregates. Tracks cumulative received cargo by type, current capacity, docked ships, and unloading history per voyage.

Commands: `RegisterStation`, `RecordDocking`, `RecordCargoReceived`, `UpdateCapacity`

Events: `StationRegistered`, `ShipDocked`, `CargoReceived`, `StationStockLow`, `CapacityUpdated`

`StationStockLow` is published externally to supply service. `CargoReceived` feeds the station inventory projection — the primary read-ready read model in the demo.

### Cross-domain event flows

```
1. Fleet:      ShipDeparted
                   │
                   ▼ (via Kafka)
   Navigation: RecordDeparture → ShipDeparted (internal)
                   │
                   ▼ (position updates over voyage)
   Navigation: ShipArrivedAtStation
                   │
          ┌────────┴────────┐
          ▼ (via Kafka)     ▼ (via Kafka)
   Cargo: UnloadingHandler  Station: RecordDocking
          (oversight: waits for ShipArrived + ManifestCreated)
          │
          ▼
   Cargo: BeginUnloading → CargoUnloaded (loop per item)
          │
          ▼ (via Kafka)
   Station: RecordCargoReceived → CargoReceived
          │
          ▼ (if stock threshold crossed)
   Station: StationStockLow
          │
          ▼ (via Kafka)
   Supply: RequestResupply → ResupplyDispatched
          │
          ▼ (via Kafka)
   Fleet:  ScheduleResupply → ResupplyScheduled
```

This chain exercises the full framework end to end: inbox assembly with oversight, cross-service adaptor/publisher, fan-out to multiple handlers, projection updates, and a command chain that spans five services.

### Shared crate

`canon-demo/shared` defines all domain event and command enums shared across services and the gateway. All services and the gateway depend on this crate. It contains no logic — only types, serialisation derives, and the Kafka topic name constants.

```rust
// shared/src/events.rs
pub enum FleetEvent { ShipRegistered(ShipRegistered), ShipDeparted(ShipDeparted), ... }
pub enum CargoEvent { ManifestCreated(ManifestCreated), CargoUnloaded(CargoUnloaded), ... }
pub enum NavigationEvent { ShipArrivedAtStation(ShipArrivedAtStation), ... }
pub enum SupplyEvent { ResupplyDispatched(ResupplyDispatched), ... }
pub enum StationEvent { CargoReceived(CargoReceived), StationStockLow(StationStockLow), ... }

pub enum DemoEvent {
    Fleet(FleetEvent),
    Cargo(CargoEvent),
    Navigation(NavigationEvent),
    Supply(SupplyEvent),
    Station(StationEvent),
}
```

### Gateway service

The gateway is an axum HTTP server. It is the only service the frontend talks to.

**REST endpoints (commands in):**

```
POST /fleet/ships                  → RegisterShip
POST /fleet/ships/:id/route        → AssignRoute
POST /fleet/ships/:id/depart       → DepartForStation
POST /cargo/manifests              → CreateManifest
POST /cargo/manifests/:id/load     → LoadCargo
POST /navigation/routes            → PlanRoute
POST /supply/resupply              → RequestResupply
POST /stations/:id/register        → RegisterStation
GET  /replay/counterfactual        → CounterfactualReplay
```

**WebSocket endpoint (events out):**

```
WS /events
```

The gateway subscribes to all Kafka topics via `canon-adaptor-kafka`. On each incoming event it deserialises to `DemoEvent` and broadcasts to all connected WebSocket clients as JSON. The frontend maintains a persistent WebSocket connection and updates its reactive Leptos signals on each received event.

**Read model endpoints (projections):**

```
GET /stations/:id/inventory        → station inventory projection (read-ready)
GET /ships/:id/history             → ship event history (read-through)
GET /cargo/manifests/:id           → manifest state (read-through)
```

### Frontend — Leptos WASM

The frontend is a single-page Leptos application compiled to WASM. It connects to the gateway over WebSocket on load and maintains a reactive signal store that mirrors the live system state.

**Views:**

- **Fleet map** — all ships, current status, assigned routes, fuel levels.
- **Station depots** — inventory levels per station, recent dockings, stock alerts.
- **Cargo tracker** — active manifests, unloading progress per voyage.
- **Supply chain** — pending resupply requests, dispatched missions.
- **Event log** — live scrolling feed of all `DemoEvent` instances as they arrive over WebSocket, with correlation ID highlighting to trace causal chains.
- **Counterfactual explorer** — select a ship, pick a historical command version, substitute a different command, and see the diff of downstream commands that would have been produced.

### Kubernetes deployment

Each service is a separate pod. The demo ships with Kubernetes manifests in `canon-demo/k8s/`.

| Pod | Image | Replicas |
|---|---|---|
| `fleet-service` | `canon-demo/fleet-service` | 2 |
| `cargo-service` | `canon-demo/cargo-service` | 2 |
| `navigation-service` | `canon-demo/navigation-service` | 2 |
| `supply-service` | `canon-demo/supply-service` | 1 |
| `station-service` | `canon-demo/station-service` | 2 |
| `gateway` | `canon-demo/gateway` | 2 |
| `frontend` | `canon-demo/frontend` | 2 |

Infrastructure (Cassandra, YugabyteDB, Kafka) is expected to be provided by the cluster operator. Connection strings are injected via environment variables. A `docker-compose.yml` is provided for local development that brings up all infrastructure and all services together.

### Canon capabilities exercised

| Capability | Demonstrated by |
|---|---|
| Aggregate hydration + `apply()` | All domain services |
| Optimistic concurrency | Fleet ship aggregate version checks |
| Snapshotting | Fleet service, every 50 events via event store consumer |
| Upcasting | Cargo manifest v1 → v2 schema migration |
| Inbox idempotency | All services, duplicate Kafka message delivery |
| Batch idempotency | window_id + processed_windows table |
| Inbox window expiry | Configurable TTL per handler, cleanup to dead letter |
| Oversight — `NotReady` | Cargo unloading waits for arrival + manifest |
| Oversight — `Discard` | Unloading window discarded on decommission event |
| Event handler fan-out | `ShipArrivedAtStation` → cargo + station handlers |
| Cross-service publish | Navigation → Cargo, Station via Kafka |
| Cross-service consume | Supply ← Station, Fleet ← Supply via Kafka |
| Projection — read-ready | Station inventory materialised view |
| Projection — read-through | Ship history, cargo manifest state |
| Projection rebuild | Station inventory rebuild via Kafka offset reset, rebuilding flag |
| Outbox staging | Events staged in YugabyteDB, drained by outbox processor to outbound queue |
| Independent consumers | Event store, projection, publisher consumers fail independently |
| Retry handling | Crash-safe retry_attempts table, configurable max retries |
| Dead letter handling | Failed processing routed to dead letter store, manual requeue via admin API |
| Counterfactual replay | Gateway `/replay/counterfactual` — operates on command history, not events |
| WebSocket event streaming | Gateway → Leptos frontend |
| Multiple replicas | All services run 2 replicas, Kafka consumer groups ensure ordered per-aggregate processing |

---

## Core type summary

```rust
pub struct AggregateId(Uuid);

pub struct Version(u64);

impl Version {
    pub fn initial() -> Self { Self(0) }
    pub fn next(self) -> Self { Self(self.0 + 1) }
}

pub enum IncomingMessage {
    Command(CommandEnvelope),
    InternalEvent(EventEnvelope),
    ExternalEvent(EventEnvelope),
}

pub enum Oversight {
    Ready,
    NotReady,
    Discard,
}
```
