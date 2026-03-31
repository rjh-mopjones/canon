# Crate Map

Canon is organised as a Cargo workspace with 30+ crates following a strict dependency
DAG. This chapter maps every crate, its purpose, its public API, and the dependency
chains between them.

## Workspace overview

The root `Cargo.toml` declares all members:

```toml
[workspace]
resolver = "2"
members = [
    "canon-core",
    "canon-core/canon-core-macros",
    "canon-test",
    "canon-event-store",
    "canon-event-store-cassandra",
    "canon-command-store",
    "canon-command-store-yugabyte",
    "canon-snapshot-store",
    "canon-snapshot-store-yugabyte",
    "canon-inbox",
    "canon-inbox-yugabyte",
    "canon-inbound-queue",
    "canon-inbound-queue-kafka",
    "canon-projection-store",
    "canon-projection-store-yugabyte",
    "canon-publisher",
    "canon-publisher-kafka",
    "canon-adaptor",
    "canon-adaptor-kafka",
    "canon-deadletter",
    "canon-deadletter-yugabyte",
    "canon-outbound-queue",
    "canon-outbound-queue-kafka",
    "canon-demo/shared",
    "canon-demo/fleet-service",
    "canon-demo/cargo-service",
    "canon-demo/navigation-service",
    "canon-demo/supply-service",
    "canon-demo/station-service",
    "canon-demo/gateway",
    "canon-demo/frontend",
]
```

Workspace-level dependencies ensure version consistency:

```toml
[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
thiserror = "2"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v4", "v5", "serde"] }
bytes = "1"
chrono = { version = "0.4", features = ["serde"] }
futures = "0.3"
tracing = "0.1"
rskafka = "0.5"
base64 = "0.22"
```

---

## Dependency graph

```
canon-core                              <-- the root of everything
    |-- canon-core-macros               (proc-macro subcrate, re-exported)
    |
    |-- canon-event-store               (trait crate)
    |       |-- canon-event-store-cassandra    (impl: scylla driver)
    |
    |-- canon-command-store             (trait crate)
    |       |-- canon-command-store-yugabyte   (impl: sqlx/postgres)
    |
    |-- canon-snapshot-store            (trait crate)
    |       |-- canon-snapshot-store-yugabyte  (impl: sqlx/postgres)
    |
    |-- canon-inbox                     (trait crate)
    |       |-- canon-inbox-yugabyte           (impl: sqlx/postgres)
    |
    |-- canon-inbound-queue             (trait crate)
    |       |-- canon-inbound-queue-kafka      (impl: rskafka)
    |
    |-- canon-outbound-queue            (trait crate)
    |       |-- canon-outbound-queue-kafka     (impl: rskafka)
    |
    |-- canon-projection-store          (trait crate)
    |       |-- canon-projection-store-yugabyte (impl: sqlx/postgres)
    |
    |-- canon-publisher                 (trait crate)
    |       |-- canon-publisher-kafka          (impl: rskafka)
    |
    |-- canon-adaptor                   (trait crate)
    |       |-- canon-adaptor-kafka            (impl: rskafka)
    |
    |-- canon-deadletter                (trait crate)
            |-- canon-deadletter-yugabyte      (impl: sqlx/postgres)

canon-test                              <-- depends on canon-core only
                                            (plus demo crates in dev-dependencies)

canon-demo/
    shared/                             <-- depends on canon-core
    fleet-service/                      <-- depends on shared + canon-core
    cargo-service/                      <-- depends on shared + canon-core
    navigation-service/                 <-- depends on shared + canon-core
    supply-service/                     <-- depends on shared + canon-core
    station-service/                    <-- depends on shared + canon-core
    gateway/                            <-- depends on shared + all impl crates
    frontend/                           <-- depends on shared (types only)
```

---

## The strict DAG rule

Implementation crates depend on **their trait crate + `canon-core` only**. There are
no cross-dependencies between implementation crates. This means:

- `canon-event-store-cassandra` depends on `canon-event-store` and `canon-core`
- `canon-event-store-cassandra` does **not** depend on `canon-inbox-yugabyte`
- `canon-publisher-kafka` does **not** depend on `canon-outbound-queue-kafka`
- `canon-deadletter-yugabyte` depends on `canon-deadletter` and `canon-core`

This strict rule ensures that any implementation can be swapped without affecting
other parts of the system. You could replace the Cassandra event store with a
Postgres event store, and nothing else in the workspace would need to change.

The only crate that depends on multiple implementation crates is the demo `gateway`,
which wires all real implementations together for the production deployment.

---

## Foundation crates

### canon-core

The root of the dependency tree. Every other crate in the workspace depends on
`canon-core` either directly or transitively. It contains:

**Core types:**
- `AggregateId(Uuid)` -- newtype wrapper, never generic
- `Version(u64)` -- monotonic version counter
- `EventEnvelope` -- event metadata + payload
- `CommandEnvelope` -- command metadata + payload
- `IncomingMessage` -- command, internal event, or external event
- `Oversight` -- `Ready`, `NotReady`, `Discard`
- `Snapshot` -- aggregate state at a version
- `WindowStatus` -- `Pending`, `Expired`, `DeadLettered`
- `DeadLetter` -- dead letter read model
- Counterfactual types: `CounterfactualRequest`, `CounterfactualResult`, `CommandDiff`

**Core traits:**
- `Aggregate` -- state hydration via version-matched combiners
- `CommandHandler<A>` -- one per command type per version
- `EventHandler` -- aggregate-agnostic, optional oversight + correlate
- `EventCombiner<A>` -- synchronous state folding
- `Projection` -- read model, idempotent apply
- `ProjectionHandler<P>` -- applies one event type to a projection
- `CounterfactualReplay` -- what-if scenario engine
- `EventStore`, `CommandStore`, `SnapshotStore` -- persistence traits
- `DeadLetterStore`, `RetryTracker` -- dead letter traits
- `Publisher` -- cross-service event publishing

**In-memory implementations** (all 10+ in `src/memory/`):
- `InMemoryEventStore`, `InMemoryCommandStore`, `InMemorySnapshotStore`
- `InMemoryInbox`, `InMemoryInboundQueue`, `InMemoryOutboundQueue`
- `InMemoryProjectionStore`, `InMemoryPublisher`, `InMemoryAdaptor`
- `InMemoryDeadLetterStore`, `InMemoryRetryTracker`, `RetryPolicy`
- `InMemoryOutboxStore`, `InMemoryOutboxPublisher`
- `InMemoryDispatcherStore`, `InMemoryInboxPort`
- `InMemoryReplayEventStore`, `DefaultCounterfactualReplay`

**Service orchestration:**
- `ServiceBuilder` -- auto-discovers registrations via `inventory`, validates exhaustiveness
- `Dispatcher` -- command handler + event handler dispatch
- `OutboxProcessor` -- drains outbox to outbound queue
- `EventStoreConsumer` -- writes events to event store + snapshots
- `ProjectionConsumer` -- applies events to read models
- `PublisherConsumer` -- publishes events to external topic

**Dependencies:**

```toml
[dependencies]
uuid = { version = "1", features = ["v4", "serde"] }
bytes = { version = "1", features = ["serde"] }
chrono = { version = "0.4", features = ["serde"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
async-trait = "0.1"
futures = "0.3"
thiserror = "1"
inventory = "0.3"
tokio = { version = "1", features = ["sync", "time", "rt", "macros"] }
tracing = "0.1"
canon-core-macros = { path = "canon-core-macros" }
```

The `inventory` crate is used for auto-registration of aggregates, command handlers,
event handlers, event combiners, and projections. Macros emit static registrations
that `ServiceBuilder` discovers at runtime.

### canon-core-macros

A `proc-macro = true` subcrate inside `canon-core/canon-core-macros/`. Re-exported
from `canon-core` so users never import it directly. Contains all eight proc-macros:

1. `#[aggregate(snapshot_every = N)]` -- generates `impl Aggregate`, `Default`, serde derives, inventory registration
2. `#[command(Aggregate, version = N, produces = [Events...])]` -- generates command metadata, serde derives
3. `#[event(Aggregate, version = N)]` -- generates event metadata, serde derives
4. `#[event_combiner(Aggregate, version = N)]` -- generates `impl EventCombiner<A>`
5. `#[command_handler(Aggregate, version = N)]` -- generates `impl CommandHandler<A>`
6. `#[event_handler]` -- generates `impl EventHandler`, optional oversight + correlate
7. `#[projection]` -- generates projection metadata
8. `#[projection_handler(ProjectionName)]` -- generates `impl ProjectionHandler<P>`

Compile-time enforcement via marker traits:
- `#[command(X, v=N)]` requires matching `#[command_handler(X, v=N)]`
- `#[event(X, v=N)]` requires matching `#[event_combiner(X, v=N)]`
- `window_ttl` without `oversight` produces a compile error

### canon-test

Integration test harness using all in-memory implementations. Provides:

- `TestHarness` -- wires all in-memory stores, exposes public fields for assertions
- `TestHarnessBuilder` -- validates aggregate registrations at build time
- Test domain (`OrderAggregate`, `PlaceOrder`, `OrderPlaced`, etc.)
- Helper functions for creating test envelopes

**Dependencies:**

```toml
[dependencies]
canon-core = { path = "../canon-core" }
async-trait = "0.1"
thiserror = "1"
uuid = { version = "1", features = ["v4"] }
bytes = "1"
chrono = "0.4"
futures = "0.3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"

[dev-dependencies]
tokio = { version = "1", features = ["full"] }
# Demo service crates for e2e tests
fleet-service = { path = "../canon-demo/fleet-service" }
navigation-service = { path = "../canon-demo/navigation-service" }
supply-service = { path = "../canon-demo/supply-service" }
station-service = { path = "../canon-demo/station-service" }
canon-demo-shared = { path = "../canon-demo/shared" }
# Tier 2: testcontainers
testcontainers = "0.27"
testcontainers-modules = { version = "0.15", features = ["kafka", "postgres", "scylladb"] }
# Infrastructure crates for real store wiring
canon-event-store-cassandra = { path = "../canon-event-store-cassandra" }
canon-command-store-yugabyte = { path = "../canon-command-store-yugabyte" }
canon-snapshot-store-yugabyte = { path = "../canon-snapshot-store-yugabyte" }
canon-projection-store-yugabyte = { path = "../canon-projection-store-yugabyte" }
canon-deadletter-yugabyte = { path = "../canon-deadletter-yugabyte" }
canon-outbound-queue-kafka = { path = "../canon-outbound-queue-kafka" }
canon-publisher-kafka = { path = "../canon-publisher-kafka" }
sqlx = { version = "0.8", features = ["runtime-tokio-rustls", "postgres", "uuid", "chrono", "json"] }
scylla = "0.15"
rskafka = "0.5"
```

The `dev-dependencies` pull in infrastructure crates for Tier 2 testcontainer tests.
These are never compiled in production builds.

---

## Trait crates

Thin crates containing only trait definitions and associated types. Each depends only
on `canon-core` and `thiserror`. They exist so that implementation crates can depend
on just the trait they implement without pulling in other implementations.

| Crate | Trait(s) re-exported | Dependencies |
|-------|---------------------|--------------|
| `canon-event-store` | `EventStore` | `canon-core`, `thiserror` |
| `canon-command-store` | `CommandStore` | `canon-core`, `thiserror` |
| `canon-snapshot-store` | `SnapshotStore` | `canon-core`, `thiserror` |
| `canon-inbox` | `Inbox` | `canon-core`, `thiserror` |
| `canon-inbound-queue` | `InboundQueue` | `canon-core`, `thiserror` |
| `canon-outbound-queue` | `OutboundQueue` | `canon-core`, `thiserror` |
| `canon-projection-store` | `ProjectionStore` | `canon-core`, `thiserror` |
| `canon-publisher` | `Publisher` (EventPublisher) | `canon-core`, `thiserror` |
| `canon-adaptor` | `EventAdaptor` | `canon-core`, `thiserror` |
| `canon-deadletter` | `DeadLetterStore` | `canon-core`, `thiserror` |

A typical trait crate `Cargo.toml`:

```toml
[package]
name = "canon-event-store"
version = "0.1.0"
edition = "2021"

[dependencies]
canon-core = { path = "../canon-core" }
thiserror = { workspace = true }
```

The trait crate pattern keeps the dependency tree flat. An implementation crate like
`canon-event-store-cassandra` depends on `canon-event-store` (trait) + `canon-core`
(types) + `scylla` (driver). It does not depend on any other infrastructure crate.

---

## Implementation crates

### YugabyteDB implementations

All YugabyteDB implementations use `sqlx` with the `postgres` feature (YugabyteDB is
wire-compatible with PostgreSQL). Each crate follows the same pattern:

1. A struct wrapping `PgPool`
2. `new(pool: PgPool)` and `from_env()` constructors
3. `#[async_trait] impl TheTrait for TheStruct`
4. A `thiserror` error type wrapping `sqlx::Error`
5. Testcontainer tests using `testcontainers-modules::postgres::Postgres`

| Crate | Implements | Key SQL operations |
|-------|-----------|-------------------|
| `canon-command-store-yugabyte` | `CommandStore` | `INSERT INTO commands`, `SELECT ... WHERE aggregate_id` |
| `canon-snapshot-store-yugabyte` | `SnapshotStore` | `INSERT ... ON CONFLICT (aggregate_id) DO UPDATE` (upsert) |
| `canon-inbox-yugabyte` | `Inbox` | Composite key dedup, window tracking, oversight evaluation |
| `canon-projection-store-yugabyte` | `ProjectionStore` | Checkpoint tracking, rebuild flag management |
| `canon-deadletter-yugabyte` | `DeadLetterStore` + `RetryTracker` | Dead letter CRUD, crash-safe retry UPSERT |

**Example dependency chain for `canon-deadletter-yugabyte`:**

```toml
[dependencies]
canon-core = { path = "../canon-core" }
canon-deadletter = { path = "../canon-deadletter" }
async-trait = { workspace = true }
thiserror = { workspace = true }
uuid = { workspace = true }
chrono = { workspace = true }
bytes = { workspace = true }
sqlx = { version = "0.8", features = [
    "runtime-tokio-rustls", "postgres", "macros",
    "migrate", "uuid", "chrono"
] }
tracing = { workspace = true }
tokio = { workspace = true }
```

Note the two Canon dependencies: `canon-core` for types and `canon-deadletter` for
the trait. No other Canon crates appear.

### Cassandra implementation

| Crate | Implements | Driver |
|-------|-----------|--------|
| `canon-event-store-cassandra` | `EventStore` | `scylla` 0.15 |

The event store uses Cassandra's wide-row model with `(aggregate_id, version)` as the
composite primary key. Events are appended in version order. Optimistic concurrency
is enforced via lightweight transactions (LWT).

```toml
[dependencies]
canon-core = { path = "../canon-core" }
canon-event-store = { path = "../canon-event-store" }
async-trait = { workspace = true }
thiserror = { workspace = true }
uuid = { workspace = true }
chrono = { workspace = true }
bytes = { workspace = true }
scylla = "0.15"
```

### Kafka implementations

All four Kafka crates use `rskafka` exclusively. No `rdkafka`, no C dependencies,
no CMake. All are pure Rust and cross-compilable to `aarch64-unknown-linux-musl`.

| Crate | Implements | Direction |
|-------|-----------|-----------|
| `canon-inbound-queue-kafka` | `InboundQueue` | Consume from `canon.{service}.inbound` |
| `canon-outbound-queue-kafka` | `OutboundQueue` | Produce to / consume from `canon.{service}.outbound` |
| `canon-publisher-kafka` | `Publisher` | Produce to `canon.{service}.events` |
| `canon-adaptor-kafka` | `EventAdaptor` | Consume from other services' `.events` topics |

They share a consistent `rskafka` usage pattern:

- **Connection:** `ClientBuilder::new(broker_list).build().await`, then
  `client.partition_client(topic, 0, UnknownTopicHandling::Retry)`
- **Produce:** `partition_client.produce(vec![record], Compression::NoCompression)`
- **Consume:** `partition_client.fetch_records(offset, 1..1_048_576, timeout_ms)` in a polling loop
- **Offset tracking:** In-memory `Mutex<i64>`, starts at 0. No Kafka-side offset commit.
- **Consumer groups:** Not used (rskafka has no consumer group abstraction). Each consumer polls partition 0 independently.
- **Errors:** Each crate defines its own `thiserror` type wrapping rskafka errors as strings.

**Example dependency chain for `canon-outbound-queue-kafka`:**

```toml
[dependencies]
canon-core = { path = "../canon-core" }
canon-outbound-queue = { path = "../canon-outbound-queue" }
async-trait = { workspace = true }
thiserror = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
uuid = { workspace = true }
bytes = { workspace = true }
chrono = { workspace = true }
tracing = { workspace = true }
tokio = { workspace = true }
rskafka = { workspace = true }
```

**Example dependency chain for `canon-adaptor-kafka`:**

```toml
[dependencies]
canon-core = { path = "../canon-core" }
canon-adaptor = { path = "../canon-adaptor" }
canon-inbox = { path = "../canon-inbox" }
async-trait = { workspace = true }
thiserror = { workspace = true }
# ... plus serde, uuid, bytes, chrono, rskafka, tokio-stream
```

Note that `canon-adaptor-kafka` also depends on `canon-inbox` because the adaptor
writes incoming external events directly to the inbox for deduplication and windowing.

---

## Demo crates

The `canon-demo/` directory contains a spaceship logistics game demonstrating all
Canon features. Each demo crate has its own `Cargo.toml`.

### canon-demo/shared

Domain types, events, commands, and topic constants shared across all demo services.
No business logic.

**Contains:**
- All command structs (`RegisterShip`, `DepartForStation`, etc.)
- All event structs (`ShipRegistered`, `ShipDeparted`, etc.)
- Aggregate definitions (`Ship`, `Manifest`, `Route`, `Inventory`, `Station`)
- Topic constants (`FLEET_INBOUND`, `FLEET_OUTBOUND`, `FLEET_EVENTS`, etc.)
- Database helpers (`create_service_pool()`)

**Depends on:** `canon-core` only

### Service crates

Each service crate is a binary that runs as a background processor in Kubernetes.
It wires real infrastructure implementations and starts the `Service` via
`ServiceBuilder`.

| Crate | Aggregate | Commands | Events |
|-------|-----------|----------|--------|
| `fleet-service` | Ship | RegisterShip, AssignRoute, DepartForStation, ScheduleResupply, DecommissionShip | ShipRegistered, RouteAssigned, ShipDeparted, ResupplyScheduled, ShipDecommissioned |
| `cargo-service` | Manifest | CreateManifest, LoadCargo, BeginUnloading, RecordUnloaded, CloseManifest | ManifestCreated, CargoLoaded, UnloadingStarted, CargoUnloaded, ManifestClosed |
| `navigation-service` | Route | PlanRoute, RecordDeparture, UpdatePosition, RecordArrival | RoutePlanned, ShipDeparted, PositionUpdated, ShipArrivedAtStation |
| `supply-service` | Inventory | RecordStock, RequestResupply, DispatchResupply, ConfirmDelivery | StockRecorded, ResupplyRequested, ResupplyDispatched, DeliveryConfirmed |
| `station-service` | Station | RegisterStation, RecordDocking, RecordCargoReceived, UpdateCapacity, DrainStock | StationRegistered, ShipDocked, CargoReceived, StationStockLow, CapacityUpdated, StockDrained |

**Dependencies (each service):** `canon-core`, `canon-demo-shared`, all real
infrastructure crates (`canon-event-store-cassandra`, `canon-command-store-yugabyte`,
etc.), `tokio`, `tracing`.

### gateway

The axum REST + WebSocket gateway. Accepts HTTP commands from the frontend, writes
them to the appropriate service's inbound Kafka topic, and broadcasts events over
WebSocket.

**Dependencies:** `axum`, `tower-http`, `tokio`, plus all real infrastructure crates
and `canon-demo-shared`.

### frontend

The Leptos 0.7 CSR WASM application built with Trunk. Communicates with the gateway
via REST and WebSocket. No Canon infrastructure dependencies -- only `canon-demo-shared`
for type definitions.

---

## Other directories

| Path | Purpose |
|------|---------|
| `canon-site/` | Landing page (static HTML/CSS) at `canon.mopjones.com` |
| `canon-site/reference/site-mockup.html` | Visual reference for landing page |
| `canon-docs/` | This documentation site (mdBook) |
| `canon-demo/k8s/` | Kubernetes manifests (kustomize base + overlays) |
| `canon-demo/k8s/base/` | Shared manifests (namespace, infra, jobs, services) |
| `canon-demo/k8s/overlays/minikube/` | Local overlay (imagePullPolicy: Never) |
| `canon-demo/k8s/overlays/gke/` | Production overlay (Ingress, Artifact Registry) |
| `canon-demo/e2e/` | Playwright end-to-end tests |
| `canon-demo/frontend/reference/mockup.html` | Visual reference for game UI |

---

## Storage strategy summary

| Concern | Backend | Why |
|---------|---------|-----|
| Event store | Cassandra | Append-optimised, high-volume, wide rows by aggregate |
| Command store | YugabyteDB | Transactional, queryable, version-range replay |
| Inbox | YugabyteDB | Strong consistency, composite key uniqueness for dedup |
| Snapshot store | YugabyteDB | Low-volume, transactional reads, single row per aggregate |
| Projection store | YugabyteDB | Queryable read models, checkpoint tracking |
| Dead letter store | YugabyteDB | Inspectable, requeueable, auditable |
| Retry attempts | YugabyteDB | Crash-safe retry counters via UPSERT |
| Outbox | YugabyteDB | Sequence-numbered staging, ACID txn with commands |
| Inbound queue | Kafka | Assembled batches, partitioned by aggregate_id |
| Outbound queue | Kafka | Fan-out to 4 consumer groups |
| Published events | Kafka | Cross-service event distribution |
| Adapted events | Kafka | Cross-service event consumption |

---

## How to add a new infrastructure crate

Follow this procedure when adding a new backend for an existing trait (e.g., a Redis
snapshot store).

### Step 1: Create the crate

```bash
cargo init canon-snapshot-store-redis --lib
```

### Step 2: Set up Cargo.toml

```toml
[package]
name = "canon-snapshot-store-redis"
version = "0.1.0"
edition = "2021"

[dependencies]
canon-core = { path = "../canon-core" }
canon-snapshot-store = { path = "../canon-snapshot-store" }
async-trait = { workspace = true }
thiserror = { workspace = true }
uuid = { workspace = true }
bytes = { workspace = true }
chrono = { workspace = true }
# Your driver crate here
redis = "0.25"

[dev-dependencies]
tokio = { workspace = true }
testcontainers = "0.27"
testcontainers-modules = { version = "0.15", features = ["redis"] }
```

Depend only on `canon-core` (types), `canon-snapshot-store` (trait), and your driver.
No cross-dependencies on other implementation crates.

### Step 3: Implement the trait

```rust
use async_trait::async_trait;
use canon_core::{AggregateId, Snapshot};
use canon_core::traits::SnapshotStore;

#[derive(Debug, thiserror::Error)]
pub enum RedisSnapshotStoreError {
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),
}

pub struct RedisSnapshotStore { /* ... */ }

#[async_trait]
impl SnapshotStore for RedisSnapshotStore {
    type Error = RedisSnapshotStoreError;

    async fn save(&self, snapshot: Snapshot) -> Result<(), Self::Error> {
        // ...
    }

    async fn load(
        &self, aggregate_id: &AggregateId,
    ) -> Result<Option<Snapshot>, Self::Error> {
        // ...
    }
}
```

### Step 4: Add testcontainer tests

```rust
#[cfg(test)]
mod tests {
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::redis::Redis;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_save_and_load() {
        let container = Redis::default().start().await.unwrap();
        // ...
    }
}
```

### Step 5: Add to workspace

Add the crate to the root `Cargo.toml` `[workspace.members]` list.

### Step 6: Wire into a service

In the service's `main.rs`, swap out `YugabyteSnapshotStore::new(pool)` for
`RedisSnapshotStore::new(client)`. Nothing else changes -- the `ServiceBuilder`
is generic over the trait, not the implementation.

---

## Dependency verification

To verify the DAG has no violations, check that no implementation crate depends on
another implementation crate:

```bash
# Should return empty for all impl crates
cargo tree -p canon-event-store-cassandra | grep "canon-.*-yugabyte\|canon-.*-kafka"
cargo tree -p canon-publisher-kafka | grep "canon-.*-yugabyte\|canon-.*-cassandra"
cargo tree -p canon-deadletter-yugabyte | grep "canon-.*-kafka\|canon-.*-cassandra"
```

Each line should produce no output. If it does, the DAG has been violated and the
dependency must be removed.
