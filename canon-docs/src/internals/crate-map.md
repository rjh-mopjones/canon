# Crate Map

Canon is organised as a Cargo workspace with a strict dependency DAG. This chapter maps
every crate, its purpose, and its dependencies.

## Dependency graph

```
canon-core
    |-- canon-core-macros          (proc-macro subcrate, re-exported)
    |
    |-- canon-event-store          (trait)
    |       |-- canon-event-store-cassandra
    |
    |-- canon-command-store        (trait)
    |       |-- canon-command-store-yugabyte
    |
    |-- canon-snapshot-store       (trait)
    |       |-- canon-snapshot-store-yugabyte
    |
    |-- canon-inbox                (trait)
    |       |-- canon-inbox-yugabyte
    |
    |-- canon-inbound-queue        (trait)
    |       |-- canon-inbound-queue-kafka
    |
    |-- canon-outbound-queue       (trait)
    |       |-- canon-outbound-queue-kafka
    |
    |-- canon-projection-store     (trait)
    |       |-- canon-projection-store-yugabyte
    |
    |-- canon-publisher            (trait)
    |       |-- canon-publisher-kafka
    |
    |-- canon-adaptor              (trait)
    |       |-- canon-adaptor-kafka
    |
    |-- canon-deadletter           (trait)
            |-- canon-deadletter-yugabyte
```

## The strict DAG rule

Implementation crates depend on their trait crate + `canon-core` only. There are **no
cross-dependencies between implementation crates**. This means:

- `canon-event-store-cassandra` depends on `canon-event-store` and `canon-core`
- `canon-event-store-cassandra` does NOT depend on `canon-inbox-yugabyte`
- `canon-publisher-kafka` does NOT depend on `canon-outbound-queue-kafka`

This ensures any implementation can be swapped without affecting other parts of the system.

## Foundation crates

### canon-core

The root of the dependency tree. Contains:

- **Core types** -- `AggregateId`, `Version`, `EventEnvelope`, `CommandEnvelope`,
  `IncomingMessage`, `Oversight`, counterfactual types
- **Core traits** -- `Aggregate`, `CommandHandler`, `EventHandler`, `EventCombiner`,
  `Projection`, `ProjectionHandler`, `CounterfactualReplay`
- **In-memory implementations** -- `InMemoryEventStore`, `InMemoryCommandStore`,
  `InMemorySnapshotStore`, `InMemoryInbox`, `InMemoryInboundQueue`,
  `InMemoryOutboundQueue`, `InMemoryProjectionStore`, `InMemoryPublisher`,
  `InMemoryAdaptor`, `InMemoryDeadLetterStore`
- **Service orchestrator** -- `ServiceBuilder`, `Service`, dispatcher, outbox processor
- **Replay engine** -- `CounterfactualReplay` implementation

### canon-core-macros

A `proc-macro = true` subcrate inside `canon-core/`. Re-exported from `canon-core`.
Contains all eight proc-macros:

1. `#[aggregate]`
2. `#[command]`
3. `#[event]`
4. `#[event_combiner]`
5. `#[command_handler]`
6. `#[event_handler]`
7. `#[projection]`
8. `#[projection_handler]`

### canon-test

Integration test harness using all in-memory implementations. Provides `TestHarness`
for wiring in-memory stores into a real `Service` via `ServiceBuilder`.

Test modules cover: snapshotting, oversight, counterfactual replay, dead lettering,
projection rebuild, inbox window expiry, idempotency, outbound fan-out.

## Trait crates

Thin crates containing only trait definitions and associated types. They re-export from
`canon-core`.

| Crate | Trait | Purpose |
|-------|-------|---------|
| `canon-event-store` | `EventStore` | Append-only event persistence |
| `canon-command-store` | `CommandStore` | Command audit trail |
| `canon-snapshot-store` | `SnapshotStore` | Aggregate state snapshots |
| `canon-inbox` | `Inbox` | Idempotent intake, windowing, oversight |
| `canon-inbound-queue` | `InboundQueue` | Assembled batches to handlers |
| `canon-outbound-queue` | `OutboundQueue` | Committed events to consumers |
| `canon-projection-store` | `ProjectionStore` | Read model persistence |
| `canon-publisher` | `EventPublisher` | Cross-service event publishing |
| `canon-adaptor` | `EventAdaptor` | Cross-service event consumption |
| `canon-deadletter` | `DeadLetterStore` | Failed message storage and requeue |

## Implementation crates

Concrete implementations of each trait crate.

### YugabyteDB implementations

| Crate | Implements | Notes |
|-------|-----------|-------|
| `canon-command-store-yugabyte` | `CommandStore` | SQL queries via `sqlx` |
| `canon-snapshot-store-yugabyte` | `SnapshotStore` | Single row per aggregate |
| `canon-inbox-yugabyte` | `Inbox` | Composite key dedup, window tracking |
| `canon-projection-store-yugabyte` | `ProjectionStore` | Read models + checkpoints |
| `canon-deadletter-yugabyte` | `DeadLetterStore` | Dead letter storage + requeue |

### Cassandra implementations

| Crate | Implements | Notes |
|-------|-----------|-------|
| `canon-event-store-cassandra` | `EventStore` | Wide rows per aggregate, version-ordered |

### Kafka implementations

| Crate | Implements | Notes |
|-------|-----------|-------|
| `canon-inbound-queue-kafka` | `InboundQueue` | `rskafka`, pure Rust |
| `canon-outbound-queue-kafka` | `OutboundQueue` | `rskafka`, pure Rust |
| `canon-publisher-kafka` | `EventPublisher` | `rskafka`, pure Rust |
| `canon-adaptor-kafka` | `EventAdaptor` | `rskafka`, pure Rust |

All Kafka crates use `rskafka` exclusively -- no `rdkafka`, no C dependencies.

## Demo crates

The `canon-demo/` directory contains a spaceship logistics game demonstrating all Canon
features:

```
canon-demo/
    shared/               -- domain types, events, commands, topic constants
    fleet-service/        -- Ship aggregate
    cargo-service/        -- Manifest aggregate
    navigation-service/   -- Route aggregate
    supply-service/       -- Inventory aggregate
    station-service/      -- Station aggregate
    gateway/              -- axum REST + WebSocket
    frontend/             -- Leptos WASM
```

## Other directories

| Path | Purpose |
|------|---------|
| `canon-site/` | Landing page (static HTML/CSS) at `canon.mopjones.com` |
| `canon-docs/` | This documentation site (mdBook) |
| `canon-demo/k8s/` | Kubernetes manifests (kustomize base + overlays) |
| `canon-demo/e2e/` | Playwright end-to-end tests |

## Storage strategy summary

| Concern | Backend | Rationale |
|---------|---------|-----------|
| Event store | Cassandra | Append-optimised, high-volume, wide rows |
| Command store | YugabyteDB | Transactional, queryable, version-range replay |
| Inbox | YugabyteDB | Strong consistency, composite key uniqueness |
| Snapshot store | YugabyteDB | Low-volume, transactional reads |
| Projection store | YugabyteDB | Queryable read models, checkpoint tracking |
| Dead letter store | YugabyteDB | Inspectable, requeueable, auditable |
| Outbox | YugabyteDB | Sequence-numbered staging, ACID transactions |
| Retry attempts | YugabyteDB | Crash-safe retry counters |
| Inbound queue | Kafka | Assembled batches, partitioned by aggregate_id |
| Outbound queue | Kafka | Fan-out to 4 consumer groups |
