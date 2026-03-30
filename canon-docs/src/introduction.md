# Canon

**Macro-driven event sourcing for Rust.**

Canon is a Rust framework for building event-sourced, CQRS services. It provides an
opinionated, production-ready pipeline that takes you from command handling through
guaranteed event delivery to projected read models -- with zero boilerplate. You define
your domain with attribute macros; Canon generates all trait implementations, dispatch
logic, version-matched routing, and automatic service registration at compile time.

Canon ships with pluggable infrastructure: YugabyteDB for commands and projections,
Cassandra for the append-only event store, and Kafka for message routing. Every
infrastructure concern sits behind a trait, so you can swap the backing store without
touching your domain code. In-memory implementations of every trait live in `canon-core`,
enabling sub-second integration tests with zero external infrastructure.

---

## Why Canon exists

Event sourcing in Rust is hard. The language gives you strong guarantees around
correctness and performance, but the ecosystem has no mature, opinionated framework that
handles the full pipeline from command intake to projected read models. Building one from
scratch means solving the same problems over and over: outbox reliability, snapshot
management, projection rebuilds, dead letter handling, cross-service event routing,
idempotency at every stage.

Canon exists to solve those problems once, correctly, and let you focus on domain logic.

### The problems Canon solves

**Dual-write bugs.** In a naive event-sourced system, you write the event to a store and
then publish it to a message broker. If the process crashes between those two writes, you
either lose the event (data loss) or publish it without persisting it (phantom events).
Canon eliminates this class of bug entirely through the outbox pattern: events are written
to an outbox table in the same ACID transaction as the command record. A background
processor drains the outbox to Kafka. The database transaction is the commit point --
either both the command and its events are persisted, or neither is.

**Boilerplate overhead.** Event sourcing requires a significant amount of wiring:
deserializing events by type and version, routing commands to the correct handler,
folding events into aggregate state, registering projections, managing snapshots. In most
frameworks, this wiring is manual and error-prone. Canon eliminates it with proc-macros
that generate all trait implementations from declarative annotations. You write your
domain structs and handlers; the macros generate everything else.

**Version drift.** As your domain evolves, events and commands change shape. Canon uses
version-matched routing: every event and command carries a schema version number, and the
framework dispatches to the handler registered at that exact version. There is no
upcasting, no downcasting, no implicit conversion. Version 1 events are always processed
by version 1 combiners. Version 2 events coexist alongside version 1 as entirely separate
types.

**Testing difficulty.** Integration testing event-sourced systems usually requires running
databases and message brokers, leading to slow, flaky test suites. Canon provides
in-memory implementations of every infrastructure trait in `canon-core`. The `canon-test`
crate wires them into a real `Service` via `ServiceBuilder`, letting you exercise the
full pipeline -- command dispatch, outbox processing, event store writes, snapshot
creation, projection updates, and cross-service publishing -- in milliseconds, with no
Docker.

**Cross-service coordination.** In a microservice architecture, services need to react to
each other's events. Canon handles this through a publisher consumer that writes events to
a `canon.{service}.events` Kafka topic, and an adaptor that subscribes to external topics
and routes matching events through the inbox. Service authors declare which external
topics to subscribe to and write `#[event_handler]` implementations. The framework handles
all routing, deduplication, windowing, and dispatch.

---

## Philosophy

Canon is built on a set of core design principles that inform every decision in the
framework.

### Hexagonal architecture

Every infrastructure concern sits behind a trait. The `EventStore` trait abstracts event
persistence. The `SnapshotStore` trait abstracts snapshot storage. The `ConsumerReceiver`
trait abstracts message consumption from the outbound queue. The `Publisher` trait
abstracts cross-service event distribution. Your domain code depends only on `canon-core`
and these traits -- it never imports a database driver or message broker client.

This means you can swap Cassandra for DynamoDB, Kafka for Pulsar, or YugabyteDB for
PostgreSQL by implementing the relevant trait in a new infrastructure crate. The domain
code, the pipeline logic, and the test harness all remain unchanged.

### Append-only truth

The event store is the single source of truth. Everything else -- aggregate state,
projections, read models -- is derived from events. Aggregate state is reconstructed by
replaying events through version-matched combiners. Projections are built by applying
events to read models. Snapshots are optimization checkpoints, not authoritative state.

This means you can always rebuild any derived view from the event history. If a projection
has a bug, fix the handler and rebuild the projection from the event stream. If you need a
new read model, deploy a new projection and replay the history to populate it.

### Crash safety

All durable state survives process death. The outbox is the commit point: events and the
command record are written in a single ACID transaction. The outbox processor picks up
committed events and publishes them to Kafka. If the process crashes after the transaction
but before publishing, the outbox processor will pick up the events on restart.

All Kafka consumers restart from their last persisted offset and rely on downstream
idempotency to skip already-processed events. The inbox deduplicates by
`(handler_id, message_id)`. The event store rejects version mismatches (optimistic
concurrency). Projections track checkpoints. Every stage is safe to replay.

### Compile-time safety

Canon enforces structural correctness at compile time through marker traits and
exhaustiveness checks:

- Every `#[command(Ship, version = 1)]` must have a matching
  `#[command_handler(Ship, version = 1)]`. Missing handler = compile error.
- Every `#[event(Ship, version = 1)]` must have a matching
  `#[event_combiner(Ship, version = 1)]`. Missing combiner = compile error.
- An `#[event_handler]` with `window_ttl` must define an `oversight` function.
  Missing oversight = compile error.
- The return type of a `#[command_handler]` must match the event type declared in
  the command's `produces` attribute. Mismatch = compile error.

These checks catch wiring errors before your code ever runs.

### Macro-driven ergonomics

Users never implement framework traits directly. Eight proc-macros generate all
implementations from clean, declarative annotations:

1. `#[aggregate]` -- generates `Aggregate` trait impl, `Default`, serde derives
2. `#[command]` -- marks a command struct, declares its aggregate and produced event
3. `#[event]` -- marks an event struct, declares its aggregate and schema version
4. `#[event_combiner]` -- generates synchronous state folding logic
5. `#[command_handler]` -- generates async command handling logic
6. `#[event_handler]` -- generates cross-cutting event reaction logic
7. `#[projection]` -- marks a read model struct
8. `#[projection_handler]` -- generates projection update logic

Each macro also emits an `inventory` registration, so `ServiceBuilder` discovers
everything automatically at startup. No manual registration, no runtime reflection.

### Testability

In-memory implementations of every infrastructure trait ship in `canon-core`. The
`canon-test` crate provides a `TestHarness` that wires all in-memory implementations into
a real `Service` via `ServiceBuilder`. You submit commands through the dispatcher, step
through the outbox processor and all three consumers, and assert that events reach the
event store, projections are updated, and events are published -- all in a single-threaded
test that runs in milliseconds.

---

## The pipeline

Canon's event sourcing pipeline is a directed graph of stages. Every message flows through
these stages in order:

```
External world
      |
      v
Adaptor (Kafka)          -- inbound events from other services
      |
      v
Inbox (YugabyteDB)       -- idempotency, windowing, oversight
      |
      v
Inbound Queue (Kafka)    -- assembled batches ready for dispatch
      |
      v
Dispatcher
  |-> Command handlers   -- validate + produce events
  |-> Event handlers     -- react to events, optionally produce commands
      |
      v
YugabyteDB ACID transaction
  |-- commands table      -- audit trail
  |-- outbox table        -- staged events
      |
      v
Outbox processor          -- drain outbox to outbound queue
      |
      v
Outbound Queue (Kafka)    -- committed events fanning out
      |
      |-> Event store consumer     -> Cassandra (+ snapshots)
      |-> Projection consumer      -> YugabyteDB read models
      |-> Internal event consumer  -> Inbox (for event handler dispatch)
      |-> Publisher consumer       -> canon.{service}.events topic
```

### Stage by stage

**Adaptor.** The entry point for external events. The adaptor subscribes to Kafka topics
published by other services (e.g., `canon.supply.events`) and routes incoming events to
the inbox. Each event is matched against registered `#[event_handler]` declarations to
find all handlers that care about it.

**Inbox.** The idempotency and assembly layer. Every incoming message (command or event) is
written to `inbox_messages` with a composite key of `(handler_id, message_id)`. Duplicate
messages are silently dropped via `ON CONFLICT DO NOTHING`. The inbox also manages
windowing: event handlers with `window_ttl` accumulate messages into correlation-keyed
windows. The `oversight` function on each handler decides when a window is ready for
dispatch (`Ready`), needs more messages (`NotReady`), or should be abandoned (`Discard`).

**Inbound Queue.** When a window is ready (or a command arrives with no windowing), the
inbox publishes the assembled batch to the inbound Kafka queue for the dispatcher to
consume.

**Dispatcher.** The central routing component. It polls the inbox for unprocessed commands,
hydrates aggregate state from the event store (with snapshot acceleration), deserializes
the command based on `command_type` and `command_version`, and calls the version-matched
`#[command_handler]`. The handler returns a single event or an error. On success, the
dispatcher writes the command record and the event to the outbox in a single YugabyteDB
ACID transaction. For event handlers, the dispatcher calls the handler with accumulated
events; if the handler returns a `CommandEnvelope`, it is re-submitted to the inbox for
command dispatch.

**Outbox.** The commit point of the system. Events and the command record are written in a
single ACID transaction. The outbox processor is a background `tokio::spawn` task that
polls the outbox table for undelivered entries (using `SELECT ... FOR UPDATE SKIP LOCKED`
for concurrent safety), publishes them to the outbound Kafka queue, and marks them as
delivered. Backpressure is managed through a bounded channel (default capacity 1024).

**Outbound Queue.** The fan-out layer. Four independent consumer groups read from the
outbound topic:

1. **Event store consumer** -- writes events to Cassandra with optimistic concurrency.
   After a confirmed write, checks whether `version % snapshot_every == 0` and takes a
   snapshot if so. On version conflict, retries up to 3 times, then dead-letters the event.

2. **Projection consumer** -- applies events to read model projections via registered
   `#[projection_handler]` implementations. Tracks progress via `projection_checkpoints`
   table. Supports full rebuild by resetting the checkpoint and replaying from the beginning.

3. **Internal event consumer** -- routes a service's own events back to the inbox for
   event handler dispatch. For each event, checks the `EventHandlerRegistration` inventory
   for matching `#[handles]` declarations and submits matching events to the inbox.

4. **Publisher consumer** -- publishes events to the `canon.{service}.events` Kafka topic
   for consumption by other services. This is how cross-service event routing works:
   fleet-service publishes `ShipDeparted` to `canon.fleet.events`, and
   navigation-service's adaptor picks it up.

### The outbox pattern in detail

The outbox pattern is the foundation of Canon's reliability guarantee. Here is the exact
sequence of operations when a command is processed:

```
1. Gateway receives POST /fleet/ships/:id/depart
2. Gateway writes CommandEnvelope to inbox_messages
3. Dispatcher polls inbox_messages, finds the command
4. Dispatcher loads aggregate state:
   a. Check snapshot store for latest snapshot
   b. Load events from event store since snapshot version
   c. Hydrate state by replaying events through #[event_combiner] impls
5. Dispatcher calls DepartForStationHandler.handle(state, cmd)
6. Handler returns Ok(ShipDeparted { ... })
7. Dispatcher executes single YugabyteDB transaction:
   BEGIN;
     INSERT INTO commands (command_id, aggregate_id, ...);
     INSERT INTO outbox (id, sequence_number, aggregate_id, envelope);
   COMMIT;
8. Outbox processor polls outbox, finds undelivered entry
9. Outbox processor publishes to canon.fleet.outbound Kafka topic
10. Outbox processor marks entry as delivered (sets delivered_at)
```

If the process crashes at any point:
- Before step 7: nothing was written, the command is retried on restart.
- After step 7, before step 9: the outbox entry exists, the processor picks it up on restart.
- After step 9, before step 10: the entry is re-delivered, downstream idempotency handles the duplicate.

There is no window where data can be lost.

---

## A quick taste

Here is what it looks like to define a complete aggregate in Canon, taken from the
fleet-service in the demo application. This is real code from the codebase, not a
contrived example.

### Define the aggregate

```rust
use canon_core::aggregate;

#[derive(Default, Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ShipStatus {
    #[default]
    Docked,
    InTransit,
    Decommissioned,
}

#[aggregate(snapshot_every = 50)]
pub struct Ship {
    pub name: String,
    pub capacity_kg: f32,
    pub status: ShipStatus,
    pub assigned_route: Option<uuid::Uuid>,
    pub current_station: Option<uuid::Uuid>,
    pub fuel_kg: f32,
}
```

The `#[aggregate(snapshot_every = 50)]` macro generates the `Aggregate` trait
implementation, `Default` derive, serde derives, and `inventory` registration. The
`snapshot_every = 50` parameter tells the event store consumer to take a snapshot every
50 events, so future state reconstructions only need to replay events since the last
snapshot.

### Define commands and events

```rust
#[canon_core::command(Ship, version = 1, produces = [ShipDeparted])]
pub struct DepartForStation {
    pub ship_id: Uuid,
    pub destination: Uuid,
}

#[canon_core::event(Ship, version = 1)]
pub struct ShipDeparted {
    pub ship_id: Uuid,
    pub destination: Uuid,
    pub fuel_at_departure: f32,
}
```

Commands declare which aggregate they target, their schema version, and which event they
produce. Events declare their aggregate and schema version. Both generate serde derives
and `inventory` registrations.

### Fold events into state

```rust
use canon_core::event_combiner;

#[event_combiner(Ship, version = 1)]
impl ShipDeparted {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::InTransit;
    }
}
```

Event combiners are synchronous, pure state folding functions. They take the event and
mutate the aggregate state. The `#[aggregate]` macro's generated `hydrate` function
calls these combiners in version-matched order when reconstructing state from stored
events.

### Handle commands

```rust
use canon_core::command_handler;

#[command_handler(Ship, version = 1)]
impl DepartForStationHandler {
    type Error = FleetError;

    fn handle(&self, state: &Ship, cmd: DepartForStation) -> Result<ShipDeparted, FleetError> {
        if state.status != ShipStatus::Docked {
            return Err(FleetError::ShipNotDocked);
        }
        Ok(ShipDeparted {
            ship_id: cmd.ship_id,
            destination: cmd.destination,
            fuel_at_departure: state.fuel_kg,
        })
    }
}
```

Command handlers validate the command against current aggregate state and return either a
single event or an error. Rejection is always `Err`, never a separate event type. The
handler sees the fully hydrated aggregate state and the deserialized command -- it never
touches envelopes, serialization, or infrastructure.

### Build read models

```rust
#[canon_core::projection]
pub struct ShipReadModel {
    pub ship_id: uuid::Uuid,
    pub name: String,
    pub capacity_kg: f32,
    pub status: String,
    pub fuel_kg: f32,
}

#[canon_core::projection_handler(ShipReadModel)]
impl ShipDepartedProjectionHandler {
    fn apply(&self, event: &ShipDeparted, store: &mut ShipReadModel) {
        store.status = "InTransit".to_string();
    }
}
```

Projections are read models built from the event stream. Projection handlers apply one
event type to the read model's state. They must be idempotent -- applying the same event
twice must produce the same result.

### Wire the service

```rust
let service = ServiceBuilder::new("fleet")
    .for_aggregate::<Ship>()
    .event_store(event_store)
    .snapshot_store(snapshot_store)
    .dead_letter_store(dead_letter_store)
    .retry_tracker(retry_tracker)
    .snapshot_state_provider(EventPayloadSnapshotProvider)
    .outbox_store(outbox_store)
    .outbox_publisher(outbox_publisher)
    .projection_checkpoint_store(projection_store)
    .publisher(publisher)
    .topic(&events_topic)
    .build()?;

service.start(shutdown_rx, Some(notify_rx), es_receiver, proj_receiver, pub_receiver).await;
```

`ServiceBuilder` discovers all macro-generated registrations via `inventory` -- command
handlers, event combiners, projection handlers, event handlers. It validates
exhaustiveness (every command has a handler, every event has a combiner), wires the
infrastructure stores, and produces a `Service` that spawns all background tasks: outbox
processor, event store consumer, projection consumer, and publisher consumer.

---

## Core types

Canon defines a small set of core types that flow through the entire pipeline. These
types are defined in `canon-core` and are used by every crate in the workspace.

### AggregateId

```rust
pub struct AggregateId(Uuid);
```

A UUID newtype that uniquely identifies an aggregate instance. All Kafka topics are
partitioned by `AggregateId`, ensuring that all events for a given aggregate are
processed in order. Canon uses this type everywhere -- never a raw `Uuid`.

### Version

```rust
pub struct Version(u64);
```

A monotonically increasing version number attached to each event. `Version::initial()`
returns 0; each subsequent event increments by 1. The event store uses versions for
optimistic concurrency: if you try to append an event at version 5 but the store is
already at version 6, the write is rejected.

### EventEnvelope

```rust
pub struct EventEnvelope {
    pub event_id: Uuid,
    pub aggregate_id: AggregateId,
    pub version: Version,
    pub event_type: String,
    pub event_version: u32,
    pub payload: Bytes,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub timestamp: DateTime<Utc>,
}
```

The envelope that wraps every event in the system. The `payload` field contains the
serialized event data as opaque bytes. The `event_type` and `event_version` fields are
used by the version-matched dispatch logic to route the event to the correct combiner or
handler. The `correlation_id` traces the full causal chain end to end; the `causation_id`
identifies the immediate cause (the command that produced this event, or the event that
triggered the handler).

### CommandEnvelope

```rust
pub struct CommandEnvelope {
    pub command_id: Uuid,
    pub aggregate_id: AggregateId,
    pub command_type: String,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub payload: Bytes,
    pub command_version: u32,
}
```

The envelope that wraps every command. Like `EventEnvelope`, the payload is opaque bytes
deserialized by the version-matched handler. The `command_version` field is critical
during counterfactual replay, where stored commands are routed to the handler registered
at their original version.

### IncomingMessage

```rust
pub enum IncomingMessage {
    Command(CommandEnvelope),
    InternalEvent(EventEnvelope),
    ExternalEvent(EventEnvelope),
}
```

The union type for all messages that enter the inbox. Commands come from the gateway.
Internal events come from the service's own outbound queue (via the internal event
consumer). External events come from other services (via the adaptor). The inbox treats
all three identically for deduplication and windowing purposes, but the dispatcher routes
them differently.

### Oversight

```rust
pub enum Oversight {
    Ready,
    NotReady,
    Discard,
}
```

The return type of an event handler's `oversight` function. Controls dispatch readiness
for windowed event handlers. `Ready` means the accumulated batch should be dispatched now.
`NotReady` means the handler is waiting for more events. `Discard` means the window
should be abandoned entirely. Event handlers without `window_ttl` default to `Ready` on
every message.

---

## Workspace structure

Canon is organized as a Cargo workspace with a strict dependency DAG. The workspace has
three layers: the core, the infrastructure crates, and the demo application.

### Core layer

```
canon-core                    -- traits, types, in-memory impls, proc-macros, pipeline logic
  canon-core-macros           -- proc-macro subcrate, re-exported from canon-core
canon-test                    -- integration test harness, in-memory only
```

`canon-core` contains everything that does not depend on external infrastructure:
the core types (`AggregateId`, `Version`, `EventEnvelope`, etc.), the core traits
(`Aggregate`, `CommandHandler`, `EventHandler`, `Projection`, etc.), in-memory
implementations of every trait, the dispatcher, the outbox processor, all three outbound
consumers, the service builder, and the proc-macros.

### Trait crates

Each infrastructure concern has a thin trait crate that re-exports from `canon-core`:

```
canon-event-store             canon-command-store          canon-snapshot-store
canon-inbox                   canon-inbound-queue          canon-outbound-queue
canon-projection-store        canon-publisher              canon-adaptor
canon-deadletter
```

These exist to enforce the dependency DAG: infrastructure implementation crates depend on
their trait crate plus `canon-core`, never on each other.

### Infrastructure crates

Each trait crate has a corresponding implementation crate:

```
canon-event-store-cassandra   -- Cassandra-backed event store
canon-command-store-yugabyte  -- YugabyteDB-backed command store + outbox
canon-snapshot-store-yugabyte -- YugabyteDB-backed snapshot store
canon-inbox-yugabyte          -- YugabyteDB-backed inbox with windowing
canon-inbound-queue-kafka     -- Kafka-backed inbound queue
canon-outbound-queue-kafka    -- Kafka-backed outbound queue
canon-projection-store-yugabyte -- YugabyteDB-backed projection checkpoints
canon-publisher-kafka         -- Kafka publisher for cross-service events
canon-adaptor-kafka           -- Kafka adaptor for external event consumption
canon-deadletter-yugabyte     -- YugabyteDB-backed dead letter store
```

All Kafka crates use `rskafka` (pure Rust, no C dependencies). This keeps the entire
framework cross-compilable from macOS to Linux with a musl target.

### Demo application

```
canon-demo/
  shared/                -- domain types, events, commands, topic constants
  fleet-service/         -- ship management (reference implementation)
  cargo-service/         -- cargo manifest management
  navigation-service/    -- route planning and position tracking
  supply-service/        -- inventory and resupply management
  station-service/       -- station registration and stock management
  gateway/               -- axum REST API + WebSocket
  frontend/              -- Leptos WASM single-page application
```

The demo is a spaceship logistics game with five services communicating through Canon's
pipeline. Each service uses its own YugabyteDB schema and Cassandra keyspace for complete
storage isolation.

---

## How Canon compares

### Versus hand-rolled event sourcing

Most Rust projects that adopt event sourcing build it from scratch: a trait for
aggregates, manual serde dispatch, hand-wired Kafka consumers, ad-hoc projection logic.
This works for a single service but breaks down as the system grows. Canon gives you the
full pipeline -- outbox, consumers, projections, snapshots, dead letters, cross-service
routing -- tested and wired, so you can focus on domain logic.

### Versus Axon Framework (Java/Kotlin)

Axon is the most mature event sourcing framework in the JVM world. Canon shares its
philosophy of annotation-driven domain modelling and automatic discovery, but differs
in several ways:

- Canon uses Rust proc-macros instead of Java annotations. The dispatch logic is generated
  at compile time, not resolved via reflection at runtime.
- Canon enforces exhaustiveness at compile time. Axon discovers handlers at runtime and
  fails with a runtime exception if a handler is missing.
- Canon separates the event store (Cassandra, append-optimized) from the command store
  (YugabyteDB, transactional) and the projection store (YugabyteDB, read-optimized).
  Axon typically uses a single store for all three roles.
- Canon's outbox pattern provides exactly-once semantics at the database level.
  Axon relies on its own event processor framework for delivery guarantees.

### Versus EventStoreDB

EventStoreDB is a purpose-built event store database. Canon is a framework, not a
database. Canon uses Cassandra as its event store and focuses on the pipeline around it:
command dispatch, outbox processing, projection management, cross-service routing. You
could implement a `canon-event-store-eventstoredb` crate to use EventStoreDB as Canon's
backing store while keeping the rest of the pipeline unchanged.

### Versus cqrs-es (Rust)

The `cqrs-es` crate provides basic CQRS/ES traits for Rust. Canon goes significantly
further: proc-macros that generate all implementations, the full outbox pipeline with
guaranteed delivery, versioned event routing, snapshot management, projection rebuilds,
dead letter handling, windowed event handlers with oversight gates, counterfactual replay,
and a complete infrastructure layer with production-ready implementations.

---

## An experiment in AI-assisted development

Canon is an experiment to see how far AI-assisted development can go -- can it generate an
entire production-grade framework? Every line of Canon was written through human-AI
collaboration using Claude Code. The codebase, the documentation, the demo application,
and the Kubernetes deployment manifests were all produced through this collaboration.

---

## What you will learn

This documentation is organized into three sections: a user guide for building services
with Canon, an internals reference for understanding the framework's implementation, and
an API reference for every public type and trait.

### User Guide

**[Getting Started](./user-guide/getting-started.md)** walks you through installing Canon,
creating your first aggregate, defining commands and events, writing handlers and
combiners, and running your service. By the end, you will have a working event-sourced
service processing commands through the full pipeline.

**[Core Concepts](./user-guide/core-concepts.md)** explains the building blocks of Canon
in depth: aggregates, commands, events, event combiners, command handlers, event handlers,
projections, the outbox pattern, and the inbox with its windowing and oversight
capabilities.

**[Macros Reference](./user-guide/macros-reference.md)** is the complete API for all eight
proc-macros: `#[aggregate]`, `#[command]`, `#[event]`, `#[event_combiner]`,
`#[command_handler]`, `#[event_handler]`, `#[projection]`, and `#[projection_handler]`.
Each macro's parameters, generated code, and compile-time checks are documented with
examples.

**[Testing](./user-guide/testing.md)** covers Canon's two-tier testing strategy: in-memory
end-to-end tests with the `TestHarness` (sub-second, no Docker), and testcontainers-based
tests with real YugabyteDB, Cassandra, and Kafka (catches serialization bugs and schema
mismatches that in-memory tests cannot).

**[Deployment](./user-guide/deployment.md)** explains how to build and deploy Canon services
to Kubernetes, including cross-compilation from macOS to Linux, Docker image construction,
minikube local development, and GKE production deployment.

### Internals

**[Architecture](./internals/architecture.md)** is a deep dive into the pipeline: the
dispatcher's command processing flow, the outbox processor's drain loop, each outbound
consumer's processing logic, the inbox's deduplication and windowing algorithm, and the
cross-service event routing mechanism.

**[Infrastructure Crates](./internals/infrastructure.md)** documents the implementation of
each infrastructure crate: the Cassandra event store's schema and query patterns, the
YugabyteDB command store's transactional write path, the Kafka crates' `rskafka` usage
patterns, and the dead letter store's retry mechanics.

**[Proc-Macro Internals](./internals/proc-macros.md)** explains how the eight proc-macros
work: the `syn` parsing, the `quote` code generation, the `inventory` registration, and
the marker trait enforcement that provides compile-time exhaustiveness checks.

### API Reference

**[Core Types](./api/core-types.md)** documents every public type in `canon-core`:
`AggregateId`, `Version`, `EventEnvelope`, `CommandEnvelope`, `IncomingMessage`,
`Oversight`, `Snapshot`, `DeadLetter`, and the counterfactual types.

**[Core Traits](./api/core-traits.md)** documents every public trait: `Aggregate`,
`CommandHandler`, `EventHandler`, `EventCombiner`, `Projection`, `ProjectionHandler`,
`EventStore`, `SnapshotStore`, `CommandStore`, `DeadLetterStore`, `Publisher`,
`ConsumerReceiver`, `InboxPort`, `RetryTracker`, `CounterfactualReplay`, and
`ReplayEventStore`.

### Live Demo

Canon includes a complete demo application -- a spaceship logistics game with five
services, a gateway, and a Leptos WASM frontend. The demo runs at
[canon.mopjones.com](https://canon.mopjones.com) and is fully playable. Every state
change in the UI (ship movement, stock levels, oversight gates, event log entries) is
driven by real events flowing through the Canon pipeline.

- [Live demo](https://canon.mopjones.com/demo)
- [Source code](https://github.com/rjh-mopjones/canon)
