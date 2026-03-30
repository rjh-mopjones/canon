# Core Concepts

This chapter is a comprehensive reference for every fundamental building block in Canon:
aggregates, commands, events, handlers, combiners, projections, the dispatcher, and the
version-matching strategy that ties them all together. Every code example is drawn from
the actual Canon codebase and demo services.

---

## Table of contents

- [Aggregates](#aggregates)
- [AggregateId](#aggregateid)
- [Version](#version)
- [Commands and CommandEnvelope](#commands-and-commandenvelope)
- [Events and EventEnvelope](#events-and-eventenvelope)
- [Command handlers](#command-handlers)
- [Event combiners](#event-combiners)
- [Event handlers](#event-handlers)
- [Projections](#projections)
- [Projection handlers](#projection-handlers)
- [The dispatcher](#the-dispatcher)
- [Version-matched routing](#version-matched-routing)
- [IncomingMessage](#incomingmessage)
- [Oversight](#oversight)
- [Counterfactual replay](#counterfactual-replay)
- [Snapshots](#snapshots)
- [Dead letters](#dead-letters)
- [The outbox pattern](#the-outbox-pattern)
- [Service lifecycle](#service-lifecycle)

---

## Aggregates

An aggregate is the consistency boundary in event sourcing. It is the unit of state that
Canon loads, validates commands against, and persists events for. In domain-driven design
terms, an aggregate is a cluster of domain objects that can be treated as a single unit for
the purposes of data changes.

Every aggregate instance in Canon has three things:

1. **State** -- the current materialised state, reconstructed by replaying events through
   version-matched event combiners.
2. **A version** -- a monotonically increasing counter (`Version(u64)`) used for optimistic
   concurrency control. Each new event increments the version by one.
3. **An identity** -- an `AggregateId(Uuid)` newtype that uniquely identifies the aggregate
   instance across the entire system.

### The State = Self pattern

Canon makes an opinionated design decision: **the aggregate struct is its own state**. There
is no separate `State` type -- the macro sets `type State = Self`. This keeps the mental
model simple: the struct you define is exactly what gets hydrated from events, serialised
into snapshots, and passed to command handlers for validation.

Here is the `Ship` aggregate from the fleet service:

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

And the `Station` aggregate from the station service:

```rust
#[canon_core::aggregate(snapshot_every = 50)]
pub struct Station {
    pub name: String,
    pub capacity_kg: f32,
    pub current_stock_kg: f32,
    pub drain_rate_kg_per_s: f32,
    pub supplied_by: Option<Uuid>,
    pub docked_ships: Vec<Uuid>,
    pub registered: bool,
    pub offline: bool,
}
```

### The Aggregate trait

The `#[aggregate]` macro generates an implementation of the `Aggregate` trait, which is
defined in `canon-core/src/traits/aggregate.rs`:

```rust
pub trait Aggregate: Sized + Send + Sync + 'static {
    type State: Default + Send + Sync + serde::Serialize;
    type Error: std::error::Error + Send + Sync + 'static;

    fn hydrate(
        state: &mut Self::State,
        events: impl Iterator<Item = EventEnvelope>,
    ) -> Result<(), Self::Error>;
}
```

Key points about this trait:

- `type State` requires `Default` because new aggregate instances start with
  `State::default()`. This is why the `#[aggregate]` macro derives `Default` on your struct.
- `type State` requires `serde::Serialize` because the state must be serialisable into
  snapshots for efficient hydration.
- `hydrate` is the only method. It takes a mutable reference to the state and an iterator of
  `EventEnvelope` values. The macro-generated implementation reads `event_type` and
  `event_version` from each envelope, deserialises the payload, and dispatches to the
  combiner registered at that exact version.
- There is no `handle` or `apply` method on the aggregate itself. Command handling is
  delegated to standalone `CommandHandler` implementations. State folding is delegated to
  `EventCombiner` implementations.

### Hydration

Hydration is the process of reconstructing aggregate state from stored events. When the
dispatcher needs to process a command, it:

1. Loads the most recent snapshot for the aggregate (if one exists).
2. Loads events from the snapshot's version forward (or from version zero if no snapshot).
3. Calls `Aggregate::hydrate(state, events)`, which iterates through each `EventEnvelope`
   and dispatches to the version-matched `#[event_combiner]` for that event type and version.
4. Returns the fully hydrated state, ready for command validation.

If no snapshot exists, all events from the very beginning of the aggregate's history are
replayed. For aggregates with long event histories, this can be slow -- which is why
`snapshot_every = N` exists.

### What the macro generates

The `#[aggregate(snapshot_every = 50)]` macro generates:

- `impl Aggregate for Ship` with `type State = Ship` and a `hydrate` implementation
  that iterates events and calls `__apply_event_combiner` for each one.
- `#[derive(Default, serde::Serialize, serde::Deserialize)]` on the struct (if not already
  present).
- An `inventory` registration so that `ServiceBuilder` can discover the aggregate
  automatically at startup.

---

## AggregateId

Canon uses a newtype wrapper around `Uuid` for aggregate identification. This is defined
in `canon-core/src/types.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AggregateId(Uuid);

impl AggregateId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}
```

The newtype is never generic and never a plain `Uuid`. This prevents accidental confusion
between aggregate IDs and other UUIDs in the system (command IDs, event IDs, correlation
IDs, etc.). All Kafka topics are partitioned by `aggregate_id`, ensuring that all events
for a single aggregate instance are processed in order.

The `Default` implementation generates a new random UUID, so each `AggregateId::default()`
produces a unique identity.

---

## Version

Version tracking enables optimistic concurrency control. The `Version` type is defined in
`canon-core/src/types.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Version(u64);

impl Version {
    pub fn initial() -> Self { Self(0) }
    pub fn from_u64(v: u64) -> Self { Self(v) }
    pub fn next(self) -> Self { Self(self.0 + 1) }
    pub fn as_u64(&self) -> u64 { self.0 }
}
```

When the dispatcher processes a command, it computes the next version as
`current_version.next()` and stamps the resulting `EventEnvelope` with it. The event store
rejects writes where the expected version does not match the stored version, preventing
lost updates from concurrent writers. This is optimistic concurrency -- no locks are held
during command processing, but a conflict at write time triggers a retry.

`Version` implements `PartialOrd` and `Ord`, so versions can be compared and sorted. It
also implements `Display`, printing the inner `u64` directly. The `From<u64>` implementation
allows `let v: Version = 99u64.into()`.

---

## Commands and CommandEnvelope

Commands represent intent -- what a user or system wants to happen. They are not facts;
they are requests that may be accepted or rejected. Each command targets a specific
aggregate instance and is versioned to support schema evolution.

### Defining commands

Commands are declared with the `#[command]` macro, specifying the target aggregate, the
schema version, and the event type the handler is expected to produce:

```rust
#[canon_core::command(Ship, version = 1, produces = [ShipRegistered])]
pub struct RegisterShip {
    pub name: String,
    pub capacity_kg: f32,
    pub home_station: Option<Uuid>,
}

#[canon_core::command(Ship, version = 1, produces = [ShipDeparted])]
pub struct DepartForStation {
    pub ship_id: Uuid,
    pub destination: Uuid,
}

#[canon_core::command(Ship, version = 1, produces = [ShipDecommissioned])]
pub struct DecommissionShip {
    pub ship_id: Uuid,
}
```

Key properties of the `#[command]` macro:

- `version` defaults to 1 if omitted.
- `produces` is **declarative metadata only** -- it documents which event the handler returns
  and is used for macro wiring, compile-time verification, and schema registry. No type is
  generated from it.
- Each command **must** have exactly one matching `#[command_handler]` at the same version.
  Missing it is a compile error.
- The macro generates serde derives and an `inventory` registration
  (`CommandRegistration`) so that `ServiceBuilder` can discover the command at startup.

### CommandEnvelope

When a command is submitted to the system (via the gateway REST API or an event handler
producing a downstream command), it is serialised and wrapped in a `CommandEnvelope` for
transport and storage:

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

Each field serves a specific purpose:

| Field | Purpose |
|-------|---------|
| `command_id` | Unique identifier for this specific command instance. Used for inbox deduplication. |
| `aggregate_id` | Which aggregate instance this command targets. Determines Kafka partition. |
| `command_type` | The type name as a string (e.g., `"DepartForStation"`). Used for dispatch routing. |
| `correlation_id` | Threads through an entire causal chain -- from the originating user action through every downstream command and event it triggers. |
| `causation_id` | Identifies the immediate cause -- which event or user action produced this command. |
| `timestamp` | When the command was created. |
| `payload` | The serialised command data as opaque `Bytes`. The command handler deserialises this using the `command_type` and `command_version` to select the right schema. |
| `command_version` | The schema version of the command. Critical for version-matched routing during counterfactual replay. |

The `command_type` and `command_version` fields together form the dispatch key. The
dispatcher looks up the registered handler function for that exact `(type, version)` pair
and invokes it.

### CommandStatus

Commands in the command store have a lifecycle tracked by `CommandStatus`:

```rust
pub enum CommandStatus {
    Pending,    // submitted but not yet processed
    Executed,   // successfully produced an event
    Failed,     // rejected by the command handler
}
```

---

## Events and EventEnvelope

Events are facts -- immutable records of something that happened. Once written to the event
store, an event is never modified or deleted. Events are the source of truth in an event
sourced system; all other state (aggregate state, projections, read models) is derived from
them.

### Defining events

Events are declared with the `#[event]` macro, specifying the aggregate they belong to and
their schema version:

```rust
#[canon_core::event(Ship, version = 1)]
pub struct ShipRegistered {
    pub ship_id: Uuid,
    pub name: String,
    pub capacity_kg: f32,
    pub home_station: Option<Uuid>,
}

#[canon_core::event(Ship, version = 1)]
pub struct ShipDeparted {
    pub ship_id: Uuid,
    pub destination: Uuid,
    pub fuel_at_departure: f32,
}

#[canon_core::event(Ship, version = 1)]
pub struct ShipDecommissioned {
    pub ship_id: Uuid,
}
```

Key properties of the `#[event]` macro:

- `version` defaults to 1 if omitted.
- Each event **must** have exactly one matching `#[event_combiner]` at the same version.
  Missing it is a compile error.
- The macro generates serde derives and an `inventory` registration (`EventRegistration`).

### Event versioning and coexistence

Events evolve by registering new versions as **separate types**. Version 1 and version 2
are distinct Rust types that coexist in the same codebase:

```rust
#[event(Ship, version = 1)]
pub struct ShipDeparted {
    pub destination: StationId,
}

#[event(Ship, version = 2)]
pub struct ShipDeparted {
    pub destination: StationId,
    pub fuel_at_departure: f32,
}
```

Each version has its own `#[event_combiner]`. During hydration, the framework reads
`event_version` from each stored `EventEnvelope` and dispatches to the combiner registered
at that exact version. Old events stored as version 1 are always processed by the version 1
combiner. New events stored as version 2 are processed by the version 2 combiner. There is
no upcasting, no downcasting, and no migration scripts.

### EventEnvelope

Every event in the store is wrapped in an `EventEnvelope`, defined in
`canon-core/src/types.rs`:

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

Each field serves a specific purpose:

| Field | Purpose |
|-------|---------|
| `event_id` | Unique identifier for this specific event instance. |
| `aggregate_id` | Which aggregate instance produced this event. Forms the partition key in Cassandra (`PRIMARY KEY (aggregate_id, version)`). |
| `version` | The aggregate version at the time this event was produced. Used for optimistic concurrency and ordering. |
| `event_type` | The type name as a string (e.g., `"ShipDeparted"`). Used for combiner dispatch. |
| `event_version` | The schema version of this event (e.g., `1` or `2`). Combined with `event_type`, this is the dispatch key for version-matched routing. |
| `payload` | The serialised event data as opaque `Bytes`. Deserialised by the version-matched combiner. |
| `correlation_id` | Inherited from the `CommandEnvelope` that produced this event. Threads through the entire causal chain. |
| `causation_id` | Set to the `command_id` of the command that produced this event. Identifies the immediate cause. |
| `timestamp` | When the event was produced. |

The separation of `event_type` + `event_version` from `payload` is what makes version-matched
routing possible. The framework never needs to deserialise the payload to decide which
combiner to call -- it reads the type and version from the envelope metadata.

---

## Command handlers

A command handler validates a command against the current aggregate state and, if the
command is valid, produces exactly one event. If the command is invalid, it returns an
error. There is no concept of a "rejection event" -- rejection is always `Err`.

### The CommandHandler trait

The trait is defined in `canon-core/src/traits/command_handler.rs`:

```rust
#[async_trait]
pub trait CommandHandler<A: Aggregate>: Send + Sync + 'static {
    type Command: Send + Sync;
    type Event: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle(
        &self,
        state: &A::State,
        command: Self::Command,
    ) -> Result<Self::Event, Self::Error>;
}
```

The handler receives:

- `state` -- a read-only reference to the current aggregate state (hydrated from events
  before the handler is called).
- `command` -- the deserialised command value.

It returns `Result<Self::Event, Self::Error>` -- either the single event that records what
happened, or an error explaining why the command was rejected.

### Defining command handlers

Command handlers are declared with the `#[command_handler]` macro. Each handler is a
standalone struct with a single `handle` method:

```rust
use crate::commands::*;
use crate::events::*;
use crate::aggregate::{Ship, ShipStatus};
use crate::error::FleetError;
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

#[command_handler(Ship, version = 1)]
impl DecommissionShipHandler {
    type Error = FleetError;

    fn handle(&self, state: &Ship, cmd: DecommissionShip) -> Result<ShipDecommissioned, FleetError> {
        if state.status == ShipStatus::Decommissioned {
            return Err(FleetError::AlreadyDecommissioned);
        }
        Ok(ShipDecommissioned {
            ship_id: cmd.ship_id,
        })
    }
}
```

Notice several patterns:

- **One handler per command per version**. `DepartForStationHandler` handles
  `DepartForStation` at version 1. If a version 2 of `DepartForStation` is introduced, it
  gets its own handler struct at version 2.
- **The handler reads state but does not mutate it.** State mutation happens later, when the
  resulting event is applied through the combiner.
- **Business rules live here.** The `DepartForStationHandler` checks that the ship is docked
  before allowing departure. The `DecommissionShipHandler` checks that the ship is not
  already decommissioned.
- **The return type must match the `produces` declaration** on the corresponding `#[command]`.
  If `#[command(Ship, version = 1, produces = [ShipDeparted])]` is declared, the handler
  must return `Result<ShipDeparted, _>`. Mismatch is a compile error.

### Error types

Each service defines its own error type using `thiserror`. No god error enum, no `anyhow`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum FleetError {
    #[error("ship is not docked")]
    ShipNotDocked,

    #[error("already decommissioned")]
    AlreadyDecommissioned,

    #[error("ship is not in transit")]
    ShipNotInTransit,

    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}
```

### What the macro generates

The `#[command_handler(Ship, version = 1)]` macro generates:

- An `impl CommandHandler<Ship> for DepartForStationHandler` that wraps the user's `handle`
  method.
- A type-erased dispatch function (`CommandDispatchFn`) that can deserialise the command
  payload, hydrate aggregate state from events, call the handler, and serialise the
  resulting event -- all without knowing concrete types at the call site.
- A `CommandHandlerRegistration` submitted to `inventory`, containing the dispatch function,
  the command type name, the command version, and the produced event metadata.

---

## Event combiners

Event combiners are the mechanism by which events mutate aggregate state. They are
synchronous, pure state folding functions -- no I/O, no side effects, no async. One
combiner exists for each event type at each version.

### The EventCombiner trait

The trait is defined in `canon-core/src/traits/event_combiner.rs`:

```rust
pub trait EventCombiner<A>: Send + Sync + 'static {
    fn combine(&self, state: &mut A);
}
```

This is deliberately minimal. The combiner receives a mutable reference to the aggregate
state and applies the event's effect. The event data is `self` -- the combiner is
implemented on the event type itself.

### Defining event combiners

Event combiners are declared with the `#[event_combiner]` macro. They are implemented
as methods on the event struct:

```rust
use crate::events::*;
use crate::aggregate::{Ship, ShipStatus};
use canon_core::event_combiner;

#[event_combiner(Ship, version = 1)]
impl ShipRegistered {
    fn combine(&self, state: &mut Ship) {
        state.name = self.name.clone();
        state.capacity_kg = self.capacity_kg;
        state.status = ShipStatus::Docked;
        state.current_station = self.home_station;
    }
}

#[event_combiner(Ship, version = 1)]
impl ShipDeparted {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::InTransit;
    }
}

#[event_combiner(Ship, version = 1)]
impl ShipDockedAtStation {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::Docked;
        state.current_station = Some(self.station_id);
    }
}

#[event_combiner(Ship, version = 1)]
impl ResupplyScheduled {
    fn combine(&self, state: &mut Ship) {
        state.fuel_kg = self.fuel_kg;
    }
}

#[event_combiner(Ship, version = 1)]
impl ShipDecommissioned {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::Decommissioned;
    }
}
```

A more complex example from the station service shows how combiners can implement
conditional logic:

```rust
#[canon_core::event_combiner(Station, version = 1)]
impl CargoReceived {
    fn combine(&self, state: &mut Station) {
        state.current_stock_kg = (state.current_stock_kg + self.weight_kg).min(state.capacity_kg);
    }
}

#[canon_core::event_combiner(Station, version = 1)]
impl ShipDocked {
    fn combine(&self, state: &mut Station) {
        if !state.docked_ships.contains(&self.ship_id) {
            state.docked_ships.push(self.ship_id);
        }
    }
}
```

### Combiners and notification events

Some events are notifications that trigger downstream processes but do not change aggregate
state. Their combiners are intentionally empty:

```rust
#[canon_core::event_combiner(Station, version = 1)]
impl StationStockLow {
    fn combine(&self, _state: &mut Station) {
        // StationStockLow is a notification event -- no state mutation required.
        // The stock level is already updated by CargoReceived.
    }
}
```

### What the macro generates

The `#[event_combiner(Ship, version = 1)]` macro generates:

- An `impl EventCombiner<Ship> for ShipDeparted` implementation.
- A type-erased apply function (`CombinerApplyFn`) that deserialises the event from
  `Bytes`, downcasts the state to the concrete aggregate type, and calls `combine`.
- An `EventCombinerRegistration` submitted to `inventory`, keyed by
  `(aggregate TypeId, event type name, event version)`.

During hydration, the `__apply_event_combiner` helper looks up the correct combiner in a
lazily-initialised `HashMap` for O(1) dispatch per event.

---

## Event handlers

Event handlers react to events -- either from this service's own aggregate (internal
events) or from other services (external events). Unlike command handlers, event handlers
are **aggregate-agnostic**: they have no aggregate type parameter. An event handler may
listen for events from any service and optionally produce a single command in response.

### The EventHandler trait

The trait is defined in `canon-core/src/traits/event_handler.rs`:

```rust
#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    type Event: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle(
        &self,
        events: Vec<Self::Event>,
    ) -> Result<Option<CommandEnvelope>, Self::Error>;

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        let _ = accumulated;
        Oversight::Ready
    }
}
```

Key differences from `CommandHandler`:

- **No aggregate parameter** -- event handlers are decoupled from any specific aggregate.
- **Receives a batch of events** (`Vec<Self::Event>`) -- when windowing is used, the handler
  receives all events accumulated in the window.
- **Returns `Option<CommandEnvelope>`** -- the handler may produce zero or one command. Never
  more than one. If it returns `None`, no downstream command is emitted.
- **Has an `oversight` method** -- controls whether the accumulated batch is ready for
  dispatch.

### Defining event handlers

Event handlers are declared with the `#[event_handler]` macro. Here is the `ResupplyHandler`
from the fleet service, which reacts to `ResupplyDispatched` events from the supply service:

```rust
use crate::commands::ScheduleResupply;
use crate::inbound::InboundResupplyDispatched as ResupplyDispatched;
use canon_core::{event_handler, AggregateId, CommandEnvelope};

#[event_handler]
impl ResupplyHandler {
    #[handles(ResupplyDispatched, version = 1)]
    fn handle(&self, events: Vec<ResupplyDispatched>) -> Option<CommandEnvelope> {
        let event = events.last()?;

        let command = ScheduleResupply {
            ship_id: event.ship_id,
            fuel_kg: event.fuel_kg,
        };
        let payload = serde_json::to_vec(&command).ok()?;

        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::from_uuid(event.ship_id),
            command_type: "ScheduleResupply".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }
}
```

The `#[handles(ResupplyDispatched, version = 1)]` attribute declares which event type and
version this handler processes. When the framework receives a `ResupplyDispatched` event at
version 1, it routes it to this handler.

### Windowed event handlers

Event handlers can accumulate events over time using `window_ttl`. When `window_ttl` is
set, events are collected in an inbox window keyed by `(handler_id, correlation_key)` until
the `oversight` method returns `Ready`:

```rust
#[event_handler(window_ttl = "30m")]
impl CargoUnloadingHandler {
    #[handles(CargoEvent, version = 1)]
    fn handle(&self, events: Vec<CargoEvent>) -> Option<CommandEnvelope> {
        // Process the full batch of accumulated events
        // ...
    }

    fn correlate(&self, message: &IncomingMessage) -> Uuid {
        // Extract a domain correlation key (e.g., manifest_id)
        // Falls back to envelope correlation_id if not provided
    }

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        // Required when window_ttl is set -- compile error without it
        if all_prerequisites_met(accumulated) {
            Oversight::Ready
        } else {
            Oversight::NotReady
        }
    }
}
```

The compile-time rule is strict: `window_ttl` without `oversight` is a compile error. The
`correlate` method is optional -- if not provided, the framework falls back to the envelope's
`correlation_id`.

### Internal vs external events

Event handlers work identically for both internal and external events:

- **Internal events**: this service's own events are routed back from the outbound queue to
  the inbox by the internal event consumer. The handler receives them as
  `IncomingMessage::InternalEvent`.
- **External events**: events from other services arrive via the adaptor (Kafka consumer).
  The handler receives them as `IncomingMessage::ExternalEvent`.

From the handler's perspective, there is no difference. The framework handles all routing,
deduplication, windowing, and dispatch. Service authors never write manual Kafka consumers
or hand-built `CommandEnvelope` construction for event routing.

---

## Projections

Projections are read models -- materialised views of aggregate state optimised for queries.
They consume events from the outbound queue and build denormalised data structures that are
efficient to read. Unlike aggregates (which are optimised for writes), projections are
optimised for reads.

### The Projection trait

The trait is defined in `canon-core/src/traits/projection.rs`:

```rust
#[async_trait]
pub trait Projection: Send + Sync + 'static {
    type Event: Send + Sync;
    type Store: ProjectionStore;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn apply(&self, event: &Self::Event, store: &Self::Store) -> Result<(), Self::Error>;

    async fn rebuild(
        &self,
        events: impl Stream<Item = Self::Event> + Send,
        store: &Self::Store,
    ) -> Result<(), Self::Error>;

    fn projection_id(&self) -> &str;
}
```

Key properties:

- `apply` **must be idempotent** -- calling it twice with the same event must produce the
  same result. The framework guarantees at-least-once delivery, so projections may see the
  same event more than once (especially after a restart, since consumers restart from offset
  zero).
- `rebuild` replays the full event history through the projection, used when the
  materialised view needs to be reconstructed from scratch.
- `projection_id` returns a unique identifier for checkpoint tracking.

### Defining projections

Projections are declared with the `#[projection]` macro:

```rust
use std::collections::HashMap;
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[canon_core::projection]
pub struct StationInventory {
    pub stations: HashMap<Uuid, StationInventoryRow>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StationInventoryRow {
    pub station_id: Uuid,
    pub name: String,
    pub capacity_kg: f32,
    pub current_stock_kg: f32,
    pub last_docking: Option<DateTime<Utc>>,
    pub offline: bool,
    pub updated_at: DateTime<Utc>,
}
```

The macro generates `Default`, serde derives, and an `inventory` registration
(`ProjectionRegistration`).

### Projection rebuild

Projections support full rebuilds for schema migration or corruption recovery. The rebuild
lifecycle is managed by `ProjectionRebuildManager`:

1. `start_rebuild(projection_id, rebuild_from)` -- sets `rebuilding = true` and resets the
   checkpoint. While rebuilding, read endpoints fall back to read-through queries against
   the event store.
2. The projection consumer resets its offset and replays events through `apply()`.
3. `complete_rebuild(projection_id)` -- sets `rebuilding = false`.

Callers can poll `is_rebuilding(projection_id)` at any time. Gateway read endpoints should
check this and serve stale-safe responses during a rebuild.

---

## Projection handlers

Projection handlers are the building blocks of a projection. Each handler applies one
event type to the projection's read model. They are analogous to event combiners, but for
projections instead of aggregates.

### The ProjectionHandler trait

The trait is defined in `canon-core/src/traits/projection_handler.rs`:

```rust
pub trait ProjectionHandler<P>: Send + Sync + 'static {
    type Event: Send + Sync;
    fn apply(&self, event: &Self::Event, store: &mut P);
}
```

### Defining projection handlers

Projection handlers are declared with the `#[projection_handler]` macro:

```rust
#[canon_core::projection_handler(StationInventory)]
impl StationRegisteredProjectionHandler {
    fn apply(&self, event: &StationRegistered, store: &mut StationInventory) {
        let now = Utc::now();
        store.stations.insert(
            event.station_id,
            StationInventoryRow {
                station_id: event.station_id,
                name: event.name.clone(),
                capacity_kg: event.capacity_kg,
                current_stock_kg: 0.0,
                last_docking: None,
                offline: false,
                updated_at: now,
            },
        );
    }
}

#[canon_core::projection_handler(StationInventory)]
impl CargoReceivedProjectionHandler {
    fn apply(&self, event: &CargoReceived, store: &mut StationInventory) {
        if let Some(row) = store.stations.get_mut(&event.station_id) {
            row.current_stock_kg += event.weight_kg;
            row.updated_at = Utc::now();
        }
    }
}

#[canon_core::projection_handler(StationInventory)]
impl ShipDockedProjectionHandler {
    fn apply(&self, event: &ShipDocked, store: &mut StationInventory) {
        if let Some(row) = store.stations.get_mut(&event.station_id) {
            row.last_docking = Some(Utc::now());
            row.updated_at = Utc::now();
        }
    }
}
```

Projection handlers must be defensive -- events may arrive for entities that do not yet
exist in the projection (e.g., a `ShipDocked` event for a station that has not been
registered yet). The pattern `if let Some(row) = store.get_mut(...)` handles this gracefully.

---

## The dispatcher

The dispatcher is the central routing component in Canon. It bridges the gap between
command submission (gateway writes a `CommandEnvelope` to the inbox) and event production
(an `EventEnvelope` appears in the outbox for downstream processing). Without the
dispatcher, commands sit in the inbox forever.

### Dispatch flow

The dispatcher follows this sequence for each command:

```text
inbox_messages row (CommandEnvelope)
  -> read command_type + command_version from envelope
  -> look up handler in the dispatch map: HashMap<(String, u32), CommandDispatchFn>
  -> load events for the aggregate (from event store / snapshot store)
  -> hydrate aggregate state via version-matched event combiners
  -> call handler.handle(state, command) via the type-erased dispatch function
  -> serialize resulting event into EventEnvelope
  -> BEGIN TRANSACTION
       INSERT INTO outbox (aggregate_id, payload = event_envelope)
       DELETE FROM inbox_messages (mark processed)
     COMMIT
```

### The DispatcherStore trait

The dispatcher is generic over `DispatcherStore`, which abstracts database operations:

```rust
#[async_trait]
pub trait DispatcherStore: Send + Sync + 'static {
    async fn poll_inbox(&self, batch_size: usize) -> Result<Vec<InboxCommandRow>, DispatcherError>;

    async fn load_events(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Vec<EventEnvelope>, DispatcherError>;

    async fn write_outbox_and_mark_processed(
        &self,
        message_id: Uuid,
        handler_id: &str,
        envelope: EventEnvelope,
    ) -> Result<(), DispatcherError>;

    async fn record_failure(
        &self,
        message_id: Uuid,
        handler_id: &str,
        error: &str,
    ) -> Result<u32, DispatcherError>;

    async fn dead_letter(
        &self,
        row: &InboxCommandRow,
        error: &str,
        attempts: u32,
    ) -> Result<(), DispatcherError>;
}
```

The critical method is `write_outbox_and_mark_processed` -- it performs the outbox write
and inbox cleanup in a single ACID transaction. This is what makes the outbox pattern work:
the event is committed to the outbox and the command is marked as processed atomically.
Either both happen or neither does.

### Batch processing and error handling

The dispatcher processes commands in batches. For each batch:

1. Poll the inbox for up to `batch_size` unprocessed commands.
2. For each command, attempt to process it.
3. On success, notify the outbox processor that new entries are available.
4. On failure, record the failure via `record_failure`. If the retry count exceeds
   `max_retries` (default 3), the command is dead-lettered.

### The dispatcher loop

The dispatcher runs as a background `tokio::spawn` task. It supports two wake mechanisms:

- **Poll interval** -- sleeps for `poll_interval_ms` (default 50ms) between empty batches.
- **Notification channel** -- when a `DispatcherNotifyReceiver` is configured, the
  dispatcher wakes immediately when new commands are written to the inbox, instead of
  waiting for the next poll cycle.

Both mechanisms integrate with a `tokio::sync::watch` shutdown channel for graceful
termination.

### Type-erased dispatch

The dispatcher does not know concrete aggregate, command, or event types. It works entirely
through type-erased function pointers registered via `inventory`. The dispatch flow is:

1. Read `command_type` and `command_version` from the `CommandEnvelope`.
2. Look up the `CommandDispatchFn` in a `HashMap<(String, u32), CommandDispatchFn>`,
   lazily initialised from `CommandHandlerRegistration` entries.
3. Call the dispatch function with the raw payload bytes, the event history, and the
   aggregate `TypeId`.
4. The dispatch function (generated by the `#[command_handler]` macro) internally:
   - Deserialises the command payload into the concrete command type.
   - Creates a default aggregate state and hydrates it from the event history using
     version-matched combiners.
   - Calls the user's `handle` method.
   - Serialises the resulting event and returns it as `HandlerDispatchResult`.

This design means the dispatcher is a single, reusable component that works with any
aggregate and any set of commands -- all wiring is discovered automatically at startup.

---

## Version-matched routing

Version-matched routing is Canon's approach to schema evolution. There is no upcasting, no
downcasting, and no migration scripts. Instead, each version of an event or command is
processed by the handler or combiner registered at that exact version.

### How it works

Every stored `EventEnvelope` carries an `event_type` (e.g., `"ShipDeparted"`) and an
`event_version` (e.g., `1` or `2`). Every stored `CommandEnvelope` carries a `command_type`
and `command_version`. These pairs are the dispatch keys.

When the framework needs to process an event or command, it:

1. Reads the type name and version from the envelope metadata.
2. Looks up the registered handler in a `HashMap` keyed by `(type_name, version)`.
3. Calls the handler with the raw payload bytes.
4. The handler deserialises the payload using the schema that matches its version.

### The combiner dispatch map

Combiner dispatch uses a lazily-initialised `HashMap` keyed by
`(TypeId, String, u32)` -- the aggregate's `TypeId`, the event type name, and the event
version. Each entry maps to a `CombinerApplyFn`:

```rust
type CombinerApplyFn =
    fn(&[u8], &mut dyn Any) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
```

The `__apply_event_combiner` function looks up the combiner for each `EventEnvelope` and
applies it to the aggregate state:

```rust
pub fn __apply_event_combiner(
    aggregate_type_id: TypeId,
    envelope: &EventEnvelope,
    state: &mut dyn Any,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let key = (aggregate_type_id, envelope.event_type.clone(), envelope.event_version);
    match map.get(&key) {
        Some(apply_fn) => apply_fn(envelope.payload.as_ref(), state),
        None => Err("no event combiner registered for ..."),
    }
}
```

### The command handler dispatch map

Command handler dispatch uses a `HashMap` keyed by `(String, u32)` -- the command type
name and version. Each entry maps to a `CommandDispatchFn`:

```rust
type CommandDispatchFn =
    fn(
        command_payload: &[u8],
        events: &[EventEnvelope],
        aggregate_type_id: TypeId,
    ) -> Result<HandlerDispatchResult, Box<dyn std::error::Error + Send + Sync>>;
```

### Why this matters

This approach has several advantages over traditional schema migration:

- **Old events are never rewritten.** They stay in the store exactly as they were recorded.
- **New versions coexist with old versions.** A system can process events from version 1 and
  version 2 simultaneously during hydration.
- **No downtime for migration.** Adding a new event version is a code change, not a data
  migration.
- **Counterfactual replay works naturally.** Stored commands carry their `command_version`,
  so the replay engine routes them to the correct handler version without any special logic.

---

## IncomingMessage

The inbox handles three types of incoming messages, represented by the `IncomingMessage`
enum in `canon-core/src/types.rs`:

```rust
pub enum IncomingMessage {
    Command(CommandEnvelope),
    InternalEvent(EventEnvelope),
    ExternalEvent(EventEnvelope),
}
```

- `Command` -- a new command submitted by a user via the gateway API, or a downstream
  command produced by an event handler.
- `InternalEvent` -- this service's own events, routed back from the outbound queue by the
  internal event consumer. Used for internal event handler dispatch.
- `ExternalEvent` -- events from other services, arriving via the adaptor's Kafka consumer.

`IncomingMessage` provides convenience accessors:

```rust
impl IncomingMessage {
    pub fn message_id(&self) -> Uuid { ... }
    pub fn aggregate_id(&self) -> &AggregateId { ... }
}
```

These are used by the inbox for deduplication (keyed by `handler_id + message_id`) and
routing (keyed by `aggregate_id` for Kafka partitioning).

---

## Oversight

Oversight controls whether a windowed event handler's accumulated batch is ready for
dispatch. It is defined in `canon-core/src/types.rs`:

```rust
pub enum Oversight {
    Ready,    // dispatch accumulated batch to queue now
    NotReady, // wait for more messages
    Discard,  // abandon this accumulation window entirely
}
```

The three variants represent:

- **Ready** -- all prerequisites are met. The inbox dispatches the accumulated batch to the
  inbound queue, and the event handler's `handle` method will be called with the full batch.
- **NotReady** -- some prerequisites are still missing. The inbox holds the window open and
  waits for more messages.
- **Discard** -- the window should be abandoned. All accumulated messages are discarded
  without processing.

### Window lifecycle

Inbox windows follow this lifecycle, tracked by `WindowStatus`:

```text
Pending -> Dispatched    (Oversight::Ready -- batch published, window cleared)
Pending -> Expired       (TTL exceeded -- moved to dead letter by cleanup task)
Expired -> DeadLettered  (cleanup task moved to dead letter store)
```

The window key is `(handler_id, correlation_key)`, where `correlation_key` comes from the
handler's `correlate` method or falls back to the envelope's `correlation_id`. Each unique
correlation key is an independent window -- a handler may have many concurrent in-flight
windows.

---

## Counterfactual replay

Counterfactual replay is Canon's "what-if" simulation capability. It answers the question:
"What would have happened if a different command had been issued at a specific point in the
aggregate's history?"

### The request/response types

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

### The CounterfactualReplay trait

```rust
#[async_trait]
pub trait CounterfactualReplay: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn replay(
        &self,
        request: CounterfactualRequest,
    ) -> Result<CounterfactualResult, Self::Error>;
}
```

### How it works

The replay engine operates on **commands, not events**:

1. Load the command history for the aggregate from the command store.
2. Hydrate aggregate state to the branch point (`branch_version`) using events from the
   `ReplayEventStore` -- a read-only event store pointing at a read replica, separate from
   the live event store.
3. Substitute the command at the branch point with `substituted_command`.
4. Re-run command handlers forward from the branch point, using version-matched dispatch
   (each stored command carries its `command_version`).
5. Diff the original commands against the counterfactual commands via `CommandDiff`.

The result shows which commands would have been added, removed, or stayed unchanged.

---

## Snapshots

Snapshots are point-in-time serialisations of aggregate state, used to avoid replaying the
full event history on every load:

```rust
pub struct Snapshot {
    pub aggregate_id: AggregateId,
    pub version: Version,
    pub state: Bytes,
    pub taken_at: DateTime<Utc>,
}
```

Snapshots are taken by the event store consumer, not the command handler. After a confirmed
Cassandra write, the consumer checks `version % N == 0` (where `N` is the `snapshot_every`
value from the `#[aggregate]` macro). If it matches, a snapshot is serialised and written to
the snapshot store.

During hydration, the dispatcher first checks for a snapshot. If one exists, it loads
events from the snapshot's version forward (instead of from version zero), deserialises the
snapshot state, and applies only the newer events.

---

## Dead letters

When a command or event fails processing beyond the configured retry limit, it is moved to
the dead letter store:

```rust
pub struct DeadLetter {
    pub id: Uuid,
    pub message_id: Uuid,
    pub handler_id: String,
    pub aggregate_id: AggregateId,
    pub payload: Bytes,
    pub error: String,
    pub attempts: u32,
    pub created_at: DateTime<Utc>,
    pub last_attempted: DateTime<Utc>,
}
```

Retry counts are tracked in a `retry_attempts` table (crash-safe). When the dispatcher's
`record_failure` returns a count exceeding `max_retries` (default 3), the message is
dead-lettered via `dead_letter`. An admin API allows requeuing dead-lettered messages back
into the inbox with a fresh `expires_at`.

Window expiry also produces dead letters: if an oversight window's TTL elapses before
`Ready` is returned, the window transitions to `Expired` and its messages are moved to the
dead letter store with reason `window_expired`.

---

## The outbox pattern

Canon uses the outbox pattern to guarantee that commands and their resulting events are
committed atomically. This is the core consistency mechanism.

The write path:

1. The command handler produces an event.
2. The dispatcher writes both the command (to the commands table) and the event (to the
   outbox table) in a **single YugabyteDB ACID transaction**.
3. The outbox processor, running as a background task, polls the outbox for undelivered
   entries (`SELECT ... FOR UPDATE SKIP LOCKED`).
4. For each entry, the processor publishes the event envelope to the outbound Kafka queue.
5. After confirmed publish, it marks the outbox row as delivered (`SET delivered_at = now()`).

The outbox is the commit point. If the transaction succeeds, the event is guaranteed to
eventually be published. If the transaction fails, neither the command nor the event is
persisted.

The outbox processor has a single responsibility: drain the outbox to the outbound queue.
It does **not** write to Cassandra, trigger projections, or publish to external topics.
Those are handled by the three independent outbound queue consumers.

---

## Service lifecycle

A Canon service is assembled by `ServiceBuilder` and started via `service.start()`. The
builder discovers all macro-generated registrations via `inventory` and validates
exhaustiveness (every command has a handler, every event has a combiner) before creating the
service.

```rust
let service = ServiceBuilder::new("fleet")
    .for_aggregate::<Ship>()
    .event_store(event_store)
    .snapshot_store(snapshot_store)
    .command_store(command_store)
    .dead_letter_store(dead_letter_store)
    .outbox_store(outbox_store)
    .outbox_publisher(outbox_publisher)
    .publisher(publisher)
    .build()?;

service.start(shutdown_rx).await;
```

Calling `start()` spawns all background tasks:

- **Dispatcher** -- polls the inbox and processes commands.
- **Outbox processor** -- drains the outbox to the outbound queue.
- **Event store consumer** -- writes events to Cassandra, takes snapshots.
- **Projection consumer** -- applies events to read models.
- **Publisher consumer** -- publishes events to `canon.{service}.events` for other services.
- **Internal event consumer** -- routes the service's own events back to the inbox for event
  handler dispatch.

Each task runs as a `tokio::spawn` with graceful shutdown via a `tokio::sync::watch`
channel. When the shutdown signal fires, all tasks drain their current work and exit
cleanly.
