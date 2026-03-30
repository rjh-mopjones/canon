# Core Concepts

This chapter covers the fundamental building blocks of Canon: aggregates, commands, events,
the event envelope, version-matched routing, and the overall event sourcing pipeline.

## Aggregates

An aggregate is the consistency boundary in event sourcing. It is the unit of state that
Canon loads, validates commands against, and persists events for. Each aggregate has:

- **State** -- the current materialised state, reconstructed by replaying events
- **A version** -- an incrementing counter used for optimistic concurrency
- **An ID** -- an `AggregateId(Uuid)` newtype that uniquely identifies the instance

In Canon, the aggregate struct *is* its own state. There is no separate state type --
this is an opinionated design decision that keeps things simple.

```rust
#[aggregate(snapshot_every = 50)]
pub struct Ship {
    status: ShipStatus,
    fuel_level: f32,
    current_station: Option<StationId>,
}
```

The `Aggregate` trait that the macro generates:

```rust
pub trait Aggregate: Sized + Send + Sync + 'static {
    type State: Default + Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    fn hydrate(
        state: &mut Self::State,
        events: impl Iterator<Item = EventEnvelope>,
    ) -> Result<(), Self::Error>;
}
```

### Hydration

Hydration is the process of reconstructing aggregate state from stored events. Canon's
hydration strategy:

1. Load the most recent snapshot for the aggregate (if any)
2. Load events from the snapshot version forward
3. Apply events via version-matched `#[event_combiner]` dispatch
4. Return the current state

If no snapshot exists, all events are replayed from version zero. Snapshotting is
optional but critical for aggregates with long event histories.

## Commands

Commands represent intent -- what a user or system wants to happen. They are versioned
and declare which event they produce:

```rust
#[command(Ship, version = 1, produces = [ShipDeparted])]
pub struct DepartForStation {
    pub destination: StationId,
}
```

Key properties:
- `version` defaults to 1 if omitted
- `produces` is declarative metadata documenting the event type the handler returns
- Each command must have exactly one matching `#[command_handler]` at the same version
- A command produces exactly one event or returns an error

### CommandEnvelope

Commands are wrapped in a `CommandEnvelope` for transport and storage:

```rust
pub struct CommandEnvelope {
    pub command_id: Uuid,
    pub aggregate_id: AggregateId,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub payload: Bytes,
    pub command_version: u32,
}
```

The `command_version` field is critical -- it enables version-matched routing during
counterfactual replay.

## Events

Events are facts -- immutable records of what happened. They are also versioned:

```rust
#[event(Ship, version = 1)]
pub struct ShipDeparted {
    pub destination: StationId,
}
```

Events evolve by registering new versions as separate types:

```rust
#[event(Ship, version = 2)]
pub struct ShipDeparted {
    pub destination: StationId,
    pub fuel_at_departure: f32,
}
```

Version 1 and version 2 coexist. During hydration, the framework reads `event_version`
from each stored envelope and dispatches to the combiner registered at that exact version.

### EventEnvelope

Every event in the store is wrapped in an `EventEnvelope`:

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

- `correlation_id` threads through an entire causal chain -- from the originating command
  to every downstream command it triggers
- `causation_id` identifies the immediate cause (which command produced this event)
- `event_version` enables version-matched routing during hydration

## Version-matched routing

There is no upcasting or downcasting in Canon. Event and command schemas evolve by
registering new versions. During hydration, the framework reads `event_version` from
each stored `EventEnvelope` and dispatches to the combiner registered at that exact
version. Each version is processed at the schema it was stored with.

This means:
- Old events are always replayed with their original combiner
- New event versions get new combiners
- No migration scripts needed for existing events
- The aggregate state accumulates changes from all versions naturally

## AggregateId

Canon uses a newtype wrapper around `Uuid` for aggregate identification:

```rust
pub struct AggregateId(Uuid);
```

This is never generic, never a plain `Uuid`. It provides:
- `AggregateId::new()` -- generate a new random ID
- `AggregateId::from_uuid(uuid)` -- wrap an existing UUID
- `aggregate_id.as_uuid()` -- access the inner UUID

## Version

Version tracking enables optimistic concurrency:

```rust
pub struct Version(u64);

impl Version {
    pub fn initial() -> Self { Self(0) }
    pub fn next(self) -> Self { Self(self.0 + 1) }
    pub fn as_u64(&self) -> u64 { self.0 }
}
```

The event store rejects writes where the expected version does not match the stored
version, preventing lost updates from concurrent writers.

## The pipeline in detail

The full message processing pipeline:

1. **External event arrives** -- another service publishes an event to Kafka
2. **Adaptor** -- `canon-adaptor-kafka` consumes the event and submits it to the inbox
3. **Inbox** -- deduplicates, assembles into a window, evaluates oversight
4. **Inbound queue** -- once the window is ready, the batch is published to Kafka
5. **Dispatcher** -- consumes from the inbound queue, routes by message type
6. **Command handler** -- validates against aggregate state, produces an event
7. **YugabyteDB transaction** -- atomically writes the command and event(s) to the outbox
8. **Outbox processor** -- drains the outbox to the outbound Kafka queue
9. **Event store consumer** -- writes to Cassandra, optionally writes a snapshot
10. **Projection consumer** -- updates read models in YugabyteDB
11. **Publisher** -- publishes to `canon.{service}.events` for other services

Every stage is independently deployable and independently recoverable. Consumers restart
from offset zero and rely on downstream idempotency.

## IncomingMessage

The inbox handles three types of incoming messages:

```rust
pub enum IncomingMessage {
    Command(CommandEnvelope),
    InternalEvent(EventEnvelope),
    ExternalEvent(EventEnvelope),
}
```

- `Command` -- a new command submitted by a user or API
- `InternalEvent` -- this service's own events routed back for event handler dispatch
- `ExternalEvent` -- events from other services arriving via the adaptor
