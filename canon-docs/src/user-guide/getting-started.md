# Getting Started

This guide walks you through building your first event-sourced service with Canon,
from workspace setup through running commands and querying projections. Every code
example is drawn from the fleet-service in `canon-demo/`, the reference implementation
that exercises the full framework.

By the end of this guide you will have:

- A working aggregate with commands, events, and state
- Command handlers that enforce business rules
- Event combiners that fold events into aggregate state
- A projection that builds a queryable read model
- Everything wired together with `ServiceBuilder`

---

## Prerequisites

Before you begin, make sure the following are installed:

**Rust toolchain (1.75+ stable)**

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update stable
```

**Docker** (for running infrastructure locally via minikube)

Docker Desktop or a compatible runtime. Required for YugabyteDB, Cassandra, and
Kafka when you move beyond in-memory testing.

**minikube** (for the full demo stack)

```bash
brew install minikube   # macOS
minikube start --cpus=4 --memory=8g
```

**Cross-compilation target** (if deploying to Kubernetes)

```bash
rustup target add aarch64-unknown-linux-musl
brew install filosottile/musl-cross/musl-cross
```

You do not need Docker or minikube to follow this guide. Everything through
the testing section uses Canon's in-memory implementations, which run without
any external infrastructure.

---

## Project structure

Canon is designed as a Cargo workspace. A typical Canon project looks like this:

```
my-project/
  Cargo.toml              # workspace root
  my-service/
    Cargo.toml
    src/
      aggregate.rs         # aggregate struct + #[aggregate] macro
      commands.rs          # command structs + #[command] macro
      events.rs            # event structs + #[event] macro
      combiners.rs         # #[event_combiner] impls
      handlers.rs          # #[command_handler] impls
      projection.rs        # #[projection] + #[projection_handler] impls
      error.rs             # thiserror error types
      lib.rs               # module declarations
      main.rs              # service wiring + ServiceBuilder
```

Each service owns its own aggregate, commands, events, handlers, and projections.
Cross-service communication happens exclusively through Kafka events.

---

## Adding Canon to your project

Add `canon-core` to your service's `Cargo.toml`. Canon is currently distributed as
path dependencies within a workspace:

```toml
[dependencies]
canon-core = { path = "../canon-core" }
uuid = { version = "1", features = ["v4", "serde"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
bytes = "1"
chrono = { version = "0.4", features = ["serde"] }
tokio = { version = "1", features = ["full"] }
```

Crates.io publication is planned for a future release.

---

## Step 1: Define the aggregate

An aggregate is the consistency boundary in your domain -- the unit of transactional
integrity. All commands targeting the same aggregate instance are serialised, and
all events it produces are versioned in sequence.

The fleet-service models ships. Here is the `Ship` aggregate from
`canon-demo/fleet-service/src/aggregate.rs`:

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

The `#[aggregate]` macro generates:

- `impl Aggregate for Ship` with `type State = Ship` -- the aggregate struct is its
  own state. This is an opinionated design choice; there is no separate state object.
- A `Default` implementation where all fields start at their zero values.
- Serde derives for serialisation to and from the event store.
- An `inventory` registration so `ServiceBuilder` discovers it automatically at startup.
- Version-matched hydration dispatch that routes stored events to the correct
  `#[event_combiner]` based on event type and version.

The `snapshot_every = 50` attribute tells Canon to write a snapshot of the aggregate
state every 50 events. When hydrating, if a snapshot exists, Canon loads it and replays
only the events after the snapshot version instead of replaying from event zero.

---

## Step 2: Define commands

Commands represent intent -- what a caller wants to happen. Each command targets a
specific aggregate, is versioned, and declares which event it produces on success.

From `canon-demo/fleet-service/src/commands.rs`:

```rust
use uuid::Uuid;

#[canon_core::command(Ship, version = 1, produces = [ShipRegistered])]
pub struct RegisterShip {
    pub name: String,
    pub capacity_kg: f32,
    #[serde(default)]
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

Key points about commands:

- **`version = 1`** -- the schema version of this command. Defaults to 1 if omitted. When
  you need to evolve a command's shape, create a new struct at version 2. The framework
  uses `command_version` from the stored envelope to route to the matching handler.

- **`produces = [ShipRegistered]`** -- declarative metadata documenting which event type
  the handler returns. This is enforced at compile time: the handler's return type must
  match. A command produces exactly one event on success or returns `Err` on rejection.

- **Every command must have a matching handler.** If you define `#[command(Ship, version = 1)]`
  for `RegisterShip` but forget to write `#[command_handler(Ship, version = 1)]`, the
  compiler produces an error.

---

## Step 3: Define events

Events are facts -- immutable records of what happened. Each event belongs to an
aggregate and is versioned. Every event must have a matching `#[event_combiner]`.

From `canon-demo/fleet-service/src/events.rs`:

```rust
use uuid::Uuid;

#[canon_core::event(Ship, version = 1)]
pub struct ShipRegistered {
    pub ship_id: Uuid,
    pub name: String,
    pub capacity_kg: f32,
    #[serde(default)]
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

#[canon_core::event(Ship, version = 1)]
pub struct ShipDockedAtStation {
    pub ship_id: Uuid,
    pub station_id: Uuid,
}
```

Events are serialised to bytes and stored in `EventEnvelope` records. The `event_type`
(derived from the struct name) and `event_version` fields are stored alongside the
payload. During hydration, these fields determine which `#[event_combiner]` to invoke.

**Event versioning**: you can define multiple versions of the same conceptual event
as separate types. For example, if `ShipDeparted` gains a new field in v2:

```rust
// v1 -- original shape
#[canon_core::event(Ship, version = 1)]
pub struct ShipDeparted {
    pub ship_id: Uuid,
    pub destination: Uuid,
    pub fuel_at_departure: f32,
}

// v2 -- adds estimated_arrival
#[canon_core::event(Ship, version = 2)]
pub struct ShipDepartedV2 {
    pub ship_id: Uuid,
    pub destination: Uuid,
    pub fuel_at_departure: f32,
    pub estimated_arrival: DateTime<Utc>,
}
```

Both versions coexist. Old events stored at v1 are routed to the v1 combiner; new
events stored at v2 are routed to the v2 combiner. No migration of historical data
is required.

---

## Step 4: Write event combiners

Event combiners are pure, synchronous state folding functions. Each one takes a
reference to the event and a mutable reference to the aggregate state, and applies
the event's changes. There must be exactly one combiner per event type per version.

From `canon-demo/fleet-service/src/combiners.rs`:

```rust
use crate::events::*;
use canon_core::event_combiner;
use crate::aggregate::{Ship, ShipStatus};

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
impl ShipDecommissioned {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::Decommissioned;
    }
}

#[event_combiner(Ship, version = 1)]
impl ShipDockedAtStation {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::Docked;
        state.current_station = Some(self.station_id);
    }
}
```

Combiners are used internally by `Aggregate::hydrate()`. When the framework loads
an aggregate, it replays all stored events (or events since the last snapshot) by
reading each envelope's `event_type` and `event_version`, then dispatching to the
matching combiner. The result is a fully hydrated aggregate state.

Combiners must be:

- **Pure** -- no side effects, no I/O, no async. They only mutate the state parameter.
- **Deterministic** -- given the same event and state, they always produce the same result.
- **Total** -- every `#[event]` must have a combiner. Missing one is a compile error.

---

## Step 5: Write command handlers

Command handlers contain business logic. They receive the current aggregate state
(hydrated from events) and the incoming command, then return either a single event
(success) or an error (rejection). Rejection is always `Err`, never a separate event type.

From `canon-demo/fleet-service/src/handlers.rs`:

```rust
use crate::commands::*;
use crate::events::*;
use canon_core::command_handler;
use crate::aggregate::{Ship, ShipStatus};
use crate::error::FleetError;

#[command_handler(Ship, version = 1)]
impl RegisterShipHandler {
    type Error = FleetError;

    fn handle(
        &self,
        _state: &Ship,
        cmd: RegisterShip,
    ) -> Result<ShipRegistered, FleetError> {
        Ok(ShipRegistered {
            ship_id: uuid::Uuid::new_v4(),
            name: cmd.name,
            capacity_kg: cmd.capacity_kg,
            home_station: cmd.home_station,
        })
    }
}

#[command_handler(Ship, version = 1)]
impl DepartForStationHandler {
    type Error = FleetError;

    fn handle(
        &self,
        state: &Ship,
        cmd: DepartForStation,
    ) -> Result<ShipDeparted, FleetError> {
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

    fn handle(
        &self,
        state: &Ship,
        cmd: DecommissionShip,
    ) -> Result<ShipDecommissioned, FleetError> {
        if state.status == ShipStatus::Decommissioned {
            return Err(FleetError::AlreadyDecommissioned);
        }
        Ok(ShipDecommissioned {
            ship_id: cmd.ship_id,
        })
    }
}
```

Notice how `DepartForStationHandler` checks `state.status` before allowing departure.
This is the core pattern: the handler reads the current state, validates the command
against business rules, and either produces an event or rejects the command with an error.

The handler's return type must match the event declared in `produces` on the corresponding
`#[command]`. If `DepartForStation` declares `produces = [ShipDeparted]` but the handler
returns `Result<ShipRegistered, FleetError>`, the compiler rejects it.

---

## Step 6: Define your error type

Each service defines its own error types using `thiserror`. Canon requires `thiserror`
in every crate -- no `anyhow`, no god error enum.

From `canon-demo/fleet-service/src/error.rs`:

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

Error variants represent domain rejections -- states where the command cannot be
fulfilled. These are distinct from infrastructure errors (database failures, network
timeouts), which are handled by the framework layer.

---

## Step 7: Wire it up with ServiceBuilder

`ServiceBuilder` auto-discovers all registered handlers, combiners, and projections
via `inventory`. It validates exhaustiveness at build time and creates a runnable
`Service` that manages all background pipeline tasks.

Here is a simplified version of the fleet-service's `main.rs`:

```rust
use canon_core::{ServiceBuilder, EventPayloadSnapshotProvider};
use fleet_service::aggregate::Ship;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to infrastructure
    let event_store = /* CassandraEventStore or InMemory */;
    let snapshot_store = /* YugabyteSnapshotStore or InMemory */;
    let outbox_store = /* YugabyteOutboxStore or InMemory */;
    let outbox_publisher = /* KafkaOutboundProducer or InMemory */;
    let projection_store = /* YugabyteProjectionStore or InMemory */;
    let publisher = /* KafkaPublisher or InMemory */;
    let dead_letter_store = /* YugabyteDeadLetterStore or InMemory */;
    let retry_tracker = /* YugabyteRetryTracker or InMemory */;

    // Build the service
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
        .build()?;

    // Start background pipeline tasks
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    service.start(shutdown_rx, None, es_receiver, proj_receiver, pub_receiver).await;

    Ok(())
}
```

`ServiceBuilder::build()` performs compile-time-equivalent validation:

- Checks that every registered `#[command]` has a matching `#[command_handler]`.
- Checks that every registered `#[event]` has a matching `#[event_combiner]`.
- Verifies that all required infrastructure components are provided.

If any check fails, `build()` returns `ServiceBuilderError` with a clear message
identifying the missing handler or component.

`service.start()` spawns four independent background tasks:

1. **Outbox processor** -- drains the outbox table and publishes to the outbound Kafka topic.
2. **Event store consumer** -- reads from the outbound topic, writes to Cassandra,
   takes snapshots when `version % N == 0`.
3. **Projection consumer** -- reads from the outbound topic, applies events to
   projection read models.
4. **Publisher consumer** -- reads from the outbound topic, publishes to the
   cross-service events topic for other services.

Each task runs independently and recovers from failures by restarting from its
last checkpoint. Graceful shutdown is coordinated through a `watch` channel.

---

## Step 8: The command write path

When a command arrives (via REST API, Kafka inbound topic, or event handler),
the framework follows this exact path:

1. **Deserialise** the `CommandEnvelope` -- extract `command_type`, `command_version`,
   `aggregate_id`, and `payload`.
2. **Hydrate** the aggregate -- load the latest snapshot (if any), then replay events
   from the event store since that snapshot version. Each event is routed to its
   version-matched `#[event_combiner]`.
3. **Dispatch** to the matching `#[command_handler]` -- the framework reads
   `command_version` and routes to the handler registered at that exact version.
4. **Execute** the handler -- pass the hydrated state and deserialised command.
   The handler returns `Ok(event)` or `Err(rejection)`.
5. **Write** in a single YugabyteDB ACID transaction: `INSERT INTO commands (...)`
   and `INSERT INTO outbox (...) x N`. The outbox is the commit point -- if this
   transaction succeeds, the event is guaranteed to be published.
6. **Outbox processor** picks up the new outbox entry and publishes the event
   envelope to the outbound Kafka topic.
7. **Three consumers** independently process the event: event store (Cassandra),
   projection (read model update), and publisher (cross-service distribution).

The entire write path from command receipt to outbox commit is synchronous within
the dispatcher. The downstream propagation (Kafka, Cassandra, projections) is
asynchronous and eventually consistent.

---

## Step 9: Add a projection

Projections build queryable read models from the event stream. They are covered
in depth in the [Projections](./projections.md) chapter, but here is a quick
example from the fleet-service.

The `ShipReadModel` projection from `canon-demo/fleet-service/src/projection.rs`:

```rust
use crate::events::{
    ShipRegistered, ShipDeparted, ShipDecommissioned,
    ShipDockedAtStation, ResupplyScheduled, RouteAssigned,
};

#[canon_core::projection]
pub struct ShipReadModel {
    pub ship_id: uuid::Uuid,
    pub name: String,
    pub capacity_kg: f32,
    pub status: String,
    pub fuel_kg: f32,
}

#[canon_core::projection_handler(ShipReadModel)]
impl ShipRegisteredProjectionHandler {
    fn apply(&self, event: &ShipRegistered, store: &mut ShipReadModel) {
        store.ship_id = event.ship_id;
        store.name = event.name.clone();
        store.capacity_kg = event.capacity_kg;
        store.status = "Docked".to_string();
        store.fuel_kg = event.capacity_kg;
    }
}

#[canon_core::projection_handler(ShipReadModel)]
impl ShipDepartedProjectionHandler {
    fn apply(&self, event: &ShipDeparted, store: &mut ShipReadModel) {
        let _ = event;
        store.status = "InTransit".to_string();
    }
}

#[canon_core::projection_handler(ShipReadModel)]
impl ShipDockedAtStationProjectionHandler {
    fn apply(&self, event: &ShipDockedAtStation, store: &mut ShipReadModel) {
        let _ = event;
        store.status = "Docked".to_string();
    }
}
```

Each `#[projection_handler]` applies one event type to the read model. The projection
consumer calls these handlers as events flow through the outbound queue, maintaining
a queryable snapshot of each ship's current state.

---

## Step 10: Module declarations

Tie everything together in `lib.rs`:

```rust
pub mod aggregate;
pub mod combiners;
pub mod commands;
pub mod error;
pub mod event_handlers;
pub mod events;
pub mod handlers;
pub mod inbound;
pub mod projection;
```

This is the complete module structure from `canon-demo/fleet-service/src/lib.rs`.
The module declarations ensure that all macro-generated registrations are linked
into the binary. If you forget to declare a module, its `#[command_handler]` or
`#[event_combiner]` registrations will not be discovered by `ServiceBuilder`.

---

## Compile-time safety

Canon enforces completeness at compile time through marker traits emitted by the macros:

- Every `#[command(X, version = N)]` must have a matching `#[command_handler(X, version = N)]` --
  compile error if missing.
- Every `#[event(X, version = N)]` must have a matching `#[event_combiner(X, version = N)]` --
  compile error if missing.
- The `#[command_handler]` return type must be the single event type named in `produces` --
  compile error if mismatched.
- `window_ttl` on an `#[event_handler]` without an `oversight` function -- compile error.
- `#[event_handler]` and `#[projection_handler]` are optional (the compiler warns for
  unhandled new event versions but does not reject them).

If you define a command without its handler, the build fails with a clear message
identifying which handler is missing and at which version.

---

## Running the demo

To see Canon in action with a fully wired system:

```bash
cd canon-demo
make k8s-up          # start minikube + infrastructure + all services
minikube tunnel      # expose frontend at localhost:80
```

This deploys YugabyteDB, Cassandra, Kafka, all five demo services, the gateway,
and the frontend. The gateway bootstraps the game state on startup (4 stations,
1 ship) and the frontend connects via WebSocket for live event updates.

To run the in-memory test suite (no Docker required):

```bash
cargo test --workspace
```

This runs the `canon-test` harness, which wires all `InMemory*` implementations
into a real `Service` via `ServiceBuilder` and exercises the full pipeline: command
dispatch, outbox processing, event store writes, projection updates, and publisher
distribution.

---

## Next steps

- [Core Concepts](./core-concepts.md) -- understand aggregates, events, and the pipeline in depth
- [Macros Reference](./macros-reference.md) -- complete reference for all 8 macros
- [Event Handlers](./event-handlers.md) -- react to events and produce commands
- [Projections](./projections.md) -- build queryable read models from events
- [Testing](./testing.md) -- test your domain with the in-memory harness
