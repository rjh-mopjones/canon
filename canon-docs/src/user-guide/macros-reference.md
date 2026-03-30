# Macros Reference

Canon provides eight proc-macros that cover the entire domain authoring experience. Users
never implement the underlying traits directly -- the macros generate all trait impls,
dispatch logic, and `inventory` registrations.

## Overview

| Macro | Purpose | Generates |
|-------|---------|-----------|
| `#[aggregate]` | Define an aggregate and its state | `Aggregate` impl, `Default`, serde, `inventory` registration |
| `#[command]` | Declare a command type | Serde derives, version metadata, compile-time wiring |
| `#[event]` | Declare an event type | Serde derives, version metadata, compile-time wiring |
| `#[event_combiner]` | State folding for an event | `EventCombiner` impl |
| `#[command_handler]` | Business logic for a command | `CommandHandler` impl, `inventory` registration |
| `#[event_handler]` | React to events, optionally produce commands | `EventHandler` impl, `inventory` registration |
| `#[projection]` | Define a read model | `Projection` impl, serde derives |
| `#[projection_handler]` | Apply events to a projection | `ProjectionHandler` impl |

## `#[aggregate(snapshot_every = N)]`

Defines an aggregate -- the consistency boundary in your domain.

```rust
#[aggregate(snapshot_every = 50)]
pub struct Ship {
    status: ShipStatus,
    fuel_level: f32,
    current_station: Option<StationId>,
}
```

**Parameters:**
- `snapshot_every = N` -- write a snapshot every N events (optional, default: no snapshots)

**Generates:**
- `impl Aggregate for Ship` with `type State = Ship` (the aggregate struct is its own state)
- Version-matched hydration dispatch in `hydrate()` -- reads `event_type` and `event_version`
  from each envelope, deserialises the payload, and dispatches to the matching
  `#[event_combiner]`
- `impl Default` -- all fields start at their zero/default values
- Serde `Serialize`/`Deserialize` derives for snapshotting
- `inventory` registration for auto-discovery by `ServiceBuilder`

## `#[command(Aggregate, version = N, produces = [Events...])]`

Declares a command type -- a versioned request to change state.

```rust
#[command(Ship, version = 1, produces = [ShipDeparted])]
pub struct DepartForStation {
    pub destination: StationId,
}
```

**Parameters:**
- First argument: the aggregate type this command targets
- `version = N` -- schema version (defaults to 1)
- `produces = [EventType]` -- declares which event the handler returns

**Notes:**
- `produces` is declarative metadata only -- no type is generated from it
- It documents the handler's return type and is used for compile-time enforcement
- Every `#[command(X, version = N)]` must have a matching `#[command_handler(X, version = N)]`
  -- compile error if missing

## `#[event(Aggregate, version = N)]`

Declares an event type -- a versioned fact recording what happened.

```rust
#[event(Ship, version = 1)]
pub struct ShipDeparted {
    pub destination: StationId,
}

// Version 2 coexists as a different type
#[event(Ship, version = 2)]
pub struct ShipDeparted {
    pub destination: StationId,
    pub fuel_at_departure: f32,
}
```

**Parameters:**
- First argument: the aggregate this event belongs to
- `version = N` -- schema version (defaults to 1)

**Notes:**
- Every `#[event(X, version = N)]` must have a matching `#[event_combiner(X, version = N)]`
  -- compile error if missing
- Different versions can coexist; during hydration, the framework dispatches to the
  combiner at the exact stored version

## `#[event_combiner(Aggregate, version = N)]`

Defines how an event modifies aggregate state. Pure, synchronous state folding.

```rust
#[event_combiner(Ship, version = 1)]
impl ShipDeparted {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::InFlight;
    }
}
```

**Parameters:**
- First argument: the aggregate type
- `version = N` -- must match the event's version

**Requirements:**
- One combiner per event per version
- Must be a pure function -- no I/O, no async
- The `self` parameter is the deserialised event; `state` is the mutable aggregate

## `#[command_handler(Aggregate, version = N)]`

Defines the business logic for processing a command. Returns a single event or an error.

```rust
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
            destination: cmd.destination,
        })
    }
}
```

**Parameters:**
- First argument: the aggregate type
- `version = N` -- must match the command's version

**Requirements:**
- The return type must be `Result<EventType, ErrorType>` where `EventType` matches the
  event declared in the command's `produces`
- Rejection is `Err`, not a separate event type
- One handler per command per version
- The handler struct name is conventional -- Canon uses `inventory` to find it

## `#[event_handler]`

Reacts to events and optionally produces a command. Event handlers are
**aggregate-agnostic** -- they have no aggregate type parameter.

### Simple event handler (no windowing)

```rust
#[event_handler]
impl ShipDepartedNotifier {
    #[handles(ShipDeparted, version = 1)]
    fn handle(&self, events: Vec<ShipDeparted>) -> Option<CommandEnvelope> {
        // React to the event, optionally produce a command
        None
    }
}
```

### Windowed event handler with oversight

```rust
#[event_handler(window_ttl = "30m")]
impl CargoUnloadingHandler {
    #[handles(CargoEvent, version = 1)]
    fn handle(&self, events: Vec<CargoEvent>) -> Option<CommandEnvelope> {
        // Process the accumulated events
        todo!()
    }

    fn correlate(&self, message: &IncomingMessage) -> Uuid {
        // Extract domain correlation key
        // Falls back to envelope correlation_id if omitted
        todo!()
    }

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        // Required when window_ttl is set
        // Returns Ready/NotReady/Discard
        if accumulated.len() >= 2 {
            Oversight::Ready
        } else {
            Oversight::NotReady
        }
    }
}
```

**Parameters:**
- `window_ttl = "duration"` -- optional, sets how long a window stays open before expiring

**Compile-time rules:**
- `window_ttl` without an `oversight` method is a compile error
- `correlate` is optional -- falls back to the envelope's `correlation_id`

**`#[handles]` attribute:**
- Declares which event type and version this handler processes
- Applied to the `handle` method

See [Event Handlers](./event-handlers.md) for a deep dive.

## `#[projection]`

Defines a read model -- a materialised view built from events.

```rust
#[projection]
pub struct StationInventory {
    pub station_id: StationId,
    pub stock_levels: HashMap<CargoType, u32>,
}
```

**Generates:**
- `Projection` trait scaffolding
- Serde derives

## `#[projection_handler(ProjectionName)]`

Defines how a specific event type updates a projection.

```rust
#[projection_handler(StationInventory)]
impl CargoReceivedHandler {
    fn apply(&self, event: &CargoReceived, store: &mut StationInventory) {
        *store.stock_levels.entry(event.cargo_type).or_insert(0) += event.quantity;
    }
}
```

**Parameters:**
- The projection type this handler applies to

**Requirements:**
- Must be idempotent -- applying the same event twice produces the same result
- The `store` parameter is the mutable projection state

See [Projections](./projections.md) for more detail.

## Compile-time enforcement summary

| Rule | Effect |
|------|--------|
| `#[command(X, v=N)]` without `#[command_handler(X, v=N)]` | Compile error |
| `#[event(X, v=N)]` without `#[event_combiner(X, v=N)]` | Compile error |
| `#[command_handler]` return type mismatch with `produces` | Compile error |
| `window_ttl` without `oversight` method | Compile error |
| `#[event_handler]` without matching events | Warning |
| `#[projection_handler]` without matching events | Warning |

Enforcement uses marker traits. `ServiceBuilder` auto-discovers all registrations via
`inventory`:

```rust
ServiceBuilder::new()
    .for_aggregate::<Ship>()
    .build()
```
