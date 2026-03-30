# Macros Reference

Canon provides eight proc-macros that cover the entire domain authoring experience. Users
never implement `Aggregate`, `CommandHandler`, `EventHandler`, `Projection`, or any other
framework trait directly. The macros generate all trait implementations, dispatch logic,
serialization derives, marker traits for compile-time enforcement, and `inventory`
registrations for runtime auto-discovery.

All eight macros live in the `canon-core-macros` subcrate (a `proc-macro = true` crate)
and are re-exported from `canon-core`. You use them via `use canon_core::*` or by
qualifying them as `#[canon_core::aggregate]`, `#[canon_core::event]`, and so on.

---

## Overview

| Macro | Annotates | Purpose | Generates |
|-------|-----------|---------|-----------|
| `#[aggregate]` | `struct` | Define an aggregate and its state | `Aggregate` impl, `Default`, serde, `SNAPSHOT_EVERY` const, `inventory` registration |
| `#[command]` | `struct` | Declare a versioned command type | Serde derives, marker trait, `CommandRegistration` via `inventory` |
| `#[event]` | `struct` | Declare a versioned event type | Serde derives, marker trait, `EventRegistration` via `inventory` |
| `#[event_combiner]` | `impl` block | Synchronous state folding for an event | Standalone combine fn, type-erased apply fn, `EventCombinerRegistration` via `inventory` |
| `#[command_handler]` | `impl` block | Business logic for a command | Handler struct, `CommandHandler` trait impl, dispatch fn, `CommandHandlerRegistration` via `inventory` |
| `#[event_handler]` | `impl` block | React to events, optionally produce commands | Handler struct, `EventHandler` trait impl, `EventHandlerRegistration` via `inventory` |
| `#[projection]` | `struct` | Define a read model | Serde derives, `projection_id()` method, `ProjectionRegistration` via `inventory` |
| `#[projection_handler]` | `impl` block | Apply events to a projection | Handler struct, `ProjectionHandler` trait impl, `ProjectionHandlerRegistration` via `inventory` |

---

## Dependency order

The macros must be applied in a specific order because later macros reference types and
marker traits created by earlier macros:

```
#[aggregate]
    |
    +---> #[event]     ---> #[event_combiner]
    |
    +---> #[command]   ---> #[command_handler]
    |
    +---> #[projection] --> #[projection_handler]
    |
    +---> #[event_handler]  (aggregate-agnostic, can come last)
```

In practice, you define your aggregate first, then events and commands, then their
combiners and handlers, and finally projections and event handlers.

---

## `#[aggregate(snapshot_every = N)]`

Defines an aggregate -- the consistency boundary in your domain. The aggregate struct
is its own state (opinionated: `type State = Self`).

### Syntax

```rust
use canon_core::aggregate;

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

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `snapshot_every` | `u64` | No | `None` | Take a snapshot every N events. When omitted, no snapshots are taken. |

### What it generates

The macro produces the following code:

**1. Derive stripping and re-application.**
Any existing `#[derive(...)]` attributes on the struct are stripped to prevent
duplicates. The macro then applies:

```rust
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
```

The `Default` derive means every field on your aggregate must implement `Default`.
For enums used as fields (like `ShipStatus`), annotate one variant with `#[default]`:

```rust
#[derive(Default, Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ShipStatus {
    #[default]
    Docked,
    InTransit,
    Decommissioned,
}
```

**2. `impl Aggregate` with version-matched hydration.**
The generated `hydrate()` method iterates over `EventEnvelope`s and dispatches each
one to the correct `#[event_combiner]` using the `event_type` and `event_version`
fields from the envelope. Dispatch goes through the `__apply_event_combiner` helper,
which uses a lazily-initialized `HashMap` built from `inventory` registrations for
O(1) lookup per event:

```rust
impl Aggregate for Ship {
    type State = Ship;          // aggregate struct IS its own state
    type Error = MacroError;

    fn hydrate(
        state: &mut Self::State,
        events: impl Iterator<Item = EventEnvelope>,
    ) -> Result<(), Self::Error> {
        let target_id = std::any::TypeId::of::<Ship>();
        for envelope in events {
            __apply_event_combiner(
                target_id,
                &envelope,
                state as &mut dyn std::any::Any,
            ).map_err(|e| MacroError(e.to_string()))?;
        }
        Ok(())
    }
}
```

**3. Snapshot cadence constant.**

```rust
impl Ship {
    pub const SNAPSHOT_EVERY: Option<u64> = Some(50);
}
```

When `snapshot_every` is omitted, this is `None`. The event store consumer checks
`version % N == 0` after a confirmed Cassandra write to decide whether to take a
snapshot.

**4. Serde support.**
The `Serialize` and `Deserialize` derives enable snapshot serialization and
deserialization. Snapshots are stored as JSON in the snapshot store.

### Real example -- Station aggregate

From the station-service:

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

All fields default to their zero values (`""`, `0.0`, `None`, `vec![]`, `false`).
Events applied via `hydrate()` build up the state from there.

### Testing

```rust
#[test]
fn aggregate_has_default() {
    let ship = Ship::default();
    assert_eq!(ship.status, ShipStatus::Docked);
    assert_eq!(ship.fuel_level, 0.0);
}

#[test]
fn aggregate_snapshot_every() {
    assert_eq!(Ship::SNAPSHOT_EVERY, Some(50));
}

#[test]
fn aggregate_serde_roundtrip() {
    let ship = Ship {
        status: ShipStatus::InFlight,
        fuel_level: 75.5,
    };
    let json = serde_json::to_vec(&ship).expect("serialize");
    let back: Ship = serde_json::from_slice(&json).expect("deserialize");
    assert_eq!(back.status, ShipStatus::InFlight);
}
```

---

## `#[command(Aggregate, version = N, produces = [Events...])]`

Declares a command type -- a versioned request to change aggregate state. Every command
must have a matching `#[command_handler]` at the same version.

### Syntax

```rust
use canon_core::command;

#[command(Ship, version = 1, produces = [ShipDeparted])]
pub struct DepartForStation {
    pub ship_id: Uuid,
    pub destination: Uuid,
}
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| First arg | `Ident` | Yes | -- | The aggregate type this command targets |
| `version` | `u32` | No | `1` | Schema version of this command |
| `produces` | `[Ident, ...]` | No | `[]` | Declares which events the handler returns |

### What it generates

**1. Derive stripping and re-application.**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
```

Note: unlike `#[aggregate]`, commands do not derive `Default`.

**2. Marker trait for compile-time enforcement.**
A hidden marker trait is generated using the command name and version:

```rust
/// Marker trait: compile error at ServiceBuilder call site if
/// no #[command_handler] satisfies this.
#[doc(hidden)]
pub trait DepartForStationV1HasHandler {}
```

This trait is only satisfied when a `#[command_handler(Ship, version = 1)]` is
defined for a handler that takes `DepartForStation`. If no handler exists, any code
that references the marker will fail to compile.

**3. `inventory` registration.**

```rust
inventory::submit! {
    CommandRegistration {
        aggregate_type_name: "Ship",
        command_type_name: "DepartForStation",
        command_version: 1,
    }
}
```

This allows `ServiceBuilder` to discover all commands at runtime and validate that
every command has a registered handler.

### The `produces` parameter

`produces` is **declarative metadata only** -- no type is generated from it. It serves
three purposes:

1. **Documentation**: makes it clear which event a command handler should return.
2. **Macro wiring**: the `#[command_handler]` macro checks that the handler's return
   type matches the event declared in `produces`.
3. **Schema registry**: can be used by tooling to map commands to their output events.

### Real example -- all fleet commands

```rust
#[canon_core::command(Ship, version = 1, produces = [ShipRegistered])]
pub struct RegisterShip {
    pub name: String,
    pub capacity_kg: f32,
    #[serde(default)]
    pub home_station: Option<Uuid>,
}

#[canon_core::command(Ship, version = 1, produces = [RouteAssigned])]
pub struct AssignRoute {
    pub ship_id: Uuid,
    pub route_id: Uuid,
}

#[canon_core::command(Ship, version = 1, produces = [ShipDeparted])]
pub struct DepartForStation {
    pub ship_id: Uuid,
    pub destination: Uuid,
}

#[canon_core::command(Ship, version = 1, produces = [ResupplyScheduled])]
pub struct ScheduleResupply {
    pub ship_id: Uuid,
    pub fuel_kg: f32,
}

#[canon_core::command(Ship, version = 1, produces = [ShipDecommissioned])]
pub struct DecommissionShip {
    pub ship_id: Uuid,
}

#[canon_core::command(Ship, version = 1, produces = [ShipDockedAtStation])]
pub struct DockShip {
    pub ship_id: Uuid,
    pub station_id: Uuid,
}
```

---

## `#[event(Aggregate, version = N)]`

Declares an event type -- a versioned fact recording what happened. Every event must
have a matching `#[event_combiner]` at the same version.

### Syntax

```rust
use canon_core::event;

#[event(Ship, version = 1)]
pub struct ShipDeparted {
    pub ship_id: Uuid,
    pub destination: Uuid,
    pub fuel_at_departure: f32,
}
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| First arg | `Ident` | Yes | -- | The aggregate this event belongs to |
| `version` | `u32` | No | `1` | Schema version of this event |

### What it generates

**1. Derive stripping and re-application.**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
```

**2. Marker trait for compile-time enforcement.**

```rust
#[doc(hidden)]
pub trait ShipDepartedV1HasCombiner {}
```

This trait is satisfied by the corresponding `#[event_combiner(Ship, version = 1)]`.

**3. `inventory` registration.**

```rust
inventory::submit! {
    EventRegistration {
        aggregate_type_name: "Ship",
        event_type_name: "ShipDeparted",
        event_version: 1,
    }
}
```

### Version coexistence

Multiple versions of the same event can coexist in the system. During hydration,
the framework reads `event_version` from the stored `EventEnvelope` and dispatches
to the combiner registered at that exact version. This enables schema evolution
without migrating historical events:

```rust
#[event(Ship, version = 1)]
pub struct ShipDeparted {
    pub destination: StationId,
}

#[event(Ship, version = 2)]
pub struct ShipDepartedV2 {
    pub destination: StationId,
    pub fuel_at_departure: f32,
}
```

Each version has its own combiner:

```rust
#[event_combiner(Ship, version = 1)]
impl ShipDeparted {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::InFlight;
    }
}

#[event_combiner(Ship, version = 2)]
impl ShipDepartedV2 {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::InFlight;
        state.fuel_level = self.fuel_at_departure;
    }
}
```

Old events stored at version 1 use the v1 combiner; new events stored at version 2
use the v2 combiner. The aggregate's `hydrate()` handles both transparently.

### Event enums for event handlers

When an event handler needs to process multiple event types (e.g., for windowed
oversight), define a serde-tagged enum that wraps the individual event structs:

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum CargoEvent {
    ManifestCreated(ManifestCreated),
    CargoLoaded(CargoLoaded),
    UnloadingStarted(UnloadingStarted),
    CargoUnloaded(CargoUnloaded),
    ManifestClosed(ManifestClosed),
}
```

This enum is not annotated with `#[event]` -- it is a plain Rust enum used as the
`#[handles]` type in event handlers.

### Real example -- all station events with inline combiners

The station-service defines events and combiners in a single file:

```rust
#[canon_core::event(Station, version = 1)]
pub struct StationRegistered {
    pub station_id: Uuid,
    pub name: String,
    pub capacity_kg: f32,
}

#[canon_core::event_combiner(Station, version = 1)]
impl StationRegistered {
    fn combine(&self, state: &mut Station) {
        state.name = self.name.clone();
        state.capacity_kg = self.capacity_kg;
        state.registered = true;
    }
}

#[canon_core::event(Station, version = 1)]
pub struct ShipDocked {
    pub station_id: Uuid,
    pub ship_id: Uuid,
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

---

## `#[event_combiner(Aggregate, version = N)]`

Defines how an event modifies aggregate state. Pure, synchronous state folding.
One combiner per event per version. This is the most important macro for aggregate
hydration -- every event that passes through `hydrate()` is routed to its combiner.

### Syntax

```rust
use canon_core::event_combiner;

#[event_combiner(Ship, version = 1)]
impl ShipDeparted {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::InFlight;
    }
}
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| First arg | `Ident` | Yes | -- | The aggregate type |
| `version` | `u32` | No | `1` | Must match the event's version |

### Requirements

- The `impl` block must be on the **event type** (e.g., `impl ShipDeparted`), not on
  the aggregate.
- The `impl` block must contain a `combine` method with the signature
  `fn combine(&self, state: &mut Aggregate)`.
- The method must be **pure** -- no I/O, no async, no side effects. It is called
  during hydration, which may happen frequently.
- `self` refers to the deserialized event; `state` is the mutable aggregate.
- One combiner per event per version. Two combiners for the same event at the same
  version will cause a compile error.

### What it generates

The macro generates three pieces of code:

**1. Standalone combine function.**
To avoid orphan rule violations (the event type and aggregate may be in different
crates), the macro does not generate a trait impl on the event type. Instead, it
creates a standalone function with `self` references rewritten to `__canon_self`:

```rust
fn __canon_combine_shipdeparted_v1(
    __canon_self: &ShipDeparted,
    state: &mut Ship,
) {
    state.status = ShipStatus::InFlight;
}
```

The `self` -> `__canon_self` rewriting is handled by `replace_self_in_tokens`, a
utility that walks the token stream and substitutes every `self` identifier.

**2. Type-erased apply function.**
This function deserializes the event from raw bytes and calls the combine function.
It is what gets registered in `inventory` and called during hydration:

```rust
fn __canon_apply_shipdeparted_v1(
    payload: &[u8],
    state: &mut dyn std::any::Any,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let event: ShipDeparted = serde_json::from_slice(payload)?;
    let state = state
        .downcast_mut::<Ship>()
        .ok_or("aggregate state type mismatch in event combiner")?;
    __canon_combine_shipdeparted_v1(&event, state);
    Ok(())
}
```

**3. Marker trait satisfaction and `inventory` registration.**

```rust
// Verify the marker trait from #[event] exists
const _: () = {
    fn __check_marker<T: ShipDepartedV1HasCombiner>() {}
};

inventory::submit! {
    EventCombinerRegistration {
        aggregate_type_id: std::any::TypeId::of::<Ship>(),
        event_type_name: "ShipDeparted",
        event_version: 1,
        apply_fn: __canon_apply_shipdeparted_v1,
    }
}
```

The marker trait check (`__check_marker`) verifies at compile time that the
`#[event(Ship, version = 1)]` annotation exists for `ShipDeparted`. If you write
an `#[event_combiner]` for an event that was never declared with `#[event]`, you
get a compile error.

### How hydration dispatch works at runtime

When `Ship::hydrate()` processes an `EventEnvelope`, it calls `__apply_event_combiner`
with the aggregate's `TypeId`, the envelope, and a mutable reference to the aggregate
state. This function consults a lazily-initialized `HashMap` keyed by
`(TypeId, event_type_name, event_version)`:

```rust
static COMBINER_MAP: OnceLock<HashMap<CombinerKey, CombinerApplyFn>> = OnceLock::new();
```

The map is built once on first use by iterating all `EventCombinerRegistration`s in
`inventory`. After that, every lookup is O(1).

### Real example -- fleet combiners

```rust
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

#[event_combiner(Ship, version = 1)]
impl ShipDockedAtStation {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::Docked;
        state.current_station = Some(self.station_id);
    }
}
```

### Notification events

Some events exist purely as signals and do not modify aggregate state. The combiner
must still exist (exhaustiveness rule), but the body can be empty:

```rust
#[event_combiner(Station, version = 1)]
impl StationStockLow {
    fn combine(&self, _state: &mut Station) {
        // Notification event -- no state mutation.
    }
}
```

### Error paths

If `hydrate()` encounters an envelope whose `event_type` and `event_version` have no
registered combiner, it returns an error:

```rust
#[test]
fn hydrate_returns_error_for_unregistered_event_type() {
    let mut ship = Ship::default();
    let events = vec![EventEnvelope {
        event_type: "NoSuchEvent".to_string(),
        event_version: 1,
        // ... other fields
    }];
    let result = Ship::hydrate(&mut ship, events.into_iter());
    assert!(result.is_err());
}

#[test]
fn hydrate_returns_error_for_unregistered_event_version() {
    let mut ship = Ship::default();
    let events = vec![EventEnvelope {
        event_type: "ShipDeparted".to_string(),
        event_version: 99,  // no combiner at v99
        // ... other fields
    }];
    let result = Ship::hydrate(&mut ship, events.into_iter());
    assert!(result.is_err());
}
```

---

## `#[command_handler(Aggregate, version = N)]`

Defines the business logic for processing a command. The user writes a synchronous
`handle` method; the macro wraps it in an async trait impl.

### Syntax

```rust
use canon_core::command_handler;

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
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| First arg | `Ident` | Yes | -- | The aggregate type |
| `version` | `u32` | No | `1` | Must match the command's version |

### Requirements

- The `impl` block must declare `type Error = SomeErrorType` where `SomeErrorType`
  implements `std::error::Error + Send + Sync + 'static`.
- The `impl` block must contain a `handle` method with exactly three parameters:
  `&self`, `state: &Aggregate`, `cmd: CommandType`.
- The return type must be `Result<EventType, ErrorType>`.
- The event type in the `Result` must match the event declared in the command's
  `produces` list.
- Rejection is always `Err`, never a separate event type.
- **The handler struct name is your choice** -- the macro generates the struct
  definition for you. You only write the `impl` block.

### What it generates

**1. Handler struct definition.**
The macro generates the struct so you do not need to define it separately:

```rust
pub struct DepartForStationHandler;
```

**2. Inherent method with user's sync body.**

```rust
impl DepartForStationHandler {
    #[doc(hidden)]
    fn __canon_handle(
        &self,
        state: &Ship,
        cmd: DepartForStation,
    ) -> Result<ShipDeparted, FleetError> {
        // ... user's body
    }
}
```

**3. Async `CommandHandler` trait impl.**
The trait requires an async method, but since command handlers are typically pure
business logic, the macro wraps the sync method:

```rust
#[async_trait]
impl CommandHandler<Ship> for DepartForStationHandler {
    type Command = DepartForStation;
    type Event = ShipDeparted;
    type Error = FleetError;

    async fn handle(
        &self,
        state: &Ship,
        command: DepartForStation,
    ) -> Result<Self::Event, Self::Error> {
        self.__canon_handle(state, command)
    }
}
```

**4. Marker trait satisfaction.**

```rust
impl DepartForStationV1HasHandler for DepartForStationHandler {}
```

This satisfies the marker trait generated by `#[command(Ship, version = 1, ...)]`.

**5. Type-erased dispatch function.**
This is the key to the Dispatcher's ability to process commands without knowing
concrete types. The function hydrates aggregate state from events, deserializes
the command, runs the handler, and serializes the resulting event:

```rust
fn __canon_dispatch_departforstation_v1(
    command_payload: &[u8],
    events: &[EventEnvelope],
    aggregate_type_id: TypeId,
) -> Result<HandlerDispatchResult, Box<dyn Error + Send + Sync>> {
    // 1. Create default aggregate state and hydrate from events
    let mut state = Ship::default();
    for envelope in events {
        __apply_event_combiner(aggregate_type_id, envelope, &mut state)?;
    }

    // 2. Deserialize the command
    let command: DepartForStation = serde_json::from_slice(command_payload)?;

    // 3. Run the handler
    let handler = DepartForStationHandler;
    let event = handler.__canon_handle(&state, command)?;

    // 4. Serialize the resulting event
    let event_payload = serde_json::to_vec(&event)?;

    Ok(HandlerDispatchResult {
        event_payload,
        event_type: "ShipDeparted",
        event_version: 1,
    })
}
```

**6. `inventory` registration.**

```rust
inventory::submit! {
    CommandHandlerRegistration {
        aggregate_type_name: "Ship",
        command_type_name: "DepartForStation",
        command_version: 1,
        handler_type_name: "DepartForStationHandler",
        dispatch_fn: __canon_dispatch_departforstation_v1,
        produces_event_type: "ShipDeparted",
        produces_event_version: 1,
    }
}
```

The `dispatch_fn` is stored in a lazily-initialized `HashMap` keyed by
`(command_type_name, command_version)` for O(1) dispatch lookup.

### How the Dispatcher uses it

The `Dispatcher` polls the inbox for unprocessed commands. For each command, it
calls `__dispatch_command` with the command type name, version, serialized payload,
and the aggregate's event history. This function looks up the registered dispatch
function and invokes it:

```rust
pub fn __dispatch_command(
    command_type: &str,
    command_version: u32,
    command_payload: &[u8],
    events: &[EventEnvelope],
    aggregate_type_id: TypeId,
) -> Result<HandlerDispatchResult, Box<dyn Error + Send + Sync>> {
    let map = HANDLER_DISPATCH_MAP.get_or_init(|| {
        // Build HashMap from all CommandHandlerRegistrations
    });
    let key = (command_type.to_owned(), command_version);
    match map.get(&key) {
        Some(dispatch_fn) => dispatch_fn(command_payload, events, aggregate_type_id),
        None => Err(format!(
            "no command handler registered for '{}' version {}",
            command_type, command_version
        ).into()),
    }
}
```

### Error handling

Command handler errors are domain errors. Define them with `thiserror` in each
service:

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

### Real example -- state validation in handlers

The `DepartForStationHandler` validates that the ship is docked before departure:

```rust
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

The `DecommissionShipHandler` prevents double-decommissioning:

```rust
#[command_handler(Ship, version = 1)]
impl DecommissionShipHandler {
    type Error = FleetError;

    fn handle(&self, state: &Ship, cmd: DecommissionShip) -> Result<ShipDecommissioned, FleetError> {
        if state.status == ShipStatus::Decommissioned {
            return Err(FleetError::AlreadyDecommissioned);
        }
        Ok(ShipDecommissioned { ship_id: cmd.ship_id })
    }
}
```

The `DockShipHandler` validates the ship is currently in transit:

```rust
#[command_handler(Ship, version = 1)]
impl DockShipHandler {
    type Error = FleetError;

    fn handle(&self, state: &Ship, cmd: DockShip) -> Result<ShipDockedAtStation, FleetError> {
        if state.status != ShipStatus::InTransit {
            return Err(FleetError::ShipNotInTransit);
        }
        Ok(ShipDockedAtStation {
            ship_id: cmd.ship_id,
            station_id: cmd.station_id,
        })
    }
}
```

### Testing command handlers

Command handlers can be tested through the `CommandHandler` trait:

```rust
#[tokio::test]
async fn command_handler_produces_events() {
    let handler = DepartForStationHandler;
    let ship = Ship {
        status: ShipStatus::Docked,
        fuel_level: 80.0,
    };
    let dest = Uuid::new_v4();
    let cmd = DepartForStation { destination: dest };

    let event = CommandHandler::<Ship>::handle(&handler, &ship, cmd)
        .await
        .expect("handle");
    assert_eq!(event.destination, dest);
}

#[tokio::test]
async fn command_handler_rejects_invalid_state() {
    let handler = DepartForStationHandler;
    let ship = Ship {
        status: ShipStatus::InFlight,
        fuel_level: 80.0,
    };
    let cmd = DepartForStation { destination: Uuid::new_v4() };

    let result = CommandHandler::<Ship>::handle(&handler, &ship, cmd).await;
    assert!(result.is_err());
}
```

---

## `#[event_handler]`

Reacts to events and optionally produces a command. Event handlers are
**aggregate-agnostic** -- they have no aggregate type parameter. They work for both
internal events (this service's own events) and external events (from other services
via the adaptor).

### Syntax -- simple (no windowing)

```rust
use canon_core::event_handler;

#[event_handler]
impl ResupplyHandler {
    #[handles(ResupplyDispatched, version = 1)]
    fn handle(&self, events: Vec<ResupplyDispatched>) -> Option<CommandEnvelope> {
        let event = events.last()?;
        // Build and return a command, or None
        Some(CommandEnvelope { /* ... */ })
    }
}
```

### Syntax -- windowed with oversight

```rust
#[event_handler(window_ttl = "30m")]
impl UnloadingHandler {
    #[handles(CargoEvent, version = 1)]
    fn handle(&self, events: Vec<CargoEvent>) -> Option<CommandEnvelope> {
        // Process the accumulated events
        todo!()
    }

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        // Required when window_ttl is set
        if has_all_required_events(accumulated) {
            Oversight::Ready
        } else {
            Oversight::NotReady
        }
    }
}
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `window_ttl` | `"duration"` | No | None | How long a window stays open before expiring |

The duration string supports three suffixes:
- `"30s"` -- 30 seconds
- `"30m"` -- 30 minutes
- `"1h"` -- 1 hour

The macro parses this at compile time into seconds using `parse_duration_to_secs`.

### The `#[handles]` attribute

Applied to the `handle` method, it declares which event type and version this handler
processes:

```rust
#[handles(ResupplyDispatched, version = 1)]
fn handle(&self, events: Vec<ResupplyDispatched>) -> Option<CommandEnvelope> { ... }
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| First arg | `Ident` | Yes | -- | The event type to handle |
| `version` | `u32` | No | `1` | The event schema version |

### The `oversight` method

Controls when a windowed event handler is ready to dispatch. Returns one of three
values:

| Value | Meaning |
|-------|---------|
| `Oversight::Ready` | All required events are present; dispatch the handler now |
| `Oversight::NotReady` | Still waiting for more events; keep the window open |
| `Oversight::Discard` | Abort this window; dead-letter the accumulated events |

The `oversight` method receives the full list of `IncomingMessage` values accumulated
in the window so far. Each message is tagged as `Command`, `InternalEvent`, or
`ExternalEvent`, allowing the oversight logic to distinguish between event sources.

### The `correlate` method (optional)

Extracts a domain-specific correlation key from an incoming message. The window key
is `(handler_id, correlation_key)`. If `correlate` is not defined, the framework
falls back to the envelope's `correlation_id`.

```rust
fn correlate(&self, message: &IncomingMessage) -> Uuid {
    // Extract a domain-specific key, e.g., manifest_id
    match message {
        IncomingMessage::InternalEvent(e) => {
            // Parse the payload to extract the key
            todo!()
        }
        _ => message.correlation_id(),
    }
}
```

### Compile-time enforcement

- **`window_ttl` without `oversight` is a compile error.** The macro checks for the
  presence of an `oversight` method and rejects the input if `window_ttl` is set
  without one:

  ```
  error: event_handler with `window_ttl` requires an `oversight` method
  ```

- **`oversight` without `window_ttl` is allowed.** You can define an oversight method
  without windowing. In this case, oversight defaults to `Ready` for each individual
  event (the framework does not accumulate).

- **`correlate` is always optional.** No enforcement -- the fallback to
  `correlation_id` is always safe.

### What it generates

**1. Handler struct definition.**

```rust
pub struct UnloadingHandler;
```

**2. Inherent method with user's handle body.**
The user's `handle` method returns `Option<CommandEnvelope>`, but the trait requires
`Result<Option<CommandEnvelope>, Error>`. The macro wraps it:

```rust
impl UnloadingHandler {
    #[doc(hidden)]
    fn __canon_handle(
        &self,
        events: Vec<CargoEvent>,
    ) -> Option<CommandEnvelope> {
        // ... user's body
    }
}
```

**3. Async `EventHandler` trait impl.**

```rust
#[async_trait]
impl EventHandler for UnloadingHandler {
    type Event = CargoEvent;
    type Error = MacroError;

    async fn handle(
        &self,
        events: Vec<CargoEvent>,
    ) -> Result<Option<CommandEnvelope>, Self::Error> {
        Ok(self.__canon_handle(events))
    }

    fn oversight(
        &self,
        accumulated: &[IncomingMessage],
    ) -> Oversight {
        // ... user's oversight body, or default Ready
    }
}
```

**4. `inventory` registration.**

```rust
inventory::submit! {
    EventHandlerRegistration {
        handler_type_name: "UnloadingHandler",
        event_type_name: "CargoEvent",
        event_version: 1,
        window_ttl_secs: Some(1800),  // 30m = 1800s
    }
}
```

### Real example -- simple event handler (fleet-service)

The `ResupplyHandler` in fleet-service listens for `ResupplyDispatched` events from
the supply-service and produces a `ScheduleResupply` command:

```rust
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

### Real example -- windowed event handler with oversight (cargo-service)

The `UnloadingHandler` in cargo-service waits for two events from different sources
before dispatching. It uses `window_ttl = "30m"` and a sophisticated oversight
function:

```rust
#[canon_core::event_handler(window_ttl = "30m")]
impl UnloadingHandler {
    #[handles(CargoEvent, version = 1)]
    fn handle(&self, events: Vec<CargoEvent>) -> Option<CommandEnvelope> {
        // Find a ManifestCreated event in the batch
        let mc = events.iter().find_map(|e| {
            if let CargoEvent::ManifestCreated(mc) = e {
                Some(mc)
            } else {
                None
            }
        })?;

        // Build a BeginUnloading command
        let payload = serde_json::to_vec(&serde_json::json!({
            "manifest_id": mc.manifest_id,
            "station_id": mc.voyage_id,
        }));
        let payload = match payload {
            Ok(p) => p,
            Err(_) => return None,
        };

        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::from_uuid(mc.manifest_id),
            command_type: "BeginUnloading".to_string(),
            correlation_id: Uuid::new_v4(),
            causation_id: mc.manifest_id,
            timestamp: chrono::Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        // Discard if ship was decommissioned
        let decommissioned = accumulated.iter().any(|m| {
            matches!(
                m,
                IncomingMessage::ExternalEvent(e) if e.event_type == "ShipDecommissioned"
            )
        });
        if decommissioned {
            return Oversight::Discard;
        }

        // Ready only when BOTH events are present
        let has_arrival = accumulated.iter().any(|m| {
            matches!(
                m,
                IncomingMessage::ExternalEvent(e) if e.event_type == "ShipArrivedAtStation"
            )
        });
        let has_manifest = accumulated.iter().any(|m| {
            matches!(
                m,
                IncomingMessage::InternalEvent(e) if e.event_type == "ManifestCreated"
            )
        });

        if has_arrival && has_manifest {
            Oversight::Ready
        } else {
            Oversight::NotReady
        }
    }
}
```

### Testing event handlers

Test both the oversight logic and the handler itself:

```rust
#[test]
fn oversight_not_ready_when_empty() {
    let handler = UnloadingHandler;
    assert_eq!(handler.oversight(&[]), Oversight::NotReady);
}

#[test]
fn oversight_ready_when_both_present() {
    let handler = UnloadingHandler;
    let accumulated = vec![
        external_event("ShipArrivedAtStation"),
        internal_event("ManifestCreated"),
    ];
    assert_eq!(handler.oversight(&accumulated), Oversight::Ready);
}

#[test]
fn oversight_discard_overrides_ready() {
    let handler = UnloadingHandler;
    let accumulated = vec![
        external_event("ShipArrivedAtStation"),
        internal_event("ManifestCreated"),
        external_event("ShipDecommissioned"),
    ];
    assert_eq!(handler.oversight(&accumulated), Oversight::Discard);
}

#[tokio::test]
async fn handle_returns_command_when_manifest_created_present() {
    let handler = UnloadingHandler;
    let events = vec![CargoEvent::ManifestCreated(ManifestCreated {
        manifest_id: Uuid::new_v4(),
        ship_id: Uuid::new_v4(),
        voyage_id: Uuid::new_v4(),
    })];

    let result = handler.handle(events).await;
    assert!(result.is_ok());
    let cmd = result.unwrap().expect("should produce command");
    assert_eq!(cmd.command_type, "BeginUnloading");
}
```

---

## `#[projection]`

Defines a read model -- a materialized view built from events. Projections are
updated by `#[projection_handler]` impls and must be idempotent.

### Syntax

```rust
use canon_core::projection;

#[projection]
pub struct ShipReadModel {
    pub ship_id: uuid::Uuid,
    pub name: String,
    pub capacity_kg: f32,
    pub status: String,
    pub fuel_kg: f32,
}
```

### Parameters

`#[projection]` takes no arguments. A compile error is emitted if any are provided:

```
error: #[projection] takes no arguments
```

### What it generates

**1. Derive stripping and re-application.**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
```

**2. Snake-case `projection_id()` method.**
The struct name is converted to snake_case and used as the projection identifier for
checkpoint tracking:

```rust
impl ShipReadModel {
    pub fn projection_id(&self) -> &str {
        "ship_read_model"
    }
}
```

The `to_snake_case` conversion inserts underscores before uppercase letters:
`StationInventory` -> `station_inventory`, `ShipReadModel` -> `ship_read_model`.

**3. `inventory` registration.**

```rust
inventory::submit! {
    ProjectionRegistration {
        projection_type_name: "ShipReadModel",
        projection_id: "ship_read_model",
    }
}
```

### Real example -- station inventory projection

```rust
#[canon_core::projection]
pub struct StationInventory {
    pub stations: HashMap<Uuid, StationInventoryRow>,
}
```

Note that `#[projection]` does not derive `Default`. If your projection needs a
`Default` impl (e.g., for initialization), define it manually:

```rust
impl Default for StationInventory {
    fn default() -> Self {
        Self {
            stations: HashMap::new(),
        }
    }
}
```

---

## `#[projection_handler(ProjectionName)]`

Defines how a specific event type updates a projection's read model. There is one
handler per event type per projection.

### Syntax

```rust
use canon_core::projection_handler;

#[projection_handler(StationInventory)]
impl CargoReceivedProjectionHandler {
    fn apply(&self, event: &CargoReceived, store: &mut StationInventory) {
        if let Some(row) = store.stations.get_mut(&event.station_id) {
            row.current_stock_kg += event.weight_kg;
            row.updated_at = Utc::now();
        }
    }
}
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| First arg | `Ident` | Yes | The projection type this handler applies to |

### Requirements

- The `impl` block must contain an `apply` method with three parameters:
  `&self`, `event: &EventType`, `store: &mut ProjectionType`.
- The event parameter **must be a reference** (`&EventType`). The macro extracts the
  inner type from the reference.
- **Must be idempotent** -- applying the same event twice should produce the same
  result. The framework handles deduplication at the inbox level, but projection
  handlers should be safe to replay during rebuilds.

### What it generates

**1. Handler struct definition.**

```rust
pub struct CargoReceivedProjectionHandler;
```

**2. `ProjectionHandler` trait impl.**

```rust
impl ProjectionHandler<StationInventory> for CargoReceivedProjectionHandler {
    type Event = CargoReceived;

    fn apply(&self, event: &Self::Event, store: &mut StationInventory) {
        // ... user's body
    }
}
```

**3. `inventory` registration.**

```rust
inventory::submit! {
    ProjectionHandlerRegistration {
        projection_type_name: "StationInventory",
        handler_type_name: "CargoReceivedProjectionHandler",
    }
}
```

### Real example -- ship read model handlers

The fleet-service defines handlers for every event that affects the ship read model:

```rust
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
impl ResupplyScheduledProjectionHandler {
    fn apply(&self, event: &ResupplyScheduled, store: &mut ShipReadModel) {
        store.fuel_kg = event.fuel_kg;
    }
}

#[canon_core::projection_handler(ShipReadModel)]
impl ShipDecommissionedProjectionHandler {
    fn apply(&self, event: &ShipDecommissioned, store: &mut ShipReadModel) {
        let _ = event;
        store.status = "Decommissioned".to_string();
    }
}
```

### Testing projection handlers

Projection handlers can be tested directly through the `ProjectionHandler` trait:

```rust
#[test]
fn station_registered_creates_row() {
    let handler = StationRegisteredProjectionHandler;
    let mut store = StationInventory::default();
    let station_id = Uuid::new_v4();
    let event = StationRegistered {
        station_id,
        name: "Alpha Station".to_string(),
        capacity_kg: 1000.0,
    };
    handler.apply(&event, &mut store);
    assert!(store.stations.contains_key(&station_id));
    let row = &store.stations[&station_id];
    assert_eq!(row.name, "Alpha Station");
}

#[test]
fn cargo_received_increases_stock() {
    let handler = CargoReceivedProjectionHandler;
    let mut store = StationInventory::default();
    // ... register station first ...

    let event = CargoReceived {
        station_id,
        manifest_id: Uuid::new_v4(),
        weight_kg: 250.0,
    };
    handler.apply(&event, &mut store);
    assert!((store.stations[&station_id].current_stock_kg - 250.0).abs() < f32::EPSILON);
}

#[test]
fn ship_docked_on_unknown_station_is_noop() {
    let handler = ShipDockedProjectionHandler;
    let mut store = StationInventory::default();
    let event = ShipDocked {
        station_id: Uuid::new_v4(),
        ship_id: Uuid::new_v4(),
    };
    handler.apply(&event, &mut store);
    assert!(store.stations.is_empty());  // no crash, no side effects
}
```

---

## Compile-time enforcement

Canon uses marker traits generated by the macros to catch wiring errors at compile
time rather than at runtime. The table below summarizes all enforcement rules.

### Exhaustiveness rules

| Declaration | Required match | Effect if missing |
|-------------|----------------|-------------------|
| `#[command(X, version = N)]` | `#[command_handler(X, version = N)]` | Compile error: marker trait `XV{N}HasHandler` unsatisfied |
| `#[event(X, version = N)]` | `#[event_combiner(X, version = N)]` | Compile error: marker trait `XV{N}HasCombiner` unsatisfied |

These are **hard errors**. Your code will not compile if a command has no handler or
an event has no combiner.

### Optional matches

| Declaration | Optional match | Effect if missing |
|-------------|----------------|-------------------|
| `#[event_handler]` | Events it handles | Warning (unhandled event versions) |
| `#[projection_handler]` | Events it handles | Warning (unhandled event versions) |

These are **soft warnings**. You do not need a handler or projection for every event --
only for the ones your service cares about.

### Structural rules

| Rule | Effect |
|------|--------|
| `#[command_handler]` return type does not match `produces` | Compile error: type mismatch |
| `window_ttl` without `oversight` method | Compile error: `"event_handler with window_ttl requires an oversight method"` |
| `#[event_combiner]` without `combine` method | Compile error: `"event_combiner impl must contain a combine method"` |
| `#[command_handler]` without `handle` method | Compile error: `"command_handler impl must contain a handle method"` |
| `#[command_handler]` without `type Error` | Compile error: `"command_handler impl must contain type Error = ..."` |
| `#[command_handler]` `handle` with wrong parameter count | Compile error: `"handle method must have 3 parameters: &self, state, command"` |
| `#[projection_handler]` without `apply` method | Compile error: `"projection_handler impl must contain an apply method"` |
| `#[projection_handler]` event param not a reference | Compile error: `"event parameter should be a reference (&EventType)"` |
| `#[projection]` with arguments | Compile error: `"#[projection] takes no arguments"` |
| `#[event_handler]` `handle` without `#[handles]` | Compile error: `"handle method must have a #[handles(EventType, version = N)] attribute"` |

### How marker traits work

When you write `#[command(Ship, version = 1, ...)]` on `DepartForStation`, the macro
generates:

```rust
pub trait DepartForStationV1HasHandler {}
```

When you write `#[command_handler(Ship, version = 1)]` on `DepartForStationHandler`,
the macro generates:

```rust
impl DepartForStationV1HasHandler for DepartForStationHandler {}
```

If the handler is missing, the trait is never satisfied, and any code that depends on
it (via `const _` assertions or `ServiceBuilder` constraints) fails to compile.

The same pattern applies to events and combiners: `#[event(Ship, version = 1)]` on
`ShipDeparted` generates `ShipDepartedV1HasCombiner`, and `#[event_combiner(Ship, version = 1)]`
satisfies it.

---

## `inventory` auto-registration

Every macro emits a static registration via `inventory::submit!`. These registrations
are collected at link time -- no manual registration calls, no builder methods listing
every type. The framework discovers everything automatically.

### Registration types

| Type | Collected by | Fields |
|------|-------------|--------|
| `CommandRegistration` | `#[command]` | `aggregate_type_name`, `command_type_name`, `command_version` |
| `EventRegistration` | `#[event]` | `aggregate_type_name`, `event_type_name`, `event_version` |
| `EventCombinerRegistration` | `#[event_combiner]` | `aggregate_type_id`, `event_type_name`, `event_version`, `apply_fn` |
| `CommandHandlerRegistration` | `#[command_handler]` | `aggregate_type_name`, `command_type_name`, `command_version`, `handler_type_name`, `dispatch_fn`, `produces_event_type`, `produces_event_version` |
| `EventHandlerRegistration` | `#[event_handler]` | `handler_type_name`, `event_type_name`, `event_version`, `window_ttl_secs` |
| `ProjectionRegistration` | `#[projection]` | `projection_type_name`, `projection_id` |
| `ProjectionHandlerRegistration` | `#[projection_handler]` | `projection_type_name`, `handler_type_name` |

### How `inventory` works

The `inventory` crate uses platform-specific linker sections to collect static
registrations across all compilation units. At runtime, `inventory::iter::<T>` yields
every value of type `T` that was registered anywhere in the binary. This is what
enables zero-configuration service wiring.

Canon wraps `inventory::submit!` behind the `__submit` re-export to keep the
dependency internal:

```rust
// In canon-core lib.rs
pub use inventory::submit as __submit;
```

### Verifying registrations in tests

You can verify that registrations exist using `inventory::iter`:

```rust
#[test]
fn inventory_has_event_combiner_registrations() {
    let count = inventory::iter::<EventCombinerRegistration>
        .into_iter()
        .count();
    assert!(count >= 2, "expected at least 2 event combiners, got {count}");
}

#[test]
fn inventory_has_command_registrations() {
    let count = inventory::iter::<CommandRegistration>
        .into_iter()
        .count();
    assert!(count >= 2, "expected at least 2 commands, got {count}");
}
```

---

## `ServiceBuilder` discovery

`ServiceBuilder` ties everything together. It scans `inventory` registrations, validates
that all commands have handlers and all events have combiners, and wires up the runtime
infrastructure:

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
```

The `.for_aggregate::<Ship>()` call is the critical piece -- it tells `ServiceBuilder`
which aggregate this service manages, enabling it to validate that:

1. Every `CommandRegistration` for this aggregate has a matching `CommandHandlerRegistration`.
2. Every `EventRegistration` for this aggregate has a matching `EventCombinerRegistration`.
3. All `EventHandlerRegistration`s reference valid event types.

After `.build()`, the service is fully wired with all stores, consumers, and background
tasks ready to start.

---

## Putting it all together -- service file layout

A typical Canon service follows this file structure:

```
my-service/src/
    aggregate.rs       -- #[aggregate], enum types
    events.rs          -- #[event] structs, optionally #[event_combiner] if inline
    combiners.rs       -- #[event_combiner] impls (if separate from events.rs)
    commands.rs        -- #[command] structs
    handlers.rs        -- #[command_handler] impls
    event_handlers.rs  -- #[event_handler] impls
    projection.rs      -- #[projection] + #[projection_handler] impls
    error.rs           -- thiserror domain errors
    lib.rs             -- pub mod declarations
    main.rs            -- ServiceBuilder wiring, Dispatcher, shutdown
```

The fleet-service in the demo follows exactly this layout:

```
fleet-service/src/
    aggregate.rs       -- Ship aggregate with ShipStatus enum
    events.rs          -- 6 event structs + FleetEvent enum
    combiners.rs       -- 6 event combiner impls
    commands.rs        -- 6 command structs
    handlers.rs        -- 6 command handler impls
    event_handlers.rs  -- ResupplyHandler (cross-service event handler)
    projection.rs      -- ShipReadModel + 6 projection handlers
    error.rs           -- FleetError enum
    inbound.rs         -- Inbound event types (from other services)
    lib.rs             -- Module declarations
    main.rs            -- Full infrastructure wiring
```

The key insight is that **all domain logic lives in the annotated structs and impl
blocks**. Infrastructure wiring happens only in `main.rs`. The macros bridge the gap
by generating the trait implementations, dispatch functions, and registrations that
connect your domain logic to the Canon runtime.
