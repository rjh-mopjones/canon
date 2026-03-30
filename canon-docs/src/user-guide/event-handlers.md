# Event Handlers

Event handlers are the reactive backbone of a Canon service. While command handlers
implement write-side business logic (validate a command, produce an event), event
handlers close the loop: they observe events -- from this service or from other
services -- and optionally produce new commands in response. This is how Canon builds
multi-step workflows, cross-service choreography, and saga-like patterns without a
central orchestrator.

This chapter covers everything you need to know to write, configure, and debug event
handlers in Canon.

---

## What event handlers do

An event handler receives a batch of events and optionally returns a single
`CommandEnvelope`. That command re-enters the local inbox for dispatch, continuing
the event chain.

Key properties:

- **Aggregate-agnostic.** Unlike command handlers, event handlers have no aggregate
  type parameter. They react to events regardless of which aggregate produced them.
  This is by design -- an event handler in the cargo service can react to a
  `ShipArrivedAtStation` event from the navigation service without knowing anything
  about the `Route` aggregate.

- **Zero or one command output.** A handler returns `Option<CommandEnvelope>`. Return
  `None` if the handler is side-effect-only (logging, metrics, notifications). Return
  `Some(envelope)` to feed a command back into the pipeline.

- **Fan-out.** Multiple handlers can register for the same event type. Each handler
  is an independent consumer with its own window and oversight state. A single
  `ShipArrivedAtStation` event can trigger handlers in the cargo service, the station
  service, and the fleet service simultaneously.

- **Source-agnostic.** Event handlers work identically for internal events (this
  service's own events, routed back via the internal event consumer) and external
  events (from other services, arriving via the adaptor). The inbox treats both the
  same way.

- **Idempotent by contract.** Every event handler must be safe to call twice with the
  same event batch. The inbox deduplicates at the message level, but downstream
  idempotency is still required because consumers restart from offset zero on
  reboot.

---

## The EventHandler trait

The trait that all event handlers implement:

```rust
#[async_trait]
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

You never implement this trait directly. The `#[event_handler]` macro generates the
trait impl, the struct definition, and the `inventory` registration.

### Associated types

| Type | Purpose |
|------|---------|
| `Event` | The concrete event type this handler consumes. Set by `#[handles]`. |
| `Error` | Always `MacroError` in macro-generated code. |

### Methods

| Method | Required | Default | Purpose |
|--------|----------|---------|---------|
| `handle` | Yes | -- | Process the event batch, optionally return a command. |
| `oversight` | No | `Oversight::Ready` | Control when the batch is dispatched. |

---

## The `#[event_handler]` macro

The macro is the only way to define an event handler. It generates:

1. A public unit struct with the handler's name.
2. An inherent method `__canon_handle` containing your logic.
3. An `impl EventHandler` that delegates to the inherent method.
4. An `inventory` registration (`EventHandlerRegistration`) so `ServiceBuilder`
   discovers the handler automatically.

### Basic syntax

```rust
#[event_handler]
impl MyHandler {
    #[handles(MyEvent, version = 1)]
    fn handle(&self, events: Vec<MyEvent>) -> Option<CommandEnvelope> {
        // ...
    }
}
```

### With windowing and oversight

```rust
#[event_handler(window_ttl = "30m")]
impl MyWindowedHandler {
    #[handles(MyEvent, version = 1)]
    fn handle(&self, events: Vec<MyEvent>) -> Option<CommandEnvelope> {
        // ...
    }

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        // ...
    }
}
```

### What the macro generates

For this input:

```rust
#[event_handler]
impl DepartureHandler {
    #[handles(ShipDeparted, version = 1)]
    fn handle(&self, events: Vec<ShipDeparted>) -> Option<CommandEnvelope> {
        let event = events.last()?;
        Some(build_plan_route_command(event))
    }
}
```

The macro produces (simplified):

```rust
pub struct DepartureHandler;

impl DepartureHandler {
    fn __canon_handle(&self, events: Vec<ShipDeparted>) -> Option<CommandEnvelope> {
        let event = events.last()?;
        Some(build_plan_route_command(event))
    }
}

#[async_trait]
impl EventHandler for DepartureHandler {
    type Event = ShipDeparted;
    type Error = MacroError;

    async fn handle(
        &self,
        events: Vec<ShipDeparted>,
    ) -> Result<Option<CommandEnvelope>, MacroError> {
        Ok(self.__canon_handle(events))
    }
}

// Auto-registration via inventory
inventory::submit! {
    EventHandlerRegistration {
        handler_type_name: "DepartureHandler",
        event_type_name: "ShipDeparted",
        event_version: 1,
        window_ttl_secs: None,
    }
}
```

---

## The `#[handles]` attribute

Every event handler's `handle` method must carry a `#[handles]` attribute that
declares the event type and version:

```rust
#[handles(EventType, version = N)]
```

This tells the framework two things:

1. **Which event type** to route to this handler. The framework matches on the
   `event_type` string from the `EventEnvelope`.
2. **Which version** to match. The `event_version` field from the envelope must
   equal `N` for the handler to fire.

### Version matching

Version matching is exact. If you have:

```rust
#[handles(ShipDeparted, version = 1)]
```

This handler fires only for events where `event_version == 1`. If the fleet service
later adds a v2 `ShipDeparted` event with additional fields, you need a separate
handler (or update the existing one to handle v2).

Unlike `#[command]` and `#[event]`, event handlers are not required to be exhaustive.
If no handler is registered for a given event type/version, the framework logs a
warning but does not produce a compile error. This is intentional -- not every event
needs a reactive handler.

### The event type parameter

The `Event` type in `#[handles]` is the deserialized Rust type. The framework
deserializes the event envelope's payload bytes into this type before calling your
handler.

For handlers that consume multiple event variants (as in the cargo service), you
can use an enum:

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

#[event_handler(window_ttl = "30m")]
impl UnloadingHandler {
    #[handles(CargoEvent, version = 1)]
    fn handle(&self, events: Vec<CargoEvent>) -> Option<CommandEnvelope> {
        let mc = events.iter().find_map(|e| {
            if let CargoEvent::ManifestCreated(mc) = e {
                Some(mc)
            } else {
                None
            }
        })?;
        // Build BeginUnloading command from the ManifestCreated data
        Some(build_begin_unloading_command(mc))
    }

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        // ...
    }
}
```

---

## Simple (immediate) event handlers

The simplest event handler processes a single event immediately, with no windowing.
When the default `oversight` function returns `Ready` on every call, the inbox
dispatches each incoming event as a batch of one:

```rust
use canon_core::{event_handler, AggregateId, CommandEnvelope};

#[event_handler]
impl DepartureHandler {
    #[handles(ShipDeparted, version = 1)]
    fn handle(&self, events: Vec<ShipDeparted>) -> Option<CommandEnvelope> {
        let event = events.last()?;

        let route_id = Uuid::new_v4();
        let command = PlanRoute {
            route_id,
            ship_id: event.ship_id,
            waypoints: vec![event.destination],
        };
        let payload = serde_json::to_vec(&command).ok()?;

        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::from_uuid(route_id),
            command_type: "PlanRoute".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }
}
```

This is the navigation service's `DepartureHandler`. When the fleet service publishes a
`ShipDeparted` event, this handler receives it and produces a `PlanRoute` command that
creates a new `Route` aggregate.

### When to use simple handlers

Use immediate (no-window) handlers when:

- The handler reacts to a single event type.
- No coordination with other events is needed.
- The handler should fire as soon as the event arrives.

Most cross-service event handlers are simple handlers. The navigation service does not
need to wait for anything else when a ship departs -- it can plan the route immediately.

---

## Windowed event handlers

Some workflows require multiple events to arrive before the handler can act. Canon
supports this through **windowed event handlers** using the `window_ttl` attribute
and the `oversight` method.

### How windows work

When a handler has `window_ttl`, the inbox accumulates incoming events into a
**window** instead of dispatching them immediately. Each window is identified by a
composite key: `(handler_id, correlation_key)`. Events with the same correlation key
accumulate in the same window.

The window lifecycle:

```
Event arrives
  -> Inbox deduplicates (handler_id + message_id)
  -> Event added to window (handler_id + correlation_key)
  -> Oversight function evaluates accumulated messages
     -> Ready:    drain window, dispatch batch to handler
     -> NotReady: keep accumulating
     -> Discard:  clear window, events are dropped
  -> If window_ttl expires while NotReady:
     -> Window status changes to Expired
     -> Messages move to dead letter store with reason "window_expired"
```

### Window status lifecycle

The `WindowStatus` enum tracks each window's state:

```rust
pub enum WindowStatus {
    Pending,      // Waiting for more messages or Ready from oversight
    Dispatched,   // Oversight returned Ready; batch was published
    Expired,      // TTL elapsed while still Pending
    DeadLettered, // Moved to dead letter store after expiry
}
```

### The `window_ttl` attribute

The `window_ttl` attribute sets the maximum time a window stays open. The syntax
accepts human-readable durations:

```rust
#[event_handler(window_ttl = "30m")]   // 30 minutes
#[event_handler(window_ttl = "5m")]    // 5 minutes
#[event_handler(window_ttl = "2h")]    // 2 hours
```

If oversight never returns `Ready` within this duration, the window expires. Expired
windows are swept by a background task, collected, and moved to the dead letter store.

### Compile-time enforcement

Specifying `window_ttl` without an `oversight` method is a compile error:

```rust
// This will NOT compile:
#[event_handler(window_ttl = "30m")]
impl BrokenHandler {
    #[handles(SomeEvent, version = 1)]
    fn handle(&self, events: Vec<SomeEvent>) -> Option<CommandEnvelope> {
        None
    }
    // Missing oversight method -- compile error!
}
```

The rationale: a window with the default `oversight` (always `Ready`) would dispatch
immediately on the first event, making the TTL meaningless. If you want immediate
dispatch, omit `window_ttl` entirely.

---

## Oversight gates

Oversight is Canon's mechanism for controlling when a batch of accumulated events is
ready for processing. The `oversight` method inspects all messages accumulated in the
current window and returns one of three verdicts.

### The Oversight enum

```rust
pub enum Oversight {
    Ready,     // Dispatch the batch now
    NotReady,  // Wait for more messages
    Discard,   // Abandon this window entirely
}
```

### Ready

Return `Ready` when the window contains all required events. The inbox drains the
window and publishes the batch to the inbound queue for dispatch.

### NotReady

Return `NotReady` when some required events have not yet arrived. The inbox keeps the
window open and re-evaluates oversight when the next event arrives.

### Discard

Return `Discard` to abandon the window. All accumulated messages are dropped. This is
useful when a terminal event arrives that makes the entire workflow invalid. For
example, if a ship is decommissioned while cargo unloading was pending, there is no
point in continuing to wait.

### Oversight evaluation

Oversight is evaluated every time a new message is added to a window. The `accumulated`
slice contains all messages in the window so far, including the one just added.

The messages in `accumulated` are `IncomingMessage` envelopes, not deserialized event
types. This is because a window may contain events of different types (internal and
external). You inspect the raw envelope metadata to make routing decisions:

```rust
fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
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
```

### Real example: cargo unloading

The cargo service's `UnloadingHandler` is the canonical example of windowed event
handling with oversight. It waits for two events from different sources before acting:

```rust
#[event_handler(window_ttl = "30m")]
impl UnloadingHandler {
    #[handles(CargoEvent, version = 1)]
    fn handle(&self, events: Vec<CargoEvent>) -> Option<CommandEnvelope> {
        // Find the ManifestCreated event to extract manifest metadata
        let mc = events.iter().find_map(|e| {
            if let CargoEvent::ManifestCreated(mc) = e {
                Some(mc)
            } else {
                None
            }
        })?;

        // Build a BeginUnloading command targeting the manifest aggregate
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
        // Discard the window if the ship was decommissioned
        let decommissioned = accumulated.iter().any(|m| {
            matches!(
                m,
                IncomingMessage::ExternalEvent(e) if e.event_type == "ShipDecommissioned"
            )
        });
        if decommissioned {
            return Oversight::Discard;
        }

        // Ready only when BOTH a ship arrival and a manifest creation are present
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

This handler demonstrates three oversight states:

1. **NotReady** -- when only one of the two required events has arrived. The window
   stays open, accumulating messages.
2. **Ready** -- when both `ShipArrivedAtStation` (external, from navigation) and
   `ManifestCreated` (internal, from cargo's own pipeline) are present. The batch is
   dispatched and the handler produces a `BeginUnloading` command.
3. **Discard** -- when `ShipDecommissioned` arrives. The window is abandoned because
   unloading a decommissioned ship is nonsensical.

---

## Correlation keys

The window key is `(handler_id, correlation_key)`. The correlation key determines
which events belong to the same window.

### Default: envelope correlation_id

If you omit the `correlate` method, Canon uses the envelope's `correlation_id` field.
Events sharing the same `correlation_id` are grouped into the same window. This works
well when the correlation chain is properly maintained through the pipeline -- a single
user action (e.g., clicking "Depart") produces a chain of events that all share the
same `correlation_id`.

### Custom correlation

Override `correlate` to extract a domain-specific key when the default is not
sufficient:

```rust
fn correlate(&self, message: &IncomingMessage) -> Uuid {
    match message {
        IncomingMessage::ExternalEvent(e) => {
            // Use the ship_id from the payload as correlation key.
            // This groups all events for the same ship into one window,
            // regardless of which correlation chain they belong to.
            extract_ship_id(&e.payload)
        }
        _ => message.correlation_id(),
    }
}
```

### Independent concurrent windows

Each unique correlation key creates an independent window. A single handler may have
many concurrent in-flight windows. For example, if cargo unloading events arrive for
three different ships simultaneously, the `UnloadingHandler` maintains three separate
windows, each with its own oversight state and TTL countdown.

### Window key vs aggregate_id

The window key is never `aggregate_id`. This is intentional. Event handlers are
aggregate-agnostic, and the correlation key comes from the handler's `correlate`
function or the envelope's `correlation_id`. Using `aggregate_id` as the window key
would couple the handler to a specific aggregate, breaking the aggregate-agnostic
design.

---

## Cross-service event handling

One of Canon's most powerful features is cross-service event choreography. Services
communicate by publishing events to Kafka topics and reacting to events from other
services.

### The cross-service flow

The demo application implements a four-service cascade:

```
Fleet:ShipDeparted
  -> Navigation:DepartureHandler produces PlanRoute
     -> Navigation:RoutePlanned
        -> Navigation:RecordArrival
           -> Navigation:ShipArrivedAtStation
              -> Station:RecordDocking
              -> Cargo:UnloadingHandler (windowed)
                 -> Station:StationStockLow
                    -> Supply:RequestResupply
                       -> Supply:ResupplyDispatched
                          -> Fleet:ResupplyHandler produces ScheduleResupply
```

### How events cross service boundaries

The routing infrastructure has three layers:

1. **Publisher consumer.** Each service's outbound queue has a publisher consumer that
   writes events to `canon.{service}.events` Kafka topics. This makes events available
   to other services.

2. **Adaptor (or cross-service consumer).** Each service subscribes to the external
   Kafka topics it cares about. When an event arrives, the consumer deserializes it
   and submits a command to the local inbox.

3. **Internal event consumer.** Each service routes its own events back to the inbox
   so internal event handlers can react to them.

From the handler's perspective, the source does not matter. Both internal and external
events arrive through the inbox and are dispatched to handlers that match on
`event_type` and `event_version`.

### Anti-corruption layer types

Each service defines its own deserialization types for foreign events. These are simple
structs that extract only the fields the consuming service needs:

```rust
// navigation-service/src/inbound.rs

/// Inbound representation of fleet-service's ShipDeparted event.
/// Navigation-service uses ship_id + destination to plan a route.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct InboundShipDeparted {
    pub ship_id: Uuid,
    pub destination: Uuid,
}
```

```rust
// fleet-service/src/inbound.rs

/// Inbound representation of supply-service's ResupplyDispatched event.
/// Fleet-service uses ship_id + fuel_kg to schedule resupply.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct InboundResupplyDispatched {
    pub ship_id: Uuid,
    pub fuel_kg: f32,
}
```

These anti-corruption layer types insulate the consuming service from changes in the
producing service's event schema. If the fleet service adds a `fuel_at_departure` field
to `ShipDeparted`, navigation-service is unaffected because `InboundShipDeparted` only
deserializes the fields it needs.

### Subscribing to external topics

Services declare which external Kafka topics they consume through `ServiceBuilder`:

```rust
ServiceBuilder::new()
    .for_aggregate::<Ship>()
    .subscribe_to("canon.navigation.events")
    .subscribe_to("canon.supply.events")
    .build()
```

The framework's adaptor handles the rest: connecting to Kafka, polling for records,
deserializing envelopes, and routing events to matching handlers through the inbox.

### Real example: the full departure flow

When a user commands a ship to depart, the following chain executes across four
services:

1. **Fleet service** receives `DepartForStation` command. Command handler produces
   `ShipDeparted` event. Event goes to outbox, then to `canon.fleet.events`.

2. **Navigation service** has a `DepartureHandler` that handles `ShipDeparted`. It
   produces a `PlanRoute` command:

   ```rust
   #[event_handler]
   impl DepartureHandler {
       #[handles(ShipDeparted, version = 1)]
       fn handle(&self, events: Vec<ShipDeparted>) -> Option<CommandEnvelope> {
           let event = events.last()?;
           let route_id = Uuid::new_v4();
           let command = PlanRoute {
               route_id,
               ship_id: event.ship_id,
               waypoints: vec![event.destination],
           };
           let payload = serde_json::to_vec(&command).ok()?;
           Some(CommandEnvelope {
               command_id: Uuid::new_v4(),
               aggregate_id: AggregateId::from_uuid(route_id),
               command_type: "PlanRoute".into(),
               correlation_id: Uuid::new_v4(),
               causation_id: Uuid::new_v4(),
               timestamp: Utc::now(),
               payload: Bytes::from(payload),
               command_version: 1,
           })
       }
   }
   ```

3. The `PlanRoute` command creates a `Route` aggregate, producing `RoutePlanned`. The
   navigation service's self-consumer sees `RoutePlanned` and submits `RecordArrival`,
   which produces `ShipArrivedAtStation` on `canon.navigation.events`.

4. **Station service** and **cargo service** both subscribe to
   `canon.navigation.events`. The station service submits `RecordDocking`. The cargo
   service's `UnloadingHandler` (windowed) waits for both the arrival event and a
   manifest creation before dispatching.

5. If station stock drops below threshold, the station service publishes
   `StationStockLow`. The **supply service** reacts by submitting `RequestResupply`,
   which eventually produces `ResupplyDispatched`.

6. **Fleet service** has a `ResupplyHandler` that reacts to `ResupplyDispatched`:

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

The entire chain -- from user click to resupply scheduling -- is driven by events
flowing through Kafka with no central coordinator.

---

## InboxPort: local command re-entry

When an event handler returns `Some(CommandEnvelope)`, the dispatcher does not send it
over the network. Instead, it submits the command back into the **local inbox** via the
`InboxPort` trait:

```rust
#[async_trait]
pub trait InboxPort: Send + Sync + 'static {
    /// Submit a command envelope to the local inbox for dispatch.
    ///
    /// Implementations must be idempotent: re-submitting the same
    /// command_id is a safe no-op.
    async fn submit(&self, command: CommandEnvelope) -> Result<(), InboxPortError>;
}
```

This is local re-entry only. The command stays within the same service boundary. If a
handler needs to trigger a command in a different service, the pattern is different:
the handler produces an event, that event gets published to Kafka, and the other
service's adaptor picks it up.

### Why local re-entry?

Local re-entry keeps the single-writer invariant intact. Each service owns its own
aggregates. An event handler in the cargo service cannot directly write to the fleet
service's inbox -- it can only produce events that the fleet service reacts to.

The `InboxPort` is injected by `ServiceBuilder` at startup. In tests, you use
`InMemoryInboxPort`, which stores submitted commands in a `Vec` behind a `Mutex`:

```rust
let port = InMemoryInboxPort::new();
// ... run the handler ...
let submitted = port.submitted().unwrap();
assert_eq!(submitted.len(), 1);
assert_eq!(submitted[0].command_type, "PlanRoute");
```

---

## Handler lifecycle: step by step

Understanding the full lifecycle helps when debugging. Here is exactly what happens
when an event reaches a handler:

1. **Event arrives.** Either the internal event consumer (for this service's own events)
   or the adaptor (for external events) receives an event envelope from Kafka.

2. **Handler matching.** The framework checks the `EventHandlerRegistration` inventory
   for all handlers whose `event_type_name` and `event_version` match the envelope.

3. **Inbox submission.** For each matching handler, the framework calls
   `Inbox::submit(handler_id, message, inbound_queue)`.

4. **Deduplication.** The inbox checks the `(handler_id, message_id)` composite key.
   If this exact combination has been seen before, the submit is a no-op.

5. **Window accumulation.** The event is added to the handler's window, keyed by
   `(handler_id, correlation_key)`. If no window exists for this key, a new one is
   created (with `expires_at` set if the handler has `window_ttl`).

6. **Oversight evaluation.** The inbox calls the handler's `oversight` function with
   all accumulated messages in the window.

7. **Dispatch decision.**
   - `Ready`: the window is drained and the batch is published to the inbound queue.
   - `NotReady`: nothing happens; the window stays open.
   - `Discard`: the window is cleared; all accumulated messages are dropped.

8. **Handler execution.** The dispatcher polls the inbound queue, deserializes the
   events into the handler's `Event` type, and calls `handle(events)`.

9. **Command re-entry.** If the handler returns `Some(CommandEnvelope)`, the dispatcher
   submits it to the inbox via `InboxPort`. This command then follows the normal
   command dispatch path (hydrate aggregate state, run command handler, write to outbox).

---

## Error handling and dead lettering

### Handler errors

If `handle()` returns `Err`, the dispatcher records the failure using the
`RetryTracker`. The `retry_attempts` table is crash-safe -- if the service restarts
mid-retry, the attempt count persists.

```
Attempt 1: handler fails -> record_failure() -> attempts = 1
Attempt 2: handler fails -> record_failure() -> attempts = 2
Attempt 3: handler fails -> record_failure() -> attempts = 3 (>= max_retries)
  -> dead_letter() -> message moves to dead letter store
```

The default `max_retries` is 3.

### Window expiry

Windows that never reach `Ready` within their TTL are expired by a background sweep
task:

1. `sweep_expired_windows()` scans all pending windows and marks those past their
   `expires_at` as `Expired`.
2. `collect_expired_windows()` drains the expired windows and returns them to the
   caller.
3. The caller moves the expired messages to the dead letter store with reason
   `window_expired`.

### Dead letter inspection and requeue

Dead-lettered messages are not lost. They are stored in the `dead_letters` table and
can be inspected via the admin API:

```
GET /admin/deadletters
```

To requeue a dead letter, the admin API calls `inbox.requeue_window()`, which:

1. Clears the dedup entries for the affected messages (so they can be reprocessed).
2. Re-submits each message through the normal inbox path.
3. Oversight runs again from scratch with a fresh `expires_at`.

This allows operators to fix the underlying issue (deploy a bug fix, update
configuration) and then replay the failed messages without data loss.

---

## Idempotency requirements

All event handlers must be safe to call twice with the same event batch. This is a
non-negotiable design requirement in Canon because:

1. **Consumers restart from offset zero.** Canon does not commit Kafka offsets. On
   service restart, every consumer replays from the beginning. Application-layer
   idempotency (inbox deduplication, `ON CONFLICT DO NOTHING` for commands) prevents
   duplicate processing.

2. **At-least-once delivery.** Kafka guarantees at-least-once delivery. A handler may
   receive the same event twice if the consumer crashes after processing but before
   advancing its in-memory offset.

3. **Dead letter requeue.** When a dead letter is requeued, the handler receives the
   same messages again.

The inbox provides message-level deduplication via the `(handler_id, message_id)`
composite key, but the handler's own command construction must also be safe. Use
deterministic command IDs when possible:

```rust
let command_id = deterministic_command_id(source_event_id, "PlanRoute");
```

This ensures that replaying the same event produces the same command ID, which is
rejected by the `ON CONFLICT DO NOTHING` clause in the inbox insert.

---

## Testing event handlers

### Unit testing oversight

The `oversight` method is synchronous and takes a simple slice, making it easy to
test in isolation:

```rust
#[test]
fn oversight_not_ready_when_only_arrival_present() {
    let handler = UnloadingHandler;
    let accumulated = vec![external_event("ShipArrivedAtStation")];
    assert_eq!(handler.oversight(&accumulated), Oversight::NotReady);
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
```

### Unit testing handle

The `handle` method is async but straightforward to test:

```rust
#[tokio::test]
async fn handle_returns_command_when_manifest_present() {
    let handler = UnloadingHandler;
    let events = vec![CargoEvent::ManifestCreated(ManifestCreated {
        manifest_id: Uuid::new_v4(),
        ship_id: Uuid::new_v4(),
        voyage_id: Uuid::new_v4(),
    })];

    let result = handler.handle(events).await.unwrap();
    let cmd = result.unwrap();
    assert_eq!(cmd.command_type, "BeginUnloading");
}

#[tokio::test]
async fn handle_returns_none_for_empty_batch() {
    let handler = UnloadingHandler;
    let result = handler.handle(vec![]).await.unwrap();
    assert!(result.is_none());
}
```

### Integration testing with InMemoryInbox

For testing the full inbox-to-handler flow, use `InMemoryInbox` with
`InMemoryInboundQueue`:

```rust
let inbox = InMemoryInbox::new();
let queue = InMemoryInboundQueue::new();

// Register handler with oversight that waits for 2 messages
inbox.register_handler("my_handler", |accumulated| {
    if accumulated.len() >= 2 {
        Oversight::Ready
    } else {
        Oversight::NotReady
    }
}).unwrap();

// First message -- oversight returns NotReady
inbox.submit("my_handler", first_event, &queue).unwrap();
assert!(queue.receive().unwrap().is_none());

// Second message -- oversight returns Ready, batch dispatched
inbox.submit("my_handler", second_event, &queue).unwrap();
let batch = queue.receive().unwrap().unwrap();
assert_eq!(batch.len(), 2);
```

---

## Summary

| Concept | Key point |
|---------|-----------|
| Aggregate-agnostic | No aggregate type parameter on `EventHandler` |
| `#[handles]` | Declares event type + version for routing |
| Default oversight | `Ready` -- immediate dispatch, no windowing |
| `window_ttl` | Requires `oversight` method (compile error without) |
| Window key | `(handler_id, correlation_key)` -- never `aggregate_id` |
| Correlation key | From `correlate` fn or fallback to `correlation_id` |
| Cross-service | Events flow via Kafka topics; handler does not know the source |
| InboxPort | Local re-entry for produced commands; cross-service is REST/events |
| Idempotency | Required. Inbox dedup + deterministic command IDs |
| Dead lettering | Max retries exceeded or window TTL expired |
| Requeue | Clears dedup, re-submits through normal inbox path |
