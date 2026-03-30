# Event Handlers

Event handlers react to events and optionally produce commands. They are the primary
mechanism for building reactive workflows in Canon -- chains of events triggering
commands that produce more events, potentially across service boundaries.

## Key properties

- **Aggregate-agnostic** -- event handlers have no aggregate type parameter. They react
  to events regardless of which aggregate produced them.
- **Optional command output** -- a handler returns `Option<CommandEnvelope>`. Return
  `None` if no command is needed.
- **Fan-out** -- multiple handlers can register for the same event type. Each handler
  processes independently.
- **Works for both internal and external events** -- the inbox routes matching events
  to the handler regardless of source.

## The EventHandler trait

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
        Oversight::Ready  // default: dispatch immediately
    }

    fn correlate(
        &self,
        message: &IncomingMessage,
    ) -> Uuid {
        message.correlation_id()  // default: use envelope correlation_id
    }
}
```

You never implement this trait directly -- the `#[event_handler]` macro generates it.

## Simple event handlers

The simplest event handler processes a single event immediately:

```rust
#[event_handler]
impl ShipDepartedNotifier {
    #[handles(ShipDeparted, version = 1)]
    fn handle(&self, events: Vec<ShipDeparted>) -> Option<CommandEnvelope> {
        // Log, notify, or produce a command
        None
    }
}
```

With the default oversight (`Ready`), the inbox dispatches the event immediately with
no windowing.

## Oversight gates

Oversight controls when accumulated events are dispatched to the handler. Three states:

```rust
pub enum Oversight {
    Ready,     // dispatch the batch now
    NotReady,  // wait for more messages
    Discard,   // abandon this window
}
```

### When to use oversight

Use oversight when your handler needs to wait for multiple events before acting. For
example, cargo unloading requires both a ship arrival event and a manifest creation event:

```rust
#[event_handler(window_ttl = "30m")]
impl CargoUnloadingHandler {
    #[handles(CargoEvent, version = 1)]
    fn handle(&self, events: Vec<CargoEvent>) -> Option<CommandEnvelope> {
        // Both ShipArrived and ManifestCreated are in the batch
        // Safe to begin unloading
        Some(build_begin_unloading_command(&events))
    }

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        let has_arrival = accumulated.iter().any(|m| {
            matches!(m, IncomingMessage::ExternalEvent(e)
                if e.event_type == "ShipArrivedAtStation")
        });
        let has_manifest = accumulated.iter().any(|m| {
            matches!(m, IncomingMessage::InternalEvent(e)
                if e.event_type == "ManifestCreated")
        });

        if accumulated.iter().any(|m| {
            matches!(m, IncomingMessage::ExternalEvent(e)
                if e.event_type == "ShipDecommissioned")
        }) {
            return Oversight::Discard;
        }

        if has_arrival && has_manifest {
            Oversight::Ready
        } else {
            Oversight::NotReady
        }
    }
}
```

### Compile-time enforcement

`window_ttl` without an `oversight` method is a compile error. This prevents accidentally
creating windows that never become ready.

## Window TTL

The `window_ttl` attribute sets how long a window stays open. If the window does not
reach `Ready` within this time, it expires:

```rust
#[event_handler(window_ttl = "30m")]
```

Expired windows are:
1. Marked with status `expired` in the inbox
2. Moved to the dead letter store with reason `window_expired`
3. Available for inspection and manual requeue via the admin API

## Correlation keys

The window key is `(handler_id, correlation_key)`. The correlation key determines which
events belong to the same window.

### Default: envelope correlation_id

If you omit the `correlate` method, Canon uses the envelope's `correlation_id`. This
means events sharing the same correlation ID are grouped into the same window.

### Custom correlation

Override `correlate` to extract a domain-specific key:

```rust
fn correlate(&self, message: &IncomingMessage) -> Uuid {
    match message {
        IncomingMessage::ExternalEvent(e) => {
            // Use the ship_id from the payload as correlation key
            extract_ship_id(&e.payload)
        }
        _ => message.correlation_id(),
    }
}
```

Each unique correlation key creates an independent window. A handler may have many
concurrent in-flight windows.

## Cross-service event handlers

Event handlers work identically for internal and external events. The routing difference
is handled by the framework:

- **Internal events**: the outbound queue's internal event consumer routes the service's
  own events back to the inbox
- **External events**: the adaptor consumes events from other services' Kafka topics and
  submits them to the inbox

From the handler's perspective, both arrive the same way. You declare which external
topics to subscribe to via `ServiceBuilder`:

```rust
ServiceBuilder::new()
    .for_aggregate::<Ship>()
    .subscribe_to("canon.navigation.events")
    .subscribe_to("canon.supply.events")
    .build()
```

## Handler lifecycle

1. Event arrives (internal or external)
2. Framework checks `EventHandlerRegistration` inventory for matching handlers
3. For each matching handler, submits to inbox: `Inbox::submit(handler_id, event_id, message)`
4. Inbox deduplicates via `handler_id + message_id` composite key
5. Event is added to the handler's window (keyed by `(handler_id, correlation_key)`)
6. Oversight is evaluated
7. If `Ready`, the batch is published to the inbound queue
8. Dispatcher calls `handle(events)`
9. If the handler returns `Some(CommandEnvelope)`, it re-enters the inbox for command dispatch

## Produced commands

When a handler produces a command, it re-enters the local inbox via `InboxPort`. This
is local re-entry only -- cross-service commands go via REST.

The produced `CommandEnvelope` carries the correlation chain forward:
- `correlation_id` -- same as the triggering event's correlation_id (preserving the causal chain)
- `causation_id` -- the event_id of the triggering event
