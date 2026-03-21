use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::OnceLock;

use crate::EventEnvelope;

// ── Command handler dispatch function ───────────────────────────────────────

/// Result of a successful type-erased command handler dispatch.
///
/// Contains the serialized event payload and metadata needed to construct
/// an `EventEnvelope` without knowing the concrete event type.
#[derive(Debug, Clone)]
pub struct HandlerDispatchResult {
    /// Serialized event payload (JSON bytes).
    pub event_payload: Vec<u8>,
    /// The event type name (e.g., "ShipDeparted").
    pub event_type: &'static str,
    /// The event schema version.
    pub event_version: u32,
}

/// Type-erased function that deserializes a command, hydrates aggregate state from
/// events, runs the command handler, and serializes the resulting event.
///
/// Parameters:
/// - `command_payload`: the serialized command bytes
/// - `events`: the event envelopes to hydrate state from
/// - `aggregate_type_id`: TypeId of the aggregate (for combiner dispatch)
///
/// Returns `Ok(HandlerDispatchResult)` on success or a boxed error on failure.
pub type CommandDispatchFn =
    fn(
        command_payload: &[u8],
        events: &[EventEnvelope],
        aggregate_type_id: TypeId,
    ) -> Result<HandlerDispatchResult, Box<dyn std::error::Error + Send + Sync>>;

/// Composite key for O(1) command handler dispatch lookup:
/// (command type name, command version).
type HandlerDispatchKey = (String, u32);

/// Lazily-initialized lookup map from handler dispatch key to dispatch function.
/// Built once on first use from `inventory` registrations, then every
/// subsequent lookup is O(1).
static HANDLER_DISPATCH_MAP: OnceLock<HashMap<HandlerDispatchKey, CommandDispatchFn>> =
    OnceLock::new();

/// Type-erased function that deserializes an event and applies it to aggregate state.
pub type CombinerApplyFn =
    fn(&[u8], &mut dyn Any) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Composite key for O(1) combiner lookup: (aggregate TypeId, event type name, event version).
type CombinerKey = (TypeId, String, u32);

/// Lazily-initialized lookup map from combiner key to apply function.
/// Built once on first use from `inventory` registrations, then every
/// subsequent lookup is O(1) instead of O(R).
static COMBINER_MAP: OnceLock<HashMap<CombinerKey, CombinerApplyFn>> = OnceLock::new();

// ── Event combiner registration ─────────────────────────────────────────────

/// Runtime registration for version-matched event combiners.
/// Collected via `inventory` — the `#[aggregate]` macro's `hydrate()` iterates these.
pub struct EventCombinerRegistration {
    pub aggregate_type_id: TypeId,
    pub event_type_name: &'static str,
    pub event_version: u32,
    /// Deserializes event from payload bytes and applies the combiner to aggregate state.
    pub apply_fn: CombinerApplyFn,
}

inventory::collect!(EventCombinerRegistration);

// ── Command registration ────────────────────────────────────────────────────

/// Metadata registration for commands. Used by `ServiceBuilder` for discovery.
pub struct CommandRegistration {
    pub aggregate_type_name: &'static str,
    pub command_type_name: &'static str,
    pub command_version: u32,
}

inventory::collect!(CommandRegistration);

// ── Event registration ──────────────────────────────────────────────────────

/// Metadata registration for events. Used by `ServiceBuilder` for discovery.
pub struct EventRegistration {
    pub aggregate_type_name: &'static str,
    pub event_type_name: &'static str,
    pub event_version: u32,
}

inventory::collect!(EventRegistration);

// ── Command handler registration ────────────────────────────────────────────

/// Metadata registration for command handlers.
///
/// In addition to metadata used by `ServiceBuilder` for validation, each
/// registration carries a `dispatch_fn` — a type-erased function pointer
/// that can deserialize the command payload, hydrate aggregate state from
/// events, run the handler, and serialize the resulting event. This enables
/// the `Dispatcher` to process commands without knowing concrete types.
pub struct CommandHandlerRegistration {
    pub aggregate_type_name: &'static str,
    pub command_type_name: &'static str,
    pub command_version: u32,
    pub handler_type_name: &'static str,
    /// Type-erased dispatch function. Deserializes the command, hydrates
    /// aggregate state from events via combiners, runs the handler, and
    /// serializes the resulting event.
    pub dispatch_fn: CommandDispatchFn,
    /// The event type name produced by this handler (e.g., "ShipDeparted").
    pub produces_event_type: &'static str,
    /// The event version produced by this handler.
    pub produces_event_version: u32,
}

inventory::collect!(CommandHandlerRegistration);

// ── Event handler registration ──────────────────────────────────────────────

/// Metadata registration for event handlers.
pub struct EventHandlerRegistration {
    pub handler_type_name: &'static str,
    pub event_type_name: &'static str,
    pub event_version: u32,
    pub window_ttl_secs: Option<u64>,
}

inventory::collect!(EventHandlerRegistration);

// ── Projection registration ─────────────────────────────────────────────────

/// Metadata registration for projections.
pub struct ProjectionRegistration {
    pub projection_type_name: &'static str,
    pub projection_id: &'static str,
}

inventory::collect!(ProjectionRegistration);

// ── Projection handler registration ─────────────────────────────────────────

/// Metadata registration for projection handlers.
pub struct ProjectionHandlerRegistration {
    pub projection_type_name: &'static str,
    pub handler_type_name: &'static str,
}

inventory::collect!(ProjectionHandlerRegistration);

// ── Hydration helper ────────────────────────────────────────────────────────

/// Looks up the registered event combiner for this event envelope and applies it.
/// Called from the `#[aggregate]` macro's generated `hydrate()`.
///
/// Uses a lazily-initialized `HashMap` for O(1) lookup per event instead of
/// scanning all registered combiners linearly.
#[doc(hidden)]
pub fn __apply_event_combiner(
    aggregate_type_id: TypeId,
    envelope: &EventEnvelope,
    state: &mut dyn Any,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let map = COMBINER_MAP.get_or_init(|| {
        let mut m = HashMap::new();
        for reg in inventory::iter::<EventCombinerRegistration> {
            let key: CombinerKey = (
                reg.aggregate_type_id,
                reg.event_type_name.to_owned(),
                reg.event_version,
            );
            m.insert(key, reg.apply_fn);
        }
        m
    });

    let key = (
        aggregate_type_id,
        envelope.event_type.clone(),
        envelope.event_version,
    );
    match map.get(&key) {
        Some(apply_fn) => apply_fn(envelope.payload.as_ref(), state),
        None => Err(format!(
            "no event combiner registered for '{}' version {}",
            envelope.event_type, envelope.event_version
        )
        .into()),
    }
}

/// Deserialize a JSON payload into a concrete type.
/// Called from macro-generated code.
#[doc(hidden)]
pub fn __deserialize<T: serde::de::DeserializeOwned>(
    payload: &[u8],
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    Ok(serde_json::from_slice(payload)?)
}

/// Serialize a value to JSON bytes.
/// Called from macro-generated code (command handler dispatch functions).
#[doc(hidden)]
pub fn __serialize<T: serde::Serialize>(
    value: &T,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    Ok(serde_json::to_vec(value)?)
}

// ── Command handler dispatch helper ─────────────────────────────────────

/// Look up the registered command handler dispatch function for a given
/// command type name and version, and invoke it with the provided command
/// payload and event history.
///
/// Used by the `Dispatcher` to process inbox commands without knowing
/// concrete aggregate or command types.
#[doc(hidden)]
pub fn __dispatch_command(
    command_type: &str,
    command_version: u32,
    command_payload: &[u8],
    events: &[EventEnvelope],
    aggregate_type_id: TypeId,
) -> Result<HandlerDispatchResult, Box<dyn std::error::Error + Send + Sync>> {
    let map = HANDLER_DISPATCH_MAP.get_or_init(|| {
        let mut m = HashMap::new();
        for reg in inventory::iter::<CommandHandlerRegistration> {
            let key: HandlerDispatchKey = (reg.command_type_name.to_owned(), reg.command_version);
            m.insert(key, reg.dispatch_fn);
        }
        m
    });

    let key = (command_type.to_owned(), command_version);
    match map.get(&key) {
        Some(dispatch_fn) => dispatch_fn(command_payload, events, aggregate_type_id),
        None => Err(format!(
            "no command handler registered for '{}' version {}",
            command_type, command_version
        )
        .into()),
    }
}
