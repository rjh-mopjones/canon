use std::any::{Any, TypeId};

use crate::EventEnvelope;

/// Type-erased function that deserializes an event and applies it to aggregate state.
pub type CombinerApplyFn =
    fn(&[u8], &mut dyn Any) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

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
pub struct CommandHandlerRegistration {
    pub aggregate_type_name: &'static str,
    pub command_type_name: &'static str,
    pub command_version: u32,
    pub handler_type_name: &'static str,
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
#[doc(hidden)]
pub fn __apply_event_combiner(
    aggregate_type_id: TypeId,
    envelope: &EventEnvelope,
    state: &mut dyn Any,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for reg in inventory::iter::<EventCombinerRegistration> {
        if reg.aggregate_type_id == aggregate_type_id
            && reg.event_type_name == envelope.event_type
            && reg.event_version == envelope.event_version
        {
            return (reg.apply_fn)(envelope.payload.as_ref(), state);
        }
    }
    Err(format!(
        "no event combiner registered for '{}' version {}",
        envelope.event_type, envelope.event_version
    )
    .into())
}

/// Deserialize a JSON payload into a concrete type.
/// Called from macro-generated code.
#[doc(hidden)]
pub fn __deserialize<T: serde::de::DeserializeOwned>(
    payload: &[u8],
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    Ok(serde_json::from_slice(payload)?)
}
