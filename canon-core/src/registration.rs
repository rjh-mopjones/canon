use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::OnceLock;

use crate::EventEnvelope;

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
