use canon_core::EventEnvelope;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Manifest aggregate state
// ---------------------------------------------------------------------------

/// Current status of a manifest in its lifecycle.
#[derive(Default, Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ManifestStatus {
    #[default]
    Open,
    Unloading,
    Closed,
}

/// A single cargo item tracked within a manifest.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CargoItem {
    pub item_id: Uuid,
    pub weight_kg: u32,
    pub unloaded: bool,
}

/// The Manifest aggregate — the domain object for cargo tracking.
///
/// Uses `#[canon_core::aggregate]` which generates:
/// - `impl Aggregate` with `type State = ManifestState`
/// - `Default`, `Serialize`, `Deserialize` derives
/// - version-matched hydration dispatch via `inventory`
#[canon_core::aggregate(snapshot_every = 50)]
pub struct ManifestState {
    pub ship_id: Option<Uuid>,
    pub voyage_id: Option<Uuid>,
    pub items: Vec<CargoItem>,
    pub status: ManifestStatus,
}

// ---------------------------------------------------------------------------
// Event combiners — manual trait impls + inventory registrations
// ---------------------------------------------------------------------------
//
// We implement these manually rather than using the #[event_combiner] macro
// because the event types are defined in the shared crate, and Rust orphan
// rules prevent implementing the macro-generated marker traits for foreign types.
// The EventCombiner<ManifestState> impls are allowed because ManifestState is local.

use crate::events::{
    CargoLoaded, CargoUnloaded, ManifestClosed, ManifestCreated, UnloadingStarted,
};

impl canon_core::EventCombiner<ManifestState> for ManifestCreated {
    fn combine(&self, state: &mut ManifestState) {
        state.ship_id = Some(self.ship_id);
        state.voyage_id = Some(self.voyage_id);
        state.status = ManifestStatus::Open;
    }
}

fn __canon_apply_manifestcreated_v1(
    payload: &[u8],
    state: &mut dyn std::any::Any,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let event: ManifestCreated = canon_core::__deserialize(payload)?;
    let state = state.downcast_mut::<ManifestState>().ok_or_else(
        || -> Box<dyn std::error::Error + Send + Sync> {
            "aggregate state type mismatch in event combiner".into()
        },
    )?;
    <ManifestCreated as canon_core::EventCombiner<ManifestState>>::combine(&event, state);
    Ok(())
}

canon_core::__submit! {
    canon_core::EventCombinerRegistration {
        aggregate_type_id: std::any::TypeId::of::<ManifestState>(),
        event_type_name: "ManifestCreated",
        event_version: 1,
        apply_fn: __canon_apply_manifestcreated_v1,
    }
}

/// CargoLoaded is registered at version 2 — the current schema version.
/// v1 events are upcast to v2 before reaching the combiner (see `upcast` module).
impl canon_core::EventCombiner<ManifestState> for CargoLoaded {
    fn combine(&self, state: &mut ManifestState) {
        state.items.push(CargoItem {
            item_id: self.item_id,
            weight_kg: self.weight_kg.max(0.0).round() as u32,
            unloaded: false,
        });
    }
}

fn __canon_apply_cargoloaded_v2(
    payload: &[u8],
    state: &mut dyn std::any::Any,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let event: CargoLoaded = canon_core::__deserialize(payload)?;
    let state = state.downcast_mut::<ManifestState>().ok_or_else(
        || -> Box<dyn std::error::Error + Send + Sync> {
            "aggregate state type mismatch in event combiner".into()
        },
    )?;
    <CargoLoaded as canon_core::EventCombiner<ManifestState>>::combine(&event, state);
    Ok(())
}

canon_core::__submit! {
    canon_core::EventCombinerRegistration {
        aggregate_type_id: std::any::TypeId::of::<ManifestState>(),
        event_type_name: "CargoLoaded",
        event_version: 2,
        apply_fn: __canon_apply_cargoloaded_v2,
    }
}

/// v1 combiner for CargoLoaded — deserializes the v1 payload (which lacks the
/// `description` and `manifest_id` fields) and applies it via the v2 combiner.
/// The combiner only uses `item_id` and `weight_kg` for state folding, so the
/// missing `manifest_id` (set to nil) and `description` (set to migration marker)
/// have no effect on aggregate state.
fn __canon_apply_cargoloaded_v1(
    payload: &[u8],
    state: &mut dyn std::any::Any,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[derive(serde::Deserialize)]
    struct CargoLoadedV1 {
        item_id: uuid::Uuid,
        weight_kg: f32,
    }
    let v1: CargoLoadedV1 = canon_core::__deserialize(payload)?;
    let v2 = CargoLoaded {
        manifest_id: uuid::Uuid::nil(),
        item_id: v1.item_id,
        weight_kg: v1.weight_kg,
        description: "(migrated from v1)".to_string(),
    };
    let state = state.downcast_mut::<ManifestState>().ok_or_else(
        || -> Box<dyn std::error::Error + Send + Sync> {
            "aggregate state type mismatch in event combiner".into()
        },
    )?;
    <CargoLoaded as canon_core::EventCombiner<ManifestState>>::combine(&v2, state);
    Ok(())
}

canon_core::__submit! {
    canon_core::EventCombinerRegistration {
        aggregate_type_id: std::any::TypeId::of::<ManifestState>(),
        event_type_name: "CargoLoaded",
        event_version: 1,
        apply_fn: __canon_apply_cargoloaded_v1,
    }
}

impl canon_core::EventCombiner<ManifestState> for UnloadingStarted {
    fn combine(&self, state: &mut ManifestState) {
        state.status = ManifestStatus::Unloading;
    }
}

fn __canon_apply_unloadingstarted_v1(
    payload: &[u8],
    state: &mut dyn std::any::Any,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let event: UnloadingStarted = canon_core::__deserialize(payload)?;
    let state = state.downcast_mut::<ManifestState>().ok_or_else(
        || -> Box<dyn std::error::Error + Send + Sync> {
            "aggregate state type mismatch in event combiner".into()
        },
    )?;
    <UnloadingStarted as canon_core::EventCombiner<ManifestState>>::combine(&event, state);
    Ok(())
}

canon_core::__submit! {
    canon_core::EventCombinerRegistration {
        aggregate_type_id: std::any::TypeId::of::<ManifestState>(),
        event_type_name: "UnloadingStarted",
        event_version: 1,
        apply_fn: __canon_apply_unloadingstarted_v1,
    }
}

impl canon_core::EventCombiner<ManifestState> for CargoUnloaded {
    fn combine(&self, state: &mut ManifestState) {
        for item in &mut state.items {
            if item.item_id == self.item_id {
                item.unloaded = true;
            }
        }
    }
}

fn __canon_apply_cargounloaded_v1(
    payload: &[u8],
    state: &mut dyn std::any::Any,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let event: CargoUnloaded = canon_core::__deserialize(payload)?;
    let state = state.downcast_mut::<ManifestState>().ok_or_else(
        || -> Box<dyn std::error::Error + Send + Sync> {
            "aggregate state type mismatch in event combiner".into()
        },
    )?;
    <CargoUnloaded as canon_core::EventCombiner<ManifestState>>::combine(&event, state);
    Ok(())
}

canon_core::__submit! {
    canon_core::EventCombinerRegistration {
        aggregate_type_id: std::any::TypeId::of::<ManifestState>(),
        event_type_name: "CargoUnloaded",
        event_version: 1,
        apply_fn: __canon_apply_cargounloaded_v1,
    }
}

impl canon_core::EventCombiner<ManifestState> for ManifestClosed {
    fn combine(&self, state: &mut ManifestState) {
        state.status = ManifestStatus::Closed;
    }
}

fn __canon_apply_manifestclosed_v1(
    payload: &[u8],
    state: &mut dyn std::any::Any,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let event: ManifestClosed = canon_core::__deserialize(payload)?;
    let state = state.downcast_mut::<ManifestState>().ok_or_else(
        || -> Box<dyn std::error::Error + Send + Sync> {
            "aggregate state type mismatch in event combiner".into()
        },
    )?;
    <ManifestClosed as canon_core::EventCombiner<ManifestState>>::combine(&event, state);
    Ok(())
}

canon_core::__submit! {
    canon_core::EventCombinerRegistration {
        aggregate_type_id: std::any::TypeId::of::<ManifestState>(),
        event_type_name: "ManifestClosed",
        event_version: 1,
        apply_fn: __canon_apply_manifestclosed_v1,
    }
}

// ---------------------------------------------------------------------------
// Hydration helper for upcast support
// ---------------------------------------------------------------------------

/// Hydrate a ManifestState from raw event envelopes, supporting v1->v2 upcast
/// for CargoLoaded events. This wraps the generated `Aggregate::hydrate` and
/// pre-processes envelopes through the upcast layer.
pub fn hydrate_with_upcast(
    state: &mut ManifestState,
    events: impl Iterator<Item = EventEnvelope>,
) -> Result<(), canon_core::MacroError> {
    let upcasted: Result<Vec<EventEnvelope>, _> =
        events.map(crate::upcast::upcast_envelope).collect();
    let upcasted = upcasted.map_err(|e| canon_core::MacroError(e.to_string()))?;
    <ManifestState as canon_core::Aggregate>::hydrate(state, upcasted.into_iter())
}
