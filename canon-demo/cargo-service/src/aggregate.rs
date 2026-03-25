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
