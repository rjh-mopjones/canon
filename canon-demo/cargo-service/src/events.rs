use uuid::Uuid;

// ---------------------------------------------------------------------------
// Cargo events (aggregate: ManifestState)
// ---------------------------------------------------------------------------

#[canon_core::event(ManifestState, version = 1)]
pub struct ManifestCreated {
    pub manifest_id: Uuid,
    pub ship_id: Uuid,
    pub voyage_id: Uuid,
}

#[canon_core::event(ManifestState, version = 2)]
pub struct CargoLoaded {
    pub manifest_id: Uuid,
    pub item_id: Uuid,
    pub weight_kg: f32,
    pub description: String,
}

#[canon_core::event(ManifestState, version = 1)]
pub struct UnloadingStarted {
    pub manifest_id: Uuid,
    pub station_id: Uuid,
}

#[canon_core::event(ManifestState, version = 1)]
pub struct CargoUnloaded {
    pub manifest_id: Uuid,
    pub item_id: Uuid,
}

#[canon_core::event(ManifestState, version = 1)]
pub struct ManifestClosed {
    pub manifest_id: Uuid,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum CargoEvent {
    ManifestCreated(ManifestCreated),
    CargoLoaded(CargoLoaded),
    UnloadingStarted(UnloadingStarted),
    CargoUnloaded(CargoUnloaded),
    ManifestClosed(ManifestClosed),
}
