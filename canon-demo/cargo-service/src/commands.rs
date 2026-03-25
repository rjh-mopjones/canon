use uuid::Uuid;

// ---------------------------------------------------------------------------
// Cargo commands (aggregate: ManifestState)
// ---------------------------------------------------------------------------

#[canon_core::command(ManifestState, version = 1, produces = [ManifestCreated])]
pub struct CreateManifest {
    pub ship_id: Uuid,
    pub voyage_id: Uuid,
}

#[canon_core::command(ManifestState, version = 1, produces = [CargoLoaded])]
pub struct LoadCargo {
    pub manifest_id: Uuid,
    pub item_id: Uuid,
    pub weight_kg: f32,
    pub description: String,
}

#[canon_core::command(ManifestState, version = 1, produces = [UnloadingStarted])]
pub struct BeginUnloading {
    pub manifest_id: Uuid,
    pub station_id: Uuid,
}

#[canon_core::command(ManifestState, version = 1, produces = [CargoUnloaded])]
pub struct RecordUnloaded {
    pub manifest_id: Uuid,
    pub item_id: Uuid,
}

#[canon_core::command(ManifestState, version = 1, produces = [ManifestClosed])]
pub struct CloseManifest {
    pub manifest_id: Uuid,
}
