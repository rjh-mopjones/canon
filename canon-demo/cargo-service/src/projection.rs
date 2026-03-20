use uuid::Uuid;

use crate::events::{
    CargoLoaded, CargoUnloaded, ManifestClosed, ManifestCreated, UnloadingStarted,
};

// ---------------------------------------------------------------------------
// Manifest read model projection
// ---------------------------------------------------------------------------

/// Read model for the manifest projection, matching the `manifest_read_models`
/// table schema in YugabyteDB.
#[canon_core::projection]
pub struct ManifestReadModel {
    pub manifest_id: Uuid,
    pub ship_id: Uuid,
    pub status: String,
    pub total_weight_kg: u32,
}

// ---------------------------------------------------------------------------
// Projection handlers — apply each event type to the read model
// ---------------------------------------------------------------------------

#[canon_core::projection_handler(ManifestReadModel)]
impl ManifestCreatedProjectionHandler {
    fn apply(&self, event: &ManifestCreated, store: &mut ManifestReadModel) {
        store.manifest_id = event.manifest_id;
        store.ship_id = event.ship_id;
        store.status = "Open".to_string();
        store.total_weight_kg = 0;
    }
}

#[canon_core::projection_handler(ManifestReadModel)]
impl CargoLoadedProjectionHandler {
    fn apply(&self, event: &CargoLoaded, store: &mut ManifestReadModel) {
        store.total_weight_kg += event.weight_kg.max(0.0).round() as u32;
    }
}

#[canon_core::projection_handler(ManifestReadModel)]
impl UnloadingStartedProjectionHandler {
    fn apply(&self, event: &UnloadingStarted, store: &mut ManifestReadModel) {
        let _ = event;
        store.status = "Unloading".to_string();
    }
}

#[canon_core::projection_handler(ManifestReadModel)]
impl CargoUnloadedProjectionHandler {
    fn apply(&self, event: &CargoUnloaded, store: &mut ManifestReadModel) {
        let _ = event;
        // Weight is not subtracted — total_weight_kg tracks loaded total, not remaining
    }
}

#[canon_core::projection_handler(ManifestReadModel)]
impl ManifestClosedProjectionHandler {
    fn apply(&self, event: &ManifestClosed, store: &mut ManifestReadModel) {
        let _ = event;
        store.status = "Closed".to_string();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use canon_core::ProjectionHandler;
    use uuid::Uuid;

    #[test]
    fn projection_manifest_created() {
        let mut model = ManifestReadModel {
            manifest_id: Uuid::nil(),
            ship_id: Uuid::nil(),
            status: String::new(),
            total_weight_kg: 0,
        };

        let manifest_id = Uuid::new_v4();
        let ship_id = Uuid::new_v4();
        let event = ManifestCreated {
            manifest_id,
            ship_id,
            voyage_id: Uuid::new_v4(),
        };

        ManifestCreatedProjectionHandler.apply(&event, &mut model);
        assert_eq!(model.manifest_id, manifest_id);
        assert_eq!(model.ship_id, ship_id);
        assert_eq!(model.status, "Open");
        assert_eq!(model.total_weight_kg, 0);
    }

    #[test]
    fn projection_cargo_loaded_accumulates_weight() {
        let mut model = ManifestReadModel {
            manifest_id: Uuid::new_v4(),
            ship_id: Uuid::new_v4(),
            status: "Open".to_string(),
            total_weight_kg: 0,
        };

        let event1 = CargoLoaded {
            manifest_id: model.manifest_id,
            item_id: Uuid::new_v4(),
            weight_kg: 100.0,
            description: "steel beams".to_string(),
        };
        let event2 = CargoLoaded {
            manifest_id: model.manifest_id,
            item_id: Uuid::new_v4(),
            weight_kg: 50.0,
            description: "copper wire".to_string(),
        };

        CargoLoadedProjectionHandler.apply(&event1, &mut model);
        assert_eq!(model.total_weight_kg, 100);

        CargoLoadedProjectionHandler.apply(&event2, &mut model);
        assert_eq!(model.total_weight_kg, 150);
    }

    #[test]
    fn projection_status_transitions() {
        let mut model = ManifestReadModel {
            manifest_id: Uuid::new_v4(),
            ship_id: Uuid::new_v4(),
            status: "Open".to_string(),
            total_weight_kg: 100,
        };

        UnloadingStartedProjectionHandler.apply(
            &UnloadingStarted {
                manifest_id: model.manifest_id,
                station_id: Uuid::new_v4(),
            },
            &mut model,
        );
        assert_eq!(model.status, "Unloading");

        ManifestClosedProjectionHandler.apply(
            &ManifestClosed {
                manifest_id: model.manifest_id,
            },
            &mut model,
        );
        assert_eq!(model.status, "Closed");
    }
}
