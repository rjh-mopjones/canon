use crate::aggregate::{ManifestState, ManifestStatus};
use crate::commands::{
    BeginUnloading, BeginUnloadingV1HasHandler, CloseManifest, CloseManifestV1HasHandler,
    CreateManifest, CreateManifestV1HasHandler, LoadCargo, LoadCargoV1HasHandler, RecordUnloaded,
    RecordUnloadedV1HasHandler,
};
use crate::error::CargoError;
use crate::events::{
    CargoLoaded, CargoUnloaded, ManifestClosed, ManifestCreated, UnloadingStarted,
};

// ---------------------------------------------------------------------------
// Command handlers — one per command per version
// ---------------------------------------------------------------------------

#[canon_core::command_handler(ManifestState, version = 1)]
impl CreateManifestHandler {
    type Error = CargoError;

    fn handle(
        &self,
        state: &ManifestState,
        cmd: CreateManifest,
    ) -> Result<ManifestCreated, CargoError> {
        // A manifest can only be created in the default (Open) initial state
        // before any events have been applied. If ship_id is already set, this
        // aggregate was already created.
        if state.ship_id.is_some() {
            return Err(CargoError::ManifestAlreadyCreated);
        }
        Ok(ManifestCreated {
            manifest_id: uuid::Uuid::new_v4(),
            ship_id: cmd.ship_id,
            voyage_id: cmd.voyage_id,
        })
    }
}

#[canon_core::command_handler(ManifestState, version = 1)]
impl LoadCargoHandler {
    type Error = CargoError;

    fn handle(&self, state: &ManifestState, cmd: LoadCargo) -> Result<CargoLoaded, CargoError> {
        if state.status != ManifestStatus::Open {
            return Err(CargoError::ManifestNotOpen);
        }
        Ok(CargoLoaded {
            manifest_id: cmd.manifest_id,
            item_id: cmd.item_id,
            weight_kg: cmd.weight_kg,
            description: cmd.description,
        })
    }
}

#[canon_core::command_handler(ManifestState, version = 1)]
impl BeginUnloadingHandler {
    type Error = CargoError;

    fn handle(
        &self,
        state: &ManifestState,
        cmd: BeginUnloading,
    ) -> Result<UnloadingStarted, CargoError> {
        if state.status != ManifestStatus::Open {
            return Err(CargoError::ManifestNotOpen);
        }
        Ok(UnloadingStarted {
            manifest_id: cmd.manifest_id,
            station_id: cmd.station_id,
        })
    }
}

#[canon_core::command_handler(ManifestState, version = 1)]
impl RecordUnloadedHandler {
    type Error = CargoError;

    fn handle(
        &self,
        state: &ManifestState,
        cmd: RecordUnloaded,
    ) -> Result<CargoUnloaded, CargoError> {
        if state.status != ManifestStatus::Unloading {
            return Err(CargoError::ManifestNotUnloading);
        }
        let item = state
            .items
            .iter()
            .find(|i| i.item_id == cmd.item_id)
            .ok_or(CargoError::ItemNotFound {
                item_id: cmd.item_id,
            })?;
        if item.unloaded {
            return Err(CargoError::ItemAlreadyUnloaded {
                item_id: cmd.item_id,
            });
        }
        Ok(CargoUnloaded {
            manifest_id: cmd.manifest_id,
            item_id: cmd.item_id,
        })
    }
}

#[canon_core::command_handler(ManifestState, version = 1)]
impl CloseManifestHandler {
    type Error = CargoError;

    fn handle(
        &self,
        state: &ManifestState,
        cmd: CloseManifest,
    ) -> Result<ManifestClosed, CargoError> {
        if state.status == ManifestStatus::Closed {
            return Err(CargoError::ManifestAlreadyClosed);
        }
        Ok(ManifestClosed {
            manifest_id: cmd.manifest_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::CargoItem;
    use canon_core::CommandHandler;
    use uuid::Uuid;

    fn default_state() -> ManifestState {
        ManifestState::default()
    }

    fn created_state() -> ManifestState {
        ManifestState {
            ship_id: Some(Uuid::new_v4()),
            voyage_id: Some(Uuid::new_v4()),
            items: vec![],
            status: ManifestStatus::Open,
        }
    }

    fn unloading_state_with_item(item_id: Uuid) -> ManifestState {
        ManifestState {
            ship_id: Some(Uuid::new_v4()),
            voyage_id: Some(Uuid::new_v4()),
            items: vec![CargoItem {
                item_id,
                weight_kg: 100,
                unloaded: false,
            }],
            status: ManifestStatus::Unloading,
        }
    }

    // ── CreateManifest tests ─────────────────────────────────────────

    #[tokio::test]
    async fn create_manifest_succeeds_on_fresh_state() {
        let handler = CreateManifestHandler;
        let cmd = CreateManifest {
            ship_id: Uuid::new_v4(),
            voyage_id: Uuid::new_v4(),
        };
        let result = handler.handle(&default_state(), cmd).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn create_manifest_fails_when_already_created() {
        let handler = CreateManifestHandler;
        let cmd = CreateManifest {
            ship_id: Uuid::new_v4(),
            voyage_id: Uuid::new_v4(),
        };
        let result = handler.handle(&created_state(), cmd).await;
        assert!(result.is_err());
    }

    // ── LoadCargo tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn load_cargo_succeeds_when_open() {
        let handler = LoadCargoHandler;
        let cmd = LoadCargo {
            manifest_id: Uuid::new_v4(),
            item_id: Uuid::new_v4(),
            weight_kg: 50.0,
            description: "steel beams".to_string(),
        };
        let result = handler.handle(&created_state(), cmd).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn load_cargo_fails_when_not_open() {
        let handler = LoadCargoHandler;
        let mut state = created_state();
        state.status = ManifestStatus::Unloading;
        let cmd = LoadCargo {
            manifest_id: Uuid::new_v4(),
            item_id: Uuid::new_v4(),
            weight_kg: 50.0,
            description: "steel beams".to_string(),
        };
        let result = handler.handle(&state, cmd).await;
        assert!(result.is_err());
    }

    // ── BeginUnloading tests ─────────────────────────────────────────

    #[tokio::test]
    async fn begin_unloading_succeeds_when_open() {
        let handler = BeginUnloadingHandler;
        let cmd = BeginUnloading {
            manifest_id: Uuid::new_v4(),
            station_id: Uuid::new_v4(),
        };
        let result = handler.handle(&created_state(), cmd).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn begin_unloading_fails_when_closed() {
        let handler = BeginUnloadingHandler;
        let mut state = created_state();
        state.status = ManifestStatus::Closed;
        let cmd = BeginUnloading {
            manifest_id: Uuid::new_v4(),
            station_id: Uuid::new_v4(),
        };
        let result = handler.handle(&state, cmd).await;
        assert!(result.is_err());
    }

    // ── RecordUnloaded tests ─────────────────────────────────────────

    #[tokio::test]
    async fn record_unloaded_succeeds_with_valid_item() {
        let handler = RecordUnloadedHandler;
        let item_id = Uuid::new_v4();
        let state = unloading_state_with_item(item_id);
        let cmd = RecordUnloaded {
            manifest_id: Uuid::new_v4(),
            item_id,
        };
        let result = handler.handle(&state, cmd).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn record_unloaded_fails_when_not_unloading() {
        let handler = RecordUnloadedHandler;
        let cmd = RecordUnloaded {
            manifest_id: Uuid::new_v4(),
            item_id: Uuid::new_v4(),
        };
        let result = handler.handle(&created_state(), cmd).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn record_unloaded_fails_for_unknown_item() {
        let handler = RecordUnloadedHandler;
        let state = unloading_state_with_item(Uuid::new_v4());
        let cmd = RecordUnloaded {
            manifest_id: Uuid::new_v4(),
            item_id: Uuid::new_v4(), // different item
        };
        let result = handler.handle(&state, cmd).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn record_unloaded_fails_for_already_unloaded_item() {
        let handler = RecordUnloadedHandler;
        let item_id = Uuid::new_v4();
        let mut state = unloading_state_with_item(item_id);
        state.items[0].unloaded = true;
        let cmd = RecordUnloaded {
            manifest_id: Uuid::new_v4(),
            item_id,
        };
        let result = handler.handle(&state, cmd).await;
        assert!(result.is_err());
    }

    // ── CloseManifest tests ──────────────────────────────────────────

    #[tokio::test]
    async fn close_manifest_succeeds_when_open() {
        let handler = CloseManifestHandler;
        let cmd = CloseManifest {
            manifest_id: Uuid::new_v4(),
        };
        let result = handler.handle(&created_state(), cmd).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn close_manifest_fails_when_already_closed() {
        let handler = CloseManifestHandler;
        let mut state = created_state();
        state.status = ManifestStatus::Closed;
        let cmd = CloseManifest {
            manifest_id: Uuid::new_v4(),
        };
        let result = handler.handle(&state, cmd).await;
        assert!(result.is_err());
    }
}
