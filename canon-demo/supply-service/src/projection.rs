use crate::aggregate::{DeliveryConfirmed, ResupplyDispatched, StockRecorded};

/// Read model for inventory state. Mirrors the `inventory_read_models` table
/// in the YugabyteDB projection store.
#[canon_core::projection]
pub struct InventoryReadModel {
    pub inventory_id: uuid::Uuid,
    pub station_id: uuid::Uuid,
    pub fuel_kg: i32,
    pub resupply_pending: bool,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Applies `StockRecorded` events to the inventory read model.
#[canon_core::projection_handler(InventoryReadModel)]
impl StockRecordedProjectionHandler {
    fn apply(&self, event: &StockRecorded, store: &mut InventoryReadModel) {
        store.inventory_id = event.inventory_id;
        store.station_id = event.station_id;
        store.fuel_kg = event.fuel_kg as i32;
        store.updated_at = chrono::Utc::now();
    }
}

/// Applies `ResupplyDispatched` events -- marks resupply as pending.
#[canon_core::projection_handler(InventoryReadModel)]
impl ResupplyDispatchedProjectionHandler {
    fn apply(&self, event: &ResupplyDispatched, store: &mut InventoryReadModel) {
        let _ = event;
        store.resupply_pending = true;
        store.updated_at = chrono::Utc::now();
    }
}

/// Applies `DeliveryConfirmed` events -- clears the pending flag.
#[canon_core::projection_handler(InventoryReadModel)]
impl DeliveryConfirmedProjectionHandler {
    fn apply(&self, event: &DeliveryConfirmed, store: &mut InventoryReadModel) {
        let _ = event;
        store.resupply_pending = false;
        store.updated_at = chrono::Utc::now();
    }
}
