/// The Inventory aggregate tracks fuel stock levels at a station and manages
/// resupply dispatch lifecycle. `pending_resupply` holds the ship id of an
/// in-flight resupply -- at most one can be active at a time.
#[canon_core::aggregate(snapshot_every = 50)]
pub struct Inventory {
    pub station_id: Option<uuid::Uuid>,
    pub fuel_kg: u32,
    pub pending_resupply: Option<uuid::Uuid>,
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

#[canon_core::command(Inventory, version = 1, produces = [StockRecorded])]
pub struct RecordStock {
    pub station_id: uuid::Uuid,
    pub fuel_kg: f32,
}

#[canon_core::command(Inventory, version = 1, produces = [ResupplyRequested])]
pub struct RequestResupply {
    pub station_id: uuid::Uuid,
    pub fuel_kg: f32,
}

#[canon_core::command(Inventory, version = 1, produces = [ResupplyDispatched])]
pub struct DispatchResupply {
    pub inventory_id: uuid::Uuid,
    pub ship_id: uuid::Uuid,
}

#[canon_core::command(Inventory, version = 1, produces = [DeliveryConfirmed])]
pub struct ConfirmDelivery {
    pub inventory_id: uuid::Uuid,
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

#[canon_core::event(Inventory, version = 1)]
pub struct StockRecorded {
    pub inventory_id: uuid::Uuid,
    pub station_id: uuid::Uuid,
    pub fuel_kg: f32,
}

#[canon_core::event(Inventory, version = 1)]
pub struct ResupplyRequested {
    pub inventory_id: uuid::Uuid,
    pub station_id: uuid::Uuid,
    pub fuel_kg: f32,
}

#[canon_core::event(Inventory, version = 1)]
pub struct ResupplyDispatched {
    pub inventory_id: uuid::Uuid,
    pub ship_id: uuid::Uuid,
    pub fuel_kg: f32,
}

#[canon_core::event(Inventory, version = 1)]
pub struct DeliveryConfirmed {
    pub inventory_id: uuid::Uuid,
    pub ship_id: uuid::Uuid,
}
