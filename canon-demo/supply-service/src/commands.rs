use crate::aggregate::{
    ConfirmDelivery, ConfirmDeliveryV1HasHandler, DeliveryConfirmed, DispatchResupply,
    DispatchResupplyV1HasHandler, Inventory, RecordStock, RecordStockV1HasHandler, RequestResupply,
    RequestResupplyV1HasHandler, ResupplyDispatched, ResupplyRequested, StockRecorded,
};
use crate::error::SupplyError;

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

/// Records the current fuel stock level at a station.
#[canon_core::command_handler(Inventory, version = 1)]
impl RecordStockHandler {
    type Error = SupplyError;

    fn handle(&self, state: &Inventory, cmd: RecordStock) -> Result<StockRecorded, SupplyError> {
        let _ = state;
        Ok(StockRecorded {
            inventory_id: cmd.station_id, // inventory keyed by station
            station_id: cmd.station_id,
            fuel_kg: cmd.fuel_kg,
        })
    }
}

/// Requests a resupply for a station.
#[canon_core::command_handler(Inventory, version = 1)]
impl RequestResupplyHandler {
    type Error = SupplyError;

    fn handle(
        &self,
        state: &Inventory,
        cmd: RequestResupply,
    ) -> Result<ResupplyRequested, SupplyError> {
        let _ = state;
        Ok(ResupplyRequested {
            inventory_id: cmd.station_id,
            station_id: cmd.station_id,
            fuel_kg: cmd.fuel_kg,
        })
    }
}

/// Dispatches a resupply ship. Fails if a resupply is already pending --
/// this is the dead-letter showcase: repeated failures accumulate retries
/// and eventually land in the dead letter store.
#[canon_core::command_handler(Inventory, version = 1)]
impl DispatchResupplyHandler {
    type Error = SupplyError;

    fn handle(
        &self,
        state: &Inventory,
        cmd: DispatchResupply,
    ) -> Result<ResupplyDispatched, SupplyError> {
        if let Some(pending_ship_id) = state.pending_resupply {
            return Err(SupplyError::ResupplyAlreadyPending { pending_ship_id });
        }
        Ok(ResupplyDispatched {
            inventory_id: cmd.inventory_id,
            ship_id: cmd.ship_id,
            fuel_kg: state.fuel_kg as f32,
        })
    }
}

/// Confirms delivery and clears the pending resupply.
#[canon_core::command_handler(Inventory, version = 1)]
impl ConfirmDeliveryHandler {
    type Error = SupplyError;

    fn handle(
        &self,
        state: &Inventory,
        cmd: ConfirmDelivery,
    ) -> Result<DeliveryConfirmed, SupplyError> {
        let ship_id = state
            .pending_resupply
            .ok_or(SupplyError::NoPendingResupply)?;
        Ok(DeliveryConfirmed {
            inventory_id: cmd.inventory_id,
            ship_id,
        })
    }
}
