use crate::aggregate::{
    DeliveryConfirmed, DeliveryConfirmedV1HasCombiner, Inventory, ResupplyDispatched,
    ResupplyDispatchedV1HasCombiner, ResupplyRequested, ResupplyRequestedV1HasCombiner,
    StockRecorded, StockRecordedV1HasCombiner,
};

// ---------------------------------------------------------------------------
// Event combiners -- synchronous, pure state folding
// ---------------------------------------------------------------------------

#[canon_core::event_combiner(Inventory, version = 1)]
impl StockRecorded {
    fn combine(&self, state: &mut Inventory) {
        state.station_id = Some(self.station_id);
        state.fuel_kg = self.fuel_kg as u32;
    }
}

#[canon_core::event_combiner(Inventory, version = 1)]
impl ResupplyRequested {
    fn combine(&self, state: &mut Inventory) {
        // Recording a request doesn't change state -- the dispatch step does.
        let _ = state;
    }
}

#[canon_core::event_combiner(Inventory, version = 1)]
impl ResupplyDispatched {
    fn combine(&self, state: &mut Inventory) {
        state.pending_resupply = Some(self.ship_id);
    }
}

#[canon_core::event_combiner(Inventory, version = 1)]
impl DeliveryConfirmed {
    fn combine(&self, state: &mut Inventory) {
        state.pending_resupply = None;
    }
}
