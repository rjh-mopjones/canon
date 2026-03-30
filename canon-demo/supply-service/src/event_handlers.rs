use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use crate::aggregate::RequestResupply;
use crate::inbound::InboundStationStockLow;
use canon_core::{event_handler, AggregateId, CommandEnvelope};

// ---------------------------------------------------------------------------
// StockLowHandler — Station:StationStockLow → Supply:RequestResupply
// ---------------------------------------------------------------------------

#[event_handler]
impl StockLowHandler {
    #[handles(InboundStationStockLow, version = 1, event_type = "StationStockLow")]
    fn handle(&self, events: Vec<InboundStationStockLow>) -> Option<CommandEnvelope> {
        let event = events.last()?;

        // Deterministic inventory aggregate ID from station_id
        let inventory_aggregate_id =
            canon_demo_shared::deterministic_command_id(event.station_id, "InventoryAggregate");

        let command = RequestResupply {
            station_id: event.station_id,
            fuel_kg: event.threshold_kg - event.current_stock_kg,
        };
        let payload = serde_json::to_vec(&command).ok()?;

        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::from_uuid(inventory_aggregate_id),
            command_type: "RequestResupply".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }
}
