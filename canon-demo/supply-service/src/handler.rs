use crate::inbound::InboundStationStockLow as StationStockLow;
use bytes::Bytes;
use canon_core::CommandEnvelope;

/// Consumes `StationStockLow` events from the station-service topic and
/// produces `RequestResupply` commands targeting the supply-service's
/// Inventory aggregate.
#[canon_core::event_handler]
impl StockAlertHandler {
    #[handles(StationStockLow, version = 1)]
    fn handle(&self, events: Vec<StationStockLow>) -> Option<CommandEnvelope> {
        // Take the last event in the batch -- the most recent stock reading.
        let event = events.last()?;

        // Build a RequestResupply command envelope.
        // The inventory aggregate is keyed by station_id (one inventory per station).
        let fuel_needed = event.threshold_kg - event.current_stock_kg;
        let payload = serde_json::to_vec(&serde_json::json!({
            "station_id": event.station_id,
            "fuel_kg": fuel_needed,
        }))
        .ok()?;

        Some(CommandEnvelope {
            command_id: uuid::Uuid::new_v4(),
            aggregate_id: canon_core::AggregateId::from_uuid(event.station_id),
            command_type: "RequestResupply".to_owned(),
            correlation_id: event.station_id, // propagate station context
            causation_id: event.station_id,
            timestamp: chrono::Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }
}
