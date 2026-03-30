use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use crate::commands::{CheckStationOffline, CheckStockLevel, RecordDocking};
use crate::events::StockDrained;
use crate::inbound::InboundShipArrivedAtStation;
use canon_core::{event_handler, AggregateId, CommandEnvelope};

// ---------------------------------------------------------------------------
// DockingHandler — Navigation:ShipArrivedAtStation → Station:RecordDocking
// ---------------------------------------------------------------------------

#[event_handler]
impl DockingHandler {
    #[handles(
        InboundShipArrivedAtStation,
        version = 1,
        event_type = "ShipArrivedAtStation"
    )]
    fn handle(&self, events: Vec<InboundShipArrivedAtStation>) -> Option<CommandEnvelope> {
        let event = events.last()?;

        let command = RecordDocking {
            station_id: event.station_id,
            ship_id: event.ship_id,
        };
        let payload = serde_json::to_vec(&command).ok()?;

        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::from_uuid(event.station_id),
            command_type: "RecordDocking".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }
}

// ---------------------------------------------------------------------------
// StockDrainedHandler — Station:StockDrained → CheckStockLevel + CheckStationOffline
// (internal event — service reacts to its own drain events)
//
// Pre-filters: only submit CheckStockLevel when remaining < 1100 kg
// (avoids dead-lettering when stock is healthy). Always submit
// CheckStationOffline when remaining <= 0.
// ---------------------------------------------------------------------------

#[event_handler]
impl StockDrainedHandler {
    #[handles(StockDrained, version = 1)]
    fn handle(&self, events: Vec<StockDrained>) -> Option<CommandEnvelope> {
        let event = events.last()?;

        // Prioritise game-over check
        if event.remaining_kg <= 0.0 {
            let command = CheckStationOffline {
                station_id: event.station_id,
            };
            let payload = serde_json::to_vec(&command).ok()?;
            return Some(CommandEnvelope {
                command_id: Uuid::new_v4(),
                aggregate_id: AggregateId::from_uuid(event.station_id),
                command_type: "CheckStationOffline".into(),
                correlation_id: Uuid::new_v4(),
                causation_id: Uuid::new_v4(),
                timestamp: Utc::now(),
                payload: Bytes::from(payload),
                command_version: 1,
            });
        }

        // Only check stock level when it's low enough to potentially trigger
        if event.remaining_kg < 1100.0 {
            let command = CheckStockLevel {
                station_id: event.station_id,
            };
            let payload = serde_json::to_vec(&command).ok()?;
            return Some(CommandEnvelope {
                command_id: Uuid::new_v4(),
                aggregate_id: AggregateId::from_uuid(event.station_id),
                command_type: "CheckStockLevel".into(),
                correlation_id: Uuid::new_v4(),
                causation_id: Uuid::new_v4(),
                timestamp: Utc::now(),
                payload: Bytes::from(payload),
                command_version: 1,
            });
        }

        None
    }
}
