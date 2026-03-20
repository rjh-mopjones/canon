use bytes::Bytes;
use canon_core::{AggregateId, CommandEnvelope, EventEnvelope, IncomingMessage, Oversight};
use uuid::Uuid;

use crate::events::CargoEvent;

// ---------------------------------------------------------------------------
// UnloadingHandler — sophisticated oversight usage
// ---------------------------------------------------------------------------
// Consumes `ShipArrivedAtStation` (external, from navigation-service) and
// `ManifestCreated` (internal). Waits until BOTH are present before dispatching.
// Discards the window if `ShipDecommissioned` arrives.

#[canon_core::event_handler(window_ttl = "30m")]
impl UnloadingHandler {
    #[handles(CargoEvent, version = 1)]
    fn handle(&self, events: Vec<CargoEvent>) -> Option<CommandEnvelope> {
        // Find a ManifestCreated event in the batch to build a BeginUnloading command
        let mc = events.iter().find_map(|e| {
            if let CargoEvent::ManifestCreated(mc) = e {
                Some(mc)
            } else {
                None
            }
        })?;

        // Build a BeginUnloading command envelope targeting the manifest aggregate.
        // The station_id would come from the ShipArrivedAtStation external event,
        // but since oversight already ensured both events are present, we construct
        // a command with the manifest_id from the internal event. In a full
        // implementation the station_id would be extracted from accumulated messages.
        let payload = serde_json::to_vec(&serde_json::json!({
            "manifest_id": mc.manifest_id,
            "station_id": Uuid::nil(),
        }));

        let payload = match payload {
            Ok(p) => p,
            Err(_) => return None,
        };

        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::from_uuid(mc.manifest_id),
            command_type: "BeginUnloading".to_string(),
            correlation_id: Uuid::new_v4(),
            causation_id: mc.manifest_id,
            timestamp: chrono::Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        // Discard if ship was decommissioned
        let decommissioned = accumulated.iter().any(|m| {
            matches!(
                m,
                IncomingMessage::ExternalEvent(e) if e.event_type == "ShipDecommissioned"
            )
        });
        if decommissioned {
            return Oversight::Discard;
        }

        // Ready only when BOTH events are present
        let has_arrival = accumulated.iter().any(|m| {
            matches!(
                m,
                IncomingMessage::ExternalEvent(e) if e.event_type == "ShipArrivedAtStation"
            )
        });
        let has_manifest = accumulated.iter().any(|m| {
            matches!(
                m,
                IncomingMessage::InternalEvent(e) if e.event_type == "ManifestCreated"
            )
        });

        if has_arrival && has_manifest {
            Oversight::Ready
        } else {
            Oversight::NotReady
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: construct an EventEnvelope for testing
// ---------------------------------------------------------------------------

/// Build a minimal `EventEnvelope` for use in tests and handler logic.
pub fn make_event_envelope(
    event_type: &str,
    event_version: u32,
    aggregate_id: Uuid,
    payload: Bytes,
) -> EventEnvelope {
    EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: AggregateId::from_uuid(aggregate_id),
        version: canon_core::Version::initial(),
        event_type: event_type.to_string(),
        event_version,
        payload,
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: chrono::Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use canon_core::EventHandler;

    fn external_event(event_type: &str) -> IncomingMessage {
        IncomingMessage::ExternalEvent(make_event_envelope(
            event_type,
            1,
            Uuid::new_v4(),
            Bytes::new(),
        ))
    }

    fn internal_event(event_type: &str) -> IncomingMessage {
        IncomingMessage::InternalEvent(make_event_envelope(
            event_type,
            1,
            Uuid::new_v4(),
            Bytes::new(),
        ))
    }

    // ── Oversight tests ───────────────────────────────────────────────

    #[test]
    fn oversight_not_ready_when_empty() {
        let handler = UnloadingHandler;
        let accumulated: Vec<IncomingMessage> = vec![];
        assert_eq!(handler.oversight(&accumulated), Oversight::NotReady);
    }

    #[test]
    fn oversight_not_ready_with_only_arrival() {
        let handler = UnloadingHandler;
        let accumulated = vec![external_event("ShipArrivedAtStation")];
        assert_eq!(handler.oversight(&accumulated), Oversight::NotReady);
    }

    #[test]
    fn oversight_not_ready_with_only_manifest() {
        let handler = UnloadingHandler;
        let accumulated = vec![internal_event("ManifestCreated")];
        assert_eq!(handler.oversight(&accumulated), Oversight::NotReady);
    }

    #[test]
    fn oversight_ready_when_both_present() {
        let handler = UnloadingHandler;
        let accumulated = vec![
            external_event("ShipArrivedAtStation"),
            internal_event("ManifestCreated"),
        ];
        assert_eq!(handler.oversight(&accumulated), Oversight::Ready);
    }

    #[test]
    fn oversight_ready_when_both_present_reverse_order() {
        let handler = UnloadingHandler;
        let accumulated = vec![
            internal_event("ManifestCreated"),
            external_event("ShipArrivedAtStation"),
        ];
        assert_eq!(handler.oversight(&accumulated), Oversight::Ready);
    }

    #[test]
    fn oversight_discard_on_decommissioned() {
        let handler = UnloadingHandler;
        let accumulated = vec![external_event("ShipDecommissioned")];
        assert_eq!(handler.oversight(&accumulated), Oversight::Discard);
    }

    #[test]
    fn oversight_discard_overrides_ready() {
        let handler = UnloadingHandler;
        let accumulated = vec![
            external_event("ShipArrivedAtStation"),
            internal_event("ManifestCreated"),
            external_event("ShipDecommissioned"),
        ];
        assert_eq!(handler.oversight(&accumulated), Oversight::Discard);
    }

    #[test]
    fn oversight_not_ready_with_unrelated_events() {
        let handler = UnloadingHandler;
        let accumulated = vec![
            external_event("StockRecorded"),
            internal_event("CargoLoaded"),
        ];
        assert_eq!(handler.oversight(&accumulated), Oversight::NotReady);
    }

    // ── Handler tests ─────────────────────────────────────────────────

    #[tokio::test]
    async fn handle_returns_command_when_manifest_created_present() {
        let handler = UnloadingHandler;
        let manifest_id = Uuid::new_v4();
        let events = vec![CargoEvent::ManifestCreated(
            crate::events::ManifestCreated {
                manifest_id,
                ship_id: Uuid::new_v4(),
                voyage_id: Uuid::new_v4(),
            },
        )];

        let result = handler.handle(events).await;
        assert!(result.is_ok());
        let cmd = result
            .expect("test: should not fail")
            .expect("test: should produce command");
        assert_eq!(cmd.command_type, "BeginUnloading");
        assert_eq!(*cmd.aggregate_id.as_uuid(), manifest_id);
    }

    #[tokio::test]
    async fn handle_returns_none_when_no_manifest_created() {
        let handler = UnloadingHandler;
        let events = vec![CargoEvent::CargoLoaded(crate::events::CargoLoaded {
            manifest_id: Uuid::new_v4(),
            item_id: Uuid::new_v4(),
            weight_kg: 100.0,
            description: "test".to_string(),
        })];

        let result = handler.handle(events).await;
        assert!(result.is_ok());
        assert!(result.expect("test: should not fail").is_none());
    }

    #[tokio::test]
    async fn handle_returns_none_for_empty_batch() {
        let handler = UnloadingHandler;
        let result = handler.handle(vec![]).await;
        assert!(result.is_ok());
        assert!(result.expect("test: should not fail").is_none());
    }
}
