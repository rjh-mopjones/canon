//! Route aggregate — the core domain entity for the navigation service.
//!
//! State is reconstructed by replaying events through version-matched
//! event combiners. Traits are implemented manually because the shared
//! crate owns the event types (Rust orphan rules prevent using
//! `#[event_combiner]` across crate boundaries).

use canon_core::{Aggregate, EventCombiner, EventEnvelope, MacroError};
use canon_demo_shared::events::{PositionUpdated, RoutePlanned, ShipArrivedAtStation};

/// Route aggregate state. Tracks ship assignment, waypoints, position, and
/// arrival status.
///
/// Snapshot cadence: every 50 events.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct Route {
    pub ship_id: Option<uuid::Uuid>,
    pub waypoints: Vec<uuid::Uuid>,
    pub current_waypoint_index: usize,
    pub arrived: bool,
}

impl Route {
    /// Snapshot cadence: take a snapshot every N events, or None for no snapshotting.
    pub const SNAPSHOT_EVERY: Option<u64> = Some(50);
}

impl Aggregate for Route {
    type State = Route;
    type Error = MacroError;

    fn hydrate(
        state: &mut Self::State,
        events: impl Iterator<Item = EventEnvelope>,
    ) -> Result<(), Self::Error> {
        for envelope in events {
            match (envelope.event_type.as_str(), envelope.event_version) {
                ("RoutePlanned", 1) => {
                    let event: RoutePlanned = serde_json::from_slice(&envelope.payload)
                        .map_err(|e| MacroError(e.to_string()))?;
                    apply_route_planned(&event, state);
                }
                ("PositionUpdated", 1) => {
                    let event: PositionUpdated = serde_json::from_slice(&envelope.payload)
                        .map_err(|e| MacroError(e.to_string()))?;
                    apply_position_updated(&event, state);
                }
                ("ShipArrivedAtStation", 1) => {
                    let event: ShipArrivedAtStation = serde_json::from_slice(&envelope.payload)
                        .map_err(|e| MacroError(e.to_string()))?;
                    apply_ship_arrived(&event, state);
                }
                (event_type, version) => {
                    return Err(MacroError(format!(
                        "no event combiner registered for '{}' version {}",
                        event_type, version
                    )));
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Event combiner functions — synchronous, pure state folding
// ---------------------------------------------------------------------------

/// Apply a RoutePlanned event to the Route state.
fn apply_route_planned(event: &RoutePlanned, state: &mut Route) {
    state.ship_id = Some(event.ship_id);
    state.waypoints = event.waypoints.clone();
    state.current_waypoint_index = 0;
    state.arrived = false;
}

/// Apply a PositionUpdated event to the Route state.
fn apply_position_updated(event: &PositionUpdated, state: &mut Route) {
    // Advance to the waypoint matching the update, or increment index
    if let Some(idx) = state.waypoints.iter().position(|w| *w == event.waypoint_id) {
        state.current_waypoint_index = idx;
    } else {
        state.current_waypoint_index += 1;
    }
}

/// Apply a ShipArrivedAtStation event to the Route state.
fn apply_ship_arrived(event: &ShipArrivedAtStation, state: &mut Route) {
    let _ = event;
    state.arrived = true;
}

// ---------------------------------------------------------------------------
// EventCombiner trait impls — satisfy the trait for local aggregate type
// ---------------------------------------------------------------------------

impl EventCombiner<Route> for RoutePlanned {
    fn combine(&self, state: &mut Route) {
        apply_route_planned(self, state);
    }
}

impl EventCombiner<Route> for PositionUpdated {
    fn combine(&self, state: &mut Route) {
        apply_position_updated(self, state);
    }
}

impl EventCombiner<Route> for ShipArrivedAtStation {
    fn combine(&self, state: &mut Route) {
        apply_ship_arrived(self, state);
    }
}

// ---------------------------------------------------------------------------
// Inventory registrations for event combiners
// ---------------------------------------------------------------------------

fn __canon_apply_routeplanned_v1(
    payload: &[u8],
    state: &mut dyn std::any::Any,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let event: RoutePlanned = serde_json::from_slice(payload)?;
    let state = state.downcast_mut::<Route>().ok_or_else(
        || -> Box<dyn std::error::Error + Send + Sync> {
            "aggregate state type mismatch in event combiner".into()
        },
    )?;
    apply_route_planned(&event, state);
    Ok(())
}

fn __canon_apply_positionupdated_v1(
    payload: &[u8],
    state: &mut dyn std::any::Any,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let event: PositionUpdated = serde_json::from_slice(payload)?;
    let state = state.downcast_mut::<Route>().ok_or_else(
        || -> Box<dyn std::error::Error + Send + Sync> {
            "aggregate state type mismatch in event combiner".into()
        },
    )?;
    apply_position_updated(&event, state);
    Ok(())
}

fn __canon_apply_shiparrived_v1(
    payload: &[u8],
    state: &mut dyn std::any::Any,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let event: ShipArrivedAtStation = serde_json::from_slice(payload)?;
    let state = state.downcast_mut::<Route>().ok_or_else(
        || -> Box<dyn std::error::Error + Send + Sync> {
            "aggregate state type mismatch in event combiner".into()
        },
    )?;
    apply_ship_arrived(&event, state);
    Ok(())
}

canon_core::__submit! {
    canon_core::EventCombinerRegistration {
        aggregate_type_id: std::any::TypeId::of::<Route>(),
        event_type_name: "RoutePlanned",
        event_version: 1,
        apply_fn: __canon_apply_routeplanned_v1,
    }
}

canon_core::__submit! {
    canon_core::EventCombinerRegistration {
        aggregate_type_id: std::any::TypeId::of::<Route>(),
        event_type_name: "PositionUpdated",
        event_version: 1,
        apply_fn: __canon_apply_positionupdated_v1,
    }
}

canon_core::__submit! {
    canon_core::EventCombinerRegistration {
        aggregate_type_id: std::any::TypeId::of::<Route>(),
        event_type_name: "ShipArrivedAtStation",
        event_version: 1,
        apply_fn: __canon_apply_shiparrived_v1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use canon_core::{AggregateId, Version};
    use chrono::Utc;
    use uuid::Uuid;

    fn make_envelope(
        agg_id: &AggregateId,
        version: Version,
        event_type: &str,
        payload: Vec<u8>,
        correlation_id: Uuid,
    ) -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: agg_id.clone(),
            version,
            event_type: event_type.to_string(),
            event_version: 1,
            payload: Bytes::from(payload),
            correlation_id,
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn route_default_state() {
        let route = Route::default();
        assert!(route.ship_id.is_none());
        assert!(route.waypoints.is_empty());
        assert_eq!(route.current_waypoint_index, 0);
        assert!(!route.arrived);
    }

    #[test]
    fn route_snapshot_every() {
        assert_eq!(Route::SNAPSHOT_EVERY, Some(50));
    }

    #[test]
    fn route_serde_roundtrip() {
        let route = Route {
            ship_id: Some(Uuid::new_v4()),
            waypoints: vec![Uuid::new_v4()],
            current_waypoint_index: 0,
            arrived: false,
        };
        let json = serde_json::to_vec(&route).expect("serialize");
        let back: Route = serde_json::from_slice(&json).expect("deserialize");
        assert_eq!(back.ship_id, route.ship_id);
        assert_eq!(back.waypoints, route.waypoints);
    }

    #[test]
    fn hydrate_route_planned() {
        let mut route = Route::default();
        let agg_id = AggregateId::new();
        let ship_id = Uuid::new_v4();
        let wp1 = Uuid::new_v4();
        let wp2 = Uuid::new_v4();
        let corr = Uuid::new_v4();

        let planned = RoutePlanned {
            route_id: *agg_id.as_uuid(),
            ship_id,
            waypoints: vec![wp1, wp2],
        };
        let payload = serde_json::to_vec(&planned).expect("serialize");
        let envelope = make_envelope(&agg_id, Version::initial(), "RoutePlanned", payload, corr);

        Route::hydrate(&mut route, std::iter::once(envelope)).expect("hydrate");

        assert_eq!(route.ship_id, Some(ship_id));
        assert_eq!(route.waypoints.len(), 2);
        assert_eq!(route.current_waypoint_index, 0);
        assert!(!route.arrived);
    }

    #[test]
    fn hydrate_full_lifecycle() {
        let mut route = Route::default();
        let agg_id = AggregateId::new();
        let ship_id = Uuid::new_v4();
        let station_id = Uuid::new_v4();
        let wp1 = Uuid::new_v4();
        let wp2 = station_id;
        let corr = Uuid::new_v4();

        let planned = RoutePlanned {
            route_id: *agg_id.as_uuid(),
            ship_id,
            waypoints: vec![wp1, wp2],
        };
        let position = PositionUpdated {
            route_id: *agg_id.as_uuid(),
            ship_id,
            waypoint_id: wp2,
        };
        let arrived = ShipArrivedAtStation {
            route_id: *agg_id.as_uuid(),
            ship_id,
            station_id,
        };

        let events = vec![
            make_envelope(
                &agg_id,
                Version::initial(),
                "RoutePlanned",
                serde_json::to_vec(&planned).expect("serialize"),
                corr,
            ),
            make_envelope(
                &agg_id,
                Version::from_u64(1),
                "PositionUpdated",
                serde_json::to_vec(&position).expect("serialize"),
                corr,
            ),
            make_envelope(
                &agg_id,
                Version::from_u64(2),
                "ShipArrivedAtStation",
                serde_json::to_vec(&arrived).expect("serialize"),
                corr,
            ),
        ];

        Route::hydrate(&mut route, events.into_iter()).expect("hydrate");

        assert_eq!(route.ship_id, Some(ship_id));
        assert_eq!(route.current_waypoint_index, 1);
        assert!(route.arrived);
    }

    #[test]
    fn hydrate_unknown_event_returns_error() {
        let mut route = Route::default();
        let agg_id = AggregateId::new();
        let events = vec![make_envelope(
            &agg_id,
            Version::initial(),
            "UnknownEvent",
            b"{}".to_vec(),
            Uuid::new_v4(),
        )];

        let result = Route::hydrate(&mut route, events.into_iter());
        assert!(result.is_err());
    }

    #[test]
    fn event_combiner_trait_route_planned() {
        let event = RoutePlanned {
            route_id: Uuid::new_v4(),
            ship_id: Uuid::new_v4(),
            waypoints: vec![Uuid::new_v4()],
        };
        let mut route = Route::default();
        <RoutePlanned as EventCombiner<Route>>::combine(&event, &mut route);
        assert_eq!(route.ship_id, Some(event.ship_id));
        assert!(!route.arrived);
    }

    #[test]
    fn event_combiner_trait_ship_arrived() {
        let event = ShipArrivedAtStation {
            route_id: Uuid::new_v4(),
            ship_id: Uuid::new_v4(),
            station_id: Uuid::new_v4(),
        };
        let mut route = Route::default();
        <ShipArrivedAtStation as EventCombiner<Route>>::combine(&event, &mut route);
        assert!(route.arrived);
    }
}
