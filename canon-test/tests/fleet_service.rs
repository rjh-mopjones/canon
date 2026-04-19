//! Integration tests for the fleet-service Ship aggregate.
//!
//! Tests validate:
//! - ServiceBuilder discovers and validates all fleet registrations
//! - Event combiners fold state correctly
//! - Command handlers enforce business rules
//! - Hydration from events (including 247-event replay for Herald)
//! - Snapshot + partial hydration
//! - Event store consumer triggers snapshots at version % 50
//! - ResupplyHandler produces ScheduleResupply commands
//! - Publisher consumer publishes to canon.fleet.events

use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;
use canon_test::harness::TestHarness;

// Pull in fleet-service domain to link its inventory registrations.
use fleet_service::aggregate::{Ship, ShipStatus};

// ── Helpers ────────────────────────────────────────────────────────────────

fn make_fleet_event(
    agg_id: &AggregateId,
    version: u64,
    event_type: &str,
    payload: &impl serde::Serialize,
) -> EventEnvelope {
    EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: agg_id.clone(),
        version: Version::from_u64(version),
        event_type: event_type.to_owned(),
        event_version: 1,
        payload: Bytes::from(serde_json::to_vec(payload).expect("serialize")),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    }
}

// ── ServiceBuilder validation ──────────────────────────────────────────────

#[test]
fn service_builder_validates_fleet_registrations() {
    // This proves that all 5 commands have handlers and all 5 events have combiners.
    let _harness = TestHarness::builder().for_aggregate::<Ship>().build();
}

// ── Event combiner tests ───────────────────────────────────────────────────

#[test]
fn ship_registered_combiner_sets_initial_state() {
    use fleet_service::events::ShipRegistered;

    let agg_id = AggregateId::new();
    let event = ShipRegistered {
        ship_id: Uuid::new_v4(),
        name: "Meridian".to_owned(),
        capacity_kg: 5000.0,
        home_station: None,
    };
    let envelope = make_fleet_event(&agg_id, 1, "ShipRegistered", &event);

    let mut state = Ship::default();
    Ship::hydrate(&mut state, std::iter::once(envelope)).expect("hydrate");

    assert_eq!(state.name, "Meridian");
    assert_eq!(state.capacity_kg, 5000.0);
    assert_eq!(state.status, ShipStatus::Docked);
}

#[test]
fn route_assigned_combiner_sets_route() {
    use fleet_service::events::{RouteAssigned, ShipRegistered};

    let agg_id = AggregateId::new();
    let ship_id = Uuid::new_v4();
    let route_id = Uuid::new_v4();

    let events = vec![
        make_fleet_event(
            &agg_id,
            1,
            "ShipRegistered",
            &ShipRegistered {
                ship_id,
                name: "Argo".to_owned(),
                capacity_kg: 3000.0,
                home_station: None,
            },
        ),
        make_fleet_event(
            &agg_id,
            2,
            "RouteAssigned",
            &RouteAssigned { ship_id, route_id },
        ),
    ];

    let mut state = Ship::default();
    Ship::hydrate(&mut state, events.into_iter()).expect("hydrate");

    assert_eq!(state.assigned_route, Some(route_id));
}

#[test]
fn ship_departed_combiner_sets_in_transit() {
    use fleet_service::events::{ShipDeparted, ShipRegistered};

    let agg_id = AggregateId::new();
    let ship_id = Uuid::new_v4();
    let voyage_id = Uuid::new_v4();
    let dest = Uuid::new_v4();

    let events = vec![
        make_fleet_event(
            &agg_id,
            1,
            "ShipRegistered",
            &ShipRegistered {
                ship_id,
                name: "Eclipse".to_owned(),
                capacity_kg: 4000.0,
                home_station: None,
            },
        ),
        make_fleet_event(
            &agg_id,
            2,
            "ShipDeparted",
            &ShipDeparted {
                ship_id,
                voyage_id,
                destination: dest,
                fuel_at_departure: 100.0,
            },
        ),
    ];

    let mut state = Ship::default();
    Ship::hydrate(&mut state, events.into_iter()).expect("hydrate");

    assert_eq!(state.status, ShipStatus::InTransit);
}

#[test]
fn resupply_scheduled_combiner_updates_fuel() {
    use fleet_service::events::{ResupplyScheduled, ShipRegistered};

    let agg_id = AggregateId::new();
    let ship_id = Uuid::new_v4();

    let events = vec![
        make_fleet_event(
            &agg_id,
            1,
            "ShipRegistered",
            &ShipRegistered {
                ship_id,
                name: "Kronos".to_owned(),
                capacity_kg: 6000.0,
                home_station: None,
            },
        ),
        make_fleet_event(
            &agg_id,
            2,
            "ResupplyScheduled",
            &ResupplyScheduled {
                ship_id,
                fuel_kg: 500.0,
            },
        ),
    ];

    let mut state = Ship::default();
    Ship::hydrate(&mut state, events.into_iter()).expect("hydrate");

    assert!((state.fuel_kg - 500.0).abs() < f32::EPSILON);
}

#[test]
fn ship_decommissioned_combiner_sets_status() {
    use fleet_service::events::{ShipDecommissioned, ShipRegistered};

    let agg_id = AggregateId::new();
    let ship_id = Uuid::new_v4();

    let events = vec![
        make_fleet_event(
            &agg_id,
            1,
            "ShipRegistered",
            &ShipRegistered {
                ship_id,
                name: "Herald".to_owned(),
                capacity_kg: 2000.0,
                home_station: None,
            },
        ),
        make_fleet_event(
            &agg_id,
            2,
            "ShipDecommissioned",
            &ShipDecommissioned { ship_id },
        ),
    ];

    let mut state = Ship::default();
    Ship::hydrate(&mut state, events.into_iter()).expect("hydrate");

    assert_eq!(state.status, ShipStatus::Decommissioned);
}

// ── Command handler tests ──────────────────────────────────────────────────

#[tokio::test]
async fn depart_rejected_when_not_docked() {
    use fleet_service::commands::DepartForStation;
    use fleet_service::handlers::DepartForStationHandler;

    let handler = DepartForStationHandler;
    let state = Ship {
        status: ShipStatus::InTransit,
        ..Ship::default()
    };

    let cmd = DepartForStation {
        ship_id: Uuid::new_v4(),
        voyage_id: Uuid::new_v4(),
        destination: Uuid::new_v4(),
    };

    let result = CommandHandler::<Ship>::handle(&handler, &state, cmd).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn depart_succeeds_when_docked() {
    use fleet_service::commands::DepartForStation;
    use fleet_service::handlers::DepartForStationHandler;

    let handler = DepartForStationHandler;
    let state = Ship {
        status: ShipStatus::Docked,
        fuel_kg: 100.0,
        ..Ship::default()
    };
    let dest = Uuid::new_v4();

    let cmd = DepartForStation {
        ship_id: Uuid::new_v4(),
        voyage_id: Uuid::new_v4(),
        destination: dest,
    };

    let event = CommandHandler::<Ship>::handle(&handler, &state, cmd)
        .await
        .expect("should succeed");

    assert_eq!(event.destination, dest);
    assert!((event.fuel_at_departure - 100.0).abs() < f32::EPSILON);
}

#[tokio::test]
async fn decommission_rejected_when_already_decommissioned() {
    use fleet_service::commands::DecommissionShip;
    use fleet_service::handlers::DecommissionShipHandler;

    let handler = DecommissionShipHandler;
    let state = Ship {
        status: ShipStatus::Decommissioned,
        ..Ship::default()
    };

    let cmd = DecommissionShip {
        ship_id: Uuid::new_v4(),
    };

    let result = CommandHandler::<Ship>::handle(&handler, &state, cmd).await;
    assert!(result.is_err());
}

// ── Herald hydration (247 events, no snapshot) ─────────────────────────────

#[test]
fn herald_hydration_replays_247_events() {
    use fleet_service::events::{ResupplyScheduled, ShipDeparted, ShipRegistered};

    let agg_id = AggregateId::new();
    let ship_id = Uuid::new_v4();
    let mut events = Vec::with_capacity(247);

    // Event 1: register
    events.push(make_fleet_event(
        &agg_id,
        1,
        "ShipRegistered",
        &ShipRegistered {
            ship_id,
            name: "Herald".to_owned(),
            capacity_kg: 2000.0,
            home_station: None,
        },
    ));

    // Alternate between depart and resupply to build up 247 events
    for i in 2..=247u64 {
        if i % 2 == 0 {
            events.push(make_fleet_event(
                &agg_id,
                i,
                "ShipDeparted",
                &ShipDeparted {
                    ship_id,
                    voyage_id: Uuid::new_v4(),
                    destination: Uuid::new_v4(),
                    fuel_at_departure: 100.0,
                },
            ));
        } else {
            events.push(make_fleet_event(
                &agg_id,
                i,
                "ResupplyScheduled",
                &ResupplyScheduled {
                    ship_id,
                    fuel_kg: 500.0,
                },
            ));
        }
    }

    assert_eq!(events.len(), 247);

    let mut state = Ship::default();
    Ship::hydrate(&mut state, events.into_iter()).expect("hydrate 247 events");

    assert_eq!(state.name, "Herald");
    // After 247 events, ship should be in transit (247 is odd -> last event is ResupplyScheduled)
    assert!((state.fuel_kg - 500.0).abs() < f32::EPSILON);
}

// ── Snapshot + partial hydration ───────────────────────────────────────────

#[test]
fn hydrate_from_snapshot_plus_remaining_events() {
    use fleet_service::events::{ShipDeparted, ShipRegistered};

    let agg_id = AggregateId::new();
    let ship_id = Uuid::new_v4();
    let harness = TestHarness::new();

    // Build 51 events: register + 50 departs
    let mut events = Vec::with_capacity(51);
    events.push(make_fleet_event(
        &agg_id,
        1,
        "ShipRegistered",
        &ShipRegistered {
            ship_id,
            name: "SnapshotShip".to_owned(),
            capacity_kg: 4000.0,
            home_station: None,
        },
    ));
    for i in 2..=51u64 {
        events.push(make_fleet_event(
            &agg_id,
            i,
            "ShipDeparted",
            &ShipDeparted {
                ship_id,
                voyage_id: Uuid::new_v4(),
                destination: Uuid::new_v4(),
                fuel_at_departure: 100.0,
            },
        ));
    }

    // Store all events
    harness.append_events(&agg_id, Version::initial(), events);

    // Save snapshot at version 50 (simulating event store consumer)
    let mut snap_state = Ship::default();
    let snap_events = harness.load_events(&agg_id);
    Ship::hydrate(&mut snap_state, snap_events[..50].iter().cloned())
        .expect("hydrate for snapshot");

    let snap_bytes =
        Bytes::from(serde_json::to_vec(&snap_state).expect("serialize snapshot state"));
    harness.save_snapshot(Snapshot {
        aggregate_id: agg_id.clone(),
        version: Version::from_u64(50),
        state: snap_bytes,
        taken_at: Utc::now(),
    });

    // Load snapshot + remaining events
    let loaded = harness.load_snapshot(&agg_id).expect("snapshot exists");
    let mut state: Ship = serde_json::from_slice(&loaded.state).expect("deserialize snapshot");

    let remaining = harness
        .event_store
        .load_from_version(&agg_id, loaded.version.next())
        .expect("load remaining");
    assert_eq!(remaining.len(), 1); // only event 51

    Ship::hydrate(&mut state, remaining.into_iter()).expect("hydrate remaining");

    assert_eq!(state.name, "SnapshotShip");
    assert_eq!(state.status, ShipStatus::InTransit);
}

// ── Event store consumer snapshot trigger ──────────────────────────────────

#[tokio::test]
async fn event_store_consumer_snapshots_at_50() {
    use fleet_service::events::ShipRegistered;

    let event_store = InMemoryEventStore::new();
    let snapshot_store = InMemorySnapshotStore::new();
    let dead_letter_store = InMemoryDeadLetterStore::new();
    let retry_tracker = InMemoryRetryTracker::new();

    let consumer = EventStoreConsumer::new(
        event_store.clone(),
        snapshot_store.clone(),
        dead_letter_store,
        retry_tracker,
        EventPayloadSnapshotProvider,
        EventStoreConsumerConfig {
            snapshot_every: 50,
            max_retries: 3,
        },
    );

    let agg_id = AggregateId::new();
    let ship_id = Uuid::new_v4();

    // Process 50 events
    for i in 1..=50u64 {
        let event = make_fleet_event(
            &agg_id,
            i,
            "ShipRegistered",
            &ShipRegistered {
                ship_id,
                name: format!("Ship-{i}"),
                capacity_kg: 1000.0,
                home_station: None,
            },
        );
        consumer.process(event).await.expect("process event");
    }

    // Snapshot should exist at version 50
    let snap = snapshot_store.load(&agg_id).expect("load").expect("exists");
    assert_eq!(snap.version.as_u64(), 50);
}

// ── Publisher consumer publishes to fleet topic ────────────────────────────

#[tokio::test]
async fn publisher_consumer_publishes_fleet_events() {
    use fleet_service::events::ShipRegistered;

    let publisher = InMemoryPublisher::new();
    let consumer = PublisherConsumer::new(publisher.clone(), "canon.fleet.events");

    let agg_id = AggregateId::new();
    let event = make_fleet_event(
        &agg_id,
        1,
        "ShipRegistered",
        &ShipRegistered {
            ship_id: Uuid::new_v4(),
            name: "TestShip".to_owned(),
            capacity_kg: 1000.0,
            home_station: None,
        },
    );

    consumer.process(&event).await.expect("publish");

    let published = publisher.published_events().expect("list");
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].1, "canon.fleet.events");
    assert_eq!(published[0].0.event_type, "ShipRegistered");
}

// ── ResupplyHandler produces ScheduleResupply command ──────────────────────

#[tokio::test]
async fn resupply_handler_produces_command() {
    use fleet_service::event_handlers::ResupplyHandler;
    use fleet_service::inbound::InboundResupplyDispatched as ResupplyDispatched;

    let handler = ResupplyHandler;
    let ship_id = Uuid::new_v4();

    let events = vec![ResupplyDispatched {
        ship_id,
        fuel_kg: 300.0,
    }];

    let result = EventHandler::handle(&handler, events)
        .await
        .expect("handle");

    let cmd = result.expect("should produce command");
    assert_eq!(cmd.command_type, "ScheduleResupply");
    assert_eq!(cmd.aggregate_id.as_uuid(), &ship_id);
}
