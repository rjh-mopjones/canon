//! End-to-end in-memory tests for the DrainStock pipeline.
//!
//! Exercises the DrainStock command through the station-service:
//!   RegisterStation → StationRegistered → (optional CargoReceived for stock)
//!   → DrainStock → StockDrained → aggregate state updated → projection updated
//!
//! Tests cover:
//! - Happy path drain through the full pipeline (dispatcher → outbox → consumers)
//! - Drain clamping when drain_kg exceeds available stock
//! - Rejection on unregistered station
//! - Rejection on depleted station (stock = 0)
//! - Rejection on offline station
//! - Aggregate hydration correctness after multiple drains

use std::any::TypeId;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::consumers::RegisteredProjection;
use canon_core::*;
use canon_test::harness::TestHarness;
use station_service::commands::{DrainStock, RegisterStation};

// Link station-service inventory registrations by importing the aggregate.
use station_service::aggregate::Station;
use station_service::commands::DrainStockHandler;

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Create an EventEnvelope for a station event.
fn make_station_event(
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

/// Create a RegisterStation command envelope targeting the given aggregate.
fn make_register_station_command(
    aggregate_id: &AggregateId,
    name: &str,
    capacity_kg: f32,
) -> CommandEnvelope {
    let cmd = RegisterStation {
        name: name.to_owned(),
        capacity_kg,
    };
    let payload = serde_json::to_vec(&cmd).expect("serialize RegisterStation");

    CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        command_type: "RegisterStation".to_owned(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from(payload),
        command_version: 1,
    }
}

/// Create a DrainStock command envelope targeting the given aggregate.
fn make_drain_stock_command(
    aggregate_id: &AggregateId,
    station_id: Uuid,
    drain_kg: f32,
) -> CommandEnvelope {
    let cmd = DrainStock {
        station_id,
        drain_kg,
    };
    let payload = serde_json::to_vec(&cmd).expect("serialize DrainStock");

    CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        command_type: "DrainStock".to_owned(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from(payload),
        command_version: 1,
    }
}

/// Build a registered station state with the given stock level.
fn registered_station_with_stock(name: &str, capacity_kg: f32, stock_kg: f32) -> Station {
    Station {
        name: name.to_string(),
        capacity_kg,
        current_stock_kg: stock_kg,
        drain_rate_kg_per_s: 1.0,
        supplied_by: None,
        docked_ships: Vec::new(),
        registered: true,
        offline: false,
    }
}

// ── Pipeline fixture for Station aggregate ────────────────────────────────────

/// Wire up the full pipeline components for Station, matching the PipelineFixture
/// pattern from e2e_pipeline.rs.
struct StationPipelineFixture {
    dispatcher: Dispatcher<InMemoryDispatcherStore>,
    dispatcher_store: InMemoryDispatcherStore,
    outbox_processor: OutboxProcessor<InMemoryOutboxStore, InMemoryOutboxPublisher>,
    outbound_queue: InMemoryOutboundQueue,
    event_store_consumer: EventStoreConsumer<
        InMemoryEventStore,
        InMemorySnapshotStore,
        InMemoryDeadLetterStore,
        InMemoryRetryTracker,
        EventPayloadSnapshotProvider,
    >,
    projection_consumer: ProjectionConsumer<InMemoryProjectionStore>,
    publisher_consumer: PublisherConsumer<InMemoryPublisher>,
    // Shared stores for assertions
    event_store: InMemoryEventStore,
    snapshot_store: InMemorySnapshotStore,
    dead_letter_store: InMemoryDeadLetterStore,
    publisher: InMemoryPublisher,
    // Consumer handles for receiving from outbound queue
    es_consumer_handle: ConsumerHandle,
    proj_consumer_handle: ConsumerHandle,
    pub_consumer_handle: ConsumerHandle,
}

impl StationPipelineFixture {
    fn new() -> Self {
        let dispatcher_event_store = InMemoryEventStore::new();
        let outbox_store = InMemoryOutboxStore::new();
        let dispatcher_store =
            InMemoryDispatcherStore::new(dispatcher_event_store, outbox_store.clone());

        let dispatcher_config = DispatcherConfig {
            batch_size: 100,
            poll_interval_ms: 10,
            aggregate_type_id: TypeId::of::<Station>(),
            max_retries: 3,
        };
        let dispatcher = Dispatcher::new(dispatcher_store.clone(), dispatcher_config);

        let outbound_queue = InMemoryOutboundQueue::new();
        let es_consumer_handle = outbound_queue
            .register_consumer()
            .expect("register event store consumer");
        let proj_consumer_handle = outbound_queue
            .register_consumer()
            .expect("register projection consumer");
        let pub_consumer_handle = outbound_queue
            .register_consumer()
            .expect("register publisher consumer");

        let outbox_publisher = InMemoryOutboxPublisher::new(outbound_queue.clone());
        let outbox_processor = OutboxProcessor::new(
            outbox_store,
            outbox_publisher,
            OutboxProcessorConfig {
                batch_size: 100,
                channel_capacity: 1024,
                poll_interval_ms: 10,
            },
        );

        let event_store = InMemoryEventStore::new();
        let snapshot_store = InMemorySnapshotStore::new();
        let dead_letter_store = InMemoryDeadLetterStore::new();
        let retry_tracker = InMemoryRetryTracker::new();
        let event_store_consumer = EventStoreConsumer::new(
            event_store.clone(),
            snapshot_store.clone(),
            dead_letter_store.clone(),
            retry_tracker,
            EventPayloadSnapshotProvider,
            EventStoreConsumerConfig::default(),
        );

        let projection_store = InMemoryProjectionStore::new();
        let projection_consumer = ProjectionConsumer::new(projection_store.clone());

        let publisher = InMemoryPublisher::new();
        let publisher_consumer = PublisherConsumer::new(publisher.clone(), "canon.station.events");

        StationPipelineFixture {
            dispatcher,
            dispatcher_store,
            outbox_processor,
            outbound_queue,
            event_store_consumer,
            projection_consumer,
            publisher_consumer,
            event_store,
            snapshot_store,
            dead_letter_store,
            publisher,
            es_consumer_handle,
            proj_consumer_handle,
            pub_consumer_handle,
        }
    }

    /// Submit a command to the dispatcher's inbox, process it through the dispatcher,
    /// drain the outbox, and feed all three consumers.
    async fn run_pipeline_for_command(
        &self,
        handler_id: &str,
        command: CommandEnvelope,
        sequence_number: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.dispatcher_store
            .enqueue_command(handler_id, command)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

        self.dispatcher
            .process_batch()
            .await
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

        self.outbox_processor
            .drain_once()
            .await
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

        if let Some(envelope) = self
            .outbound_queue
            .receive(&self.es_consumer_handle)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?
        {
            self.event_store_consumer
                .process(envelope)
                .await
                .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
        }

        if let Some(envelope) = self
            .outbound_queue
            .receive(&self.proj_consumer_handle)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?
        {
            self.projection_consumer
                .process(&envelope, sequence_number)
                .await
                .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
        }

        if let Some(envelope) = self
            .outbound_queue
            .receive(&self.pub_consumer_handle)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?
        {
            self.publisher_consumer
                .process(&envelope)
                .await
                .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
        }

        Ok(())
    }
}

// ── ServiceBuilder validation ─────────────────────────────────────────────────

#[test]
fn service_builder_validates_station_registrations() {
    let _harness = TestHarness::builder().for_aggregate::<Station>().build();
}

// ── Test 1: Happy path drain through full pipeline ────────────────────────────

#[tokio::test]
async fn e2e_drain_stock_happy_path() {
    let mut fixture = StationPipelineFixture::new();
    let agg_id = AggregateId::new();

    // Register a counting projection.
    let apply_count = Arc::new(AtomicU32::new(0));
    let counter_clone = Arc::clone(&apply_count);
    fixture.projection_consumer.register(RegisteredProjection {
        projection_id: "station-drain-test".to_owned(),
        apply_fn: Box::new(move |_proj_id, _envelope| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    });

    // Step 1: Register the station so it has initial state.
    let register_cmd = make_register_station_command(&agg_id, "Alpha Depot", 5000.0);
    fixture
        .run_pipeline_for_command("Station", register_cmd, 1)
        .await
        .expect("register station pipeline");

    // Verify registration event reached event store.
    let events = fixture.event_store.load(&agg_id).expect("load events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "StationRegistered");

    // The station starts with 0 stock after registration. The StationRegistered
    // combiner sets capacity but not stock. We need to hydrate to see what the
    // dispatcher sees. The dispatcher hydrates from its own event store (which
    // records the outbox-written event on process). Since the station starts with
    // 0 stock, a DrainStock would fail with StockDepleted.
    //
    // The DrainStock handler requires current_stock_kg > 0. With the default
    // aggregate state after registration (stock = 0), it would fail. We need
    // CargoReceived to add stock. But there's no easy way to inject that into
    // the dispatcher's own event store through the pipeline without a separate
    // command.
    //
    // Instead, let's test the handler directly with the TestHarness pattern
    // (as done in fleet_service.rs and navigation_lifecycle.rs).
    // The pipeline test verifies that registration goes through the full pipeline;
    // the drain handler tests below verify the DrainStock logic with crafted state.

    // Assert: projection was applied.
    assert_eq!(
        apply_count.load(Ordering::SeqCst),
        1,
        "projection should have been applied once for registration"
    );

    // Assert: publisher published to the correct topic.
    let published = fixture.publisher.published_events().expect("published");
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].1, "canon.station.events");
    assert_eq!(published[0].0.event_type, "StationRegistered");
}

// ── Test 2: DrainStock handler happy path via TestHarness ─────────────────────

#[tokio::test]
async fn drain_stock_handler_happy_path() {
    use station_service::events::StockDrained;

    let harness = TestHarness::new();
    let agg_id = AggregateId::new();
    let station_id = Uuid::new_v4();
    let correlation_id = Uuid::new_v4();

    // Set up: register station with stock.
    let registered = station_service::events::StationRegistered {
        station_id,
        name: "Alpha Depot".to_owned(),
        capacity_kg: 5000.0,
    };
    let cargo = station_service::events::CargoReceived {
        station_id,
        manifest_id: Uuid::new_v4(),
        weight_kg: 1000.0,
    };

    let env1 = make_station_event(&agg_id, 1, "StationRegistered", &registered);
    let env2 = EventEnvelope {
        correlation_id,
        ..make_station_event(&agg_id, 2, "CargoReceived", &cargo)
    };
    harness.append_events(&agg_id, Version::initial(), vec![env1]);
    harness.append_events(&agg_id, Version::from_u64(1), vec![env2]);

    // Hydrate state.
    let events = harness.load_events(&agg_id);
    let mut state = Station::default();
    Station::hydrate(&mut state, events.into_iter()).expect("hydrate");
    assert!(state.registered);
    assert!((state.current_stock_kg - 1000.0).abs() < f32::EPSILON);

    // Execute DrainStock command.
    let handler = DrainStockHandler;
    let cmd = DrainStock {
        station_id,
        drain_kg: 300.0,
    };
    let event: StockDrained = CommandHandler::<Station>::handle(&handler, &state, cmd)
        .await
        .expect("DrainStock should succeed");

    assert!((event.drain_kg - 300.0).abs() < f32::EPSILON);
    assert!((event.remaining_kg - 700.0).abs() < f32::EPSILON);
    assert_eq!(event.station_id, station_id);

    // Store the event and verify in event store.
    let env3 = EventEnvelope {
        correlation_id,
        ..make_station_event(&agg_id, 3, "StockDrained", &event)
    };
    harness.append_events(&agg_id, Version::from_u64(2), vec![env3]);

    harness.assert_event_count(&agg_id, 3);
    let all_events = harness.load_events(&agg_id);
    assert_eq!(all_events[2].event_type, "StockDrained");
    assert_eq!(all_events[2].version.as_u64(), 3);

    // Hydrate from event store and verify state.
    let mut final_state = Station::default();
    Station::hydrate(&mut final_state, all_events.into_iter()).expect("final hydrate");
    assert!((final_state.current_stock_kg - 700.0).abs() < f32::EPSILON);
}

// ── Test 3: DrainStock clamping ──────────────────────────────────────────────

#[tokio::test]
async fn drain_stock_clamps_to_available_stock() {
    use station_service::events::StockDrained;

    let state = registered_station_with_stock("Beta Relay", 5000.0, 50.0);
    let station_id = Uuid::new_v4();

    let handler = DrainStockHandler;
    let cmd = DrainStock {
        station_id,
        drain_kg: 200.0, // Request more than available (50 kg)
    };
    let event: StockDrained = CommandHandler::<Station>::handle(&handler, &state, cmd)
        .await
        .expect("DrainStock should succeed with clamping");

    // Should clamp to available stock.
    assert!(
        (event.drain_kg - 50.0).abs() < f32::EPSILON,
        "drain_kg should be clamped to available stock (50), got {}",
        event.drain_kg
    );
    assert!(
        (event.remaining_kg - 0.0).abs() < f32::EPSILON,
        "remaining_kg should be 0 after clamped drain, got {}",
        event.remaining_kg
    );
}

// ── Test 4: DrainStock on unregistered station ────────────────────────────────

#[tokio::test]
async fn drain_stock_rejects_unregistered_station() {
    let state = Station::default(); // Not registered
    let handler = DrainStockHandler;
    let cmd = DrainStock {
        station_id: Uuid::new_v4(),
        drain_kg: 10.0,
    };

    let result = CommandHandler::<Station>::handle(&handler, &state, cmd).await;
    assert!(
        result.is_err(),
        "should reject drain on unregistered station"
    );

    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("not registered"),
        "error should mention 'not registered', got: {err}"
    );
}

// ── Test 5: DrainStock on depleted station ────────────────────────────────────

#[tokio::test]
async fn drain_stock_rejects_depleted_station() {
    let state = registered_station_with_stock("Gamma Outpost", 5000.0, 0.0);
    let handler = DrainStockHandler;
    let cmd = DrainStock {
        station_id: Uuid::new_v4(),
        drain_kg: 10.0,
    };

    let result = CommandHandler::<Station>::handle(&handler, &state, cmd).await;
    assert!(result.is_err(), "should reject drain on depleted station");

    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("depleted"),
        "error should mention 'depleted', got: {err}"
    );
}

// ── Test 6: DrainStock on offline station ─────────────────────────────────────

#[tokio::test]
async fn drain_stock_rejects_offline_station() {
    let mut state = registered_station_with_stock("Delta Prime", 5000.0, 500.0);
    state.offline = true;

    let handler = DrainStockHandler;
    let cmd = DrainStock {
        station_id: Uuid::new_v4(),
        drain_kg: 10.0,
    };

    let result = CommandHandler::<Station>::handle(&handler, &state, cmd).await;
    assert!(result.is_err(), "should reject drain on offline station");

    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("offline"),
        "error should mention 'offline', got: {err}"
    );
}

// ── Test 7: Aggregate hydration after multiple drains ─────────────────────────

#[test]
fn aggregate_hydration_after_multiple_drains() {
    use station_service::events::{CargoReceived, StationRegistered, StockDrained};

    let harness = TestHarness::new();
    let agg_id = AggregateId::new();
    let station_id = Uuid::new_v4();

    // Register station.
    let env1 = make_station_event(
        &agg_id,
        1,
        "StationRegistered",
        &StationRegistered {
            station_id,
            name: "Alpha Depot".to_owned(),
            capacity_kg: 10000.0,
        },
    );
    harness.append_events(&agg_id, Version::initial(), vec![env1]);

    // Add stock via CargoReceived.
    let env2 = make_station_event(
        &agg_id,
        2,
        "CargoReceived",
        &CargoReceived {
            station_id,
            manifest_id: Uuid::new_v4(),
            weight_kg: 5000.0,
        },
    );
    harness.append_events(&agg_id, Version::from_u64(1), vec![env2]);

    // Drain 1: 1000 kg, remaining = 4000.
    let env3 = make_station_event(
        &agg_id,
        3,
        "StockDrained",
        &StockDrained {
            station_id,
            drain_kg: 1000.0,
            remaining_kg: 4000.0,
        },
    );
    harness.append_events(&agg_id, Version::from_u64(2), vec![env3]);

    // Drain 2: 1500 kg, remaining = 2500.
    let env4 = make_station_event(
        &agg_id,
        4,
        "StockDrained",
        &StockDrained {
            station_id,
            drain_kg: 1500.0,
            remaining_kg: 2500.0,
        },
    );
    harness.append_events(&agg_id, Version::from_u64(3), vec![env4]);

    // Drain 3: 2500 kg, remaining = 0.
    let env5 = make_station_event(
        &agg_id,
        5,
        "StockDrained",
        &StockDrained {
            station_id,
            drain_kg: 2500.0,
            remaining_kg: 0.0,
        },
    );
    harness.append_events(&agg_id, Version::from_u64(4), vec![env5]);

    // Verify event count.
    harness.assert_event_count(&agg_id, 5);

    // Hydrate from all events and verify final state.
    let all_events = harness.load_events(&agg_id);
    assert_eq!(all_events[0].event_type, "StationRegistered");
    assert_eq!(all_events[1].event_type, "CargoReceived");
    assert_eq!(all_events[2].event_type, "StockDrained");
    assert_eq!(all_events[3].event_type, "StockDrained");
    assert_eq!(all_events[4].event_type, "StockDrained");

    // Sequential versions.
    for (i, event) in all_events.iter().enumerate() {
        assert_eq!(
            event.version.as_u64(),
            (i + 1) as u64,
            "event at index {i} should have version {}",
            i + 1
        );
    }

    let mut state = Station::default();
    Station::hydrate(&mut state, all_events.into_iter()).expect("hydrate all events");

    assert_eq!(state.name, "Alpha Depot");
    assert!(state.registered);
    assert!(!state.offline);
    assert!((state.capacity_kg - 10000.0).abs() < f32::EPSILON);
    assert!(
        (state.current_stock_kg - 0.0).abs() < f32::EPSILON,
        "stock should be 0 after three drains totaling 5000 kg, got {}",
        state.current_stock_kg
    );
}

// ── Test 8: StockDrained event reaches event store via pipeline ──────────────

#[tokio::test]
async fn drain_stock_event_reaches_event_store_via_consumer() {
    use station_service::events::StockDrained;

    // Manually create a StockDrained event and feed it to the event store consumer,
    // verifying it is stored correctly (same pattern as e2e_pipeline::e2e_outbox_ordering).
    let event_store = InMemoryEventStore::new();
    let snapshot_store = InMemorySnapshotStore::new();
    let dead_letter_store = InMemoryDeadLetterStore::new();
    let retry_tracker = InMemoryRetryTracker::new();

    let es_consumer = EventStoreConsumer::new(
        event_store.clone(),
        snapshot_store,
        dead_letter_store,
        retry_tracker,
        EventPayloadSnapshotProvider,
        EventStoreConsumerConfig::default(),
    );

    let agg_id = AggregateId::new();
    let station_id = Uuid::new_v4();

    // Event 1: StationRegistered (required for well-formed stream).
    let registered = station_service::events::StationRegistered {
        station_id,
        name: "Alpha Depot".to_owned(),
        capacity_kg: 5000.0,
    };
    let env1 = make_station_event(&agg_id, 1, "StationRegistered", &registered);
    es_consumer
        .process(env1)
        .await
        .expect("process StationRegistered");

    // Event 2: StockDrained.
    let drained = StockDrained {
        station_id,
        drain_kg: 150.0,
        remaining_kg: 850.0,
    };
    let env2 = make_station_event(&agg_id, 2, "StockDrained", &drained);
    es_consumer
        .process(env2)
        .await
        .expect("process StockDrained");

    // Assert: event store has both events.
    let events = event_store.load(&agg_id).expect("load");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event_type, "StationRegistered");
    assert_eq!(events[1].event_type, "StockDrained");
    assert_eq!(events[1].version.as_u64(), 2);

    // Deserialize the StockDrained payload and verify fields.
    let stored_event: StockDrained =
        serde_json::from_slice(&events[1].payload).expect("deserialize StockDrained");
    assert!((stored_event.drain_kg - 150.0).abs() < f32::EPSILON);
    assert!((stored_event.remaining_kg - 850.0).abs() < f32::EPSILON);
}

// ── Test 9: StockDrained publishes to station topic ──────────────────────────

#[tokio::test]
async fn drain_stock_publishes_to_station_topic() {
    use station_service::events::StockDrained;

    let publisher = InMemoryPublisher::new();
    let consumer = PublisherConsumer::new(publisher.clone(), "canon.station.events");

    let agg_id = AggregateId::new();
    let station_id = Uuid::new_v4();

    let event = make_station_event(
        &agg_id,
        1,
        "StockDrained",
        &StockDrained {
            station_id,
            drain_kg: 42.0,
            remaining_kg: 958.0,
        },
    );

    consumer.process(&event).await.expect("publish");

    let published = publisher.published_events().expect("list");
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].1, "canon.station.events");
    assert_eq!(published[0].0.event_type, "StockDrained");
}

// ── Test 10: Full pipeline register + drain via dispatcher ───────────────────
//
// The InMemoryDispatcherStore's write_outbox_and_mark_processed writes to the
// outbox but does NOT feed events back into the dispatcher's event store. This
// mirrors the real architecture where the dispatcher reads from YugabyteDB and
// the event store consumer writes to Cassandra — they are separate stores.
//
// To test multi-command sequences on the same aggregate, we replicate the
// consumer's events into the dispatcher's event store between steps. This
// simulates the real system where YugabyteDB's event store (which the
// dispatcher reads) is populated by the command write path.

#[tokio::test]
async fn e2e_full_pipeline_register_then_drain() {
    let fixture = StationPipelineFixture::new();
    let agg_id = AggregateId::new();

    // Step 1: Register station through the pipeline.
    let register_cmd = make_register_station_command(&agg_id, "Alpha Depot", 5000.0);
    fixture
        .run_pipeline_for_command("Station", register_cmd, 1)
        .await
        .expect("register station");

    // Verify StationRegistered event in consumer event store.
    let events = fixture.event_store.load(&agg_id).expect("load events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "StationRegistered");

    // Replicate the StationRegistered event into the dispatcher's event store
    // so the dispatcher can hydrate state for the next command.
    let dispatcher_es = fixture.dispatcher_store.event_store();
    dispatcher_es
        .append(&agg_id, Version::initial(), vec![events[0].clone()])
        .expect("replicate StationRegistered to dispatcher event store");

    // Step 2: RecordCargoReceived to add stock.
    let cargo_cmd = {
        let cmd = station_service::commands::RecordCargoReceived {
            station_id: *agg_id.as_uuid(),
            manifest_id: Uuid::new_v4(),
            weight_kg: 1000.0,
        };
        let payload = serde_json::to_vec(&cmd).expect("serialize RecordCargoReceived");
        CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: agg_id.clone(),
            command_type: "RecordCargoReceived".to_owned(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        }
    };

    fixture
        .run_pipeline_for_command("Station", cargo_cmd, 2)
        .await
        .expect("record cargo received");

    // Verify CargoReceived event.
    let events = fixture.event_store.load(&agg_id).expect("load events");
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].event_type, "CargoReceived");

    // Replicate the CargoReceived event into the dispatcher's event store.
    dispatcher_es
        .append(&agg_id, Version::from_u64(1), vec![events[1].clone()])
        .expect("replicate CargoReceived to dispatcher event store");

    // Step 3: DrainStock through the pipeline.
    let drain_cmd = make_drain_stock_command(&agg_id, *agg_id.as_uuid(), 300.0);
    fixture
        .run_pipeline_for_command("Station", drain_cmd, 3)
        .await
        .expect("drain stock");

    // Verify StockDrained event reached the consumer event store.
    let events = fixture
        .event_store
        .load(&agg_id)
        .expect("load events after drain");
    assert_eq!(
        events.len(),
        3,
        "should have 3 events: registered, cargo, drained"
    );
    assert_eq!(events[2].event_type, "StockDrained");
    assert_eq!(events[2].version.as_u64(), 3);

    // Deserialize and verify the drained event payload.
    let drained: station_service::events::StockDrained =
        serde_json::from_slice(&events[2].payload).expect("deserialize StockDrained");
    assert!(
        (drained.drain_kg - 300.0).abs() < f32::EPSILON,
        "drain_kg should be 300, got {}",
        drained.drain_kg
    );
    assert!(
        (drained.remaining_kg - 700.0).abs() < f32::EPSILON,
        "remaining_kg should be 700, got {}",
        drained.remaining_kg
    );

    // Verify publisher received the StockDrained event on the station topic.
    let published = fixture.publisher.published_events().expect("published");
    assert_eq!(published.len(), 3, "should have published 3 events");
    assert_eq!(published[2].1, "canon.station.events");
    assert_eq!(published[2].0.event_type, "StockDrained");

    // Verify outbox is fully drained.
    assert_eq!(
        fixture.dispatcher_store.outbox_store().undelivered_count(),
        0,
        "outbox should be fully drained"
    );

    // Verify no dead letters were created (all events processed successfully).
    let dead_letters = fixture
        .dead_letter_store
        .list(None)
        .expect("list dead letters");
    assert!(
        dead_letters.is_empty(),
        "no dead letters should have been created"
    );

    // Verify snapshot store state (3 events, snapshot_every=50 so no snapshot yet).
    let snapshot = fixture.snapshot_store.load(&agg_id).expect("load snapshot");
    assert!(
        snapshot.is_none(),
        "no snapshot should exist yet (only 3 events, snapshot_every=50)"
    );
}

// ── Test 11: StockDrained combiner updates state correctly ───────────────────

#[test]
fn stock_drained_combiner_updates_state() {
    use station_service::events::{CargoReceived, StationRegistered, StockDrained};

    let agg_id = AggregateId::new();
    let station_id = Uuid::new_v4();

    let events = vec![
        make_station_event(
            &agg_id,
            1,
            "StationRegistered",
            &StationRegistered {
                station_id,
                name: "Test Station".to_owned(),
                capacity_kg: 2000.0,
            },
        ),
        make_station_event(
            &agg_id,
            2,
            "CargoReceived",
            &CargoReceived {
                station_id,
                manifest_id: Uuid::new_v4(),
                weight_kg: 1000.0,
            },
        ),
        make_station_event(
            &agg_id,
            3,
            "StockDrained",
            &StockDrained {
                station_id,
                drain_kg: 250.0,
                remaining_kg: 750.0,
            },
        ),
    ];

    let mut state = Station::default();
    Station::hydrate(&mut state, events.into_iter()).expect("hydrate");

    assert_eq!(state.name, "Test Station");
    assert!(state.registered);
    assert!((state.current_stock_kg - 750.0).abs() < f32::EPSILON);
    assert!((state.capacity_kg - 2000.0).abs() < f32::EPSILON);
}
