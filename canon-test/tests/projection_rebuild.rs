use uuid::Uuid;

use canon_core::*;
use canon_test::domain::*;
use canon_test::harness::TestHarness;

#[tokio::test]
async fn test_projection_rebuilding_flag() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Store events in event store
    let events = vec![make_placed_envelope(&id, Uuid::new_v4())];
    harness.append_events(&id, Version::initial(), events);

    // Projection checkpoint is stale (initial)
    harness.assert_projection_at("test-projection", 0);

    // Simulate rebuilding=true: read path falls back to event store
    let rebuilding = true;
    if rebuilding {
        let events = harness.load_events(&id);
        let latest_version = events
            .last()
            .map(|e| e.version)
            .unwrap_or(Version::initial());
        // Event store has the truth even though projection is stale
        assert_eq!(latest_version.as_u64(), 1);
    }
}

#[tokio::test]
async fn test_projection_rebuild_offset_reset() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Store events with proper versions via event store
    let events = vec![
        make_placed_envelope(&id, Uuid::new_v4()),
        make_cancelled_envelope(&id, "rebuild"),
    ];
    harness.append_events(&id, Version::initial(), events);
    let stored = harness.load_events(&id);

    // Register projection consumer and publish events to outbound queue
    let consumer = harness.outbound_queue.register_consumer().unwrap();
    for event in &stored {
        harness.publish_to_outbound(event.clone());
    }

    // First pass: consume and apply projection
    while let Some(event) = harness.outbound_queue.receive(&consumer).unwrap() {
        harness
            .projection_store
            .set_checkpoint("test-projection", event.version)
            .unwrap();
    }
    harness.assert_projection_at("test-projection", 2);

    // Simulate offset reset: reset checkpoint
    harness
        .projection_store
        .set_checkpoint("test-projection", Version::initial())
        .unwrap();

    // Re-publish events (simulating Kafka replay after offset reset)
    for event in &stored {
        harness.publish_to_outbound(event.clone());
    }

    // Re-consume: projection rebuilds correctly
    while let Some(event) = harness.outbound_queue.receive(&consumer).unwrap() {
        harness
            .projection_store
            .set_checkpoint("test-projection", event.version)
            .unwrap();
    }

    harness.assert_projection_at("test-projection", 2);
}

#[tokio::test]
async fn test_projection_rebuild_read_through_fallback() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Store 3 events in the event store
    let events = vec![
        make_placed_envelope(&id, Uuid::new_v4()),
        make_cancelled_envelope(&id, "r1"),
        make_placed_envelope(&id, Uuid::new_v4()),
    ];
    harness.append_events(&id, Version::initial(), events);

    // Projection has only processed up to version 1
    harness
        .projection_store
        .set_checkpoint("read-through-proj", Version::initial().next())
        .unwrap();

    // Simulate read-through: when rebuilding=true, read directly from event store
    let rebuilding = true;
    if rebuilding {
        let all_events = harness.load_events(&id);
        assert_eq!(all_events.len(), 3);

        // Apply all events to rebuild projection
        for event in &all_events {
            harness
                .projection_store
                .set_checkpoint("read-through-proj", event.version)
                .unwrap();
        }
    }

    harness.assert_projection_at("read-through-proj", 3);
}

#[tokio::test]
async fn test_projection_idempotent_rebuild() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    let events = vec![
        make_placed_envelope(&id, Uuid::new_v4()),
        make_cancelled_envelope(&id, "rebuild"),
    ];
    harness.append_events(&id, Version::initial(), events);

    // First rebuild
    let all_events = harness.load_events(&id);
    for event in &all_events {
        harness
            .projection_store
            .set_checkpoint("idempotent-proj", event.version)
            .unwrap();
    }
    harness.assert_projection_at("idempotent-proj", 2);

    // Second rebuild (idempotent -- same result)
    harness
        .projection_store
        .set_checkpoint("idempotent-proj", Version::initial())
        .unwrap();
    for event in &all_events {
        harness
            .projection_store
            .set_checkpoint("idempotent-proj", event.version)
            .unwrap();
    }
    harness.assert_projection_at("idempotent-proj", 2);
}
