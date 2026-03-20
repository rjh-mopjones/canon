use uuid::Uuid;

use canon_core::*;
use canon_test::domain::*;

#[tokio::test]
async fn test_projection_rebuilding_flag() {
    let event_store = InMemoryEventStore::new();
    let projection_store = InMemoryProjectionStore::new();
    let id = AggregateId::new();

    // Store events in event store
    let events = vec![make_placed_envelope(&id, Uuid::new_v4())];
    event_store.append(&id, Version::initial(), events).unwrap();

    // Projection checkpoint is stale (initial)
    let stale = projection_store.get_checkpoint("test-projection").unwrap();
    assert_eq!(stale, Version::initial());

    // Simulate rebuilding=true: read path falls back to event store
    let rebuilding = true;
    if rebuilding {
        let events = event_store.load(&id).unwrap();
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
    let projection_store = InMemoryProjectionStore::new();
    let outbound_queue = InMemoryOutboundQueue::new();
    let event_store = InMemoryEventStore::new();
    let id = AggregateId::new();

    // Store events with proper versions via event store
    let events = vec![
        make_placed_envelope(&id, Uuid::new_v4()),
        make_cancelled_envelope(&id, "rebuild"),
    ];
    event_store.append(&id, Version::initial(), events).unwrap();
    let stored = event_store.load(&id).unwrap();

    // Register projection consumer and publish events to outbound queue
    let consumer = outbound_queue.register_consumer().unwrap();
    for event in &stored {
        outbound_queue.publish(event.clone()).unwrap();
    }

    // First pass: consume and apply projection
    while let Some(event) = outbound_queue.receive(&consumer).unwrap() {
        projection_store
            .set_checkpoint("test-projection", event.version)
            .unwrap();
    }
    assert_eq!(
        projection_store
            .get_checkpoint("test-projection")
            .unwrap()
            .as_u64(),
        2
    );

    // Simulate offset reset: reset checkpoint
    projection_store
        .set_checkpoint("test-projection", Version::initial())
        .unwrap();

    // Re-publish events (simulating Kafka replay after offset reset)
    for event in &stored {
        outbound_queue.publish(event.clone()).unwrap();
    }

    // Re-consume: projection rebuilds correctly
    while let Some(event) = outbound_queue.receive(&consumer).unwrap() {
        projection_store
            .set_checkpoint("test-projection", event.version)
            .unwrap();
    }

    assert_eq!(
        projection_store
            .get_checkpoint("test-projection")
            .unwrap()
            .as_u64(),
        2
    );
}
