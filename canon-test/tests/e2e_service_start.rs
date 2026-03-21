//! Tests for `Service::start()` and consumer run loops.
//!
//! These tests verify the runtime polling/spawning that `Service::start()`
//! orchestrates — the outbox processor, event store consumer, projection
//! consumer, and publisher consumer all running as background tasks.

use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::consumers::EventPayloadSnapshotProvider;
use canon_core::memory::{
    InMemoryConsumerReceiver, InMemoryDeadLetterStore, InMemoryEventStore, InMemoryOutboundQueue,
    InMemoryOutboxPublisher, InMemoryOutboxStore, InMemoryProjectionStore, InMemoryPublisher,
    InMemoryRetryTracker, InMemorySnapshotStore,
};
use canon_core::{AggregateId, EventEnvelope, InMemoryCommandStore, ServiceBuilder, Version};

// ── Helpers ─────────────────────────────────────────────────────────────────

fn make_event(aggregate_id: &AggregateId, version: u64) -> EventEnvelope {
    EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        version: Version::from_u64(version),
        event_type: "TestEvent".into(),
        event_version: 1,
        payload: Bytes::from_static(b"{\"test\":true}"),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    }
}

/// Build consumer receivers from a shared outbound queue.
fn make_receivers(
    queue: &InMemoryOutboundQueue,
) -> (
    InMemoryConsumerReceiver,
    InMemoryConsumerReceiver,
    InMemoryConsumerReceiver,
) {
    let es = InMemoryConsumerReceiver::new(queue.clone()).expect("es receiver");
    let proj = InMemoryConsumerReceiver::new(queue.clone()).expect("proj receiver");
    let pub_rx = InMemoryConsumerReceiver::new(queue.clone()).expect("pub receiver");
    (es, proj, pub_rx)
}

// ── Test 1: service.start() processes events automatically ──────────────────

#[tokio::test]
async fn test_service_start_processes_events_automatically() {
    // Shared outbound queue: outbox publisher writes here, consumers read from here.
    let outbound_queue = InMemoryOutboundQueue::new();

    // Shared stores (cloned before building the service so we can assert afterwards).
    let outbox_store = InMemoryOutboxStore::new();
    let event_store = InMemoryEventStore::new();
    let publisher = InMemoryPublisher::new();
    let projection_store = InMemoryProjectionStore::new();

    // Wire the outbox publisher to deliver to the outbound queue.
    let outbox_publisher = InMemoryOutboxPublisher::new(outbound_queue.clone());

    // Clone stores for post-assertion.
    let event_store_clone = event_store.clone();
    let publisher_clone = publisher.clone();

    // Build the service.
    let service = ServiceBuilder::new("test-service")
        .event_store(event_store)
        .snapshot_store(InMemorySnapshotStore::new())
        .dead_letter_store(InMemoryDeadLetterStore::new())
        .retry_tracker(InMemoryRetryTracker::new())
        .snapshot_state_provider(EventPayloadSnapshotProvider)
        .outbox_store(outbox_store.clone())
        .outbox_publisher(outbox_publisher)
        .projection_checkpoint_store(projection_store)
        .publisher(publisher)
        .command_store(InMemoryCommandStore::new())
        .build()
        .expect("service should build");

    // Create a shutdown channel.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Create an outbox notify channel so the outbox processor wakes immediately.
    let (notify_tx, notify_rx) = canon_core::new_outbox_notify_channel(16);

    // Create consumer receivers.
    let (es_rx, proj_rx, pub_rx) = make_receivers(&outbound_queue);

    // Spawn service.start() in a background task.
    let service_handle = tokio::spawn(async move {
        service
            .start(shutdown_rx, Some(notify_rx), es_rx, proj_rx, pub_rx)
            .await
    });

    // Simulate the command handler write path: insert an event into the outbox.
    let aggregate_id = AggregateId::new();
    let event = make_event(&aggregate_id, 1);
    outbox_store
        .insert(event)
        .expect("outbox insert should succeed");

    // Notify the outbox processor that new entries are available.
    notify_tx
        .send(())
        .await
        .expect("notify send should succeed");

    // Wait for the pipeline to process (outbox → outbound queue → consumers).
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Assert: event store consumer received and stored the event.
    let events = event_store_clone
        .load(&aggregate_id)
        .expect("load should succeed");
    assert_eq!(
        events.len(),
        1,
        "event store should have 1 event, found {}",
        events.len()
    );

    // Assert: publisher consumer received and published the event.
    let published = publisher_clone
        .published_events()
        .expect("published_events should succeed");
    assert_eq!(
        published.len(),
        1,
        "publisher should have 1 event, found {}",
        published.len()
    );
    assert_eq!(published[0].1, "canon.test-service.events");

    // Shutdown.
    let _ = shutdown_tx.send(true);

    // Service should exit cleanly within 2s.
    tokio::time::timeout(Duration::from_secs(2), service_handle)
        .await
        .expect("service should stop within 2s")
        .expect("service task should not panic");
}

// ── Test 2: shutdown signal cleanly stops all tasks ─────────────────────────

#[tokio::test]
async fn test_service_start_shutdown() {
    let outbound_queue = InMemoryOutboundQueue::new();
    let outbox_publisher = InMemoryOutboxPublisher::new(outbound_queue.clone());

    let service = ServiceBuilder::new("shutdown-test")
        .event_store(InMemoryEventStore::new())
        .snapshot_store(InMemorySnapshotStore::new())
        .dead_letter_store(InMemoryDeadLetterStore::new())
        .retry_tracker(InMemoryRetryTracker::new())
        .snapshot_state_provider(EventPayloadSnapshotProvider)
        .outbox_store(InMemoryOutboxStore::new())
        .outbox_publisher(outbox_publisher)
        .projection_checkpoint_store(InMemoryProjectionStore::new())
        .publisher(InMemoryPublisher::new())
        .command_store(InMemoryCommandStore::new())
        .build()
        .expect("service should build");

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (es_rx, proj_rx, pub_rx) = make_receivers(&outbound_queue);

    let service_handle = tokio::spawn(async move {
        service
            .start(shutdown_rx, None, es_rx, proj_rx, pub_rx)
            .await
    });

    // Immediately send shutdown.
    let _ = shutdown_tx.send(true);

    // Service should exit cleanly within 2s (no hang).
    tokio::time::timeout(Duration::from_secs(2), service_handle)
        .await
        .expect("service should stop within 2s")
        .expect("service task should not panic");
}

// ── Test 3: consumer run loops process multiple events continuously ─────────

#[tokio::test]
async fn test_consumer_run_loop_processes_multiple_events() {
    // This test exercises the event store consumer loop directly by
    // pushing events into the outbound queue at intervals and verifying
    // the consumer picks them all up continuously.

    let outbound_queue = InMemoryOutboundQueue::new();
    let event_store = InMemoryEventStore::new();
    let event_store_clone = event_store.clone();

    let outbox_publisher = InMemoryOutboxPublisher::new(outbound_queue.clone());
    let outbox_store = InMemoryOutboxStore::new();

    let service = ServiceBuilder::new("multi-event-test")
        .event_store(event_store)
        .snapshot_store(InMemorySnapshotStore::new())
        .dead_letter_store(InMemoryDeadLetterStore::new())
        .retry_tracker(InMemoryRetryTracker::new())
        .snapshot_state_provider(EventPayloadSnapshotProvider)
        .outbox_store(outbox_store.clone())
        .outbox_publisher(outbox_publisher)
        .projection_checkpoint_store(InMemoryProjectionStore::new())
        .publisher(InMemoryPublisher::new())
        .command_store(InMemoryCommandStore::new())
        .build()
        .expect("service should build");

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (notify_tx, notify_rx) = canon_core::new_outbox_notify_channel(16);
    let (es_rx, proj_rx, pub_rx) = make_receivers(&outbound_queue);

    let service_handle = tokio::spawn(async move {
        service
            .start(shutdown_rx, Some(notify_rx), es_rx, proj_rx, pub_rx)
            .await
    });

    // Push 3 events at intervals (different aggregates to avoid version conflicts).
    let ids: Vec<AggregateId> = (0..3).map(|_| AggregateId::new()).collect();
    for (i, id) in ids.iter().enumerate() {
        outbox_store
            .insert(make_event(id, 1))
            .expect("insert should succeed");
        let _ = notify_tx.send(()).await;

        // Small interval between events.
        if i < 2 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Wait for processing.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Assert: all 3 events are in the event store.
    for id in &ids {
        let events = event_store_clone.load(id).expect("load should succeed");
        assert_eq!(
            events.len(),
            1,
            "each aggregate should have 1 event in the event store"
        );
    }

    // Shutdown.
    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(2), service_handle)
        .await
        .expect("service should stop within 2s")
        .expect("service task should not panic");
}

// ── Test 4: consumer run loop survives processing errors ────────────────────

#[tokio::test]
async fn test_consumer_run_loop_survives_errors() {
    // Push a valid event, then a duplicate (version conflict), then another
    // valid event. The consumer should process 2 events total and keep running.

    let outbound_queue = InMemoryOutboundQueue::new();
    let event_store = InMemoryEventStore::new();
    let event_store_clone = event_store.clone();

    let outbox_publisher = InMemoryOutboxPublisher::new(outbound_queue.clone());
    let outbox_store = InMemoryOutboxStore::new();

    let service = ServiceBuilder::new("error-resilience-test")
        .event_store(event_store)
        .snapshot_store(InMemorySnapshotStore::new())
        .dead_letter_store(InMemoryDeadLetterStore::new())
        .retry_tracker(InMemoryRetryTracker::new())
        .snapshot_state_provider(EventPayloadSnapshotProvider)
        .outbox_store(outbox_store.clone())
        .outbox_publisher(outbox_publisher)
        .projection_checkpoint_store(InMemoryProjectionStore::new())
        .publisher(InMemoryPublisher::new())
        .command_store(InMemoryCommandStore::new())
        .build()
        .expect("service should build");

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (notify_tx, notify_rx) = canon_core::new_outbox_notify_channel(16);
    let (es_rx, proj_rx, pub_rx) = make_receivers(&outbound_queue);

    let service_handle = tokio::spawn(async move {
        service
            .start(shutdown_rx, Some(notify_rx), es_rx, proj_rx, pub_rx)
            .await
    });

    let aggregate_id = AggregateId::new();

    // Event 1: valid, version 1.
    outbox_store
        .insert(make_event(&aggregate_id, 1))
        .expect("insert should succeed");
    let _ = notify_tx.send(()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Event 2: duplicate version 1 (same aggregate) — will cause a version conflict.
    outbox_store
        .insert(make_event(&aggregate_id, 1))
        .expect("insert should succeed");
    let _ = notify_tx.send(()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Event 3: valid, version 2 — should still be processed despite the prior error.
    outbox_store
        .insert(make_event(&aggregate_id, 2))
        .expect("insert should succeed");
    let _ = notify_tx.send(()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Assert: 2 events in the event store (version 1 and 2), not 3.
    let events = event_store_clone
        .load(&aggregate_id)
        .expect("load should succeed");
    assert_eq!(
        events.len(),
        2,
        "event store should have 2 events (error event skipped), found {}",
        events.len()
    );

    // The service should still be alive (not crashed).
    assert!(
        !service_handle.is_finished(),
        "service should still be running after error"
    );

    // Shutdown.
    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(2), service_handle)
        .await
        .expect("service should stop within 2s")
        .expect("service task should not panic");
}
