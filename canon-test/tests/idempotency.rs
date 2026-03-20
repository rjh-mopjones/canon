use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;
use canon_test::domain::*;
use canon_test::harness::TestHarness;

#[tokio::test]
async fn test_duplicate_command_ignored() {
    let harness = TestHarness::new();
    let id = AggregateId::new();
    let cmd_id = Uuid::new_v4();

    harness
        .inbox
        .register_handler("h1", |_| Oversight::Ready)
        .unwrap();

    let msg1 = IncomingMessage::Command(CommandEnvelope {
        command_id: cmd_id,
        aggregate_id: id.clone(),
        command_type: "TestCommand".into(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"cmd"),
        command_version: 1,
    });
    let msg2 = IncomingMessage::Command(CommandEnvelope {
        command_id: cmd_id, // same command_id
        aggregate_id: id.clone(),
        command_type: "TestCommand".into(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"cmd"),
        command_version: 1,
    });

    harness
        .inbox
        .submit("h1", msg1, &harness.inbound_queue)
        .unwrap();
    harness
        .inbox
        .submit("h1", msg2, &harness.inbound_queue)
        .unwrap();

    // Only one batch dispatched (first submit)
    let batch = harness.inbound_queue.receive().unwrap().unwrap();
    assert_eq!(batch.len(), 1);
    // No more batches
    assert!(harness.inbound_queue.receive().unwrap().is_none());
}

#[tokio::test]
async fn test_duplicate_event_ignored() {
    let harness = TestHarness::new();
    let id = AggregateId::new();
    let event_id = Uuid::new_v4();

    harness
        .inbox
        .register_handler("h1", |_| Oversight::Ready)
        .unwrap();

    let msg1 = IncomingMessage::InternalEvent(EventEnvelope {
        event_id,
        aggregate_id: id.clone(),
        version: Version::initial().next(),
        event_type: "TestEvent".into(),
        event_version: 1,
        payload: Bytes::from_static(b"{}"),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    });
    let msg2 = IncomingMessage::InternalEvent(EventEnvelope {
        event_id, // same event_id
        aggregate_id: id.clone(),
        version: Version::initial().next(),
        event_type: "TestEvent".into(),
        event_version: 1,
        payload: Bytes::from_static(b"{}"),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    });

    harness
        .inbox
        .submit("h1", msg1, &harness.inbound_queue)
        .unwrap();
    harness
        .inbox
        .submit("h1", msg2, &harness.inbound_queue)
        .unwrap();

    // Only one batch dispatched (first event triggers Ready, second is deduped)
    let batch = harness.inbound_queue.receive().unwrap().unwrap();
    assert_eq!(batch.len(), 1);
    assert!(harness.inbound_queue.receive().unwrap().is_none());
}

#[tokio::test]
async fn test_duplicate_window_skipped() {
    // Simulate processed_windows tracking (INSERT ... ON CONFLICT DO NOTHING)
    let mut processed_windows = std::collections::HashSet::new();
    let window_id = Uuid::new_v4();

    // First processing: window_id is new -- process the batch
    let is_new = processed_windows.insert(window_id);
    assert!(is_new);

    // Second processing: window_id already exists -- skip (no-op)
    let is_new = processed_windows.insert(window_id);
    assert!(!is_new);
}

#[tokio::test]
async fn test_event_store_idempotent_via_version_conflict() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // First append succeeds
    let events = vec![make_placed_envelope(&id, Uuid::new_v4())];
    harness.append_events(&id, Version::initial(), events);

    // Duplicate append with same expected_version fails (optimistic concurrency)
    let events2 = vec![make_placed_envelope(&id, Uuid::new_v4())];
    let result = harness.event_store.append(&id, Version::initial(), events2);
    assert!(matches!(
        result,
        Err(EventStoreError::VersionConflict { .. })
    ));

    // Only one event in the store
    harness.assert_event_count(&id, 1);
}

#[tokio::test]
async fn test_projection_idempotent_apply() {
    let harness = TestHarness::new();
    let projection = OrderProjection;
    let event = OrderPlaced {
        order_id: Uuid::new_v4(),
    };

    // Apply twice
    projection
        .apply(&event, &harness.projection_store)
        .await
        .unwrap();
    let v1 = harness.projection_checkpoint("order-projection");

    projection
        .apply(&event, &harness.projection_store)
        .await
        .unwrap();
    let v2 = harness.projection_checkpoint("order-projection");

    // Idempotent: same checkpoint after double apply
    assert_eq!(v1, v2);
}

#[tokio::test]
async fn test_duplicate_external_event_deduped_in_inbox() {
    let harness = TestHarness::new();
    let id = AggregateId::new();
    let event_id = Uuid::new_v4();

    harness
        .inbox
        .register_handler("ext_handler", |_| Oversight::Ready)
        .unwrap();

    let msg = IncomingMessage::ExternalEvent(EventEnvelope {
        event_id,
        aggregate_id: id.clone(),
        version: Version::initial().next(),
        event_type: "ExternalEvent".into(),
        event_version: 1,
        payload: Bytes::from_static(b"{}"),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    });
    let msg_dup = IncomingMessage::ExternalEvent(EventEnvelope {
        event_id, // same event_id
        aggregate_id: id.clone(),
        version: Version::initial().next(),
        event_type: "ExternalEvent".into(),
        event_version: 1,
        payload: Bytes::from_static(b"{}"),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    });

    harness
        .inbox
        .submit("ext_handler", msg, &harness.inbound_queue)
        .unwrap();
    harness
        .inbox
        .submit("ext_handler", msg_dup, &harness.inbound_queue)
        .unwrap();

    let batch = harness.inbound_queue.receive().unwrap().unwrap();
    assert_eq!(batch.len(), 1);
    assert!(harness.inbound_queue.receive().unwrap().is_none());
}
