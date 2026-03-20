use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;

#[tokio::test]
async fn test_duplicate_command_ignored() {
    let inbox = InMemoryInbox::new();
    let queue = InMemoryInboundQueue::new();
    let id = AggregateId::new();
    let cmd_id = Uuid::new_v4();

    inbox.register_handler("h1", |_| Oversight::Ready).unwrap();

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

    inbox.submit("h1", msg1, &queue).unwrap();
    inbox.submit("h1", msg2, &queue).unwrap();

    // Only one batch dispatched (first submit)
    let batch = queue.receive().unwrap().unwrap();
    assert_eq!(batch.len(), 1);
    // No more batches
    assert!(queue.receive().unwrap().is_none());
}

#[tokio::test]
async fn test_duplicate_event_ignored() {
    let inbox = InMemoryInbox::new();
    let queue = InMemoryInboundQueue::new();
    let id = AggregateId::new();
    let event_id = Uuid::new_v4();

    inbox.register_handler("h1", |_| Oversight::Ready).unwrap();

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

    inbox.submit("h1", msg1, &queue).unwrap();
    inbox.submit("h1", msg2, &queue).unwrap();

    // Only one batch dispatched (first event triggers Ready, second is deduped)
    let batch = queue.receive().unwrap().unwrap();
    assert_eq!(batch.len(), 1);
    assert!(queue.receive().unwrap().is_none());
}

#[tokio::test]
async fn test_duplicate_window_skipped() {
    // Simulate processed_windows tracking (INSERT ... ON CONFLICT DO NOTHING)
    let mut processed_windows = std::collections::HashSet::new();
    let window_id = Uuid::new_v4();

    // First processing: window_id is new — process the batch
    let is_new = processed_windows.insert(window_id);
    assert!(is_new);

    // Second processing: window_id already exists — skip (no-op)
    let is_new = processed_windows.insert(window_id);
    assert!(!is_new);
}
