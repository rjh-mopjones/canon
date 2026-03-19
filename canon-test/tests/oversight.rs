use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;

#[tokio::test]
async fn test_oversight_not_ready_accumulation() {
    let inbox = InMemoryInbox::new();
    let queue = InMemoryInboundQueue::new();
    let id = AggregateId::new();

    inbox
        .register_handler("h1", |_| Oversight::NotReady)
        .unwrap();

    // Submit 3 messages — all accumulate, nothing dispatched
    for _ in 0..3 {
        let msg = IncomingMessage::Command(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: id.clone(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"{}"),
            command_version: 1,
        });
        inbox.submit("h1", msg, &queue).unwrap();
    }

    assert!(queue.receive().unwrap().is_none());
}

#[tokio::test]
async fn test_oversight_discard() {
    let inbox = InMemoryInbox::new();
    let queue = InMemoryInboundQueue::new();
    let id = AggregateId::new();

    inbox
        .register_handler("h1", |_| Oversight::Discard)
        .unwrap();

    let msg = IncomingMessage::Command(CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: id.clone(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"{}"),
        command_version: 1,
    });
    inbox.submit("h1", msg, &queue).unwrap();

    // Window cleared, nothing dispatched
    assert!(queue.receive().unwrap().is_none());
}

#[tokio::test]
async fn test_oversight_ready_dispatch() {
    let inbox = InMemoryInbox::new();
    let queue = InMemoryInboundQueue::new();
    let id = AggregateId::new();

    inbox
        .register_handler("h1", |_| Oversight::Ready)
        .unwrap();

    let msg = IncomingMessage::Command(CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: id.clone(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"{}"),
        command_version: 1,
    });
    inbox.submit("h1", msg, &queue).unwrap();

    // Batch dispatched to inbound queue
    let batch = queue.receive().unwrap().unwrap();
    assert_eq!(batch.len(), 1);
}
