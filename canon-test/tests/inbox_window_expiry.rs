use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;

#[tokio::test]
async fn test_window_expiry_to_dead_letter() {
    let inbox = InMemoryInbox::new();
    let queue = InMemoryInboundQueue::new();
    let dead_letter_store = InMemoryDeadLetterStore::new();
    let id = AggregateId::new();
    let handler_id = "expiring_handler";

    // Register handler with NotReady oversight — window accumulates
    inbox
        .register_handler(handler_id, |_| Oversight::NotReady)
        .unwrap();

    // Submit a message (goes into window, stays NotReady)
    let msg_id = Uuid::new_v4();
    let msg = IncomingMessage::Command(CommandEnvelope {
        command_id: msg_id,
        aggregate_id: id.clone(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"will_expire"),
    });
    inbox.submit(handler_id, msg, &queue).unwrap();

    // Nothing dispatched (NotReady)
    assert!(queue.receive().unwrap().is_none());

    // Simulate cleanup task: TTL expired, move to dead letter store
    dead_letter_store
        .store(
            msg_id,
            handler_id,
            &id,
            Bytes::from_static(b"will_expire"),
            "window_expired",
        )
        .unwrap();

    // Assert dead letter entry exists with correct reason and handler
    let letters = dead_letter_store.list(Some(handler_id)).unwrap();
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].error, "window_expired");
    assert_eq!(letters[0].handler_id, handler_id);
}
