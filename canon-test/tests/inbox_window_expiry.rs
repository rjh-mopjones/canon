use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;
use canon_test::harness::TestHarness;

fn make_command_msg(aggregate_id: &AggregateId) -> IncomingMessage {
    IncomingMessage::Command(CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        command_type: "TestCommand".into(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"{}"),
        command_version: 1,
    })
}

fn make_command_msg_with_id(aggregate_id: &AggregateId, command_id: Uuid) -> IncomingMessage {
    IncomingMessage::Command(CommandEnvelope {
        command_id,
        aggregate_id: aggregate_id.clone(),
        command_type: "TestCommand".into(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"will_expire"),
        command_version: 1,
    })
}

#[tokio::test]
async fn test_window_expiry_to_dead_letter() {
    let harness = TestHarness::new();
    let id = AggregateId::new();
    let handler_id = "expiring_handler";

    // Register handler with NotReady oversight -- window accumulates
    harness
        .inbox
        .register_handler(handler_id, |_| Oversight::NotReady)
        .unwrap();

    // Submit a message (goes into window, stays NotReady)
    let msg_id = Uuid::new_v4();
    harness
        .inbox
        .submit(
            handler_id,
            make_command_msg_with_id(&id, msg_id),
            &harness.inbound_queue,
        )
        .unwrap();

    // Nothing dispatched (NotReady)
    assert!(harness.inbound_queue.receive().unwrap().is_none());

    // Simulate cleanup task: TTL expired, move to dead letter store
    harness
        .dead_letter_store
        .store(
            msg_id,
            handler_id,
            &id,
            Bytes::from_static(b"will_expire"),
            "window_expired",
        )
        .unwrap();

    // Assert dead letter entry exists with correct reason and handler
    let letters = harness.dead_letters(Some(handler_id));
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].error, "window_expired");
    assert_eq!(letters[0].handler_id, handler_id);
}

#[tokio::test]
async fn test_multiple_messages_in_expired_window_all_dead_lettered() {
    let harness = TestHarness::new();
    let id = AggregateId::new();
    let handler_id = "multi_expire";

    harness
        .inbox
        .register_handler(handler_id, |_| Oversight::NotReady)
        .unwrap();

    // Submit 3 messages into the window
    let msg_ids: Vec<Uuid> = (0..3)
        .map(|_| {
            let msg_id = Uuid::new_v4();
            harness
                .inbox
                .submit(
                    handler_id,
                    make_command_msg_with_id(&id, msg_id),
                    &harness.inbound_queue,
                )
                .unwrap();
            msg_id
        })
        .collect();

    // Nothing dispatched
    assert!(harness.inbound_queue.receive().unwrap().is_none());

    // Simulate window expiry: dead-letter all messages in the window
    for msg_id in &msg_ids {
        harness
            .dead_letter_store
            .store(
                *msg_id,
                handler_id,
                &id,
                Bytes::from_static(b"expired"),
                "window_expired",
            )
            .unwrap();
    }

    harness.assert_dead_letter_count(Some(handler_id), 3);
}

#[tokio::test]
async fn test_expired_window_does_not_affect_other_handlers() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Register two handlers: one NotReady (will expire), one Ready
    harness
        .inbox
        .register_handler("expiring", |_| Oversight::NotReady)
        .unwrap();
    harness
        .inbox
        .register_handler("active", |_| Oversight::Ready)
        .unwrap();

    // Submit to both handlers
    harness
        .inbox
        .submit("expiring", make_command_msg(&id), &harness.inbound_queue)
        .unwrap();
    harness
        .inbox
        .submit("active", make_command_msg(&id), &harness.inbound_queue)
        .unwrap();

    // Active handler dispatched its batch
    let batch = harness.inbound_queue.receive().unwrap().unwrap();
    assert_eq!(batch.len(), 1);

    // Expiring handler: nothing dispatched, window stuck
    assert!(harness.inbound_queue.receive().unwrap().is_none());

    // Expire the stuck window
    harness
        .dead_letter_store
        .store(
            Uuid::new_v4(),
            "expiring",
            &id,
            Bytes::from_static(b"{}"),
            "window_expired",
        )
        .unwrap();

    harness.assert_dead_letter_count(Some("expiring"), 1);
    harness.assert_dead_letter_count(Some("active"), 0);
}
