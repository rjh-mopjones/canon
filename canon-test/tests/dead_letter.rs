use bytes::Bytes;
use uuid::Uuid;

use canon_core::*;
use canon_test::harness::TestHarness;

#[tokio::test]
async fn test_dead_letter_after_max_retries() {
    let harness = TestHarness::new();
    let id = AggregateId::new();
    let message_id = Uuid::new_v4();
    let handler_id = "test_handler";
    let max_retries = 3;

    // Simulate retry loop: after exhausting max retries, write to dead letter
    for attempt in 1..=max_retries {
        if attempt == max_retries {
            harness
                .dead_letter_store
                .store(
                    message_id,
                    handler_id,
                    &id,
                    Bytes::from_static(b"failed_payload"),
                    "max retries exceeded",
                )
                .unwrap();
        }
    }

    let letters = harness.dead_letters(Some(handler_id));
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].handler_id, handler_id);
    assert_eq!(letters[0].error, "max retries exceeded");
    assert_eq!(letters[0].message_id, message_id);
}

#[tokio::test]
async fn test_dead_letter_requeue() {
    let harness = TestHarness::new();
    let id = AggregateId::new();
    let message_id = Uuid::new_v4();
    let handler_id = "requeue_handler";

    let dl_id = harness
        .dead_letter_store
        .store(
            message_id,
            handler_id,
            &id,
            Bytes::from_static(b"payload"),
            "transient error",
        )
        .unwrap();

    // Mark for requeue
    harness.dead_letter_store.requeue(dl_id).unwrap();

    let letters = harness.dead_letters(Some(handler_id));
    assert_eq!(letters.len(), 1);
    assert!(letters[0].requeue);
}

#[tokio::test]
async fn test_dead_letter_discard() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    let dl_id = harness
        .dead_letter_store
        .store(
            Uuid::new_v4(),
            "handler",
            &id,
            Bytes::from_static(b"bad"),
            "permanent error",
        )
        .unwrap();

    harness.dead_letter_store.discard(dl_id).unwrap();
    harness.assert_dead_letter_count(None, 0);
}

#[tokio::test]
async fn test_dead_letter_multiple_handlers_isolation() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Dead-letter entries for two different handlers
    harness
        .dead_letter_store
        .store(
            Uuid::new_v4(),
            "handler_a",
            &id,
            Bytes::from_static(b"{}"),
            "err_a",
        )
        .unwrap();
    harness
        .dead_letter_store
        .store(
            Uuid::new_v4(),
            "handler_b",
            &id,
            Bytes::from_static(b"{}"),
            "err_b",
        )
        .unwrap();

    harness.assert_dead_letter_count(None, 2);
    harness.assert_dead_letter_count(Some("handler_a"), 1);
    harness.assert_dead_letter_count(Some("handler_b"), 1);
}

#[tokio::test]
async fn test_dead_letter_retry_tracking_simulation() {
    let harness = TestHarness::new();
    let id = AggregateId::new();
    let message_id = Uuid::new_v4();
    let max_retries = 3u32;

    // Simulate retry counting (in production this is the retry_attempts table)
    let mut retry_count = 0u32;
    for _ in 0..5 {
        retry_count += 1;
        if retry_count >= max_retries {
            harness
                .dead_letter_store
                .store(
                    message_id,
                    "retry_handler",
                    &id,
                    Bytes::from_static(b"payload"),
                    &format!("failed after {} attempts", retry_count),
                )
                .unwrap();
            break;
        }
    }

    let letters = harness.dead_letters(Some("retry_handler"));
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].error, "failed after 3 attempts");
}
