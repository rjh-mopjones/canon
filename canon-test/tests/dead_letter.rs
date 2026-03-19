use bytes::Bytes;
use uuid::Uuid;

use canon_core::*;

#[tokio::test]
async fn test_dead_letter_after_max_retries() {
    let dead_letter_store = InMemoryDeadLetterStore::new();
    let id = AggregateId::new();
    let message_id = Uuid::new_v4();
    let handler_id = "test_handler";
    let max_retries = 3;

    // Simulate retry loop: after exhausting max retries, write to dead letter
    for attempt in 1..=max_retries {
        if attempt == max_retries {
            dead_letter_store
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

    let letters = dead_letter_store.list(Some(handler_id)).unwrap();
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].handler_id, handler_id);
    assert_eq!(letters[0].error, "max retries exceeded");
    assert_eq!(letters[0].message_id, message_id);
}
