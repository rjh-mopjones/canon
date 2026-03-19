use uuid::Uuid;

use canon_core::EventHandler;
use canon_test::domain::*;

#[tokio::test]
async fn test_handler_produces_command() {
    let handler = ProducingHandler;
    let events = vec![OrderPlaced {
        order_id: Uuid::new_v4(),
    }];

    let result = handler.handle(events).await.unwrap();

    assert!(result.is_some());
}

#[tokio::test]
async fn test_handler_produces_nothing() {
    let handler = SilentHandler;
    let events = vec![OrderPlaced {
        order_id: Uuid::new_v4(),
    }];

    let result = handler.handle(events).await.unwrap();

    assert!(result.is_none());
}

#[tokio::test]
async fn test_fan_out_multiple_handlers() {
    let handler_a = ProducingHandler;
    let handler_b = SilentHandler;

    let events = vec![OrderPlaced {
        order_id: Uuid::new_v4(),
    }];

    // Both handlers receive the same events (fan-out)
    let result_a = handler_a.handle(events.clone()).await.unwrap();
    let result_b = handler_b.handle(events).await.unwrap();

    // Both called, each with its own result
    assert!(result_a.is_some());
    assert!(result_b.is_none());
}
