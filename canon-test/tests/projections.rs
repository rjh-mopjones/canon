use uuid::Uuid;

use canon_core::*;
use canon_test::domain::*;

#[tokio::test]
async fn test_projection_apply() {
    let store = InMemoryProjectionStore::new();
    let projection = OrderProjection;
    let event = OrderPlaced {
        order_id: Uuid::new_v4(),
    };

    projection.apply(&event, &store).await.unwrap();

    let checkpoint = store.get_checkpoint_sync("order-projection").unwrap();
    assert_eq!(checkpoint.as_u64(), 1);
}

#[tokio::test]
async fn test_projection_idempotent() {
    let store = InMemoryProjectionStore::new();
    let projection = OrderProjection;
    let event = OrderPlaced {
        order_id: Uuid::new_v4(),
    };

    projection.apply(&event, &store).await.unwrap();
    let after_first = store.get_checkpoint_sync("order-projection").unwrap();

    projection.apply(&event, &store).await.unwrap();
    let after_second = store.get_checkpoint_sync("order-projection").unwrap();

    assert_eq!(after_first, after_second);
}
