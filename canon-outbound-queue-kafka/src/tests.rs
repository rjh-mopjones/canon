use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::{AggregateId, EventEnvelope, Version};
use canon_outbound_queue::OutboundQueue;

use crate::{KafkaOutboundQueue, KafkaOutboundQueueConfig};

fn test_config(group_id: &str) -> KafkaOutboundQueueConfig {
    let brokers =
        std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let topic = format!("canon.test.outbound.{}", Uuid::new_v4());
    KafkaOutboundQueueConfig {
        brokers,
        topic,
        group_id: group_id.to_string(),
        session_timeout_ms: 6000,
        enable_auto_commit: false,
    }
}

fn make_event(aggregate_id: &AggregateId) -> EventEnvelope {
    EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        version: Version::initial().next(),
        event_type: "TestEvent".into(),
        event_version: 1,
        payload: Bytes::from_static(b"{}"),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    }
}

/// Try to receive with retries, allowing time for Kafka consumer group rebalancing.
async fn receive_with_retry(
    queue: &KafkaOutboundQueue,
    retries: u32,
) -> Option<EventEnvelope> {
    for _ in 0..retries {
        if let Ok(Some(env)) = queue.receive().await {
            return Some(env);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
}

#[tokio::test]
#[ignore] // Requires running Kafka broker
async fn test_publish_and_consume_roundtrip() {
    let config = test_config("test-roundtrip");
    let queue =
        KafkaOutboundQueue::new(&config).expect("failed to create outbound queue");

    let agg_id = AggregateId::new();
    let event = make_event(&agg_id);
    let event_id = event.event_id;

    queue.publish(event).await.expect("publish failed");

    let received = receive_with_retry(&queue, 50)
        .await
        .expect("did not receive published event");

    assert_eq!(received.event_id, event_id);
    assert_eq!(received.aggregate_id, agg_id);
    assert_eq!(received.event_type, "TestEvent");
    assert_eq!(received.event_version, 1);

    queue.commit().await.expect("commit failed");
}

#[tokio::test]
#[ignore] // Requires running Kafka broker
async fn test_multiple_consumer_groups_independent() {
    let base_config = test_config("group-a");
    let topic = base_config.topic.clone();

    // Two consumers with different group IDs on the same topic
    let config_a = KafkaOutboundQueueConfig {
        group_id: format!("group-a-{}", Uuid::new_v4()),
        ..base_config.clone()
    };
    let config_b = KafkaOutboundQueueConfig {
        group_id: format!("group-b-{}", Uuid::new_v4()),
        topic: topic.clone(),
        ..base_config
    };

    let queue_a = KafkaOutboundQueue::new(&config_a)
        .expect("failed to create consumer A queue");
    let queue_b =
        KafkaOutboundQueue::new(&config_b).expect("failed to create consumer B queue");

    let agg_id = AggregateId::new();
    let event = make_event(&agg_id);
    let event_id = event.event_id;

    queue_a.publish(event).await.expect("publish failed");

    // Both consumer groups should receive the same event independently
    let received_a = receive_with_retry(&queue_a, 50)
        .await
        .expect("consumer A did not receive event");
    let received_b = receive_with_retry(&queue_b, 50)
        .await
        .expect("consumer B did not receive event");

    assert_eq!(received_a.event_id, event_id);
    assert_eq!(received_b.event_id, event_id);

    queue_a.commit().await.expect("commit A failed");
    queue_b.commit().await.expect("commit B failed");
}

#[tokio::test]
#[ignore] // Requires running Kafka broker
async fn test_offset_not_committed_on_failure() {
    let config = test_config(&format!("test-no-commit-{}", Uuid::new_v4()));
    let queue =
        KafkaOutboundQueue::new(&config).expect("failed to create outbound queue");

    let agg_id = AggregateId::new();
    let event = make_event(&agg_id);
    let event_id = event.event_id;

    queue.publish(event).await.expect("publish failed");

    // Receive but do NOT commit
    let received = receive_with_retry(&queue, 50)
        .await
        .expect("did not receive published event");
    assert_eq!(received.event_id, event_id);

    // Drop the queue (simulating a failure / restart without commit)
    drop(queue);

    // Create a new consumer with the same group ID — should re-receive the message
    // because the offset was never committed.
    let queue2 =
        KafkaOutboundQueue::new(&config).expect("failed to create second queue");

    let re_received = receive_with_retry(&queue2, 50)
        .await
        .expect("did not re-receive event after restart without commit");
    assert_eq!(re_received.event_id, event_id);

    queue2.commit().await.expect("commit failed");
}
