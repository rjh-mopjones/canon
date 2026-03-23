use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::kafka::apache::{self, Kafka};
use uuid::Uuid;

use canon_core::{AggregateId, EventEnvelope, Version};
use canon_outbound_queue::OutboundQueue;

use crate::{KafkaOutboundQueue, KafkaOutboundQueueConfig};

async fn setup_kafka_container() -> (ContainerAsync<Kafka>, String) {
    let container = Kafka::default()
        .start()
        .await
        .expect("Failed to start Kafka container");

    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("Failed to get Kafka host port");

    let broker = format!("127.0.0.1:{}", port);
    (container, broker)
}

fn test_config(brokers: &str, group_id: &str) -> KafkaOutboundQueueConfig {
    let topic = format!("canon.test.outbound.{}", Uuid::new_v4());
    KafkaOutboundQueueConfig {
        brokers: brokers.to_string(),
        topic,
        group_id: group_id.to_string(),
        session_timeout_ms: 6000,
        enable_auto_commit: false,
        receive_timeout_ms: 100,
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

/// Try to receive with retries, allowing time for Kafka to be ready.
async fn receive_with_retry(queue: &KafkaOutboundQueue, retries: u32) -> Option<EventEnvelope> {
    for _ in 0..retries {
        if let Ok(Some(env)) = queue.receive().await {
            return Some(env);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread")]
async fn test_publish_and_consume_roundtrip() {
    let (_container, broker) = setup_kafka_container().await;

    let config = test_config(&broker, "test-roundtrip");
    let queue = KafkaOutboundQueue::new(&config)
        .await
        .expect("failed to create outbound queue");

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

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_consumers_same_topic() {
    let (_container, broker) = setup_kafka_container().await;

    let base_config = test_config(&broker, "group-a");
    let topic = base_config.topic.clone();

    // Two consumers on the same topic -- both should receive the event
    // since rskafka has no consumer groups (each has independent in-memory offset)
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
        .await
        .expect("failed to create consumer A queue");
    let queue_b = KafkaOutboundQueue::new(&config_b)
        .await
        .expect("failed to create consumer B queue");

    let agg_id = AggregateId::new();
    let event = make_event(&agg_id);
    let event_id = event.event_id;

    queue_a.publish(event).await.expect("publish failed");

    // Both consumers should receive the same event independently
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

#[tokio::test(flavor = "multi_thread")]
async fn test_restart_replays_from_zero() {
    let (_container, broker) = setup_kafka_container().await;

    let config = test_config(&broker, &format!("test-replay-{}", Uuid::new_v4()));

    let queue = KafkaOutboundQueue::new(&config)
        .await
        .expect("failed to create outbound queue");

    let agg_id = AggregateId::new();
    let event = make_event(&agg_id);
    let event_id = event.event_id;

    queue.publish(event).await.expect("publish failed");

    // Receive but do NOT worry about commit -- rskafka commit is no-op
    let received = receive_with_retry(&queue, 50)
        .await
        .expect("did not receive published event");
    assert_eq!(received.event_id, event_id);

    // Drop the queue (simulating a restart)
    drop(queue);

    // Create a new consumer with the same config -- should re-receive from offset 0
    let queue2 = KafkaOutboundQueue::new(&config)
        .await
        .expect("failed to create second queue");

    let re_received = receive_with_retry(&queue2, 50)
        .await
        .expect("did not re-receive event after restart");
    assert_eq!(re_received.event_id, event_id);
}
