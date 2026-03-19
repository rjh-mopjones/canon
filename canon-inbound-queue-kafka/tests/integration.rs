use bytes::Bytes;
use canon_core::{AggregateId, CommandEnvelope, IncomingMessage};
use canon_inbound_queue_kafka::KafkaInboundQueue;
use canon_queue::InboundQueue;
use chrono::Utc;
use uuid::Uuid;

fn brokers() -> String {
    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn make_command(aggregate_id: &AggregateId) -> IncomingMessage {
    IncomingMessage::Command(CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"{}"),
        command_version: 1,
    })
}

#[tokio::test]
#[ignore = "requires running Kafka broker"]
async fn test_publish_and_consume_roundtrip() {
    let group = format!("test-roundtrip-{}", Uuid::new_v4());
    let service = format!("roundtrip-{}", Uuid::new_v4().simple());

    let queue = KafkaInboundQueue::new(&brokers(), &service, &group)
        .await
        .expect("failed to create queue");

    let agg_id = AggregateId::new();
    let msg = make_command(&agg_id);
    let original_id = msg.message_id();

    queue.publish(vec![msg], &agg_id).await.expect("publish failed");

    let mut received = None;
    for _ in 0..50 {
        if let Some(batch) = queue.receive().await.expect("receive failed") {
            received = Some(batch);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let batch = received.expect("did not receive message within timeout");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].message_id(), original_id);
    queue.commit().await.expect("commit failed");
}

#[tokio::test]
#[ignore = "requires running Kafka broker"]
async fn test_partition_key_is_aggregate_id() {
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::Message;

    let group = format!("test-partition-{}", Uuid::new_v4());
    let service = format!("partition-{}", Uuid::new_v4().simple());

    let queue = KafkaInboundQueue::new(&brokers(), &service, &group)
        .await
        .expect("failed to create queue");

    let agg_id = AggregateId::new();
    let msg = make_command(&agg_id);

    queue.publish(vec![msg], &agg_id).await.expect("publish failed");

    let raw_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers())
        .set("group.id", format!("raw-{}", Uuid::new_v4()))
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("raw consumer");

    raw_consumer.subscribe(&[queue.topic()]).expect("subscribe");

    let mut found_key = None;
    for _ in 0..50 {
        match tokio::time::timeout(std::time::Duration::from_millis(200), raw_consumer.recv()).await
        {
            Ok(Ok(m)) => {
                if let Some(key) = m.key() {
                    found_key = Some(String::from_utf8_lossy(key).to_string());
                }
                break;
            }
            _ => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }

    let key = found_key.expect("did not find message key");
    assert_eq!(key, agg_id.as_uuid().to_string());
}

#[tokio::test]
#[ignore = "requires running Kafka broker"]
async fn test_offset_not_committed_on_handler_failure() {
    let group = format!("test-nocommit-{}", Uuid::new_v4());
    let service = format!("nocommit-{}", Uuid::new_v4().simple());

    let queue = KafkaInboundQueue::new(&brokers(), &service, &group)
        .await
        .expect("failed to create queue");

    let agg_id = AggregateId::new();
    let msg = make_command(&agg_id);
    let original_id = msg.message_id();

    queue.publish(vec![msg], &agg_id).await.expect("publish failed");

    let mut received = None;
    for _ in 0..50 {
        if let Some(batch) = queue.receive().await.expect("receive failed") {
            received = Some(batch);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(received.is_some(), "should have received message");

    drop(queue);

    let queue2 = KafkaInboundQueue::new(&brokers(), &service, &group)
        .await
        .expect("failed to create second queue");

    let mut re_received = None;
    for _ in 0..50 {
        if let Some(batch) = queue2.receive().await.expect("receive failed") {
            re_received = Some(batch);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let batch = re_received.expect("message should be re-delivered after no commit");
    assert_eq!(batch[0].message_id(), original_id);
}
