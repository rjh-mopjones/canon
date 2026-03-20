# canon-inbound-queue-kafka

Kafka-backed implementation of the [`InboundQueue`](../canon-inbound-queue) port for Canon.

Delivery channel between the inbox and the dispatcher. Partitioned by `aggregate_id`
for strict per-aggregate ordering. Offsets committed only after successful dispatch.

## Usage

```rust
use canon_inbound_queue_kafka::KafkaInboundQueue;

let queue = KafkaInboundQueue::new(
    &std::env::var("KAFKA_BROKERS")?,
    "my-service",
    "my-consumer-group",
).await?;
```

## Environment

| Variable        | Description                       |
|-----------------|-----------------------------------|
| `KAFKA_BROKERS` | Comma-separated Kafka broker list |

## Dependencies

- [`canon-inbound-queue`](../canon-inbound-queue) -- `InboundQueue` trait
- [`canon-core`](../canon-core) -- `IncomingMessage`, `AggregateId`
