# canon-outbound-queue-kafka

Kafka-backed implementation of the [`OutboundQueue`](../canon-outbound-queue) port for Canon.

Fan-out bus for committed events. Three independent consumer groups — `event-store`,
`projection`, `publisher` — each receive every event independently.

## Usage

```rust
use canon_outbound_queue_kafka::KafkaOutboundQueue;

let queue = KafkaOutboundQueue::new(&std::env::var("KAFKA_BROKERS")?, "my-service").await?;
queue.publish(envelope).await?;
let stream = queue.subscribe("event-store").await?;
```

## Environment

| Variable        | Description                       |
|-----------------|-----------------------------------|
| `KAFKA_BROKERS` | Comma-separated Kafka broker list |

## Dependencies

- [`canon-outbound-queue`](../canon-outbound-queue) — `OutboundQueue` trait
- [`canon-core`](../canon-core) — `EventEnvelope`, `AggregateId`
