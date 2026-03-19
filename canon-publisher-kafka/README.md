# canon-publisher-kafka

Kafka-backed implementation of the `EventPublisher` trait from `canon-publisher`.

## Overview

Publishes confirmed events to external Kafka topics for cross-service consumption. Each service publishes to `canon.{service_name}.events`, partitioned by `aggregate_id` to preserve per-aggregate ordering.

## Usage

```rust
use canon_publisher_kafka::KafkaPublisher;
use canon_publisher::EventPublisher;

let publisher = KafkaPublisher::new("kafka:9092", "fleet")?;
// or from KAFKA_BROKERS env var:
let publisher = KafkaPublisher::from_env("fleet")?;

// Topic is canon.fleet.events
publisher.publish(&envelope, &publisher.topic()).await?;
```

## Configuration

| Env var | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker addresses |

## Idempotency

Tracks published `event_id`s in memory to skip duplicate publishes. Combined with Kafka's at-least-once delivery and downstream idempotent consumers, this provides end-to-end exactly-once semantics.

## Dependencies

- `canon-core`, `canon-publisher`
- `rdkafka` (librdkafka via cmake-build)
- `async-trait`, `thiserror`, `serde`, `serde_json`, `tokio`, `tracing`
