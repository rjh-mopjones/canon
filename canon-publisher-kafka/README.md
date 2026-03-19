# canon-publisher-kafka

Kafka-backed implementation of the [`EventPublisher`](../canon-publisher) port for Canon.

Forwards committed events from `canon.{service}.outbound` to the external topic
`canon.{service}.events` for consumption by other services' adaptors.

## Position in the pipeline

```
canon-outbound-queue-kafka
      ├──▶ event-store consumer  →  Cassandra
      ├──▶ projection consumer   →  YugabyteDB
      └──▶ publisher consumer    →  canon.{service}.events  ←── this crate
```

## Usage

```rust
use canon_publisher_kafka::KafkaPublisher;

let publisher = KafkaPublisher::new(
    &std::env::var("KAFKA_BROKERS")?,
    "navigation",
)?;
```

## Environment

| Variable        | Description                       |
|-----------------|-----------------------------------|
| `KAFKA_BROKERS` | Comma-separated Kafka broker list |

## Dependencies

- [`canon-publisher`](../canon-publisher) — `EventPublisher` trait
- [`canon-outbound-queue`](../canon-outbound-queue) — source queue (injected)
- [`canon-core`](../canon-core) — `EventEnvelope`, `AggregateId`
