# canon-adaptor-kafka

Kafka-backed implementation of the [`EventAdaptor`](../canon-adaptor) port for Canon.

Anti-corruption layer at the service boundary. Consumes events from upstream services
and submits them to the local inbox as `IncomingMessage::ExternalEvent`, where they
participate in oversight windows alongside internal messages.

## Position in the pipeline

```
canon.{upstream}.events
      │
      ▼
canon-adaptor-kafka          ←── this crate
      │
      ▼
canon-inbox-yugabyte         (IncomingMessage::ExternalEvent)
      │
      ▼
Dispatcher → event handlers
```

## Consumer group naming

`"{local_service}-{handler_id}"` — scoped per handler, allowing multiple handlers to
independently consume the same upstream topic.

## Usage

```rust
use canon_adaptor_kafka::KafkaEventAdaptor;

let adaptor = KafkaEventAdaptor::new(
    &std::env::var("KAFKA_BROKERS")?,
    "cargo-service",
    inbox_port,
);

adaptor.consume_upstream("navigation", "unloading-handler").await?;
```

## Environment

| Variable        | Description                       |
|-----------------|-----------------------------------|
| `KAFKA_BROKERS` | Comma-separated Kafka broker list |

## Dependencies

- [`canon-adaptor`](../canon-adaptor) — `EventAdaptor` trait
- [`canon-inbox`](../canon-inbox) — `Inbox` trait (injected)
- [`canon-core`](../canon-core) — `EventEnvelope`, `IncomingMessage`, `AggregateId`
