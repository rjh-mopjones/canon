# canon-publisher

Trait (port) crate for the Canon event publisher.

## Trait

```rust
#[async_trait]
pub trait EventPublisher: Send + Sync + 'static {
    async fn publish(&self, envelope: &EventEnvelope, topic: &str) -> Result<(), PublisherError>;
}
```

## Error types

- `PublisherError::Publish` — wraps any underlying publish failure

## Re-exports

- `EventEnvelope`, `AggregateId` from `canon-core`

## Implementations

| Crate | Backend |
|---|---|
| `canon-publisher-kafka` | Apache Kafka via rdkafka |

## Dependencies

- `canon-core`
- `async-trait`
- `thiserror`
