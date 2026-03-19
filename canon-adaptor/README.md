# canon-adaptor

Part of the [Canon](https://github.com/rjh-mopjones/canon) event sourcing framework.

## Overview

`canon-adaptor` defines the `EventAdaptor` trait — the inbound port through which external events from other services enter a Canon service. It abstracts over the transport used to subscribe to event topics (e.g. Kafka). The infrastructure implementation lives in `canon-adaptor-kafka`.

## Trait

```rust
#[async_trait]
pub trait EventAdaptor: Send + Sync + 'static {
    /// Subscribe to a topic. Offsets committed only after successful processing.
    async fn subscribe(
        &self,
        topic: &str,
    ) -> Result<
        Box<dyn Stream<Item = Result<EventEnvelope, AdaptorError>> + Send + Unpin>,
        AdaptorError,
    >;
}
```

## Error types

```rust
#[derive(Debug, thiserror::Error)]
pub enum AdaptorError {
    #[error("adaptor error: {0}")]
    Adaptor(#[from] Box<dyn std::error::Error + Send + Sync>),
}
```

## Re-exports

- `EventEnvelope` — re-exported from `canon-core`

## Usage

```rust
use canon_adaptor::{EventAdaptor, AdaptorError, EventEnvelope};

async fn consume(adaptor: &dyn EventAdaptor) -> Result<(), AdaptorError> {
    let stream = adaptor.subscribe("canon.fleet.events").await?;
    // process events from stream...
    Ok(())
}
```

## Dependencies

```toml
[dependencies]
canon-core = { path = "../canon-core" }
async-trait = { workspace = true }
thiserror = { workspace = true }
futures = { workspace = true }
```
