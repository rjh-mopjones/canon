# canon-inbox

Part of the [Canon](https://github.com/rjh-mopjones/canon) event sourcing framework.

## Overview

`canon-inbox` defines the `Inbox` port — the trait that abstracts idempotent message intake with windowed accumulation and oversight-driven dispatch. It handles deduplication via a `handler_id` + `message_id` composite key, accumulates messages into windows keyed by `(handler_id, correlation_key)`, and evaluates oversight after each non-duplicate submission. The `correlation_key` is resolved from the handler's `correlate` fn, falling back to the envelope's `correlation_id`. Each unique `(handler_id, correlation_key)` pair is an independent window, enabling cross-aggregate windowing. The infrastructure implementation lives in `canon-inbox-yugabyte`.

## Trait

```rust
#[async_trait]
pub trait Inbox: Send + Sync + 'static {
    /// Register a handler at startup. Must be called before submit().
    async fn register_handler(&self, registration: HandlerRegistration) -> Result<(), InboxError>;

    /// Submit a message for a specific handler.
    /// Idempotent — same handler_id + message_id is a no-op.
    /// Runs oversight after each non-duplicate submission.
    async fn submit(
        &self,
        handler_id: &str,
        message_id: uuid::Uuid,
        message: IncomingMessage,
    ) -> Result<(), InboxError>;

    /// Attempt to mark a window as processed (consumer-side batch idempotency).
    /// Returns Ok(true) if the batch is new, Ok(false) if already processed.
    async fn try_mark_window_processed(
        &self,
        window_id: uuid::Uuid,
        handler_id: &str,
    ) -> Result<bool, InboxError>;
}
```

## Error types

```rust
#[derive(Debug, thiserror::Error)]
pub enum InboxError {
    #[error("inbox error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}
```

## Re-exports

The following types are re-exported from `canon-core`:

- `AggregateId`
- `CommandEnvelope`
- `EventEnvelope`
- `IncomingMessage`
- `Oversight`

## Usage

```rust
use canon_inbox::{Inbox, InboxError, HandlerRegistration, IncomingMessage};

async fn register(inbox: &impl Inbox) -> Result<(), InboxError> {
    inbox.register_handler(HandlerRegistration {
        handler_id: "my-handler".to_string(),
        event_types: vec!["ShipDeparted".to_string()],
    }).await
}
```

## Dependencies

```toml
[dependencies]
canon-core = { path = "../canon-core" }
```
