use async_trait::async_trait;
use futures::Stream;

pub use canon_core::EventEnvelope;

#[derive(Debug, thiserror::Error)]
pub enum AdaptorError {
    #[error("adaptor error: {0}")]
    Adaptor(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// TODO: Add a `commit()` method to `EventAdaptor` to support offset commit
/// after confirmed downstream processing for stream-based consumers.
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
