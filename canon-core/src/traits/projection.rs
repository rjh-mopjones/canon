use async_trait::async_trait;
use futures::Stream;

/// Builds and maintains a read model from the event stream.
/// apply() must be idempotent — calling it twice with the same event is safe.
/// Projections produce no commands.
#[async_trait]
pub trait Projection: Send + Sync + 'static {
    type Event: Send + Sync;
    type Store: ProjectionStore;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn apply(&self, event: &Self::Event, store: &Self::Store) -> Result<(), Self::Error>;

    /// Called on startup when the checkpoint is stale. Replays full history.
    async fn rebuild(
        &self,
        events: impl Stream<Item = Self::Event> + Send,
        store: &Self::Store,
    ) -> Result<(), Self::Error>;

    /// Unique key for checkpoint tracking in the projection store.
    fn projection_id(&self) -> &str;
}

/// Marker trait. Implemented by canon-projection-store-yugabyte and InMemoryProjectionStore.
pub trait ProjectionStore: Send + Sync + 'static {}
