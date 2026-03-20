use async_trait::async_trait;
use futures::Stream;

use crate::Version;

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

/// Error type for projection rebuild operations.
#[derive(Debug, thiserror::Error)]
pub enum ProjectionRebuildError {
    /// The projection is already rebuilding.
    #[error("projection '{projection_id}' is already rebuilding")]
    AlreadyRebuilding { projection_id: String },

    /// The projection is not currently rebuilding, so it cannot be completed.
    #[error("projection '{projection_id}' is not rebuilding")]
    NotRebuilding { projection_id: String },

    /// The requested rebuild_from version is ahead of the current checkpoint.
    #[error(
        "rebuild_from version {requested} is ahead of current checkpoint {current} for '{projection_id}'"
    )]
    VersionAhead {
        projection_id: String,
        requested: Version,
        current: Version,
    },

    /// Internal store error.
    #[error("store error: {0}")]
    Store(Box<dyn std::error::Error + Send + Sync>),
}

/// Manages the lifecycle of projection rebuilds.
///
/// The rebuild flow:
/// 1. `start_rebuild(projection_id, rebuild_from)` — sets `rebuilding = true` and resets
///    the checkpoint to the target version. While rebuilding, read endpoints must fall back
///    to read-through and never serve stale materialised views.
/// 2. The projection consumer resets its offset on the outbound queue topic to the target
///    checkpoint and replays events through the projection's `apply()`.
/// 3. `complete_rebuild(projection_id)` — sets `rebuilding = false`.
///
/// `is_rebuilding(projection_id)` can be polled at any time (e.g. by read endpoints).
#[async_trait]
pub trait ProjectionRebuildManager: Send + Sync + 'static {
    /// Begin a rebuild for the given projection.
    ///
    /// Sets `rebuilding = true` and resets the checkpoint to `rebuild_from`.
    /// If `rebuild_from` is `None`, resets to `Version::initial()` (full replay).
    ///
    /// Returns `Err(AlreadyRebuilding)` if a rebuild is already in progress.
    /// Returns `Err(VersionAhead)` if `rebuild_from` is greater than the current checkpoint.
    async fn start_rebuild(
        &self,
        projection_id: &str,
        rebuild_from: Option<Version>,
    ) -> Result<(), ProjectionRebuildError>;

    /// Check whether a projection is currently rebuilding.
    ///
    /// Read endpoints should call this and fall back to read-through when `true`.
    async fn is_rebuilding(&self, projection_id: &str) -> Result<bool, ProjectionRebuildError>;

    /// Mark a rebuild as complete.
    ///
    /// Sets `rebuilding = false`.
    /// Returns `Err(NotRebuilding)` if the projection is not currently rebuilding.
    async fn complete_rebuild(&self, projection_id: &str) -> Result<(), ProjectionRebuildError>;

    /// Get the current checkpoint version for a projection.
    ///
    /// During a rebuild, this returns the reset-to version (i.e. `rebuild_from`).
    async fn get_checkpoint(&self, projection_id: &str) -> Result<Version, ProjectionRebuildError>;
}
