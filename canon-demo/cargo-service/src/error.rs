/// Errors specific to the cargo-service domain logic.
#[derive(Debug, thiserror::Error)]
pub enum CargoError {
    #[error("manifest is not in Open status")]
    ManifestNotOpen,

    #[error("manifest is not in Unloading status")]
    ManifestNotUnloading,

    #[error("manifest has already been created")]
    ManifestAlreadyCreated,

    #[error("manifest is already closed")]
    ManifestAlreadyClosed,

    #[error("cargo item not found: {item_id}")]
    ItemNotFound { item_id: uuid::Uuid },

    #[error("cargo item already unloaded: {item_id}")]
    ItemAlreadyUnloaded { item_id: uuid::Uuid },

    #[error("serialization error: {0}")]
    Serialization(String),
}

/// Error type for the UnloadingHandler event handler.
#[derive(Debug, thiserror::Error)]
pub enum UnloadingError {
    #[error("no ManifestCreated event found in batch")]
    NoManifestCreated,

    #[error("serialization error: {0}")]
    Serialization(String),
}

/// Error type for the ManifestProjection.
#[derive(Debug, thiserror::Error)]
pub enum ProjectionError {
    #[error("projection store error: {0}")]
    StoreError(String),
}

/// Error type for the upcast module.
#[derive(Debug, thiserror::Error)]
pub enum UpcastError {
    #[error("deserialization error: {0}")]
    Deserialization(String),

    #[error("unknown event type/version: {event_type} v{event_version}")]
    UnknownEventVersion {
        event_type: String,
        event_version: u32,
    },
}
