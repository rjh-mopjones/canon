/// Domain errors for the station service.
#[derive(Debug, thiserror::Error)]
pub enum StationError {
    #[error("station already registered")]
    AlreadyRegistered,

    #[error("station not registered")]
    NotRegistered,

    #[error("station name must not be empty")]
    EmptyName,

    #[error("capacity must be greater than zero")]
    InvalidCapacity,

    #[error("cargo weight must be greater than zero")]
    InvalidWeight,

    #[error("ship {ship_id} is already docked at this station")]
    ShipAlreadyDocked { ship_id: uuid::Uuid },

    #[error("stock level is within normal range")]
    StockLevelNormal,

    #[error("station is already offline")]
    AlreadyOffline,

    #[error("station is offline")]
    StationOffline,

    #[error("serialization error: {0}")]
    Serialization(String),
}
