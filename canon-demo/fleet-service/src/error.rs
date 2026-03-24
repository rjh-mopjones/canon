#[derive(Debug, thiserror::Error)]
pub enum FleetError {
    #[error("ship is not docked")]
    ShipNotDocked,

    #[error("already decommissioned")]
    AlreadyDecommissioned,

    #[error("ship is not in transit")]
    ShipNotInTransit,

    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}
