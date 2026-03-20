#[derive(Debug, thiserror::Error)]
pub enum FleetError {
    #[error("ship is not docked")]
    ShipNotDocked,

    #[error("already decommissioned")]
    AlreadyDecommissioned,

    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}
