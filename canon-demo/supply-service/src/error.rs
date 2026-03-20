/// Errors produced by the supply-service domain.
#[derive(Debug, thiserror::Error)]
pub enum SupplyError {
    /// A resupply dispatch was attempted while another dispatch is already pending.
    #[error("resupply already pending for inventory (pending ship: {pending_ship_id})")]
    ResupplyAlreadyPending { pending_ship_id: uuid::Uuid },

    /// A delivery confirmation was attempted but no resupply is pending.
    #[error("no pending resupply to confirm")]
    NoPendingResupply,

    /// Serialization or deserialization failure.
    #[error("serialization error: {0}")]
    Serialization(String),
}
