use uuid::Uuid;

/// Station aggregate — represents a physical station with docking and cargo capabilities.
///
/// Snapshotted every 50 events for efficient state reconstruction.
///
/// Stock drains over time at `drain_rate_kg_per_s`. When stock drops below 20% of capacity,
/// `StationStockLow` fires to trigger the resupply chain. At 0% stock, `StationOffline` fires.
///
/// Supply chain ring: each station knows which station supplies it via `supplied_by`.
#[canon_core::aggregate(snapshot_every = 50)]
pub struct Station {
    pub name: String,
    pub capacity_kg: f32,
    pub current_stock_kg: f32,
    pub drain_rate_kg_per_s: f32,
    pub supplied_by: Option<Uuid>,
    pub docked_ships: Vec<Uuid>,
    pub registered: bool,
    pub offline: bool,
}
