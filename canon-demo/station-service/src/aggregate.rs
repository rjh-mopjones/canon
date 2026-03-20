use uuid::Uuid;

/// Station aggregate — represents a physical station with docking and cargo capabilities.
///
/// Snapshotted every 50 events for efficient state reconstruction.
#[canon_core::aggregate(snapshot_every = 50)]
pub struct Station {
    pub name: String,
    pub capacity_kg: f32,
    pub current_stock_kg: f32,
    pub docked_ships: Vec<Uuid>,
    pub registered: bool,
}
