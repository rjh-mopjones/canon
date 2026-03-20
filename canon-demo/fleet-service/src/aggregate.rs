use canon_core::aggregate;

#[derive(Default, Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ShipStatus {
    #[default]
    Docked,
    InTransit,
    Decommissioned,
}

#[aggregate(snapshot_every = 50)]
pub struct Ship {
    pub name: String,
    pub capacity_kg: f32,
    pub status: ShipStatus,
    pub assigned_route: Option<uuid::Uuid>,
    pub fuel_kg: f32,
}
