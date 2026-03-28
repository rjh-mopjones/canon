use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ── Ship state (GET /ships) ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct ShipStateResponse {
    pub id: Uuid,
    pub name: String,
    pub status: String,
    pub station_id: Option<Uuid>,
    pub route_label: String,
    pub fuel_pct: u32,
    pub aggregate_version: u64,
    pub last_snapshot_version: u64,
    pub correlation_id: Uuid,
}

// ── Station state (GET /stations) ────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct StationStateResponse {
    pub id: Uuid,
    pub name: String,
    pub capacity_kg: f32,
    pub current_stock_kg: f32,
}

// ── Station inventory (GET /stations/:id/inventory) ──────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct StationInventoryResponse {
    pub station_id: Uuid,
    pub name: String,
    pub capacity_kg: f32,
    pub current_stock_kg: f32,
}

// ── Oversight windows (GET /admin/oversight/windows) ─────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct OversightWindowResponse {
    pub window_id: Uuid,
    pub handler_id: String,
    pub correlation_key: Uuid,
    pub ship_name: String,
    pub dest_label: String,
    pub status: String,
    pub requirements: Vec<RequirementResponse>,
    pub ttl_remaining_secs: u32,
    pub ttl_total_secs: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct RequirementResponse {
    pub label: String,
    pub met: bool,
}

// ── Dead letters (GET /admin/deadletters) ───────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct DeadLetterResponse {
    pub id: Uuid,
    pub event_type: String,
    pub service: String,
    pub aggregate_id: String,
    pub error: String,
    pub attempts: u32,
    pub requeued: bool,
    pub created_at: String,
}

// ── Command request bodies ──────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterShipRequest {
    pub name: String,
    pub capacity_kg: f32,
}

#[derive(Debug, Deserialize)]
pub struct AssignRouteRequest {
    pub route_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct DepartForStationRequest {
    pub destination: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct ScheduleResupplyRequest {
    pub fuel_kg: f32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateManifestRequest {
    pub ship_id: Uuid,
    pub voyage_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct LoadCargoRequest {
    pub item_id: Uuid,
    pub weight_kg: f32,
    pub description: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PlanRouteRequest {
    pub route_id: Uuid,
    pub ship_id: Uuid,
    pub waypoints: Vec<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestResupplyRequest {
    pub station_id: Uuid,
    pub fuel_kg: f32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterStationRequest {
    pub name: String,
    pub capacity_kg: f32,
}

#[derive(Debug, Deserialize)]
pub struct BeginUnloadingRequest {
    pub station_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct RecordDockingRequest {
    pub ship_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct RecordCargoReceivedRequest {
    pub manifest_id: Uuid,
    pub weight_kg: f32,
}

// ── Event history response ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct EventHistoryEntry {
    pub event_id: Uuid,
    pub version: u64,
    pub event_type: String,
    pub event_version: u32,
    pub correlation_id: Uuid,
    pub timestamp: String,
    pub payload: serde_json::Value,
}

// ── Counterfactual replay ───────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CounterfactualQuery {
    pub aggregate_id: Uuid,
    pub branch_version: u64,
    pub command_type: String,
    /// Received via query params but used by the full counterfactual replay engine
    /// in the domain service, not the simplified gateway endpoint.
    #[allow(dead_code)]
    pub command_payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct CommandDiffResponse {
    pub added: Vec<CommandEnvelopeResponse>,
    pub removed: Vec<CommandEnvelopeResponse>,
    pub unchanged: Vec<CommandEnvelopeResponse>,
}

#[derive(Debug, Serialize)]
pub struct CommandEnvelopeResponse {
    pub command_id: Uuid,
    pub aggregate_id: Uuid,
    pub command_type: String,
    pub command_version: u32,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub timestamp: String,
}

// ── Game state snapshot (GET /game/:session_id) ────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct GameStateResponse {
    /// False while bootstrap events are still flowing through the pipeline.
    /// The frontend should show a loading screen until this is true.
    pub ready: bool,
    pub ship: Option<ShipStateResponse>,
    pub stations: Vec<GameStationResponse>,
    pub cargo: Option<GameCargoResponse>,
    pub oversight: Option<OversightWindowResponse>,
    pub events: Vec<GameEventResponse>,
    pub game_over: bool,
    pub infra: InfraStatusResponse,
}

#[derive(Debug, Clone, Serialize)]
pub struct GameStationResponse {
    pub id: Uuid,
    pub name: String,
    pub stock_pct: f64,
    pub capacity_kg: f32,
    pub stock_low: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct GameCargoResponse {
    pub manifest_id: Uuid,
    pub voyage_id: Uuid,
    pub destination_station_id: Option<Uuid>,
    pub amount_pct: u32,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GameEventResponse {
    pub id: Uuid,
    pub timestamp: String,
    pub version: u64,
    pub service: String,
    pub event_name: String,
    pub aggregate_id: Uuid,
    pub correlation_id: Uuid,
}

#[derive(Debug, Clone, Serialize)]
pub struct InfraStatusResponse {
    pub kafka: bool,
    pub yugabyte: bool,
    pub cassandra: bool,
}

// ── Generic command accepted response ───────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct CommandAcceptedResponse {
    pub command_id: Uuid,
    pub aggregate_id: Uuid,
    pub correlation_id: Uuid,
}
