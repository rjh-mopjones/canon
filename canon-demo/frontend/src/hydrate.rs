use crate::state::AppState;

/// Fetch initial state from gateway REST endpoints.
/// Falls back silently when the gateway is unavailable (demo mode uses defaults).
pub fn hydrate_from_gateway(_state: AppState) {
    // In production, this would fetch:
    //   GET /ships           -> Vec<ShipState>
    //   GET /stations        -> Vec<StationState>
    //   GET /admin/oversight/windows -> Vec<OversightWindow>
    //   GET /admin/deadletters       -> Vec<DeadLetterEntry>
    //
    // Since the gateway is not yet available, the frontend starts with
    // default_ships() and default_stations() defined in state.rs.
    // When the gateway is ready, this function will use gloo_net::http::Request
    // to fetch and patch the signals.
}
