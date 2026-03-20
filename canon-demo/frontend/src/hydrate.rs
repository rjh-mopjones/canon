use crate::state::AppState;

/// Fetch initial state from the gateway on mount.
/// In demo/offline mode, this uses hardcoded data.
///
/// When the gateway is available, this will fetch:
/// - GET /ships
/// - GET /stations
/// - GET /admin/oversight/windows
/// - GET /admin/deadletters
pub fn hydrate_initial_state(_state: &AppState) {
    // In demo mode, ships and stations are defined inline in components.
    // When the gateway is available, this will fetch via gloo-net HTTP.
    leptos::logging::log!("hydrate: running in demo mode with inline data");
}
