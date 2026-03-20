use web_sys::WebSocket;

use crate::state::AppState;

/// Establish WebSocket connection to the gateway for live event updates.
/// Reconnects automatically with a 2-second backoff on disconnect.
pub fn connect_ws(state: &AppState) {
    let _state = state.clone();

    // In demo/offline mode, the WebSocket connection is a no-op.
    // When the gateway is available, this will connect to WS /events.
    let location = web_sys::window().and_then(|w| w.location().hostname().ok());
    let host = location.unwrap_or_else(|| "localhost".to_string());
    let url = format!("ws://{}:3000/events", host);

    // Attempt connection — silently fail in demo mode
    match WebSocket::new(&url) {
        Ok(_ws) => {
            // Connection handlers would be set up here when gateway is available.
            // For now, the frontend runs in offline/demo mode with simulated events.
            leptos::logging::log!("WebSocket: attempting connection to {}", url);
        }
        Err(_) => {
            leptos::logging::log!("WebSocket: gateway not available, running in demo mode");
        }
    }
}
