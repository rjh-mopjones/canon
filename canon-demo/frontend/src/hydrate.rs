use crate::state::{AppState, DeadLetterEntry, OversightWindow, ShipState, StationState};
use gloo_net::http::Request;
use leptos::prelude::*;

/// Base URL for the gateway API.
const BASE_URL: &str = "http://localhost:8080";

/// Fetch initial data from the gateway and populate AppState signals.
///
/// Fires all four requests concurrently on mount. Errors are silently
/// ignored (the WebSocket will patch in live data as it arrives).
pub fn hydrate_on_mount(state: AppState) {
    let state_ships = state.clone();
    leptos::task::spawn_local(async move {
        if let Ok(resp) = Request::get(&format!("{BASE_URL}/ships")).send().await {
            if let Ok(ships) = resp.json::<Vec<ShipState>>().await {
                state_ships.ships.set(ships);
            }
        }
    });

    let state_stations = state.clone();
    leptos::task::spawn_local(async move {
        if let Ok(resp) = Request::get(&format!("{BASE_URL}/stations")).send().await {
            if let Ok(stations) = resp.json::<Vec<StationState>>().await {
                state_stations.stations.set(stations);
            }
        }
    });

    let state_oversight = state.clone();
    leptos::task::spawn_local(async move {
        if let Ok(resp) = Request::get(&format!("{BASE_URL}/admin/oversight/windows"))
            .send()
            .await
        {
            if let Ok(windows) = resp.json::<Vec<OversightWindow>>().await {
                state_oversight.oversight_windows.set(windows);
            }
        }
    });

    let state_dl = state.clone();
    leptos::task::spawn_local(async move {
        if let Ok(resp) = Request::get(&format!("{BASE_URL}/admin/deadletters"))
            .send()
            .await
        {
            if let Ok(letters) = resp.json::<Vec<DeadLetterEntry>>().await {
                state_dl.dead_letters.set(letters);
            }
        }
    });
}
