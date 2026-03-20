use frontend::app::App;
use frontend::hydrate::hydrate_on_mount;
use frontend::state::AppState;
use frontend::ws::connect_ws;
use leptos::prelude::*;

fn main() {
    let state = AppState::new();

    // Start WebSocket connection to gateway.
    connect_ws(state.clone());

    // Fetch initial data from gateway REST endpoints.
    hydrate_on_mount(state.clone());

    // Mount the Leptos application into <body>.
    leptos::mount::mount_to_body(move || {
        view! { <App state=state.clone() /> }
    });
}
