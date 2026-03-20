use leptos::prelude::*;
use wasm_bindgen::prelude::*;
use web_sys::{MessageEvent, WebSocket};

use crate::state::AppState;

/// Attempt to connect to the gateway WebSocket.
/// Falls back silently when the gateway is unavailable (demo mode).
pub fn connect_ws(state: AppState) {
    let window = match web_sys::window() {
        Some(w) => w,
        None => return,
    };
    let location = window.location();
    let host = location.host().unwrap_or_else(|_| "localhost:3000".into());
    let protocol = if location.protocol().unwrap_or_default() == "https:" {
        "wss"
    } else {
        "ws"
    };
    let url = format!("{protocol}://{host}/events");

    let ws = match WebSocket::new(&url) {
        Ok(ws) => ws,
        Err(_) => return, // gateway not available — run in demo mode
    };

    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

    // on message
    let state_msg = state;
    let onmessage = Closure::<dyn FnMut(MessageEvent)>::new(move |evt: MessageEvent| {
        if let Some(text) = evt.data().as_string() {
            handle_ws_message(&text, state_msg);
        }
    });
    ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    onmessage.forget();

    // on close — reconnect after 2s
    let state_close = state;
    let onclose = Closure::<dyn FnMut()>::new(move || {
        let st = state_close;
        let _ = gloo_timers::callback::Timeout::new(2_000, move || {
            connect_ws(st);
        });
    });
    ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));
    onclose.forget();

    // on error — ignore, onclose will fire
    let onerror = Closure::<dyn FnMut()>::new(move || {});
    ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));
    onerror.forget();
}

fn handle_ws_message(text: &str, _state: AppState) {
    // Parse incoming WsMessage and patch signals.
    // In demo mode (no gateway), events are generated locally, so this is a
    // secondary path. Full implementation applies ShipUpdate, StationUpdate,
    // OversightUpdate, InfraStatus patches to the appropriate signals.
    if let Ok(crate::state::WsMessage::InfraStatus(infra)) =
        serde_json::from_str::<crate::state::WsMessage>(text)
    {
        _state.infra.set(crate::state::InfraStatus {
            kafka: infra.kafka,
            yugabyte: infra.yugabyte,
            cassandra: infra.cassandra,
        });
    } else if let Ok(_msg) = serde_json::from_str::<crate::state::WsMessage>(text) {
        // Other message types will be handled when the gateway is live.
        // In demo mode, local simulation drives the UI.
    }
}
