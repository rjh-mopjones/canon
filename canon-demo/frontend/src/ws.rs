use crate::state::{
    AppState, DeadLetterEntry, InfraStatusMsg, LiveEvent, OversightWindow, ShipState, StationState,
};
use leptos::prelude::*;
use serde::Deserialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{CloseEvent, ErrorEvent, MessageEvent, WebSocket};

/// Maximum number of events to keep in the log.
const MAX_EVENTS: usize = 60;

/// Tagged enum for incoming WebSocket messages from the gateway.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum WsMessage {
    Event(LiveEvent),
    ShipUpdate(ShipState),
    StationUpdate(StationState),
    OversightUpdate(OversightWindow),
    DeadLetter(DeadLetterEntry),
    InfraStatus(InfraStatusMsg),
}

/// Dispatch a parsed WsMessage to the appropriate signal on AppState.
fn dispatch_message(state: &AppState, msg: WsMessage) {
    match msg {
        WsMessage::Event(event) => {
            state.events.update(|events| {
                events.push_front(event);
                while events.len() > MAX_EVENTS {
                    events.pop_back();
                }
            });
        }
        WsMessage::ShipUpdate(ship) => {
            state.ships.update(|ships| {
                if let Some(existing) = ships.iter_mut().find(|s| s.id == ship.id) {
                    *existing = ship;
                } else {
                    ships.push(ship);
                }
            });
        }
        WsMessage::StationUpdate(station) => {
            state.stations.update(|stations| {
                if let Some(existing) = stations.iter_mut().find(|s| s.id == station.id) {
                    *existing = station;
                } else {
                    stations.push(station);
                }
            });
        }
        WsMessage::OversightUpdate(window) => {
            state.oversight_windows.update(|windows| {
                if let Some(existing) = windows
                    .iter_mut()
                    .find(|w| w.handler_id == window.handler_id)
                {
                    *existing = window;
                } else {
                    windows.push(window);
                }
            });
        }
        WsMessage::DeadLetter(entry) => {
            state.dead_letters.update(|letters| {
                letters.push(entry);
            });
        }
        WsMessage::InfraStatus(status) => {
            state.infra_status.set(status);
        }
    }
}

/// Connect to the gateway WebSocket and dispatch messages to AppState.
///
/// Implements reconnect with 2-second backoff. All closures are stored in
/// a boxed container to prevent them from being dropped prematurely.
pub fn connect_ws(state: AppState) {
    connect_ws_inner(state);
}

fn connect_ws_inner(state: AppState) {
    let ws = match WebSocket::new("ws://localhost:8080/events") {
        Ok(ws) => ws,
        Err(_) => {
            schedule_reconnect(state);
            return;
        }
    };

    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

    // onopen — only mark connected after the handshake completes.
    let state_open = state.clone();
    let onopen = Closure::<dyn Fn(web_sys::Event)>::new(move |_e: web_sys::Event| {
        state_open.ws_connected.set(true);
    });
    ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
    onopen.forget();

    // onmessage
    let state_msg = state.clone();
    let onmessage = Closure::<dyn Fn(MessageEvent)>::new(move |e: MessageEvent| {
        if let Ok(text) = e.data().dyn_into::<js_sys::JsString>() {
            let s: String = text.into();
            if let Ok(msg) = serde_json::from_str::<WsMessage>(&s) {
                dispatch_message(&state_msg, msg);
            }
        }
    });
    ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    onmessage.forget();

    // onerror
    let onerror = Closure::<dyn Fn(ErrorEvent)>::new(move |_e: ErrorEvent| {
        // Error is handled by onclose which fires after onerror.
    });
    ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));
    onerror.forget();

    // onclose
    let state_close = state.clone();
    let onclose = Closure::<dyn Fn(CloseEvent)>::new(move |_e: CloseEvent| {
        state_close.ws_connected.set(false);
        schedule_reconnect(state_close.clone());
    });
    ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));
    onclose.forget();
}

/// Schedule a reconnection attempt after 2 seconds.
fn schedule_reconnect(state: AppState) {
    let mut called = false;
    let cb = Closure::<dyn FnMut()>::new(move || {
        if !called {
            called = true;
            connect_ws_inner(state.clone());
        }
    });
    let _ = web_sys::window().map(|w| {
        let _ = w.set_timeout_with_callback_and_timeout_and_arguments_0(
            cb.as_ref().unchecked_ref(),
            2000,
        );
    });
    cb.forget();
}
