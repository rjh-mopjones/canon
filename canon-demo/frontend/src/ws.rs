//! WebSocket client for snapshot-push game state.

use std::cell::Cell;
use std::rc::Rc;

use leptos::prelude::*;
use wasm_bindgen::prelude::*;
use web_sys::{MessageEvent, WebSocket};

use crate::gateway::gateway_ws_url;
use crate::hydrate::{apply_snapshot, fetch_game_state, GameStateResponse};
use crate::state::{AppState, ConnectionStatus};

const MAX_BACKOFF_MS: u32 = 30_000;
const INITIAL_BACKOFF_MS: u32 = 2_000;
const FALLBACK_POLL_MS: u32 = 500;

#[derive(serde::Deserialize)]
struct WsGameStateMessage {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(flatten)]
    snapshot: GameStateResponse,
}

pub fn connect_ws(state: AppState) {
    connect_ws_with_backoff(state, INITIAL_BACKOFF_MS);
}

fn connect_ws_with_backoff(state: AppState, backoff_ms: u32) {
    let url = gateway_ws_url();
    let ws = match WebSocket::new(&url) {
        Ok(ws) => ws,
        Err(_) => {
            state.connection.set(ConnectionStatus::Disconnected);
            start_polling_fallback(state);
            schedule_reconnect(state, backoff_ms);
            return;
        }
    };

    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);
    state.connection.set(ConnectionStatus::Reconnecting);
    state.ws.set(Some(ws.clone()));

    let was_opened = Rc::new(Cell::new(false));
    let ws_for_open = ws.clone();

    let state_open = state;
    let was_opened_open = Rc::clone(&was_opened);
    let onopen = Closure::<dyn FnMut()>::new(move || {
        was_opened_open.set(true);
        state_open.connection.set(ConnectionStatus::Connected);

        if let Some(session_id) = state_open.session_id.get_untracked() {
            register_session(state_open, ws_for_open.clone(), session_id);
        } else {
            create_session_and_register(state_open, ws_for_open.clone());
        }
    });
    ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
    onopen.forget();

    let state_msg = state;
    let onmessage = Closure::<dyn FnMut(MessageEvent)>::new(move |evt: MessageEvent| {
        if let Some(text) = evt.data().as_string() {
            handle_ws_message(&text, state_msg);
        }
    });
    ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    onmessage.forget();

    let state_close = state;
    let was_opened_close = Rc::clone(&was_opened);
    let onclose = Closure::<dyn FnMut()>::new(move || {
        state_close.connection.set(ConnectionStatus::Reconnecting);
        start_polling_fallback(state_close);
        let next_backoff = if was_opened_close.get() {
            INITIAL_BACKOFF_MS
        } else {
            (backoff_ms * 2).min(MAX_BACKOFF_MS)
        };
        schedule_reconnect(state_close, next_backoff);
    });
    ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));
    onclose.forget();

    let onerror = Closure::<dyn FnMut()>::new(move || {});
    ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));
    onerror.forget();
}

fn schedule_reconnect(state: AppState, backoff_ms: u32) {
    wasm_bindgen_futures::spawn_local(async move {
        gloo_timers::future::TimeoutFuture::new(backoff_ms).await;
        if state.connection.get_untracked() != ConnectionStatus::Connected {
            connect_ws_with_backoff(state, backoff_ms);
        }
    });
}

fn start_polling_fallback(state: AppState) {
    wasm_bindgen_futures::spawn_local(async move {
        loop {
            gloo_timers::future::TimeoutFuture::new(FALLBACK_POLL_MS).await;
            if state.connection.get_untracked() == ConnectionStatus::Connected {
                break;
            }
            let Some(session_id) = state.session_id.get_untracked() else {
                continue;
            };
            if let Some(snapshot) = fetch_game_state(session_id).await {
                apply_snapshot(state, snapshot);
            }
        }
    });
}

pub fn create_session_and_register(state: AppState, ws: WebSocket) {
    let base = crate::gateway::gateway_base_url();
    wasm_bindgen_futures::spawn_local(async move {
        let url = format!("{base}/sessions");
        let resp = match gloo_net::http::Request::post(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                web_sys::console::warn_1(&format!("Failed to create session: {e}").into());
                return;
            }
        };

        if !resp.ok() {
            web_sys::console::warn_1(&format!("POST /sessions failed: {}", resp.status()).into());
            return;
        }

        #[derive(serde::Deserialize)]
        struct SessionResponse {
            session_id: uuid::Uuid,
        }

        let session: SessionResponse = match resp.json().await {
            Ok(s) => s,
            Err(e) => {
                web_sys::console::warn_1(&format!("Failed to parse session response: {e}").into());
                return;
            }
        };

        state.session_id.set(Some(session.session_id));
        register_session(state, ws, session.session_id);
        crate::hydrate::hydrate_from_gateway(state, session.session_id);
    });
}

fn register_session(state: AppState, ws: WebSocket, session_id: uuid::Uuid) {
    let reg_msg = serde_json::json!({
        "type": "RegisterSession",
        "session_id": session_id.to_string()
    });
    if let Ok(json) = serde_json::to_string(&reg_msg) {
        let _ = ws.send_with_str(&json);
    }
    if state.connection.get_untracked() != ConnectionStatus::Connected {
        state.connection.set(ConnectionStatus::Connected);
    }
}

fn handle_ws_message(text: &str, state: AppState) {
    let msg: WsGameStateMessage = match serde_json::from_str::<WsGameStateMessage>(text) {
        Ok(m) if m.msg_type == "GameState" => m,
        Ok(_) => return,
        Err(e) => {
            web_sys::console::warn_1(&format!("WS parse error: {e}").into());
            return;
        }
    };

    apply_snapshot(state, msg.snapshot);
}
