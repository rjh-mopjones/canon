//! HTTP polling client for game state.
//!
//! Creates a session via POST /sessions, then polls GET /game/:session_id
//! at 500ms intervals. No WebSocket — simple, reliable, stateless HTTP.

use leptos::prelude::*;

use crate::gateway::gateway_base_url;
use crate::hydrate::{apply_snapshot, fetch_game_state};
use crate::state::{AppState, ConnectionStatus};

const POLL_INTERVAL_MS: u32 = 500;

/// Entry point: create session and start polling loop.
pub fn start_session_and_poll(state: AppState) {
    wasm_bindgen_futures::spawn_local(async move {
        // Create session
        let base = gateway_base_url();
        let url = format!("{base}/sessions");
        let resp = match gloo_net::http::Request::post(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                web_sys::console::warn_1(&format!("Failed to create session: {e}").into());
                state.connection.set(ConnectionStatus::Disconnected);
                // Retry after a delay
                gloo_timers::future::TimeoutFuture::new(2000).await;
                start_session_and_poll(state);
                return;
            }
        };

        if !resp.ok() {
            web_sys::console::warn_1(&format!("POST /sessions failed: {}", resp.status()).into());
            state.connection.set(ConnectionStatus::Disconnected);
            gloo_timers::future::TimeoutFuture::new(2000).await;
            start_session_and_poll(state);
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
                state.connection.set(ConnectionStatus::Disconnected);
                return;
            }
        };

        state.session_id.set(Some(session.session_id));
        state.connection.set(ConnectionStatus::Connected);

        // Initial hydration
        if let Some(snapshot) = fetch_game_state(session.session_id).await {
            apply_snapshot(state, snapshot);
        }

        // Poll loop
        loop {
            gloo_timers::future::TimeoutFuture::new(POLL_INTERVAL_MS).await;

            let Some(session_id) = state.session_id.get_untracked() else {
                continue;
            };

            match fetch_game_state(session_id).await {
                Some(snapshot) => {
                    apply_snapshot(state, snapshot);
                    if state.connection.get_untracked() != ConnectionStatus::Connected {
                        state.connection.set(ConnectionStatus::Connected);
                    }
                }
                None => {
                    state.connection.set(ConnectionStatus::Disconnected);
                }
            }
        }
    });
}

/// Create a new session and update the session_id signal.
/// The existing poll loop will pick up the new session_id automatically.
pub fn create_new_session(state: AppState) {
    wasm_bindgen_futures::spawn_local(async move {
        let base = gateway_base_url();
        let url = format!("{base}/sessions");
        let resp = match gloo_net::http::Request::post(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                web_sys::console::warn_1(&format!("Failed to create session: {e}").into());
                return;
            }
        };

        if !resp.ok() {
            return;
        }

        #[derive(serde::Deserialize)]
        struct SessionResponse {
            session_id: uuid::Uuid,
        }

        if let Ok(session) = resp.json::<SessionResponse>().await {
            state.session_id.set(Some(session.session_id));
        }
    });
}
