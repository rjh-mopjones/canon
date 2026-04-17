//! HTTP polling client for game state.
//!
//! Creates a session via POST /sessions, then polls GET /game/:session_id
//! adaptively. Active gameplay polls at 100ms; idle/background states back off.

use leptos::prelude::*;

use crate::gateway::gateway_base_url;
use crate::hydrate::{apply_snapshot, fetch_game_state};
use crate::state::{AppState, ConnectionStatus, PendingCommand, ShipStatus};

const ACTIVE_POLL_INTERVAL_MS: u32 = 100;
const IDLE_POLL_INTERVAL_MS: u32 = 500;
const BACKGROUND_POLL_INTERVAL_MS: u32 = 1_000;

/// Number of consecutive poll failures before assuming the session is stale
/// (e.g., gateway restarted during a deploy) and creating a new one.
const MAX_POLL_FAILURES: u32 = 5;

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
        let mut consecutive_failures: u32 = 0;
        loop {
            gloo_timers::future::TimeoutFuture::new(next_poll_interval_ms(&state)).await;

            let Some(session_id) = state.session_id.get_untracked() else {
                continue;
            };

            match fetch_game_state(session_id).await {
                Some(snapshot) => {
                    consecutive_failures = 0;
                    apply_snapshot(state, snapshot);
                    if state.connection.get_untracked() != ConnectionStatus::Connected {
                        state.connection.set(ConnectionStatus::Connected);
                    }
                }
                None => {
                    consecutive_failures += 1;
                    if consecutive_failures >= MAX_POLL_FAILURES {
                        // Session is stale (gateway restarted during deploy).
                        // Restart with a fresh session.
                        web_sys::console::warn_1(
                            &format!(
                                "Session {session_id} lost after {consecutive_failures} poll failures, reconnecting"
                            )
                            .into(),
                        );
                        state.connection.set(ConnectionStatus::Reconnecting);
                        gloo_timers::future::TimeoutFuture::new(1000).await;
                        start_session_and_poll(state);
                        return;
                    }
                    state.connection.set(ConnectionStatus::Disconnected);
                }
            }
        }
    });
}

fn next_poll_interval_ms(state: &AppState) -> u32 {
    if document_hidden() {
        return BACKGROUND_POLL_INTERVAL_MS;
    }

    let command_pending = state
        .pending_command
        .with_untracked(|pending| *pending != PendingCommand::None);
    let ship_in_transit = state.ships.with_untracked(|ships| {
        ships
            .first()
            .is_some_and(|ship| ship.status == ShipStatus::Transit)
    });
    let oversight_visible = state
        .oversight
        .with_untracked(|oversight| oversight.visible);

    if command_pending || ship_in_transit || oversight_visible {
        ACTIVE_POLL_INTERVAL_MS
    } else {
        IDLE_POLL_INTERVAL_MS
    }
}

fn document_hidden() -> bool {
    web_sys::window()
        .and_then(|window| window.document())
        .map(|document| document.hidden())
        .unwrap_or(false)
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
