//! HTTP polling client for game state.
//!
//! Creates a session via POST /sessions, then polls GET /game/:session_id
//! adaptively. Requests carry `If-None-Match` so the server answers `304`
//! when the projection has not advanced — most polls cost <100 bytes and
//! issue zero DB queries on the gateway.

use leptos::prelude::*;

use crate::gateway::gateway_base_url;
use crate::hydrate::{apply_snapshot, fetch_game_state, FetchResult};
use crate::state::{AppState, ConnectionStatus, PendingCommand, ShipStatus};

const ACTIVE_POLL_INTERVAL_MS: u32 = 250;
const IDLE_POLL_INTERVAL_MS: u32 = 1_000;
const BACKGROUND_POLL_INTERVAL_MS: u32 = 2_000;

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

        // Initial hydration captures the first ETag for the loop below.
        let mut last_etag: Option<String> = None;
        let mut last_session_id = session.session_id;
        if let FetchResult::Snapshot { snapshot, etag } =
            fetch_game_state(session.session_id, None).await
        {
            apply_snapshot(state, *snapshot);
            last_etag = etag;
        }

        let mut consecutive_failures: u32 = 0;
        loop {
            gloo_timers::future::TimeoutFuture::new(next_poll_interval_ms(&state)).await;

            let Some(session_id) = state.session_id.get_untracked() else {
                continue;
            };

            // Session rotated (e.g. gateway redeployed) — discard the old ETag
            // so the next request starts from scratch with the new projection.
            if session_id != last_session_id {
                last_session_id = session_id;
                last_etag = None;
            }

            match fetch_game_state(session_id, last_etag.as_deref()).await {
                FetchResult::Snapshot { snapshot, etag } => {
                    consecutive_failures = 0;
                    apply_snapshot(state, *snapshot);
                    last_etag = etag;
                    if state.connection.get_untracked() != ConnectionStatus::Connected {
                        state.connection.set(ConnectionStatus::Connected);
                    }
                }
                FetchResult::NotModified => {
                    consecutive_failures = 0;
                    if state.connection.get_untracked() != ConnectionStatus::Connected {
                        state.connection.set(ConnectionStatus::Connected);
                    }
                }
                FetchResult::Error => {
                    consecutive_failures += 1;
                    if consecutive_failures >= MAX_POLL_FAILURES {
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
