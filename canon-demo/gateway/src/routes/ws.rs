//! WS /events — per-session filtered WebSocket event streaming.
//!
//! Each client connects, then sends a `RegisterSession` message with their
//! session_id. After that, only events matching the session's aggregate IDs
//! are forwarded. InfraStatus messages always pass through.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures::{SinkExt, StreamExt};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/events", get(ws_handler))
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// Message sent by the frontend to associate the WS with a session.
#[derive(serde::Deserialize)]
struct RegisterSessionMsg {
    #[serde(rename = "type")]
    msg_type: String,
    session_id: Uuid,
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.event_tx.subscribe();

    let session_id: Arc<RwLock<Option<Uuid>>> = Arc::new(RwLock::new(None));

    // Send task: forward matching broadcast events to this client.
    let session_id_send = session_id.clone();
    let sessions_send = state.sessions.clone();
    let send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            let sid = *session_id_send.read().await;
            let forward = should_forward(&msg, sid, &sessions_send).await;
            if forward && sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // Receive task: listen for RegisterSession messages from the client.
    let session_id_recv = session_id.clone();
    let sessions = state.sessions.clone();
    let recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Text(text) => {
                    if let Ok(reg) = serde_json::from_str::<RegisterSessionMsg>(&text) {
                        if reg.msg_type == "RegisterSession" {
                            let store = sessions.read().await;
                            if let Some(session) = store.get(&reg.session_id) {
                                session.ws_connected.store(true, Ordering::Relaxed);
                                let mut sid = session_id_recv.write().await;
                                *sid = Some(reg.session_id);
                                tracing::info!(session_id = %reg.session_id, "WS registered session");
                            }
                        }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = send_task => {},
        _ = recv_task => {},
    }

    // Cleanup: atomically transition ws_connected from true→false.
    // Only the connection that succeeds at compare_exchange spawns the
    // cleanup task, preventing duplicate 60s timers when rapid
    // disconnect/reconnect cycles occur.
    let sid = *session_id.read().await;
    if let Some(id) = sid {
        let should_spawn_cleanup = {
            let store = state.sessions.read().await;
            store.get(&id).is_some_and(|session| {
                session
                    .ws_connected
                    .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            })
        };

        if should_spawn_cleanup {
            // Abort drain task immediately to stop flooding the pipeline.
            // Keep the session metadata for 10s so a reconnecting WS can
            // re-register, but the drain stops right away.
            {
                let mut store = state.sessions.write().await;
                if let Some(session) = store.get_mut(&id) {
                    if let Some(handle) = session.drain_handle.take() {
                        handle.abort();
                        tracing::info!(session_id = %id, "drain task aborted on WS disconnect");
                    }
                }
            }

            // Remove session metadata after 60s if WS hasn't reconnected.
            // 10s was too aggressive — brief network hiccups or tab
            // backgrounding would kill the session and reset game state.
            let sessions = state.sessions.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                let mut store = sessions.write().await;
                if let Some(session) = store.get(&id) {
                    if !session.ws_connected.load(Ordering::Acquire) {
                        tracing::info!(session_id = %id, "session expired (WS disconnected for 60s)");
                        store.remove(&id);
                    }
                }
            });
        }
    }
}

/// Check if a broadcast message should be forwarded to this session's WS.
async fn should_forward(
    json: &str,
    session_id: Option<Uuid>,
    sessions: &crate::session::SessionStore,
) -> bool {
    // InfraStatus always passes through
    if json.contains("\"type\":\"InfraStatus\"") {
        return true;
    }

    let session_id = match session_id {
        Some(session_id) => session_id,
        None => return false, // No session registered yet
    };
    let aggregate_ids = {
        let store = sessions.read().await;
        match store.get(&session_id) {
            Some(session) => session.aggregate_id_set(),
            None => return false,
        }
    };

    // Fast-path: check aggregate_id field
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(json) {
        if let Some(agg_str) = val.get("aggregate_id").and_then(|v| v.as_str()) {
            if let Ok(agg_uuid) = Uuid::parse_str(agg_str) {
                if aggregate_ids.contains(&agg_uuid) {
                    return true;
                }
            }
        }
        // Check payload station_id / ship_id for cross-service events
        // (e.g. navigation route events have their own aggregate_id but carry
        // a station_id/ship_id in payload that matches the session)
        if let Some(payload) = val.get("payload") {
            for field in &["station_id", "ship_id"] {
                if let Some(id_str) = payload.get(*field).and_then(|v| v.as_str()) {
                    if let Ok(id_uuid) = Uuid::parse_str(id_str) {
                        if aggregate_ids.contains(&id_uuid) {
                            return true;
                        }
                    }
                }
            }
        }
    }

    false
}
