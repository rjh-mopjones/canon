//! WS /events — per-session filtered WebSocket event streaming.
//!
//! Each client connects, then sends a `RegisterSession` message with their
//! session_id. After that, only events matching the session's aggregate IDs
//! are forwarded. InfraStatus messages always pass through.

use std::collections::HashSet;
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

    // Session filter: None until RegisterSession is received.
    let filter: Arc<RwLock<Option<HashSet<Uuid>>>> = Arc::new(RwLock::new(None));
    let session_id: Arc<RwLock<Option<Uuid>>> = Arc::new(RwLock::new(None));

    // Send task: forward matching broadcast events to this client.
    let filter_send = filter.clone();
    let send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            let f = filter_send.read().await;
            if should_forward(&msg, f.as_ref()) {
                if sender.send(Message::Text(msg)).await.is_err() {
                    break;
                }
            }
        }
    });

    // Receive task: listen for RegisterSession messages from the client.
    let filter_recv = filter.clone();
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
                                let id_set = session.ids.aggregate_id_set();
                                session.ws_connected.store(true, Ordering::Relaxed);
                                let mut f = filter_recv.write().await;
                                *f = Some(id_set);
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

    // Cleanup: mark WS disconnected, schedule session removal after grace period.
    let sid = { session_id.read().await.clone() };
    if let Some(id) = sid {
        // Mark disconnected
        {
            let store = state.sessions.read().await;
            if let Some(session) = store.get(&id) {
                session.ws_connected.store(false, Ordering::Relaxed);
            }
        }

        // Grace period: remove session after 60s if WS hasn't reconnected.
        let sessions = state.sessions.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            let mut store = sessions.write().await;
            if let Some(session) = store.get(&id) {
                if !session.ws_connected.load(Ordering::Relaxed) {
                    tracing::info!(session_id = %id, "session expired (WS disconnected for 60s)");
                    if let Some(removed) = store.remove(&id) {
                        if let Some(handle) = removed.drain_handle {
                            handle.abort();
                        }
                    }
                }
            }
        });
    }
}

/// Check if a broadcast message should be forwarded to this session's WS.
fn should_forward(json: &str, filter: Option<&HashSet<Uuid>>) -> bool {
    // InfraStatus always passes through
    if json.contains("\"type\":\"InfraStatus\"") {
        return true;
    }

    let filter = match filter {
        Some(f) => f,
        None => return false, // No session registered yet
    };

    // Fast-path: check aggregate_id field
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(json) {
        if let Some(agg_str) = val.get("aggregate_id").and_then(|v| v.as_str()) {
            if let Ok(agg_uuid) = Uuid::parse_str(agg_str) {
                if filter.contains(&agg_uuid) {
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
                        if filter.contains(&id_uuid) {
                            return true;
                        }
                    }
                }
            }
        }
    }

    false
}
