//! WS /events — per-session snapshot push.
//!
//! Each client connects, sends `RegisterSession`, and then receives full game
//! state snapshots whenever any relevant aggregate changes.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures::{SinkExt, StreamExt};
use tokio::sync::{mpsc, RwLock};
use tracing::warn;
use uuid::Uuid;

use crate::routes::game::build_game_state;
use crate::state::{AppState, GatewayNotification};
use crate::types::WsGameStateMessage;

pub fn router() -> Router<AppState> {
    Router::new().route("/events", get(ws_handler))
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

#[derive(serde::Deserialize)]
struct RegisterSessionMsg {
    #[serde(rename = "type")]
    msg_type: String,
    session_id: Uuid,
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let mut rx = state.event_tx.subscribe();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Message>();

    let session_id: Arc<RwLock<Option<Uuid>>> = Arc::new(RwLock::new(None));

    let writer = tokio::spawn(async move {
        while let Some(message) = out_rx.recv().await {
            if ws_sender.send(message).await.is_err() {
                break;
            }
        }
    });

    let send_state = state.clone();
    let send_session_id = session_id.clone();
    let send_out = out_tx.clone();
    let send_task = tokio::spawn(async move {
        while let Ok(notification) = rx.recv().await {
            let Some(current_session_id) = *send_session_id.read().await else {
                continue;
            };

            if !should_forward(&notification, current_session_id, &send_state).await {
                continue;
            }

            let awaited_event_id = match notification {
                GatewayNotification::AggregateChanged { event_id, .. } => Some(event_id),
                GatewayNotification::InfraChanged => None,
            };

            if let Some(message) =
                snapshot_message_for_session(&send_state, current_session_id, awaited_event_id)
                    .await
            {
                if send_out.send(Message::Text(message)).is_err() {
                    break;
                }
            }
        }
    });

    let recv_state = state.clone();
    let recv_session_id = session_id.clone();
    let recv_out = out_tx.clone();
    let recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_receiver.next().await {
            match msg {
                Message::Text(text) => {
                    if let Ok(reg) = serde_json::from_str::<RegisterSessionMsg>(&text) {
                        if reg.msg_type != "RegisterSession" {
                            continue;
                        }

                        {
                            let mut store = recv_state.sessions.write().await;
                            if let Some(session) = store.get_mut(&reg.session_id) {
                                session.ws_connected.store(true, Ordering::Relaxed);
                                if session.drain_handle.is_none() {
                                    session.drain_handle =
                                        Some(crate::session::spawn_session_drain(
                                            recv_state.pool_for_service("station").clone(),
                                            session.ids.station_ids,
                                        ));
                                }
                            } else {
                                continue;
                            }
                        }

                        *recv_session_id.write().await = Some(reg.session_id);

                        if let Some(message) =
                            snapshot_message_for_session(&recv_state, reg.session_id, None).await
                        {
                            if recv_out.send(Message::Text(message)).is_err() {
                                break;
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
        _ = writer => {},
        _ = send_task => {},
        _ = recv_task => {},
    }

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
            {
                let mut store = state.sessions.write().await;
                if let Some(session) = store.get_mut(&id) {
                    if let Some(handle) = session.drain_handle.take() {
                        handle.abort();
                    }
                }
            }

            let sessions = state.sessions.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                let mut store = sessions.write().await;
                if let Some(session) = store.get(&id) {
                    if !session.ws_connected.load(Ordering::Acquire) {
                        store.remove(&id);
                    }
                }
            });
        }
    }
}

async fn should_forward(
    notification: &GatewayNotification,
    session_id: Uuid,
    state: &AppState,
) -> bool {
    match notification {
        GatewayNotification::InfraChanged => true,
        GatewayNotification::AggregateChanged {
            aggregate_id,
            related_ids,
            ..
        } => {
            let (base_ids, related_ids_lock) = {
                let sessions = state.sessions.read().await;
                let Some(session) = sessions.get(&session_id) else {
                    return false;
                };
                (
                    session.ids.aggregate_id_set(),
                    session.related_aggregate_ids.clone(),
                )
            };
            let related_tracked = related_ids_lock.read().await.clone();

            aggregate_id_in_sets(*aggregate_id, related_ids, &base_ids, &related_tracked)
        }
    }
}

fn aggregate_id_in_sets(
    aggregate_id: Uuid,
    related_ids: &[Uuid],
    base_ids: &HashSet<Uuid>,
    tracked_ids: &HashSet<Uuid>,
) -> bool {
    base_ids.contains(&aggregate_id)
        || tracked_ids.contains(&aggregate_id)
        || related_ids
            .iter()
            .any(|id| base_ids.contains(id) || tracked_ids.contains(id))
}

async fn snapshot_message_for_session(
    state: &AppState,
    session_id: Uuid,
    awaited_event_id: Option<Uuid>,
) -> Option<String> {
    for attempt in 0..4 {
        let built = match build_game_state(state, session_id).await {
            Ok(built) => built,
            Err(err) => {
                warn!(session_id = %session_id, error = %err, "failed to build session snapshot");
                return None;
            }
        };

        let (related_ids_lock, base_ids) = {
            let sessions = state.sessions.read().await;
            let session = sessions.get(&session_id)?;
            (
                session.related_aggregate_ids.clone(),
                session.ids.aggregate_id_set(),
            )
        };
        {
            let mut related_ids = related_ids_lock.write().await;
            *related_ids = built
                .tracked_aggregate_ids
                .difference(&base_ids)
                .copied()
                .collect();
        }

        let event_seen = awaited_event_id.is_none_or(|event_id| {
            built
                .snapshot
                .events
                .iter()
                .any(|event| event.id == event_id)
        });

        if event_seen || attempt == 3 {
            return serde_json::to_string(&WsGameStateMessage::from(built.snapshot)).ok();
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    None
}
