//! Admin/troubleshooting endpoint support for Canon services.
//!
//! Provides [`AdminHandler`] for inspecting retry state, inbox windows,
//! and triggering projection rebuilds. Auto-mounted by `ServiceBuilder`
//! when admin endpoints are enabled.
//!
//! ## Endpoints
//!
//! - `GET /debug/retries` — lists messages in retry state with attempt counts
//! - `GET /debug/inbox/windows` — lists inbox windows with optional filters
//! - `POST /debug/projections/:id/rebuild` — triggers a projection rebuild

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::memory::{InMemoryInbox, InMemoryRetryTracker, InboxWindowInfo};
use crate::traits::ProjectionRebuildManager;
use crate::{Version, WindowStatus};

// ── Error ────────────────────────────────────────────────────────────────────

/// Errors produced by admin operations.
#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    /// Failed to list retry attempts.
    #[error("retry tracker error: {0}")]
    RetryTracker(String),

    /// Failed to list inbox windows.
    #[error("inbox error: {0}")]
    Inbox(String),

    /// Failed to trigger or inspect a projection rebuild.
    #[error("projection rebuild error: {0}")]
    ProjectionRebuild(String),
}

// ── Response types ───────────────────────────────────────────────────────────

/// JSON-serializable response for a single retry attempt entry.
#[derive(Debug, Clone, Serialize)]
pub struct RetryStatusResponse {
    pub message_id: Uuid,
    pub handler_id: String,
    pub attempts: u32,
    pub last_attempted: DateTime<Utc>,
}

/// Filter parameters for inbox window listing.
#[derive(Debug, Clone, Default)]
pub struct InboxWindowFilter {
    pub handler_id: Option<String>,
    pub status: Option<WindowStatus>,
    pub correlation_key: Option<Uuid>,
}

/// JSON-serializable response for a single inbox window.
#[derive(Debug, Clone, Serialize)]
pub struct InboxWindowResponse {
    pub handler_id: String,
    pub correlation_key: Uuid,
    pub window_id: Uuid,
    pub status: String,
    pub message_count: usize,
    pub expires_in_ms: Option<i64>,
    pub created_at: DateTime<Utc>,
}

/// JSON-serializable response for a projection rebuild trigger.
#[derive(Debug, Clone, Serialize)]
pub struct RebuildResponse {
    pub projection_id: String,
    pub status: String,
}

// ── AdminHandler ─────────────────────────────────────────────────────────────

/// Unified admin handler providing retry listing, inbox window inspection,
/// and projection rebuild triggering.
///
/// Created by [`ServiceBuilder`](crate::ServiceBuilder) when admin endpoints
/// are enabled. Downstream crates (e.g. gateway) wire this into their web
/// framework.
///
/// # Example
///
/// ```ignore
/// let admin = service.admin_handler().unwrap();
/// let retries = admin.list_retries().await?;
/// let windows = admin.list_inbox_windows(InboxWindowFilter::default()).await?;
/// let rebuild = admin.trigger_rebuild("station_inventory").await?;
/// ```
pub struct AdminHandler<PRM> {
    retry_tracker: InMemoryRetryTracker,
    inbox: InMemoryInbox,
    rebuild_manager: PRM,
}

impl<PRM> AdminHandler<PRM>
where
    PRM: ProjectionRebuildManager,
{
    /// Create a new `AdminHandler` from the given components.
    pub fn new(
        retry_tracker: InMemoryRetryTracker,
        inbox: InMemoryInbox,
        rebuild_manager: PRM,
    ) -> Self {
        Self {
            retry_tracker,
            inbox,
            rebuild_manager,
        }
    }

    /// List all messages currently in the retry_attempts table.
    ///
    /// Returns a snapshot of all retried messages with their attempt counts.
    pub async fn list_retries(&self) -> Result<Vec<RetryStatusResponse>, AdminError> {
        tracing::debug!("admin: listing retry attempts");

        let attempts = self
            .retry_tracker
            .list_all()
            .map_err(|e| AdminError::RetryTracker(e.to_string()))?;

        let responses: Vec<RetryStatusResponse> = attempts
            .into_iter()
            .map(|a| RetryStatusResponse {
                message_id: a.message_id,
                handler_id: a.handler_id,
                attempts: a.attempts,
                last_attempted: a.last_attempted,
            })
            .collect();

        tracing::debug!(count = responses.len(), "admin: retry attempts listed");

        Ok(responses)
    }

    /// List inbox windows with optional filters.
    ///
    /// Supports filtering by handler_id, status, and correlation_key.
    /// Shows message count per window and time-to-expiry.
    pub async fn list_inbox_windows(
        &self,
        filter: InboxWindowFilter,
    ) -> Result<Vec<InboxWindowResponse>, AdminError> {
        tracing::debug!(
            handler_id = ?filter.handler_id,
            status = ?filter.status,
            correlation_key = ?filter.correlation_key,
            "admin: listing inbox windows"
        );

        let windows = self
            .inbox
            .list_windows(
                filter.handler_id.as_deref(),
                filter.status,
                filter.correlation_key,
            )
            .map_err(|e| AdminError::Inbox(e.to_string()))?;

        let responses: Vec<InboxWindowResponse> =
            windows.into_iter().map(window_info_to_response).collect();

        tracing::debug!(count = responses.len(), "admin: inbox windows listed");

        Ok(responses)
    }

    /// Trigger a projection rebuild.
    ///
    /// Sets `rebuilding = true`, resets the checkpoint, and returns confirmation.
    /// Idempotent: returns an error if the projection is already rebuilding.
    pub async fn trigger_rebuild(
        &self,
        projection_id: &str,
    ) -> Result<RebuildResponse, AdminError> {
        tracing::info!(
            projection_id = %projection_id,
            "admin: triggering projection rebuild"
        );

        self.rebuild_manager
            .start_rebuild(projection_id, None)
            .await
            .map_err(|e| AdminError::ProjectionRebuild(e.to_string()))?;

        tracing::info!(
            projection_id = %projection_id,
            "admin: projection rebuild started"
        );

        Ok(RebuildResponse {
            projection_id: projection_id.to_owned(),
            status: "rebuild_started".to_owned(),
        })
    }

    /// Check whether a projection is currently rebuilding.
    pub async fn is_rebuilding(&self, projection_id: &str) -> Result<bool, AdminError> {
        self.rebuild_manager
            .is_rebuilding(projection_id)
            .await
            .map_err(|e| AdminError::ProjectionRebuild(e.to_string()))
    }

    /// Get the current checkpoint version for a projection.
    pub async fn get_rebuild_checkpoint(&self, projection_id: &str) -> Result<Version, AdminError> {
        self.rebuild_manager
            .get_checkpoint(projection_id)
            .await
            .map_err(|e| AdminError::ProjectionRebuild(e.to_string()))
    }
}

/// Convert an [`InboxWindowInfo`] to an [`InboxWindowResponse`].
fn window_info_to_response(info: InboxWindowInfo) -> InboxWindowResponse {
    InboxWindowResponse {
        handler_id: info.handler_id,
        correlation_key: info.correlation_key,
        window_id: info.window_id,
        status: info.status.as_str().to_owned(),
        message_count: info.message_count,
        expires_in_ms: info.expires_in_ms,
        created_at: info.created_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::inbound_queue::InMemoryInboundQueue;
    use crate::memory::{
        InMemoryInbox, InMemoryProjectionRebuildManager, InMemoryProjectionStore,
        InMemoryRetryTracker,
    };
    use crate::{AggregateId, CommandEnvelope, IncomingMessage, Oversight, Version};
    use bytes::Bytes;

    fn make_admin_handler() -> AdminHandler<InMemoryProjectionRebuildManager> {
        let retry_tracker = InMemoryRetryTracker::new();
        let inbox = InMemoryInbox::new();
        let projection_store = InMemoryProjectionStore::new();
        let rebuild_manager = InMemoryProjectionRebuildManager::new(projection_store);
        AdminHandler::new(retry_tracker, inbox, rebuild_manager)
    }

    fn make_command_msg(aggregate_id: &AggregateId) -> IncomingMessage {
        IncomingMessage::Command(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            command_type: "TestCommand".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"{}"),
            command_version: 1,
        })
    }

    // -- list_retries tests --

    #[tokio::test]
    async fn list_retries_empty() {
        let handler = make_admin_handler();
        let retries = handler.list_retries().await.unwrap();
        assert!(retries.is_empty());
    }

    #[tokio::test]
    async fn list_retries_returns_tracked_messages() {
        let retry_tracker = InMemoryRetryTracker::new();
        let inbox = InMemoryInbox::new();
        let projection_store = InMemoryProjectionStore::new();
        let rebuild_manager = InMemoryProjectionRebuildManager::new(projection_store);

        let msg_id = Uuid::new_v4();
        use crate::traits::RetryTracker as _;
        retry_tracker.increment(msg_id, "handler_a").unwrap();
        retry_tracker.increment(msg_id, "handler_a").unwrap();

        let handler = AdminHandler::new(retry_tracker, inbox, rebuild_manager);
        let retries = handler.list_retries().await.unwrap();

        assert_eq!(retries.len(), 1);
        assert_eq!(retries[0].message_id, msg_id);
        assert_eq!(retries[0].handler_id, "handler_a");
        assert_eq!(retries[0].attempts, 2);
    }

    #[tokio::test]
    async fn list_retries_multiple_messages() {
        let retry_tracker = InMemoryRetryTracker::new();
        let inbox = InMemoryInbox::new();
        let projection_store = InMemoryProjectionStore::new();
        let rebuild_manager = InMemoryProjectionRebuildManager::new(projection_store);

        use crate::traits::RetryTracker as _;
        let msg_a = Uuid::new_v4();
        let msg_b = Uuid::new_v4();
        retry_tracker.increment(msg_a, "handler_a").unwrap();
        retry_tracker.increment(msg_b, "handler_b").unwrap();
        retry_tracker.increment(msg_b, "handler_b").unwrap();
        retry_tracker.increment(msg_b, "handler_b").unwrap();

        let handler = AdminHandler::new(retry_tracker, inbox, rebuild_manager);
        let retries = handler.list_retries().await.unwrap();

        assert_eq!(retries.len(), 2);
        // Find each by message_id (HashMap order not guaranteed)
        let a = retries.iter().find(|r| r.message_id == msg_a).unwrap();
        let b = retries.iter().find(|r| r.message_id == msg_b).unwrap();
        assert_eq!(a.attempts, 1);
        assert_eq!(b.attempts, 3);
    }

    // -- list_inbox_windows tests --

    #[tokio::test]
    async fn list_inbox_windows_empty() {
        let handler = make_admin_handler();
        let windows = handler
            .list_inbox_windows(InboxWindowFilter::default())
            .await
            .unwrap();
        assert!(windows.is_empty());
    }

    #[tokio::test]
    async fn list_inbox_windows_shows_pending_window() {
        let retry_tracker = InMemoryRetryTracker::new();
        let inbox = InMemoryInbox::new();
        let queue = InMemoryInboundQueue::new();
        let projection_store = InMemoryProjectionStore::new();
        let rebuild_manager = InMemoryProjectionRebuildManager::new(projection_store);

        let agg_id = AggregateId::new();
        inbox
            .register_handler("test_handler", |_| Oversight::NotReady)
            .unwrap();
        inbox
            .submit("test_handler", make_command_msg(&agg_id), &queue)
            .unwrap();

        let handler = AdminHandler::new(retry_tracker, inbox, rebuild_manager);
        let windows = handler
            .list_inbox_windows(InboxWindowFilter::default())
            .await
            .unwrap();

        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].handler_id, "test_handler");
        assert_eq!(windows[0].status, "pending");
        assert_eq!(windows[0].message_count, 1);
    }

    #[tokio::test]
    async fn list_inbox_windows_filter_by_handler_id() {
        let retry_tracker = InMemoryRetryTracker::new();
        let inbox = InMemoryInbox::new();
        let queue = InMemoryInboundQueue::new();
        let projection_store = InMemoryProjectionStore::new();
        let rebuild_manager = InMemoryProjectionRebuildManager::new(projection_store);

        let agg_a = AggregateId::new();
        let agg_b = AggregateId::new();
        inbox
            .register_handler("handler_a", |_| Oversight::NotReady)
            .unwrap();
        inbox
            .register_handler("handler_b", |_| Oversight::NotReady)
            .unwrap();
        inbox
            .submit("handler_a", make_command_msg(&agg_a), &queue)
            .unwrap();
        inbox
            .submit("handler_b", make_command_msg(&agg_b), &queue)
            .unwrap();

        let handler = AdminHandler::new(retry_tracker, inbox, rebuild_manager);

        // Filter by handler_a only
        let windows = handler
            .list_inbox_windows(InboxWindowFilter {
                handler_id: Some("handler_a".to_owned()),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].handler_id, "handler_a");
    }

    #[tokio::test]
    async fn list_inbox_windows_filter_by_status() {
        let retry_tracker = InMemoryRetryTracker::new();
        let inbox = InMemoryInbox::new();
        let queue = InMemoryInboundQueue::new();
        let projection_store = InMemoryProjectionStore::new();
        let rebuild_manager = InMemoryProjectionRebuildManager::new(projection_store);

        let agg_id = AggregateId::new();
        inbox
            .register_handler("test_handler", |_| Oversight::NotReady)
            .unwrap();
        inbox
            .set_handler_ttl("test_handler", std::time::Duration::from_secs(0))
            .unwrap();
        inbox
            .submit("test_handler", make_command_msg(&agg_id), &queue)
            .unwrap();

        // Let the window expire
        std::thread::sleep(std::time::Duration::from_millis(10));
        inbox.sweep_expired_windows().unwrap();

        let handler = AdminHandler::new(retry_tracker, inbox, rebuild_manager);

        // Filter by expired status
        let expired = handler
            .list_inbox_windows(InboxWindowFilter {
                status: Some(WindowStatus::Expired),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].status, "expired");

        // Filter by pending status (should return empty since it's expired)
        let pending = handler
            .list_inbox_windows(InboxWindowFilter {
                status: Some(WindowStatus::Pending),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(pending.is_empty());
    }

    // -- trigger_rebuild tests --

    #[tokio::test]
    async fn trigger_rebuild_starts_rebuild() {
        let retry_tracker = InMemoryRetryTracker::new();
        let inbox = InMemoryInbox::new();
        let projection_store = InMemoryProjectionStore::new();
        projection_store
            .set_checkpoint_sync("station_inventory", Version::from_u64(10))
            .unwrap();
        let rebuild_manager = InMemoryProjectionRebuildManager::new(projection_store);

        let handler = AdminHandler::new(retry_tracker, inbox, rebuild_manager.clone());

        let response = handler.trigger_rebuild("station_inventory").await.unwrap();
        assert_eq!(response.projection_id, "station_inventory");
        assert_eq!(response.status, "rebuild_started");

        // Verify the rebuild is in progress
        assert!(rebuild_manager
            .is_rebuilding("station_inventory")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn trigger_rebuild_idempotent_rejects_double_rebuild() {
        let retry_tracker = InMemoryRetryTracker::new();
        let inbox = InMemoryInbox::new();
        let projection_store = InMemoryProjectionStore::new();
        projection_store
            .set_checkpoint_sync("station_inventory", Version::from_u64(10))
            .unwrap();
        let rebuild_manager = InMemoryProjectionRebuildManager::new(projection_store);

        let handler = AdminHandler::new(retry_tracker, inbox, rebuild_manager);

        // First rebuild succeeds
        handler.trigger_rebuild("station_inventory").await.unwrap();

        // Second rebuild fails (already rebuilding)
        let err = handler.trigger_rebuild("station_inventory").await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn trigger_rebuild_resets_checkpoint() {
        let retry_tracker = InMemoryRetryTracker::new();
        let inbox = InMemoryInbox::new();
        let projection_store = InMemoryProjectionStore::new();
        projection_store
            .set_checkpoint_sync("station_inventory", Version::from_u64(10))
            .unwrap();
        let rebuild_manager = InMemoryProjectionRebuildManager::new(projection_store);

        let handler = AdminHandler::new(retry_tracker, inbox, rebuild_manager.clone());

        handler.trigger_rebuild("station_inventory").await.unwrap();

        // Checkpoint should be reset to initial (full replay, rebuild_from = None)
        let checkpoint = handler
            .get_rebuild_checkpoint("station_inventory")
            .await
            .unwrap();
        assert_eq!(checkpoint, Version::initial());
    }

    // -- response serialization tests --

    #[test]
    fn retry_status_response_serializes_to_json() {
        let response = RetryStatusResponse {
            message_id: Uuid::new_v4(),
            handler_id: "unloading_handler".to_owned(),
            attempts: 2,
            last_attempted: Utc::now(),
        };

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["handler_id"], "unloading_handler");
        assert_eq!(json["attempts"], 2);
        assert!(json["message_id"].is_string());
    }

    #[test]
    fn inbox_window_response_serializes_to_json() {
        let response = InboxWindowResponse {
            handler_id: "unloading_handler".to_owned(),
            correlation_key: Uuid::new_v4(),
            window_id: Uuid::new_v4(),
            status: "pending".to_owned(),
            message_count: 3,
            expires_in_ms: Some(145_000),
            created_at: Utc::now(),
        };

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["handler_id"], "unloading_handler");
        assert_eq!(json["status"], "pending");
        assert_eq!(json["message_count"], 3);
        assert_eq!(json["expires_in_ms"], 145_000);
    }

    #[test]
    fn rebuild_response_serializes_to_json() {
        let response = RebuildResponse {
            projection_id: "station_inventory".to_owned(),
            status: "rebuild_started".to_owned(),
        };

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["projection_id"], "station_inventory");
        assert_eq!(json["status"], "rebuild_started");
    }
}
