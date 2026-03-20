use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("bad request: {0}")]
    #[allow(dead_code)]
    BadRequest(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("event store error: {0}")]
    EventStore(#[from] canon_event_store::EventStoreError),

    #[error("snapshot store error: {0}")]
    SnapshotStore(#[from] canon_snapshot_store::SnapshotStoreError),

    #[error("dead letter error: {0}")]
    DeadLetter(#[from] canon_deadletter::DeadLetterError),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            GatewayError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            GatewayError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            GatewayError::Database(_)
            | GatewayError::EventStore(_)
            | GatewayError::SnapshotStore(_)
            | GatewayError::DeadLetter(_)
            | GatewayError::Serialization(_)
            | GatewayError::Internal(_) => {
                tracing::error!(error = %self, "gateway error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_owned(),
                )
            }
        };

        (status, serde_json::json!({ "error": message }).to_string()).into_response()
    }
}
