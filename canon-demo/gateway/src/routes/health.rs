use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/health", get(health_check))
}

/// GET /health -- lightweight liveness/readiness probe for Kubernetes.
///
/// Returns 200 OK with no body. This endpoint is intentionally cheap:
/// it confirms the HTTP server is accepting connections without touching
/// any downstream infrastructure (YugabyteDB, Cassandra, Kafka).
async fn health_check() -> StatusCode {
    StatusCode::OK
}
