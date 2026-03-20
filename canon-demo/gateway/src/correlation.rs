use axum::http::HeaderMap;
use uuid::Uuid;

/// Header name for correlation ID propagation.
pub const CORRELATION_HEADER: &str = "x-correlation-id";

/// Extract the correlation ID from request headers, or generate a new one.
pub fn extract_correlation_id(headers: &HeaderMap) -> Uuid {
    headers
        .get(CORRELATION_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_else(Uuid::new_v4)
}
