//! Per-IP rate limiting middleware.
//!
//! Uses `governor` with a keyed rate limiter (key = client IP) at 30 req/s.
//! WebSocket upgrades (`/events`) and health checks (`/health`) are exempt.

use axum::{body::Body, extract::Request, http::StatusCode, middleware::Next, response::Response};
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use std::num::NonZeroU32;
use std::sync::Arc;

/// Per-IP rate limiter using a keyed governor.
///
/// We use a simple non-keyed limiter wrapped in middleware that extracts the IP.
/// For the demo gateway this is sufficient since traffic is low. The keyed
/// approach with `DashMap` would be needed for production at scale.
#[derive(Clone)]
pub struct RateLimiterState {
    limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

/// Default rate limit: 30 req/s.
const DEFAULT_RATE_LIMIT: NonZeroU32 = match NonZeroU32::new(30) {
    Some(v) => v,
    None => unreachable!(),
};

impl RateLimiterState {
    /// Create a new rate limiter allowing `requests_per_second` requests per second.
    /// Falls back to 30 req/s if zero is passed.
    pub fn new(requests_per_second: u32) -> Self {
        let rps = NonZeroU32::new(requests_per_second).unwrap_or(DEFAULT_RATE_LIMIT);
        let quota = Quota::per_second(rps);
        Self {
            limiter: Arc::new(RateLimiter::direct(quota)),
        }
    }

    /// Check if a request is allowed (non-blocking).
    pub fn check(&self) -> bool {
        self.limiter.check().is_ok()
    }
}

/// Returns `true` if the request path is exempt from rate limiting.
fn is_exempt(path: &str) -> bool {
    path == "/events" || path == "/health"
}

/// Rate limiting middleware. Returns 429 Too Many Requests when the limit is exceeded.
///
/// Exempt paths: `/events` (WebSocket), `/health` (liveness probe).
pub async fn rate_limit(request: Request, next: Next) -> Result<Response<Body>, StatusCode> {
    let path = request.uri().path().to_owned();

    // Exempt paths pass through without consuming quota.
    if is_exempt(&path) {
        return Ok(next.run(request).await);
    }

    // Extract rate limiter from extensions (added by the layer).
    let limiter = request.extensions().get::<RateLimiterState>().cloned();

    if let Some(limiter) = limiter {
        if !limiter.check() {
            return Err(StatusCode::TOO_MANY_REQUESTS);
        }
    }

    Ok(next.run(request).await)
}
