//! Auth gate middleware.
//!
//! When `CANON_AUTH_PASSWORD` is set, all requests require authentication via one of:
//! 1. `X-Canon-Debug` header matching `CANON_DEBUG_KEY` (bypasses all auth)
//! 2. `X-Canon-Auth` header matching the password (sets `canon_auth` cookie)
//! 3. Valid `canon_auth` cookie
//!
//! When `CANON_AUTH_PASSWORD` is NOT set, all requests pass through (public mode).

use axum::{
    body::Body,
    extract::Request,
    http::{header, HeaderValue, StatusCode},
    middleware::Next,
    response::Response,
};
use std::sync::OnceLock;

/// Header name for auth password.
const AUTH_HEADER: &str = "x-canon-auth";

/// Header name for debug key.
const DEBUG_HEADER: &str = "x-canon-debug";

/// Cookie name for session auth.
const AUTH_COOKIE: &str = "canon_auth";

/// Header returned when debug key is used (carries session info).
const SESSION_HEADER: &str = "x-canon-session";

/// Cached auth config loaded once from environment at first request.
static AUTH_CONFIG: OnceLock<AuthConfig> = OnceLock::new();

struct AuthConfig {
    /// If `None`, public mode (no auth required).
    password: Option<String>,
    /// Optional debug key that bypasses all auth.
    debug_key: Option<String>,
}

fn load_auth_config() -> &'static AuthConfig {
    AUTH_CONFIG.get_or_init(|| AuthConfig {
        password: std::env::var("CANON_AUTH_PASSWORD")
            .ok()
            .filter(|s| !s.is_empty()),
        debug_key: std::env::var("CANON_DEBUG_KEY")
            .ok()
            .filter(|s| !s.is_empty()),
    })
}

/// Extract a named cookie value from the `Cookie` header.
fn extract_cookie<'a>(cookie_header: &'a str, name: &str) -> Option<&'a str> {
    for pair in cookie_header.split(';') {
        let pair = pair.trim();
        if let Some(value) = pair.strip_prefix(name) {
            let value = value.trim_start();
            if let Some(value) = value.strip_prefix('=') {
                return Some(value.trim());
            }
        }
    }
    None
}

/// Auth gate middleware. See module docs for the auth flow.
///
/// The `/health` endpoint is always exempt from auth (needed for K8s probes).
pub async fn auth_gate(request: Request, next: Next) -> Result<Response<Body>, StatusCode> {
    let config = load_auth_config();

    // Public mode: no password configured, pass through.
    let password = match &config.password {
        Some(pw) => pw,
        None => return Ok(next.run(request).await),
    };

    // Health check is always public (K8s liveness/readiness probes).
    // WebSocket endpoint is always public (HTTP/2 WS doesn't carry cookies;
    // session-level auth is handled by the WS handler itself).
    let path = request.uri().path();
    if path == "/health" || path == "/events" {
        return Ok(next.run(request).await);
    }

    let headers = request.headers();

    // 1. Check X-Canon-Debug header against CANON_DEBUG_KEY.
    if let Some(debug_key) = &config.debug_key {
        if let Some(provided) = headers.get(DEBUG_HEADER) {
            if let Ok(provided_str) = provided.to_str() {
                if provided_str == debug_key {
                    let mut response = next.run(request).await;
                    // Return debug session info in response header.
                    if let Ok(val) = HeaderValue::from_str("debug-authenticated") {
                        response.headers_mut().insert(SESSION_HEADER, val);
                    }
                    return Ok(response);
                }
            }
        }
    }

    // 2. Check X-Canon-Auth header against password.
    if let Some(provided) = headers.get(AUTH_HEADER) {
        if let Ok(provided_str) = provided.to_str() {
            if provided_str == password {
                let mut response = next.run(request).await;
                // Set canon_auth cookie so subsequent browser requests are authenticated.
                let cookie_value = format!(
                    "{AUTH_COOKIE}={password}; Path=/; HttpOnly; Secure; SameSite=None; Max-Age=86400"
                );
                if let Ok(val) = HeaderValue::from_str(&cookie_value) {
                    response.headers_mut().insert(header::SET_COOKIE, val);
                }
                return Ok(response);
            }
        }
    }

    // 3. Check canon_auth cookie.
    if let Some(cookie_header) = headers.get(header::COOKIE) {
        if let Ok(cookie_str) = cookie_header.to_str() {
            if let Some(cookie_val) = extract_cookie(cookie_str, AUTH_COOKIE) {
                if cookie_val == password {
                    return Ok(next.run(request).await);
                }
            }
        }
    }

    // 4. Unauthorized.
    Err(StatusCode::UNAUTHORIZED)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_cookie_single() {
        assert_eq!(
            extract_cookie("canon_auth=secret123", "canon_auth"),
            Some("secret123")
        );
    }

    #[test]
    fn test_extract_cookie_multiple() {
        assert_eq!(
            extract_cookie("other=val; canon_auth=abc; more=def", "canon_auth"),
            Some("abc")
        );
    }

    #[test]
    fn test_extract_cookie_missing() {
        assert_eq!(
            extract_cookie("other=val; something=else", "canon_auth"),
            None
        );
    }

    #[test]
    fn test_extract_cookie_empty() {
        assert_eq!(extract_cookie("", "canon_auth"), None);
    }

    #[test]
    fn test_extract_cookie_partial_name_match() {
        // "canon_auth_extra" should NOT match "canon_auth"
        assert_eq!(extract_cookie("canon_auth_extra=val", "canon_auth"), None);
    }
}
