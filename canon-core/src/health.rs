//! Kubernetes-compatible health probe support for Canon services.
//!
//! Provides the [`HealthCheck`] trait for infrastructure dependency checking,
//! plus JSON-serializable response types that downstream crates (e.g. gateway)
//! can serve on `/health/live`, `/health/ready`, and `/health/startup` endpoints.
//!
//! Each infrastructure implementation (event store, snapshot store, etc.) implements
//! [`HealthCheck`]. [`ServiceBuilder`](crate::ServiceBuilder) collects them automatically,
//! and the resulting [`Service`](crate::Service) exposes them via
//! [`Service::health_checks()`](crate::Service::health_checks).
//!
//! # Endpoint semantics
//!
//! - **`/health/live`** — Liveness probe. Returns 200 if the process is running.
//!   No dependency checks. If the HTTP server can respond, it's alive.
//! - **`/health/ready`** — Readiness probe. Checks connectivity to all registered
//!   infrastructure. Returns 200 if all pass, 503 with per-dependency detail if any fail.
//! - **`/health/startup`** — Startup probe. Same checks as readiness.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde::Serialize;

// ── Trait ────────────────────────────────────────────────────────────────────

/// A health check for an infrastructure dependency.
///
/// Each infrastructure store (Cassandra, YugabyteDB, Kafka, etc.) implements
/// this trait. In-memory stores always return `Ok(())`.
#[async_trait]
pub trait HealthCheck: Send + Sync + 'static {
    /// A human-readable name for this dependency (e.g. `"cassandra"`, `"yugabyte"`).
    fn name(&self) -> &str;

    /// Check connectivity to the dependency.
    ///
    /// Returns `Ok(())` if the dependency is reachable and healthy, or
    /// `Err(description)` with a human-readable error message otherwise.
    async fn check(&self) -> Result<(), String>;
}

// ── Response types ──────────────────────────────────────────────────────────

/// Status of a single dependency health check.
#[derive(Debug, Clone, Serialize)]
pub struct HealthCheckDetail {
    /// `"ok"` or `"error"`.
    pub status: String,
    /// Latency of the health check in milliseconds.
    pub latency_ms: u64,
    /// Error message, if the check failed. `None` when healthy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response for `/health/live`.
#[derive(Debug, Clone, Serialize)]
pub struct LivenessResponse {
    /// Always `"ok"` — if we can build this response, the process is alive.
    pub status: String,
}

impl Default for LivenessResponse {
    fn default() -> Self {
        Self {
            status: "ok".to_string(),
        }
    }
}

/// Response for `/health/ready` and `/health/startup`.
#[derive(Debug, Clone, Serialize)]
pub struct ReadinessResponse {
    /// `"ready"` when all checks pass, `"not_ready"` when any fail.
    pub status: String,
    /// Per-dependency check results, keyed by dependency name.
    pub checks: HashMap<String, HealthCheckDetail>,
}

// ── Checker ─────────────────────────────────────────────────────────────────

/// Runs all registered health checks and produces response types.
///
/// Created by [`ServiceBuilder`](crate::ServiceBuilder) and accessible via
/// [`Service::health_checks()`](crate::Service::health_checks).
#[derive(Clone)]
pub struct HealthChecker {
    checks: Arc<Vec<Box<dyn HealthCheck>>>,
}

impl HealthChecker {
    /// Create a new `HealthChecker` from a list of health checks.
    pub fn new(checks: Vec<Box<dyn HealthCheck>>) -> Self {
        Self {
            checks: Arc::new(checks),
        }
    }

    /// Liveness check. Always returns `200 OK` — no dependency checks.
    pub fn liveness(&self) -> LivenessResponse {
        LivenessResponse::default()
    }

    /// Readiness check. Checks all registered dependencies.
    ///
    /// Returns a [`ReadinessResponse`] with per-dependency status and latency.
    /// The overall status is `"ready"` only when every check passes.
    pub async fn readiness(&self) -> ReadinessResponse {
        self.run_checks().await
    }

    /// Startup check. Identical to readiness — checks all registered dependencies.
    pub async fn startup(&self) -> ReadinessResponse {
        self.run_checks().await
    }

    /// Returns true when the overall readiness status is `"ready"`.
    pub async fn is_ready(&self) -> bool {
        self.readiness().await.status == "ready"
    }

    /// Run all checks and build the response.
    async fn run_checks(&self) -> ReadinessResponse {
        let mut checks = HashMap::new();
        let mut all_ok = true;

        for check in self.checks.iter() {
            let name = check.name().to_string();
            let start = Instant::now();
            let result = check.check().await;
            let latency_ms = start.elapsed().as_millis() as u64;

            let detail = match result {
                Ok(()) => {
                    tracing::debug!(dependency = %name, latency_ms, "health check passed");
                    HealthCheckDetail {
                        status: "ok".to_string(),
                        latency_ms,
                        error: None,
                    }
                }
                Err(err) => {
                    tracing::warn!(dependency = %name, latency_ms, error = %err, "health check failed");
                    all_ok = false;
                    HealthCheckDetail {
                        status: "error".to_string(),
                        latency_ms,
                        error: Some(err),
                    }
                }
            };

            checks.insert(name, detail);
        }

        let status = if all_ok {
            "ready".to_string()
        } else {
            "not_ready".to_string()
        };

        ReadinessResponse { status, checks }
    }
}

// ── In-memory health check ─────────────────────────────────────────────────

/// A health check that always succeeds. Used by in-memory store implementations.
pub struct InMemoryHealthCheck {
    name: String,
}

impl InMemoryHealthCheck {
    /// Create a new in-memory health check with the given dependency name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[async_trait]
impl HealthCheck for InMemoryHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Failing health check for tests ---------------------------------------

    struct FailingHealthCheck {
        name: String,
        error: String,
    }

    impl FailingHealthCheck {
        fn new(name: impl Into<String>, error: impl Into<String>) -> Self {
            Self {
                name: name.into(),
                error: error.into(),
            }
        }
    }

    #[async_trait]
    impl HealthCheck for FailingHealthCheck {
        fn name(&self) -> &str {
            &self.name
        }

        async fn check(&self) -> Result<(), String> {
            Err(self.error.clone())
        }
    }

    // -- Tests ----------------------------------------------------------------

    #[test]
    fn liveness_always_ok() {
        let checker = HealthChecker::new(vec![]);
        let response = checker.liveness();
        assert_eq!(response.status, "ok");
    }

    #[test]
    fn liveness_response_serializes() {
        let response = LivenessResponse::default();
        let json = serde_json::to_value(&response).expect("serialize");
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn readiness_all_pass() {
        let checks: Vec<Box<dyn HealthCheck>> = vec![
            Box::new(InMemoryHealthCheck::new("cassandra")),
            Box::new(InMemoryHealthCheck::new("yugabyte")),
            Box::new(InMemoryHealthCheck::new("kafka")),
        ];
        let checker = HealthChecker::new(checks);
        let response = checker.readiness().await;

        assert_eq!(response.status, "ready");
        assert_eq!(response.checks.len(), 3);
        assert_eq!(response.checks["cassandra"].status, "ok");
        assert_eq!(response.checks["yugabyte"].status, "ok");
        assert_eq!(response.checks["kafka"].status, "ok");
        assert!(response.checks["cassandra"].error.is_none());
    }

    #[tokio::test]
    async fn readiness_one_fails() {
        let checks: Vec<Box<dyn HealthCheck>> = vec![
            Box::new(InMemoryHealthCheck::new("cassandra")),
            Box::new(FailingHealthCheck::new("yugabyte", "connection refused")),
            Box::new(InMemoryHealthCheck::new("kafka")),
        ];
        let checker = HealthChecker::new(checks);
        let response = checker.readiness().await;

        assert_eq!(response.status, "not_ready");
        assert_eq!(response.checks.len(), 3);
        assert_eq!(response.checks["cassandra"].status, "ok");
        assert_eq!(response.checks["yugabyte"].status, "error");
        assert_eq!(
            response.checks["yugabyte"].error.as_deref(),
            Some("connection refused")
        );
        assert_eq!(response.checks["kafka"].status, "ok");
    }

    #[tokio::test]
    async fn readiness_all_fail() {
        let checks: Vec<Box<dyn HealthCheck>> = vec![
            Box::new(FailingHealthCheck::new("cassandra", "timeout")),
            Box::new(FailingHealthCheck::new("yugabyte", "auth failed")),
        ];
        let checker = HealthChecker::new(checks);
        let response = checker.readiness().await;

        assert_eq!(response.status, "not_ready");
        assert_eq!(response.checks["cassandra"].status, "error");
        assert_eq!(
            response.checks["cassandra"].error.as_deref(),
            Some("timeout")
        );
        assert_eq!(response.checks["yugabyte"].status, "error");
        assert_eq!(
            response.checks["yugabyte"].error.as_deref(),
            Some("auth failed")
        );
    }

    #[tokio::test]
    async fn readiness_empty_checks_is_ready() {
        let checker = HealthChecker::new(vec![]);
        let response = checker.readiness().await;

        assert_eq!(response.status, "ready");
        assert!(response.checks.is_empty());
    }

    #[tokio::test]
    async fn startup_delegates_to_run_checks() {
        let checks: Vec<Box<dyn HealthCheck>> =
            vec![Box::new(InMemoryHealthCheck::new("cassandra"))];
        let checker = HealthChecker::new(checks);
        let response = checker.startup().await;

        assert_eq!(response.status, "ready");
        assert_eq!(response.checks.len(), 1);
        assert_eq!(response.checks["cassandra"].status, "ok");
    }

    #[tokio::test]
    async fn is_ready_returns_true_when_all_pass() {
        let checks: Vec<Box<dyn HealthCheck>> = vec![Box::new(InMemoryHealthCheck::new("test"))];
        let checker = HealthChecker::new(checks);
        assert!(checker.is_ready().await);
    }

    #[tokio::test]
    async fn is_ready_returns_false_when_any_fail() {
        let checks: Vec<Box<dyn HealthCheck>> =
            vec![Box::new(FailingHealthCheck::new("test", "down"))];
        let checker = HealthChecker::new(checks);
        assert!(!checker.is_ready().await);
    }

    #[tokio::test]
    async fn readiness_records_latency() {
        let checks: Vec<Box<dyn HealthCheck>> = vec![Box::new(InMemoryHealthCheck::new("fast"))];
        let checker = HealthChecker::new(checks);
        let response = checker.readiness().await;

        // In-memory checks should complete in well under 1 second.
        assert!(response.checks["fast"].latency_ms < 1000);
    }

    #[test]
    fn readiness_response_serializes() {
        let mut checks = HashMap::new();
        checks.insert(
            "cassandra".to_string(),
            HealthCheckDetail {
                status: "ok".to_string(),
                latency_ms: 2,
                error: None,
            },
        );
        checks.insert(
            "yugabyte".to_string(),
            HealthCheckDetail {
                status: "error".to_string(),
                latency_ms: 150,
                error: Some("connection refused".to_string()),
            },
        );

        let response = ReadinessResponse {
            status: "not_ready".to_string(),
            checks,
        };

        let json = serde_json::to_value(&response).expect("serialize");
        assert_eq!(json["status"], "not_ready");
        assert_eq!(json["checks"]["cassandra"]["status"], "ok");
        assert_eq!(json["checks"]["cassandra"]["latency_ms"], 2);
        // `error` should be absent for ok checks (skip_serializing_if)
        assert!(json["checks"]["cassandra"].get("error").is_none());
        assert_eq!(json["checks"]["yugabyte"]["status"], "error");
        assert_eq!(json["checks"]["yugabyte"]["error"], "connection refused");
    }

    #[test]
    fn in_memory_health_check_name() {
        let check = InMemoryHealthCheck::new("test-store");
        assert_eq!(check.name(), "test-store");
    }

    #[tokio::test]
    async fn in_memory_health_check_always_ok() {
        let check = InMemoryHealthCheck::new("test-store");
        assert!(check.check().await.is_ok());
    }

    #[test]
    fn health_checker_is_clone() {
        let checker = HealthChecker::new(vec![]);
        let _cloned = checker.clone();
    }
}
