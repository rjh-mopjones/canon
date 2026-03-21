//! YugabyteDB-backed implementations for the dead letter subsystem.
//!
//! Currently provides [`YugabyteRetryTracker`], a crash-safe implementation of
//! [`RetryTracker`] backed by the `retry_attempts` table.

mod retry_tracker;

pub use retry_tracker::{YugabyteRetryTracker, YugabyteRetryTrackerError};
