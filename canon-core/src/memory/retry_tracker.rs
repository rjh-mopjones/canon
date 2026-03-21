use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use uuid::Uuid;

use crate::error::RetryError;
use crate::traits::retry_tracker::{RetryAttempt, RetryTracker};

/// In-memory implementation of [`RetryTracker`].
///
/// Suitable for testing. In production, the retry_attempts table lives in
/// YugabyteDB so that counts survive process crashes.
#[derive(Clone)]
pub struct InMemoryRetryTracker {
    inner: Arc<Mutex<HashMap<Uuid, RetryAttempt>>>,
}

impl InMemoryRetryTracker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryRetryTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryRetryTracker {
    /// List all retry attempts currently tracked.
    ///
    /// Returns a snapshot of all entries in the retry_attempts table.
    pub fn list_all(&self) -> Result<Vec<RetryAttempt>, RetryError> {
        let store = self.inner.lock().map_err(|_| RetryError::Poisoned)?;
        Ok(store.values().cloned().collect())
    }
}

impl RetryTracker for InMemoryRetryTracker {
    type Error = RetryError;

    fn increment(&self, message_id: Uuid, handler_id: &str) -> Result<u32, Self::Error> {
        let mut store = self.inner.lock().map_err(|_| RetryError::Poisoned)?;
        let entry = store.entry(message_id).or_insert_with(|| RetryAttempt {
            message_id,
            handler_id: handler_id.to_owned(),
            attempts: 0,
            last_attempted: Utc::now(),
        });
        entry.attempts += 1;
        entry.last_attempted = Utc::now();
        Ok(entry.attempts)
    }

    fn get(&self, message_id: Uuid) -> Result<Option<RetryAttempt>, Self::Error> {
        let store = self.inner.lock().map_err(|_| RetryError::Poisoned)?;
        Ok(store.get(&message_id).cloned())
    }

    fn remove(&self, message_id: Uuid) -> Result<(), Self::Error> {
        let mut store = self.inner.lock().map_err(|_| RetryError::Poisoned)?;
        store.remove(&message_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn increment_creates_entry_on_first_call() {
        let tracker = InMemoryRetryTracker::new();
        let msg_id = Uuid::new_v4();
        let count = tracker.increment(msg_id, "handler_a").unwrap();
        assert_eq!(count, 1);

        let attempt = tracker.get(msg_id).unwrap().unwrap();
        assert_eq!(attempt.attempts, 1);
        assert_eq!(attempt.handler_id, "handler_a");
    }

    #[test]
    fn increment_accumulates() {
        let tracker = InMemoryRetryTracker::new();
        let msg_id = Uuid::new_v4();

        assert_eq!(tracker.increment(msg_id, "h1").unwrap(), 1);
        assert_eq!(tracker.increment(msg_id, "h1").unwrap(), 2);
        assert_eq!(tracker.increment(msg_id, "h1").unwrap(), 3);

        let attempt = tracker.get(msg_id).unwrap().unwrap();
        assert_eq!(attempt.attempts, 3);
    }

    #[test]
    fn get_returns_none_for_unknown_message() {
        let tracker = InMemoryRetryTracker::new();
        let result = tracker.get(Uuid::new_v4()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn remove_deletes_entry() {
        let tracker = InMemoryRetryTracker::new();
        let msg_id = Uuid::new_v4();
        tracker.increment(msg_id, "h1").unwrap();
        assert!(tracker.get(msg_id).unwrap().is_some());

        tracker.remove(msg_id).unwrap();
        assert!(tracker.get(msg_id).unwrap().is_none());
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let tracker = InMemoryRetryTracker::new();
        let result = tracker.remove(Uuid::new_v4());
        assert!(result.is_ok());
    }
}
