use bytes::Bytes;
use uuid::Uuid;

use crate::error::{DeadLetterError, RetryError};
use crate::memory::dead_letter::InMemoryDeadLetterStore;
use crate::memory::retry_tracker::InMemoryRetryTracker;
use crate::traits::retry_tracker::RetryTracker;
use crate::AggregateId;

/// The default maximum number of retry attempts before a message is dead-lettered.
pub const DEFAULT_MAX_RETRIES: u32 = 3;

/// Outcome of a retry-or-dead-letter decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome {
    /// The message should be retried. Contains the current attempt count.
    Retry { attempt: u32 },
    /// The message has reached max retries and was dead-lettered.
    DeadLettered,
}

/// Error type for [`RetryPolicy`] operations.
#[derive(Debug, thiserror::Error)]
pub enum RetryPolicyError {
    #[error("retry tracker error: {0}")]
    Tracker(#[from] RetryError),
    #[error("dead letter store error: {0}")]
    DeadLetter(#[from] DeadLetterError),
}

/// Coordinates retry tracking and dead-letter escalation.
///
/// On each failure the caller calls [`record_failure`]. The policy increments
/// the retry counter and, when the count reaches `max_retries`, writes the
/// message to the dead-letter store and removes the retry record.
///
/// `max_retries` is configurable via [`ServiceBuilder`] (default: 3).
#[derive(Clone)]
pub struct RetryPolicy {
    tracker: InMemoryRetryTracker,
    dead_letters: InMemoryDeadLetterStore,
    max_retries: u32,
}

impl RetryPolicy {
    /// Create a new retry policy with the given components and limit.
    pub fn new(
        tracker: InMemoryRetryTracker,
        dead_letters: InMemoryDeadLetterStore,
        max_retries: u32,
    ) -> Self {
        Self {
            tracker,
            dead_letters,
            max_retries,
        }
    }

    /// Create a retry policy with [`DEFAULT_MAX_RETRIES`].
    pub fn with_defaults(
        tracker: InMemoryRetryTracker,
        dead_letters: InMemoryDeadLetterStore,
    ) -> Self {
        Self::new(tracker, dead_letters, DEFAULT_MAX_RETRIES)
    }

    /// The configured maximum number of retries.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Record a processing failure for the given message.
    ///
    /// - Increments the attempt counter.
    /// - If the new count reaches `max_retries`, writes the message to the
    ///   dead-letter store and removes the retry record.
    /// - Returns [`RetryOutcome::Retry`] or [`RetryOutcome::DeadLettered`].
    pub fn record_failure(
        &self,
        message_id: Uuid,
        handler_id: &str,
        aggregate_id: &AggregateId,
        payload: Bytes,
        error: &str,
    ) -> Result<RetryOutcome, RetryPolicyError> {
        let attempts = self.tracker.increment(message_id, handler_id)?;

        if attempts >= self.max_retries {
            self.dead_letters
                .store(message_id, handler_id, aggregate_id, payload, error)?;
            self.tracker.remove(message_id)?;
            return Ok(RetryOutcome::DeadLettered);
        }

        Ok(RetryOutcome::Retry { attempt: attempts })
    }

    /// Clear the retry record for a message that has been successfully processed.
    pub fn clear(&self, message_id: Uuid) -> Result<(), RetryPolicyError> {
        self.tracker.remove(message_id)?;
        Ok(())
    }

    /// Access the underlying retry tracker.
    pub fn tracker(&self) -> &InMemoryRetryTracker {
        &self.tracker
    }

    /// Access the underlying dead-letter store.
    pub fn dead_letters(&self) -> &InMemoryDeadLetterStore {
        &self.dead_letters
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AggregateId;

    fn make_policy(max_retries: u32) -> RetryPolicy {
        RetryPolicy::new(
            InMemoryRetryTracker::new(),
            InMemoryDeadLetterStore::new(),
            max_retries,
        )
    }

    #[test]
    fn returns_retry_below_max() {
        let policy = make_policy(3);
        let msg = Uuid::new_v4();
        let agg = AggregateId::new();

        let outcome = policy
            .record_failure(msg, "h1", &agg, Bytes::from_static(b"{}"), "conflict")
            .unwrap();
        assert_eq!(outcome, RetryOutcome::Retry { attempt: 1 });

        let outcome = policy
            .record_failure(msg, "h1", &agg, Bytes::from_static(b"{}"), "conflict")
            .unwrap();
        assert_eq!(outcome, RetryOutcome::Retry { attempt: 2 });
    }

    #[test]
    fn dead_letters_at_max() {
        let policy = make_policy(3);
        let msg = Uuid::new_v4();
        let agg = AggregateId::new();
        let payload = Bytes::from_static(b"{}");

        // Attempts 1 and 2: Retry
        policy
            .record_failure(msg, "h1", &agg, payload.clone(), "conflict")
            .unwrap();
        policy
            .record_failure(msg, "h1", &agg, payload.clone(), "conflict")
            .unwrap();

        // Attempt 3: dead-lettered
        let outcome = policy
            .record_failure(msg, "h1", &agg, payload, "conflict")
            .unwrap();
        assert_eq!(outcome, RetryOutcome::DeadLettered);

        // Retry record removed
        assert!(policy.tracker().get(msg).unwrap().is_none());

        // Dead letter created
        let letters = policy.dead_letters().list(Some("h1")).unwrap();
        assert_eq!(letters.len(), 1);
        assert_eq!(letters[0].message_id, msg);
    }

    #[test]
    fn dead_letters_at_max_one() {
        let policy = make_policy(1);
        let msg = Uuid::new_v4();
        let agg = AggregateId::new();

        let outcome = policy
            .record_failure(msg, "h1", &agg, Bytes::from_static(b"{}"), "boom")
            .unwrap();
        assert_eq!(outcome, RetryOutcome::DeadLettered);
    }

    #[test]
    fn clear_removes_retry_record() {
        let policy = make_policy(3);
        let msg = Uuid::new_v4();
        let agg = AggregateId::new();

        policy
            .record_failure(msg, "h1", &agg, Bytes::from_static(b"{}"), "conflict")
            .unwrap();
        assert!(policy.tracker().get(msg).unwrap().is_some());

        policy.clear(msg).unwrap();
        assert!(policy.tracker().get(msg).unwrap().is_none());
    }

    #[test]
    fn requeue_clears_retry_count() {
        // Simulate: message fails 2 times, then gets dead-lettered on 3rd
        let policy = make_policy(3);
        let msg = Uuid::new_v4();
        let agg = AggregateId::new();
        let payload = Bytes::from_static(b"{}");

        policy
            .record_failure(msg, "h1", &agg, payload.clone(), "conflict")
            .unwrap();
        policy
            .record_failure(msg, "h1", &agg, payload.clone(), "conflict")
            .unwrap();
        let outcome = policy
            .record_failure(msg, "h1", &agg, payload.clone(), "conflict")
            .unwrap();
        assert_eq!(outcome, RetryOutcome::DeadLettered);

        // Requeue the dead letter
        let letters = policy.dead_letters().list(None).unwrap();
        assert_eq!(letters.len(), 1);
        let dl_id = letters[0].id;
        policy.dead_letters().requeue(dl_id).unwrap();

        // After requeue, the retry record was already cleaned up.
        // A fresh failure starts at attempt 1 again.
        let outcome = policy
            .record_failure(msg, "h1", &agg, payload, "conflict again")
            .unwrap();
        assert_eq!(outcome, RetryOutcome::Retry { attempt: 1 });
    }

    #[test]
    fn default_max_retries_is_three() {
        let policy =
            RetryPolicy::with_defaults(InMemoryRetryTracker::new(), InMemoryDeadLetterStore::new());
        assert_eq!(policy.max_retries(), DEFAULT_MAX_RETRIES);
        assert_eq!(policy.max_retries(), 3);
    }

    #[test]
    fn independent_messages_have_separate_counts() {
        let policy = make_policy(3);
        let msg_a = Uuid::new_v4();
        let msg_b = Uuid::new_v4();
        let agg = AggregateId::new();
        let payload = Bytes::from_static(b"{}");

        policy
            .record_failure(msg_a, "h1", &agg, payload.clone(), "err")
            .unwrap();
        policy
            .record_failure(msg_a, "h1", &agg, payload.clone(), "err")
            .unwrap();

        // msg_b is at attempt 1, msg_a is at attempt 2
        let outcome_b = policy
            .record_failure(msg_b, "h1", &agg, payload.clone(), "err")
            .unwrap();
        assert_eq!(outcome_b, RetryOutcome::Retry { attempt: 1 });

        let attempt_a = policy.tracker().get(msg_a).unwrap().unwrap();
        assert_eq!(attempt_a.attempts, 2);
    }
}
