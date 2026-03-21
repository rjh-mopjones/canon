# canon-deadletter-yugabyte

YugabyteDB-backed implementations for the dead letter subsystem.

## RetryTracker

[`YugabyteRetryTracker`] implements the [`RetryTracker`](../canon-core/src/traits/retry_tracker.rs)
trait using the `retry_attempts` table. Retry counts survive process restarts because they
are persisted in YugabyteDB rather than held in memory.

### Table schema

```sql
CREATE TABLE retry_attempts (
    message_id     UUID         PRIMARY KEY,
    handler_id     TEXT         NOT NULL,
    attempts       INT          NOT NULL DEFAULT 0,
    last_attempted TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

### Usage

```rust
use canon_deadletter_yugabyte::YugabyteRetryTracker;

let tracker = YugabyteRetryTracker::from_env().await?;
```

## Environment

| Variable       | Description                  |
|----------------|------------------------------|
| `YUGABYTE_URL` | YugabyteDB connection string |

## Dependencies

- [`canon-deadletter`](../canon-deadletter) -- `DeadLetterStore` trait
- [`canon-core`](../canon-core) -- `RetryTracker` trait, `RetryAttempt`
