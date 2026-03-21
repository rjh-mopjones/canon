# canon-deadletter-yugabyte

Part of the [Canon](https://github.com/rjh-mopjones/canon) event sourcing framework.

## Overview

YugabyteDB-backed implementations for the dead letter subsystem:

- **`YugabyteDeadLetterStore`** -- implements `DeadLetterStore` using the `dead_letters` table.
- **`YugabyteRetryTracker`** -- implements `RetryTracker` using the `retry_attempts` table.

## DeadLetterStore

Stores messages that have exhausted their retry budget. Provides admin operations for
inspection, requeue, and discard.

| Method    | SQL                                                        | Behaviour                                      |
|-----------|------------------------------------------------------------|-------------------------------------------------|
| `store`   | `INSERT INTO dead_letters (...) ON CONFLICT (id) DO NOTHING` | Persists a dead letter with attempts = 1        |
| `list`    | `SELECT ... FROM dead_letters [WHERE handler_id = $1]`     | Returns all (or filtered) dead letters          |
| `requeue` | `DELETE FROM dead_letters WHERE id = $1`                   | Removes the row; caller re-enters into inbox    |
| `discard` | `DELETE FROM dead_letters WHERE id = $1`                   | Permanently removes the row                     |

## RetryTracker

Crash-safe retry counting via the `retry_attempts` table. Retry counts survive process
restarts because they are persisted in YugabyteDB rather than held in memory.

### Table schema

```sql
CREATE TABLE retry_attempts (
    message_id     UUID         PRIMARY KEY,
    handler_id     TEXT         NOT NULL,
    attempts       INT          NOT NULL DEFAULT 0,
    last_attempted TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

## Construction

```rust
// From an existing sqlx::PgPool
let store = YugabyteDeadLetterStore::new(pool.clone());
let tracker = YugabyteRetryTracker::new(pool);

// From the YUGABYTE_URL environment variable
let store = YugabyteDeadLetterStore::from_env().await?;
let tracker = YugabyteRetryTracker::from_env().await?;
```

## Environment

| Variable       | Description                  |
|----------------|------------------------------|
| `YUGABYTE_URL` | YugabyteDB connection string |

## Dependencies

- [`canon-deadletter`](../canon-deadletter) -- `DeadLetterStore` trait
- [`canon-core`](../canon-core) -- `RetryTracker` trait, `RetryAttempt`, `AggregateId`, `DeadLetter`
