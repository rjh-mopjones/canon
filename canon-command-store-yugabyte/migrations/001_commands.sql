CREATE TABLE IF NOT EXISTS commands (
    command_id      UUID        PRIMARY KEY,
    aggregate_id    UUID        NOT NULL,
    command_type    TEXT        NOT NULL DEFAULT '',
    command_version INT         NOT NULL DEFAULT 1,
    payload         BYTEA       NOT NULL,
    correlation_id  UUID,
    causation_id    UUID,
    status          TEXT        NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS commands_aggregate_idx
    ON commands (aggregate_id, created_at);
