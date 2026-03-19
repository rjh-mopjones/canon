CREATE TABLE inbox_messages (
    handler_id   TEXT        NOT NULL,
    message_id   UUID        NOT NULL,
    aggregate_id UUID        NOT NULL,
    message_type TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (handler_id, message_id)
);

CREATE TABLE inbox_windows (
    handler_id   TEXT        NOT NULL,
    aggregate_id UUID        NOT NULL,
    window_id    UUID        NOT NULL DEFAULT gen_random_uuid(),
    messages     JSONB       NOT NULL DEFAULT '[]',
    status       TEXT        NOT NULL DEFAULT 'pending',
    expires_at   TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (handler_id, aggregate_id)
);

CREATE TABLE processed_windows (
    window_id    UUID        PRIMARY KEY,
    handler_id   TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
