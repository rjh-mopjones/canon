CREATE TABLE IF NOT EXISTS retry_attempts (
    message_id     UUID         PRIMARY KEY,
    handler_id     TEXT         NOT NULL,
    attempts       INT          NOT NULL DEFAULT 0,
    last_attempted TIMESTAMPTZ  NOT NULL DEFAULT now()
);
