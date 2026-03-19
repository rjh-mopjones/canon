CREATE TABLE IF NOT EXISTS projections (
    projection_id TEXT        NOT NULL,
    aggregate_id  UUID        NOT NULL,
    state         JSONB       NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (projection_id, aggregate_id)
);

CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_id TEXT        PRIMARY KEY,
    last_version  BIGINT      NOT NULL DEFAULT 0,
    rebuilding    BOOLEAN     NOT NULL DEFAULT false,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
