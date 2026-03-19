CREATE TABLE snapshots (
    aggregate_id UUID        NOT NULL,
    version      BIGINT      NOT NULL,
    state        BYTEA       NOT NULL,
    taken_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (aggregate_id, version)
);
