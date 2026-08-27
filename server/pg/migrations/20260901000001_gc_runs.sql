-- +goose Up
-- +goose StatementBegin
-- GC task state shared by all replicas. "running" is only meaningful while
-- the GC advisory lock is held.
CREATE TABLE gc_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state text NOT NULL,
    phase text NOT NULL DEFAULT '',
    params jsonb NOT NULL,
    stats jsonb NOT NULL DEFAULT '{}',
    error text NOT NULL DEFAULT '',
    started_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE gc_runs;
-- +goose StatementEnd
