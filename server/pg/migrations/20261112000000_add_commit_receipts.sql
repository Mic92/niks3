-- +goose Up
-- +goose StatementBegin

-- A commit deletes its pending closure. The receipt lets a client that lost
-- the answer repeat the commit, until the cleanup of pending closures drops it.
CREATE TABLE commit_receipts
(
    pending_closure_id bigint PRIMARY KEY,
    committed_at timestamp NOT NULL
);
CREATE INDEX commit_receipts_committed_at_idx ON commit_receipts (committed_at);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP TABLE commit_receipts;

-- +goose StatementEnd
