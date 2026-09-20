-- +goose Up
-- +goose StatementBegin

-- The build farm has its own scheduler now.
DROP TABLE IF EXISTS claims;
DROP SEQUENCE IF EXISTS claim_token;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

CREATE UNLOGGED TABLE claims
(
    key bytea PRIMARY KEY,
    token bigint NOT NULL,
    heartbeat_at timestamptz NOT NULL DEFAULT now()
);

CREATE SEQUENCE claim_token;

-- +goose StatementEnd
