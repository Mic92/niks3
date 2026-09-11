-- +goose Up
-- +goose StatementBegin

-- Ephemeral build claims for the build farm. Unlogged because a lost claim
-- only costs a duplicate build.
CREATE UNLOGGED TABLE claims
(
    key bytea PRIMARY KEY,
    token bigint NOT NULL,
    heartbeat_at timestamptz NOT NULL DEFAULT now()
);

CREATE SEQUENCE claim_token;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP SEQUENCE claim_token;
DROP TABLE claims;

-- +goose StatementEnd
