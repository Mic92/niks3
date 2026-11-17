-- +goose Up
-- +goose StatementBegin

-- Which build of a store path the cache publishes. The first push to sign a
-- narinfo key claims it. A later push with another build of the same path
-- is answered with the claimed narinfo and publishes that one.
CREATE TABLE publish_claims
(
    key varchar(1024) PRIMARY KEY,
    nar_key varchar(1024) NOT NULL,
    refs varchar(1024)[] NOT NULL,
    narinfo jsonb NOT NULL
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP TABLE publish_claims;

-- +goose StatementEnd
