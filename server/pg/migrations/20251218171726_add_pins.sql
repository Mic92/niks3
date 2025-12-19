-- Pins are named references to closures that are protected from garbage collection.
-- They allow deployments to retrieve specific store paths via:
--   nix-store -r $(curl cache.domain.tld/pins/<name>)

-- +goose Up
-- +goose StatementBegin

CREATE TABLE pins
(
    name varchar(256) PRIMARY KEY,
    narinfo_key varchar(1024) NOT NULL REFERENCES closures (key),
    store_path text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT timezone('UTC', now()),
    updated_at timestamptz NOT NULL DEFAULT timezone('UTC', now())
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP TABLE pins;

-- +goose StatementEnd
