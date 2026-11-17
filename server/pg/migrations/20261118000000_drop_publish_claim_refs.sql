-- +goose Up
ALTER TABLE publish_claims DROP COLUMN refs;

-- +goose Down
ALTER TABLE publish_claims ADD COLUMN refs varchar(1024)[] NOT NULL DEFAULT '{}';
