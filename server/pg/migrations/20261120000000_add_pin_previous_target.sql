-- +goose Up
ALTER TABLE pins
ADD COLUMN previous_narinfo_key varchar(1024),
ADD COLUMN moved_at timestamptz;

-- +goose Down
ALTER TABLE pins
DROP COLUMN moved_at,
DROP COLUMN previous_narinfo_key;
