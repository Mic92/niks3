-- +goose Up
-- +goose StatementBegin
DROP TRIGGER IF EXISTS object_stats_insert ON objects;
DROP TRIGGER IF EXISTS object_stats_update ON objects;
DROP TRIGGER IF EXISTS object_stats_delete ON objects;
DROP FUNCTION IF EXISTS object_stats_insert();
DROP FUNCTION IF EXISTS object_stats_update();
DROP FUNCTION IF EXISTS object_stats_delete();

-- The server counts the live objects again once this time has passed.
ALTER TABLE object_stats ADD COLUMN recount_at timestamp NOT NULL DEFAULT '-infinity';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE object_stats DROP COLUMN recount_at;
-- +goose StatementEnd
