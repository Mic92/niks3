-- +goose Up
-- +goose StatementBegin
ALTER TABLE pending_closures ADD COLUMN roots varchar(1024)[] NOT NULL DEFAULT '{}';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE pending_closures DROP COLUMN roots;
-- +goose StatementEnd
