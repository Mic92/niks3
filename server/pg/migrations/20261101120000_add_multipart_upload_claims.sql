-- +goose Up
-- +goose StatementBegin
ALTER TABLE multipart_uploads
ADD COLUMN completing boolean NOT NULL DEFAULT false,
ADD COLUMN aborting boolean NOT NULL DEFAULT false;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE multipart_uploads
DROP COLUMN aborting,
DROP COLUMN completing;
-- +goose StatementEnd
