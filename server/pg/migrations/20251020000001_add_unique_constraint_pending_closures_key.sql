-- Add UNIQUE constraint on pending_closures.key to prevent duplicate closures from retry requests
-- +goose Up
-- +goose StatementBegin

-- First, clean up any existing duplicates (keep the oldest pending_closure for each key)
DELETE FROM pending_closures
WHERE id NOT IN (
    SELECT MIN(id)
    FROM pending_closures
    GROUP BY key
);

-- Now add the UNIQUE constraint
ALTER TABLE pending_closures ADD CONSTRAINT pending_closures_key_unique UNIQUE (key);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

ALTER TABLE pending_closures DROP CONSTRAINT pending_closures_key_unique;

-- +goose StatementEnd
