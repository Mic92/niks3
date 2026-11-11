-- +goose Up
-- +goose StatementBegin

-- Counts the times the farm lead was granted. A scheduler tags what it tells
-- its workers with the number it got, so they can tell it from its predecessor.
CREATE SEQUENCE farm_lead_generation;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP SEQUENCE farm_lead_generation;

-- +goose StatementEnd
