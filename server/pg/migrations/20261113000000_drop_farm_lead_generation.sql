-- +goose Up
-- +goose StatementBegin

DROP SEQUENCE IF EXISTS farm_lead_generation;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

CREATE SEQUENCE IF NOT EXISTS farm_lead_generation;

-- +goose StatementEnd
