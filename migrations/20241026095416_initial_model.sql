-- +goose Up
-- +goose StatementBegin
CREATE TABLE closures
(
    nar_hash   char(32) primary key,
    updated_at timestamp not null
);

CREATE TABLE uploads
(
    id               int generated always as identity primary key,
    started_at       timestamp not null,
    closure_nar_hash char(32)  not null references closures (nar_hash)
);

CREATE TABLE objects
(
    nar_hash        char(32) primary key,
    reference_count integer not null
);

-- partial index to find objects with reference_count == 0
CREATE INDEX objects_reference_count_zero_idx ON objects (nar_hash) WHERE reference_count = 0;

CREATE TABLE IF NOT EXISTS closure_objects
(
    closure_nar_hash char(32) not null references closures (nar_hash),
    nar_hash         char(32) not null references objects (nar_hash)
);

CREATE INDEX closure_objects_closure_nar_hash_idx ON closure_objects (closure_nar_hash);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP INDEX closure_objects_closure_nar_hash_idx;
DROP TABLE closure_objects;
DROP INDEX objects_reference_count_zero_idx;
DROP TABLE objects;
DROP TABLE uploads;
DROP TABLE closures;

-- +goose StatementEnd
