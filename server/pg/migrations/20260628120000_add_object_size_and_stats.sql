-- +goose Up
-- +goose StatementBegin

-- Client-reported object size in bytes. Nullable: not every object carries one
-- (e.g. tombstones). NULL is excluded from byte totals.
ALTER TABLE objects ADD COLUMN size bigint;
ALTER TABLE pending_objects ADD COLUMN size bigint;

-- Single-row running totals of the live object set, maintained by a trigger on
-- objects (functions/2_object_stats_trigger.sql) so inventory metrics avoid a
-- full-table scan.
CREATE TABLE object_stats (
    id boolean PRIMARY KEY DEFAULT true CHECK (id),
    object_count bigint NOT NULL DEFAULT 0,
    total_bytes bigint NOT NULL DEFAULT 0
);

-- Seed from existing live objects (total_bytes starts at 0: old rows have NULL size).
INSERT INTO object_stats (id, object_count, total_bytes)
SELECT
    true,
    count(*),
    coalesce(sum(size), 0)
FROM objects
WHERE deleted_at IS null;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP TABLE object_stats;
ALTER TABLE pending_objects DROP COLUMN size;
ALTER TABLE objects DROP COLUMN size;

-- +goose StatementEnd
