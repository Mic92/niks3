-- The GIN index on refs was never used by any query.
-- All recursive CTE queries use `o.key = ANY(cr.refs)` which uses the PK index on objects.key,
-- not the GIN index on refs. The GIN index only helps with `refs @> ARRAY[...]` queries
-- which don't exist in the codebase.

-- +goose Up
DROP INDEX IF EXISTS objects_refs_gin;

-- +goose Down
CREATE INDEX objects_refs_gin ON objects USING gin (refs);
