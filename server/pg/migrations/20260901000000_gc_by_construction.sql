-- +goose Up
-- +goose StatementBegin

-- The reaper holds row locks across the S3 delete, so one tombstone
-- timestamp suffices.
DROP INDEX IF EXISTS objects_first_deleted_at_idx;
ALTER TABLE objects DROP COLUMN first_deleted_at;
CREATE INDEX objects_deleted_at_idx ON objects (deleted_at, key) WHERE deleted_at IS NOT NULL;

-- Reaper and push both look up pending_objects by key.
CREATE INDEX IF NOT EXISTS pending_objects_key_idx ON pending_objects (key);

-- object_stats: one update per statement instead of per row. Created here
-- rather than in functions/ so startup takes no lock on objects.
DROP TRIGGER IF EXISTS object_stats_trigger ON objects;
DROP FUNCTION IF EXISTS object_stats_apply();

CREATE FUNCTION object_stats_apply_delta(d_count bigint, d_bytes bigint)
RETURNS void AS $$
BEGIN
    IF d_count <> 0 OR d_bytes <> 0 THEN
        UPDATE object_stats
        SET object_count = object_count + d_count,
            total_bytes = total_bytes + d_bytes
        WHERE id;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION object_stats_after_insert()
RETURNS trigger AS $$
BEGIN
    PERFORM object_stats_apply_delta(
        (SELECT count(*) FROM new_rows WHERE deleted_at IS NULL),
        (SELECT COALESCE(sum(size), 0)::bigint FROM new_rows WHERE deleted_at IS NULL)
    );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION object_stats_after_delete()
RETURNS trigger AS $$
BEGIN
    PERFORM object_stats_apply_delta(
        -(SELECT count(*) FROM old_rows WHERE deleted_at IS NULL),
        -(SELECT COALESCE(sum(size), 0)::bigint FROM old_rows WHERE deleted_at IS NULL)
    );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION object_stats_after_update()
RETURNS trigger AS $$
BEGIN
    PERFORM object_stats_apply_delta(
        (SELECT count(*) FROM new_rows WHERE deleted_at IS NULL)
        - (SELECT count(*) FROM old_rows WHERE deleted_at IS NULL),
        (SELECT COALESCE(sum(size), 0)::bigint FROM new_rows WHERE deleted_at IS NULL)
        - (SELECT COALESCE(sum(size), 0)::bigint FROM old_rows WHERE deleted_at IS NULL)
    );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER object_stats_insert
AFTER INSERT ON objects
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION object_stats_after_insert();

CREATE TRIGGER object_stats_delete
AFTER DELETE ON objects
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION object_stats_after_delete();

CREATE TRIGGER object_stats_update
AFTER UPDATE ON objects
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION object_stats_after_update();

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER object_stats_insert ON objects;
DROP TRIGGER object_stats_delete ON objects;
DROP TRIGGER object_stats_update ON objects;
DROP FUNCTION object_stats_after_insert();
DROP FUNCTION object_stats_after_delete();
DROP FUNCTION object_stats_after_update();
DROP FUNCTION object_stats_apply_delta(bigint, bigint);
DROP INDEX pending_objects_key_idx;
DROP INDEX objects_deleted_at_idx;
ALTER TABLE objects ADD COLUMN first_deleted_at timestamp;
-- +goose StatementEnd
