-- +goose up

-- +goose statementbegin
-- Maintain object_stats running totals for the live object set.
--
-- A row contributes (1, size) while alive (deleted_at IS NULL), else (0, 0).
-- The totals change by contribution(new rows) - contribution(old rows), so
-- insert, delete, tombstone and resurrect need no branching on the operation.
-- The triggers run once per statement, because a commit touching n objects
-- would otherwise update the single stats row n times.
CREATE OR REPLACE FUNCTION object_stats_insert()
RETURNS trigger AS $$
BEGIN
    UPDATE object_stats s
    SET object_count = s.object_count + d.n,
        total_bytes = s.total_bytes + d.bytes
    FROM (
        SELECT count(*) AS n, coalesce(sum(size), 0) AS bytes
        FROM new_rows WHERE deleted_at IS NULL
    ) AS d
    WHERE s.id AND (d.n <> 0 OR d.bytes <> 0);

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION object_stats_delete()
RETURNS trigger AS $$
BEGIN
    UPDATE object_stats s
    SET object_count = s.object_count - d.n,
        total_bytes = s.total_bytes - d.bytes
    FROM (
        SELECT count(*) AS n, coalesce(sum(size), 0) AS bytes
        FROM old_rows WHERE deleted_at IS NULL
    ) AS d
    WHERE s.id AND (d.n <> 0 OR d.bytes <> 0);

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION object_stats_update()
RETURNS trigger AS $$
BEGIN
    UPDATE object_stats s
    SET object_count = s.object_count + n.n - o.n,
        total_bytes = s.total_bytes + n.bytes - o.bytes
    FROM (
        SELECT count(*) AS n, coalesce(sum(size), 0) AS bytes
        FROM new_rows WHERE deleted_at IS NULL
    ) AS n,
    (
        SELECT count(*) AS n, coalesce(sum(size), 0) AS bytes
        FROM old_rows WHERE deleted_at IS NULL
    ) AS o
    WHERE s.id AND (n.n <> o.n OR n.bytes <> o.bytes);

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- This file runs on every start. Creating or dropping a trigger locks the
-- whole table, and that lock waits for every transaction using objects
-- while holding every later query on it behind itself, so a replica starting
-- during a mark or a large commit would stall all pushes until that ended.
-- The triggers are created once; the functions above are replaced in place.
-- The row-level trigger they replace is dropped only where it still exists.
-- Any other change to a trigger's own definition needs a versioned migration.
DO $do$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'objects'::regclass AND tgname = 'object_stats_trigger'
    ) THEN
        DROP TRIGGER object_stats_trigger ON objects;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'objects'::regclass AND tgname = 'object_stats_insert'
    ) THEN
        CREATE TRIGGER object_stats_insert
        AFTER INSERT ON objects
        REFERENCING NEW TABLE AS new_rows
        FOR EACH STATEMENT EXECUTE FUNCTION object_stats_insert();
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'objects'::regclass AND tgname = 'object_stats_update'
    ) THEN
        CREATE TRIGGER object_stats_update
        AFTER UPDATE ON objects
        REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
        FOR EACH STATEMENT EXECUTE FUNCTION object_stats_update();
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'objects'::regclass AND tgname = 'object_stats_delete'
    ) THEN
        CREATE TRIGGER object_stats_delete
        AFTER DELETE ON objects
        REFERENCING OLD TABLE AS old_rows
        FOR EACH STATEMENT EXECUTE FUNCTION object_stats_delete();
    END IF;
END
$do$;

DROP FUNCTION IF EXISTS object_stats_apply();
-- +goose statementend
