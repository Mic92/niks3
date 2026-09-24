-- +goose up

-- +goose statementbegin
CREATE OR REPLACE FUNCTION commit_pending_closure(closure_id bigint)
RETURNS void AS $$
DECLARE
    is_inserted BOOLEAN;
    closure_key VARCHAR;
    now timestamp without time zone := timezone('UTC', now());
BEGIN
    -- Hold the pending closure for the rest of the commit. Each statement
    -- below reads on a snapshot of its own, and without the lock GC's pending
    -- cleanup could delete the pending rows between the closure insert and
    -- the object upsert: the commit would succeed having recorded none of the
    -- objects, which the cleanup had just tombstoned for the sweep. The
    -- cleanup skips a closure that is locked, and a closure the cleanup took
    -- first is gone once this lock is granted.
    SELECT key INTO closure_key
    FROM pending_closures WHERE id = closure_id
    FOR UPDATE;

    if closure_key is null then
        RAISE EXCEPTION 'Closure does not exist: id=%', closure_id;
    end if;

    -- Commit the pending closure and capture the inserted value
    INSERT INTO closures (updated_at, key)
    VALUES (now, closure_key)
    ON CONFLICT (key)
    DO UPDATE SET updated_at = now
    RETURNING (xmax = 0) AS is_inserted
    INTO is_inserted;

    -- Commit the pending objects with their references. Rows are upserted in
    -- key order so concurrent commits of closures that share objects take
    -- their row locks in the same order and cannot deadlock.
    INSERT INTO objects (key, refs, size)
    SELECT key, refs, size FROM pending_objects
    WHERE pending_closure_id = closure_id
    ORDER BY key
    ON CONFLICT (key)
    DO UPDATE SET
        -- If object exists, merge references (union of arrays, removing duplicates)
        refs = (
            SELECT ARRAY(
                SELECT DISTINCT unnest(
                    objects.refs || EXCLUDED.refs
                )
            )
        ),
        -- Keep an existing size; set it only when currently unknown.
        size = COALESCE(objects.size, EXCLUDED.size),
        -- Resurrect previously tombstoned objects
        deleted_at = NULL,
        first_deleted_at = NULL;

    -- Delete the pending objects
    DELETE FROM pending_objects WHERE pending_closure_id = closure_id;

    -- Delete the pending closure
    DELETE FROM pending_closures WHERE id = closure_id;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend
