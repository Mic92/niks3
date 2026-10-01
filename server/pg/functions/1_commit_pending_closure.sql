-- +goose up

-- +goose statementbegin
CREATE OR REPLACE FUNCTION commit_pending_closure(closure_id bigint)
RETURNS void AS $$
DECLARE
    closure_key VARCHAR;
    now timestamp without time zone := timezone('UTC', now());
BEGIN
    -- Lock the pending closure so the cleanup cannot delete its rows mid-commit.
    SELECT key INTO closure_key
    FROM pending_closures WHERE id = closure_id
    FOR UPDATE;

    if closure_key is null then
        RAISE EXCEPTION 'Closure does not exist: id=%', closure_id;
    end if;

    -- Commit the pending closure
    INSERT INTO closures (updated_at, key)
    VALUES (now, closure_key)
    ON CONFLICT (key)
    DO UPDATE SET updated_at = now;

    -- Commit the pending objects with their references
    INSERT INTO objects (key, refs, size)
    SELECT key, refs, size FROM pending_objects
    WHERE pending_closure_id = closure_id
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
        first_deleted_at = NULL
    -- most rows are live already; the conflict still locks them
    WHERE objects.deleted_at IS NOT NULL
        OR NOT (objects.refs @> EXCLUDED.refs)
        OR (objects.size IS NULL AND EXCLUDED.size IS NOT NULL);

    -- Delete the pending objects
    DELETE FROM pending_objects WHERE pending_closure_id = closure_id;

    -- Delete the pending closure
    DELETE FROM pending_closures WHERE id = closure_id;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend
