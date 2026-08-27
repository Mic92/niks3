-- +goose up

-- +goose statementbegin
CREATE OR REPLACE FUNCTION commit_pending_closure(closure_id bigint)
RETURNS void AS $$
DECLARE
    is_inserted BOOLEAN;
    closure_key VARCHAR;
    now timestamp without time zone := timezone('UTC', now());
BEGIN
    -- Commit the pending closure and capture the inserted value
    INSERT INTO closures (updated_at, key)
    SELECT now, key FROM pending_closures WHERE id = closure_id
    ON CONFLICT (key)
    DO UPDATE SET updated_at = now
    RETURNING (xmax = 0) AS is_inserted, key AS closure_key
    INTO is_inserted, closure_key;

    if closure_key is null then
        RAISE EXCEPTION 'Closure does not exist: id=%', closure_id;
    end if;

    -- Upsert all closure objects, resurrecting tombstones. Skip no-op updates.
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
        deleted_at = NULL
    WHERE objects.deleted_at IS NOT NULL
        OR (objects.size IS NULL AND EXCLUDED.size IS NOT NULL)
        OR NOT (objects.refs @> EXCLUDED.refs);

    -- Delete the pending objects
    DELETE FROM pending_objects WHERE pending_closure_id = closure_id;

    -- Delete the pending closure
    DELETE FROM pending_closures WHERE id = closure_id;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend
