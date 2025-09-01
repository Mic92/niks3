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

    -- Commit the pending objects with their references
    INSERT INTO objects (key, refs)
    SELECT key, refs FROM pending_objects
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
        -- Resurrect previously tombstoned objects
        deleted_at = NULL;

    -- Delete the pending objects
    DELETE FROM pending_objects WHERE pending_closure_id = closure_id;

    -- Delete the pending closure
    DELETE FROM pending_closures WHERE id = closure_id;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend
