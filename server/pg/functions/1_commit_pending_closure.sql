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

    -- If the closure was inserted, commit the pending objects
    IF is_inserted THEN
        -- Commit the pending objects that we don't already have and the corresponding closure_objects
        WITH pending_keys AS (
          SELECT key
          FROM pending_objects
          WHERE pending_closure_id = closure_id
        ), insert_objects AS (
          INSERT INTO objects (key)
          SELECT key FROM pending_keys
          ON CONFLICT (key) DO NOTHING
          RETURNING key
        )
        INSERT INTO closure_objects (closure_key, object_key)
        SELECT closure_key, key
        FROM pending_keys;
    END IF;

    -- Delete the pending objects
    DELETE FROM pending_objects WHERE pending_closure_id = closure_id;

    -- Delete the pending closure
    DELETE FROM pending_closures WHERE id = closure_id;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend
