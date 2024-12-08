-- +goose up

-- +goose statementbegin
CREATE OR REPLACE FUNCTION commit_pending_closure(closure_id bigint)
RETURNS void AS $$
DECLARE
    is_inserted BOOLEAN;
    closure_key VARCHAR;
BEGIN
    -- Commit the pending closure and capture the inserted value
    WITH inserted_cte AS (
        INSERT INTO closures (updated_at, key)
        SELECT timezone('UTC', NOW()), key FROM pending_closures WHERE id = closure_id
        ON CONFLICT (key)
        DO UPDATE SET updated_at = timezone('UTC', NOW())
        RETURNING (xmax = 0) AS inserted, key
    )
    SELECT inserted, key INTO is_inserted, closure_key FROM inserted_cte;

    if closure_key is null then
        RAISE EXCEPTION 'Closure does not exist: id=%', closure_id;
    end if;

    -- log the closure key

    -- If the closure was inserted, commit the pending objects
    IF is_inserted THEN
        -- Commit the pending objects that we don't already have
        INSERT INTO objects (key)
        SELECT key FROM pending_objects WHERE pending_closure_id = closure_id
        ON CONFLICT (key)
        DO NOTHING;

        -- Commit the pending objects closure
        INSERT INTO closure_objects (closure_key, object_key)
        SELECT closure_key, key FROM pending_objects WHERE pending_closure_id = closure_id;
    END IF;

    -- Delete the pending objects
    DELETE FROM pending_objects WHERE pending_closure_id = closure_id;

    -- Delete the pending closure
    DELETE FROM pending_closures WHERE id = closure_id;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend
