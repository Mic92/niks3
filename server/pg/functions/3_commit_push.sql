-- +goose up

-- +goose statementbegin
DROP FUNCTION IF EXISTS commit_push(bigint);

CREATE FUNCTION commit_push(push_id bigint)
RETURNS bigint AS $$
DECLARE
    push_roots varchar[];
    missing varchar;
    folded bigint;
BEGIN
    -- Locked, so cleanup cannot delete the push mid-commit.
    SELECT roots INTO push_roots FROM pending_closures WHERE id = push_id FOR UPDATE;

    IF push_roots IS NULL THEN
        RAISE EXCEPTION 'Push does not exist: id=%', push_id;
    END IF;

    -- EXCEPT is hashed whatever the planner expects from a new push's rows.
    WITH own AS MATERIALIZED (
        SELECT key, refs FROM pending_objects WHERE pending_closure_id = push_id
    )
    SELECT d.key INTO missing
    FROM (
        SELECT unnest(push_roots) AS key
        UNION
        SELECT unnest(refs) FROM own
        EXCEPT
        SELECT key FROM own
    ) AS d
    WHERE NOT EXISTS (SELECT 1 FROM objects o WHERE o.key = d.key AND o.deleted_at IS NULL)
    LIMIT 1;

    IF missing IS NOT NULL THEN
        RAISE EXCEPTION 'Push object missing: %', missing;
    END IF;

    -- DISTINCT: ON CONFLICT cannot update one row twice in a statement.
    INSERT INTO closures (updated_at, key)
    SELECT timezone('UTC', now()), key
    FROM (SELECT DISTINCT unnest(push_roots) AS key) AS root
    -- Byte order, as in commit_pending_closure.
    ORDER BY key COLLATE "C"
    ON CONFLICT (key) DO UPDATE SET updated_at = EXCLUDED.updated_at;

    -- The push's key is its first root, so this adds nothing new to closures.
    SELECT commit_pending_closure(push_id) INTO folded;

    RETURN folded;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend
