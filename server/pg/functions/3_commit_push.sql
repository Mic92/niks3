-- +goose up

-- +goose statementbegin
CREATE OR REPLACE FUNCTION commit_push(push_id bigint)
RETURNS void AS $$
DECLARE
    push_roots varchar[];
    missing varchar;
BEGIN
    SELECT roots INTO push_roots FROM pending_closures WHERE id = push_id;

    IF push_roots IS NULL THEN
        RAISE EXCEPTION 'Push does not exist: id=%', push_id;
    END IF;

    WITH own AS MATERIALIZED (
        SELECT key, refs FROM pending_objects WHERE pending_closure_id = push_id
    )
    SELECT d.key INTO missing
    FROM (
        SELECT unnest(push_roots) AS key
        UNION
        SELECT unnest(refs) FROM own
    ) AS d
    WHERE NOT EXISTS (SELECT 1 FROM objects o WHERE o.key = d.key AND o.deleted_at IS NULL)
      AND NOT EXISTS (SELECT 1 FROM own WHERE own.key = d.key)
    LIMIT 1;

    IF missing IS NOT NULL THEN
        RAISE EXCEPTION 'Push object missing: %', missing;
    END IF;

    INSERT INTO closures (updated_at, key)
    SELECT timezone('UTC', now()), unnest(push_roots)
    ON CONFLICT (key) DO UPDATE SET updated_at = EXCLUDED.updated_at;

    -- The push's key is its first root, so this adds nothing new to closures.
    PERFORM commit_pending_closure(push_id);
END;
$$ LANGUAGE plpgsql;
-- +goose statementend
