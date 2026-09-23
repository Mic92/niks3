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

    -- Objects the push left out because they were live when it started may
    -- have been collected since. Everything under the roots must be a live
    -- row or one of the push's own pending objects.
    WITH RECURSIVE reach AS (
        SELECT unnest(push_roots) AS key
        UNION
        SELECT unnest(o.refs)
        FROM reach r
        CROSS JOIN LATERAL (
            SELECT refs FROM objects WHERE key = r.key AND deleted_at IS NULL
            UNION ALL
            SELECT refs FROM pending_objects
            WHERE pending_closure_id = push_id AND key = r.key
        ) o
    )
    SELECT r.key INTO missing
    FROM reach r
    WHERE NOT EXISTS (SELECT 1 FROM objects o WHERE o.key = r.key AND o.deleted_at IS NULL)
      AND NOT EXISTS (
          SELECT 1 FROM pending_objects p
          WHERE p.pending_closure_id = push_id AND p.key = r.key
      )
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
