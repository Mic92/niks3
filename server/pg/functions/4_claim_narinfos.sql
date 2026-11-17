-- +goose up

-- +goose statementbegin
-- Claims narinfo keys for the builds a push is about to publish.
--
-- A key is won if nobody claimed it, if the claim names the same NAR, or if
-- the claim is dead: GC has marked or deleted its NAR and no push holds it.
-- A dead build's narinfo row is marked as well, and the caller deletes the
-- file before it answers, so that no client sees it next to the new build.
--
-- A lost key makes the push publish the claimed build. Its narinfo row gets
-- the claimed references, and the claimed NAR becomes one of its pending
-- objects, which keeps GC away from it.
--
-- Outputs of one derivation are claimed together: rows are taken in byte
-- order, so a second push waits at the first key and then loses all of them.
--
-- Returns {"lost": {key: narinfo}, "taken": [key]}.
CREATE OR REPLACE FUNCTION claim_narinfos(
    push_id bigint, keys varchar[], nar_keys varchar[], narinfos jsonb[]
)
RETURNS jsonb AS $$
DECLARE
    now timestamp without time zone := timezone('UTC', now());
    won varchar[];
    taken varchar[];
    lost jsonb;
BEGIN
    -- Locked, so the cleanup cannot delete the push while it claims.
    PERFORM 1 FROM pending_closures WHERE id = push_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Push does not exist: id=%', push_id;
    END IF;

    WITH inserted AS (
        INSERT INTO publish_claims (key, nar_key, refs, narinfo)
        SELECT t.key, t.nar_key, po.refs, t.narinfo
        FROM unnest(keys, nar_keys, narinfos) AS t (key, nar_key, narinfo)
        INNER JOIN pending_objects AS po ON po.pending_closure_id = push_id AND po.key = t.key
        ORDER BY t.key COLLATE "C"
        ON CONFLICT (key) DO NOTHING
        RETURNING key
    )
    SELECT coalesce(array_agg(key), '{}') INTO won FROM inserted;

    -- Claims of others, on a snapshot taken after their inserts committed.
    PERFORM 1 FROM publish_claims
    WHERE key = any(keys) AND NOT (key = any(won))
    ORDER BY key COLLATE "C"
    FOR UPDATE;

    WITH replaced AS (
        UPDATE publish_claims AS c
        SET nar_key = t.nar_key, refs = po.refs, narinfo = t.narinfo
        FROM unnest(keys, nar_keys, narinfos) AS t (key, nar_key, narinfo)
        INNER JOIN pending_objects AS po ON po.pending_closure_id = push_id AND po.key = t.key
        WHERE c.key = t.key
            AND NOT (c.key = any(won))
            AND NOT EXISTS (SELECT 1 FROM objects AS o WHERE o.key = c.nar_key AND o.deleted_at IS NULL)
            AND NOT EXISTS (SELECT 1 FROM pending_objects AS held WHERE held.key = c.nar_key)
        RETURNING c.key
    )
    SELECT coalesce(array_agg(key), '{}') INTO taken FROM replaced;

    UPDATE objects
    SET deleted_at = now, first_deleted_at = coalesce(first_deleted_at, now)
    WHERE key = any(taken) AND deleted_at IS NULL;

    SELECT coalesce(jsonb_object_agg(c.key, c.narinfo), '{}') INTO lost
    FROM publish_claims AS c
    INNER JOIN unnest(keys, nar_keys) AS t (key, nar_key) ON t.key = c.key
    WHERE c.nar_key <> t.nar_key;

    UPDATE pending_objects AS po
    SET refs = c.refs
    FROM publish_claims AS c
    WHERE po.pending_closure_id = push_id AND po.key = c.key AND lost ? c.key;

    INSERT INTO pending_objects (pending_closure_id, key, refs)
    SELECT push_id, c.nar_key, '{}'
    FROM publish_claims AS c
    WHERE lost ? c.key
    ORDER BY c.nar_key COLLATE "C"
    ON CONFLICT DO NOTHING;

    RETURN jsonb_build_object('lost', lost, 'taken', to_jsonb(taken));
END;
$$ LANGUAGE plpgsql;
-- +goose statementend
