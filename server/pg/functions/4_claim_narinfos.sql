-- +goose up

-- +goose statementbegin
DROP FUNCTION IF EXISTS claim_narinfos(bigint, varchar[], varchar[], jsonb[]);

-- Claims narinfo keys for the builds a push is about to publish. A claim
-- lasts until a push commits the key or GC deletes it.
--
-- Per key the push learns one of:
--   won:       it publishes its own build.
--   lost:      another push claimed a different build. The push publishes
--              that one: its narinfo row references the claimed NAR in
--              place of its own, and the claimed NAR becomes one of its
--              pending objects, which keeps GC away from it.
--   published: a push committed the key meanwhile. Nothing is to publish.
--              The commit of this push leaves the NAR of the row as it is.
--
-- A claim is dead once GC has marked or deleted its NAR and no push holds
-- it. The next push wins the key.
--
-- taken lists won keys under which a build that GC has marked is still in
-- the bucket. The caller deletes these files before it answers, so that no
-- client sees them next to the new build.
--
-- Outputs of one derivation are claimed together: rows are taken in byte
-- order, so a second push waits at the first key and then loses all of them.
--
-- A push that wins every key outright is done after the insert. Anything
-- else needs in_transaction: the caller holds the claim rows until it has
-- deleted the taken files. Without it the call fails with SQLSTATE NK301.
--
-- Returns {"lost": {key: narinfo}, "published": {key: NAR key}, "taken": [key]}.
CREATE OR REPLACE FUNCTION claim_narinfos(
    push_id bigint, keys varchar[], nar_keys varchar[], narinfos jsonb[], in_transaction boolean
)
RETURNS jsonb AS $$
DECLARE
    won varchar[];
    replaced varchar[];
    taken varchar[];
    lost jsonb;
    lost_keys varchar[];
    published jsonb;
    published_keys varchar[];
BEGIN
    -- Locked, so the cleanup cannot delete the push while it claims.
    PERFORM 1 FROM pending_closures WHERE id = push_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Push does not exist: id=%', push_id;
    END IF;

    WITH inserted AS (
        INSERT INTO publish_claims (key, nar_key, narinfo)
        SELECT t.key, t.nar_key, t.narinfo
        FROM unnest(keys, nar_keys, narinfos) AS t (key, nar_key, narinfo)
        ORDER BY t.key COLLATE "C"
        ON CONFLICT (key) DO NOTHING
        RETURNING key
    )
    SELECT coalesce(array_agg(key), '{}') INTO won FROM inserted;

    -- Looked up after the insert, which waited for a commit of the same key.
    IF cardinality(won) = cardinality(keys) AND NOT EXISTS (
        SELECT 1 FROM objects AS o
        INNER JOIN unnest(keys, nar_keys) AS t (key, nar_key) ON t.key = o.key
        WHERE o.deleted_at IS NULL OR NOT (t.nar_key = any(o.refs))
    ) THEN
        RETURN '{}'::jsonb;
    END IF;

    IF NOT in_transaction THEN
        RAISE EXCEPTION 'claim needs a transaction' USING ERRCODE = 'NK301';
    END IF;

    SELECT
        coalesce(jsonb_object_agg(o.key, (SELECT r FROM unnest(o.refs) AS r WHERE r LIKE 'nar/%' LIMIT 1)), '{}'),
        coalesce(array_agg(o.key), '{}')
    INTO published, published_keys
    FROM objects AS o
    WHERE o.key = any(keys) AND o.deleted_at IS NULL;

    IF cardinality(published_keys) > 0 THEN
        DELETE FROM publish_claims WHERE key = any(published_keys) AND key = any(won);
    END IF;

    -- Claims of others, on a snapshot taken after their inserts committed.
    PERFORM 1 FROM publish_claims
    WHERE key = any(keys) AND NOT (key = any(won))
    ORDER BY key COLLATE "C"
    FOR UPDATE;

    SELECT coalesce(array_agg(c.key), '{}') INTO replaced
    FROM publish_claims AS c
    WHERE c.key = any(keys)
        AND NOT (c.key = any(won))
        AND NOT EXISTS (SELECT 1 FROM objects AS o WHERE o.key = c.nar_key AND o.deleted_at IS NULL)
        AND NOT EXISTS (SELECT 1 FROM pending_objects AS held WHERE held.key = c.nar_key);

    IF cardinality(replaced) > 0 THEN
        UPDATE publish_claims AS c
        SET nar_key = t.nar_key, narinfo = t.narinfo
        FROM unnest(keys, nar_keys, narinfos) AS t (key, nar_key, narinfo)
        WHERE c.key = t.key AND c.key = any(replaced);
    END IF;

    SELECT coalesce(jsonb_object_agg(c.key, c.narinfo), '{}'), coalesce(array_agg(c.key), '{}')
    INTO lost, lost_keys
    FROM publish_claims AS c
    INNER JOIN unnest(keys, nar_keys) AS t (key, nar_key) ON t.key = c.key
    WHERE c.nar_key <> t.nar_key;

    UPDATE pending_objects AS po
    SET refs = array_replace(po.refs, t.nar_key, c.nar_key)
    FROM publish_claims AS c
    INNER JOIN unnest(keys, nar_keys) AS t (key, nar_key) ON t.key = c.key
    WHERE po.pending_closure_id = push_id AND po.key = c.key AND c.key = any(lost_keys);

    INSERT INTO pending_objects (pending_closure_id, key, refs)
    SELECT push_id, c.nar_key, '{}'
    FROM publish_claims AS c
    WHERE c.key = any(lost_keys)
    ORDER BY c.nar_key COLLATE "C"
    ON CONFLICT DO NOTHING;

    SELECT coalesce(array_agg(o.key), '{}') INTO taken
    FROM objects AS o
    INNER JOIN unnest(keys, nar_keys) AS t (key, nar_key) ON t.key = o.key
    WHERE o.deleted_at IS NOT NULL
        AND NOT (t.nar_key = any(o.refs))
        AND (o.key = any(won) OR o.key = any(replaced));

    RETURN jsonb_build_object('lost', lost, 'published', published, 'taken', to_jsonb(taken));
END;
$$ LANGUAGE plpgsql;
-- +goose statementend
