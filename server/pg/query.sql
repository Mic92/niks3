-- name: InsertPendingClosure :one
INSERT INTO pending_closures (started_at, key)
VALUES (timezone('UTC', now()), $1)
RETURNING *;

-- name: InsertPendingObjects :copyfrom
INSERT INTO pending_objects (pending_closure_id, key) VALUES ($1, $2);

-- name: GetExistingObjects :many
WITH ct AS (
    SELECT timezone('UTC', now()) AS now
)

SELECT
    o.key AS key,
    CASE
        WHEN o.deleted_at IS NULL THEN NULL
        ELSE ct.now - o.deleted_at
    END AS deleted_at
FROM objects AS o, ct
WHERE key = any($1::varchar []);

-- name: CommitPendingClosure :exec
SELECT commit_pending_closure($1::bigint);

-- name: CleanupPendingClosures :exec
WITH cutoff_time AS (
    SELECT timezone('UTC', now()) - interval '1 second' * $1 AS time
),

old_closures AS (
    SELECT id
    FROM pending_closures, cutoff_time
    WHERE started_at < cutoff_time.time
),

-- Insert pending objects into objects table if they don't already exist
-- We mark them as deleted so they can be cleaned up later
inserted_objects AS (
    INSERT INTO objects (key, deleted_at)
    SELECT
        po.key,
        cutoff_time.time
    FROM pending_objects AS po
    JOIN old_closures oc ON po.pending_closure_id = oc.id, cutoff_time
    ON CONFLICT (key) DO NOTHING
    RETURNING key
),

-- Delete pending objects that were inserted into the objects table
deleted_pending_objects AS (
    DELETE FROM pending_objects
    USING old_closures
    WHERE pending_objects.pending_closure_id = old_closures.id
    RETURNING pending_closure_id
)

-- Delete pending closures older than the specified interval
-- This will cascade to pending_objects
DELETE FROM pending_closures
USING old_closures
WHERE pending_closures.id = old_closures.id;

-- name: GetClosure :one
SELECT updated_at FROM closures WHERE key = $1 LIMIT 1;

-- name: GetClosureObjects :many
SELECT object_key FROM closure_objects WHERE closure_key = $1;

-- name: DeleteClosures :exec
DELETE FROM closures WHERE updated_at < $1;

-- name: MarkObjectsForDeletion :many
WITH ct AS (
    SELECT timezone('UTC', now()) AS now
),

stale_objects AS (
    SELECT o.key
    FROM objects AS o, ct
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM closure_objects AS co
            WHERE co.object_key = o.key
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pending_objects AS po
            WHERE po.key = o.key
        )
        AND (
            o.deleted_at IS NULL
            OR o.deleted_at < ct.now - interval '1 hour'
        )
    FOR UPDATE
    LIMIT $1
)

UPDATE objects
SET deleted_at = ct.now
FROM stale_objects, ct
WHERE objects.key = stale_objects.key
RETURNING objects.key;

-- name: MarkObjectsAsActive :exec
UPDATE objects SET deleted_at = NULL WHERE key = any($1::varchar []);

-- name: DeleteObjects :exec
DELETE FROM objects WHERE key = any($1::varchar []);
