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
SELECT cleanup_pending_closures($1::int);

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
        o.key NOT IN (
            SELECT co.object_key
            FROM closure_objects AS co
            WHERE co.object_key = o.key
        )
        AND (
            o.key NOT IN (
                SELECT po.key
                FROM pending_objects AS po
                WHERE po.key = o.key
            )
        )
        AND (
            o.deleted_at IS NULL
            OR o.deleted_at < ct.now - INTERVAL '1 hour'
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
