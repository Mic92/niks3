-- name: InsertPendingClosure :one
INSERT INTO pending_closures (started_at, key)
VALUES (timezone('UTC', now()), $1)
RETURNING *;

-- name: InsertPendingObjects :copyfrom
INSERT INTO pending_objects (pending_closure_id, key, refs) VALUES ($1, $2, $3);

-- name: GetPendingObjectKeys :many
SELECT key FROM pending_objects
WHERE pending_closure_id = $1;

-- name: GetExistingObjects :many
WITH ct AS (
    SELECT timezone('UTC', now()) AS now
)

SELECT
    o.key AS key,
    (CASE
        WHEN o.first_deleted_at IS NULL THEN NULL
        ELSE ct.now - o.first_deleted_at
    END)::interval AS deleted_at
FROM objects AS o, ct
WHERE key = any($1::varchar []);

-- name: CommitPendingClosure :exec
SELECT commit_pending_closure($1::bigint);

-- name: CleanupPendingClosures :execrows
WITH cutoff_time AS (
    SELECT timezone('UTC', now()) - interval '1 second' * $1::int AS time
),

old_closures AS (
    SELECT id
    FROM pending_closures, cutoff_time
    WHERE started_at < cutoff_time.time
),

-- Insert pending objects into objects table if they don't already exist
-- We mark them as deleted so they can be cleaned up later
inserted_objects AS (
    INSERT INTO objects (key, refs, deleted_at, first_deleted_at)
    SELECT
        po.key,
        po.refs,
        cutoff_time.time,
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
SELECT updated_at FROM closures
WHERE key = $1 LIMIT 1;

-- name: GetClosureObjects :many
-- Return objects reachable from the given closure key
WITH RECURSIVE closure_reach AS (
    -- Start with the provided closure key
    SELECT o.key, o.refs 
    FROM objects o
    WHERE o.key = $1
    UNION
    -- Recursively add all referenced objects
    SELECT o.key, o.refs 
    FROM objects o
    INNER JOIN closure_reach cr ON o.key = ANY(cr.refs)
)
SELECT DISTINCT key FROM closure_reach;

-- name: DeleteClosures :execrows
-- Delete old closures, but exclude any that are pinned
DELETE FROM closures
WHERE closures.updated_at < $1
  AND closures.key NOT IN (SELECT narinfo_key FROM pins);

-- name: MarkObjectsAsActive :exec
UPDATE objects SET deleted_at = NULL
WHERE key = any($1::varchar []);

-- name: DeleteObjects :exec
DELETE FROM objects
WHERE key = any($1::varchar []);

-- name: InsertMultipartUpload :exec
INSERT INTO multipart_uploads (pending_closure_id, object_key, upload_id)
VALUES ($1, $2, $3);

-- name: GetOldMultipartUploads :many
SELECT upload_id, object_key
FROM multipart_uploads mu
JOIN pending_closures pc ON mu.pending_closure_id = pc.id
WHERE pc.started_at < timezone('UTC', now()) - interval '1 second' * $1::int;

-- name: DeleteMultipartUpload :exec
DELETE FROM multipart_uploads
WHERE upload_id = $1;

-- name: GetMultipartUpload :one
SELECT pending_closure_id, object_key, upload_id
FROM multipart_uploads
WHERE upload_id = $1 AND object_key = $2;

-- name: MarkStaleObjects :execrows
WITH RECURSIVE ct AS (
    SELECT timezone('UTC', now()) AS now
),
-- Find all objects reachable from any closure
closure_reach AS (
    -- Start with all closure keys
    SELECT o.key, o.refs
    FROM objects o
    INNER JOIN closures c ON o.key = c.key
    UNION
    -- Recursively add all referenced objects
    SELECT o.key, o.refs
    FROM objects o
    INNER JOIN closure_reach cr ON o.key = ANY(cr.refs)
),
reachable_objects AS (
    SELECT DISTINCT key FROM closure_reach
),
stale_objects AS (
    SELECT o.key
    FROM objects AS o, ct
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM reachable_objects ro
            WHERE ro.key = o.key
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pending_objects AS po
            WHERE po.key = o.key
        )
        AND o.deleted_at IS NULL  -- Only mark fresh objects
    FOR UPDATE
)
UPDATE objects
SET
    deleted_at = ct.now,
    first_deleted_at = COALESCE(first_deleted_at, ct.now)
FROM stale_objects, ct
WHERE objects.key = stale_objects.key;

-- name: GetObjectsReadyForDeletion :many
-- Returns objects marked for >= grace_period, safe to delete from S3
SELECT key
FROM objects
WHERE first_deleted_at IS NOT NULL
  AND deleted_at IS NOT NULL
  AND first_deleted_at <= timezone('UTC', now()) - interval '1 second' * sqlc.arg(grace_period_seconds)::int
LIMIT sqlc.arg(limit_count);

-- Pin queries

-- name: UpsertPin :exec
-- Create or update a pin. Updates the narinfo_key, store_path, and updated_at if the pin already exists.
INSERT INTO pins (name, narinfo_key, store_path, created_at, updated_at)
VALUES ($1, $2, $3, timezone('UTC', now()), timezone('UTC', now()))
ON CONFLICT (name) DO UPDATE SET
    narinfo_key = EXCLUDED.narinfo_key,
    store_path = EXCLUDED.store_path,
    updated_at = timezone('UTC', now());

-- name: GetPin :one
SELECT name, narinfo_key, store_path, created_at, updated_at
FROM pins
WHERE name = $1;

-- name: DeletePin :exec
DELETE FROM pins
WHERE name = $1;

-- name: ListPins :many
SELECT name, narinfo_key, store_path, created_at, updated_at
FROM pins
ORDER BY name;
