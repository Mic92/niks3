-- name: InsertPendingClosure :one
INSERT INTO pending_closures (started_at, key)
VALUES (timezone('UTC', now()), $1)
RETURNING *;

-- name: InsertPendingObjects :copyfrom
INSERT INTO pending_objects (pending_closure_id, key, refs, size) VALUES ($1, $2, $3, $4);

-- name: GetObjectStats :one
SELECT object_count, total_bytes FROM object_stats WHERE id;

-- name: CountPendingClosures :one
SELECT count(*) FROM pending_closures;

-- name: GetPendingObjectKeys :many
SELECT key FROM pending_objects
WHERE pending_closure_id = $1;

-- name: GetLiveObjects :many
-- Run after the pending_objects rows are committed. FOR SHARE waits for any
-- reaper batch holding these rows. Later batches see the pending rows and
-- skip them. Tombstoned rows count as absent: their S3 object may be gone.
WITH locked AS (
    SELECT key, deleted_at FROM objects
    WHERE key = any($1::varchar [])
    FOR SHARE
)

SELECT key FROM locked WHERE deleted_at IS NULL;

-- name: CommitPendingClosure :exec
SELECT commit_pending_closure($1::bigint);

-- name: RegisterCompletedObject :execrows
-- Record an uploaded object before its closure commits so later closures do
-- not re-offer it. Reads refs/size from the caller's own pending_objects row
-- and key-share locks it, so CleanupPendingClosures cannot remove the row
-- between the check and the write. No row means the closure was cleaned up.
WITH po AS (
    SELECT p.key, p.refs, p.size FROM pending_objects AS p
    WHERE p.pending_closure_id = $1 AND p.key = $2
    FOR KEY SHARE
)

INSERT INTO objects (key, refs, size)
SELECT po.key, po.refs, po.size FROM po
ON CONFLICT (key) DO UPDATE SET
    refs = (
        SELECT ARRAY(
            SELECT DISTINCT unnest(objects.refs || excluded.refs)
        )
    ),
    size = coalesce(objects.size, excluded.size),
    deleted_at = NULL;

-- name: CleanupPendingClosures :execrows
WITH cutoff_time AS (
    SELECT timezone('UTC', now()) - interval '1 second' * $1::int AS time
),

old_closures AS (
    SELECT id
    FROM pending_closures, cutoff_time
    WHERE started_at < cutoff_time.time
),

-- Whatever an abandoned upload left in S3 becomes a tombstone so the reaper
-- removes it after the normal grace period.
inserted_objects AS (
    INSERT INTO objects (key, refs, deleted_at)
    SELECT
        po.key,
        po.refs,
        timezone('UTC', now())
    FROM pending_objects AS po
    JOIN old_closures oc ON po.pending_closure_id = oc.id
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

-- name: DeleteTombstonedObjects :execrows
DELETE FROM objects
WHERE key = any($1::varchar []) AND deleted_at IS NOT NULL;

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

-- name: GetRedundantMultipartUploads :many
-- Upload IDs other pending_closures opened for object_key, used to abort
-- duplicates once one upload of the NAR completes.
SELECT upload_id
FROM multipart_uploads
WHERE object_key = $1 AND upload_id <> $2;

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
        AND o.deleted_at IS NULL
)
UPDATE objects
SET deleted_at = ct.now
FROM stale_objects, ct
WHERE objects.key = stale_objects.key;

-- name: LockObjectsForDeletion :many
-- The transaction stays open across the S3 delete. Pairs with GetLiveObjects.
SELECT o.key
FROM objects AS o
WHERE o.deleted_at IS NOT NULL
  AND o.deleted_at <= sqlc.arg(cutoff)::timestamp
  AND o.key > sqlc.arg(after_key)::varchar
  AND NOT EXISTS (SELECT 1 FROM pending_objects AS po WHERE po.key = o.key)
ORDER BY o.key
LIMIT sqlc.arg(limit_count)
FOR UPDATE OF o;

-- name: GetPendingKeysAmong :many
-- Fresh-snapshot recheck after the row locks are taken.
SELECT DISTINCT key FROM pending_objects
WHERE key = any($1::varchar []);

-- name: GetClosureForShare :one
-- Lock the closure row so concurrent GC cannot delete it between the
-- existence check and the pin upsert.
SELECT updated_at FROM closures
WHERE key = $1 LIMIT 1
FOR SHARE;

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

-- name: InsertGCRun :one
INSERT INTO gc_runs (state, params) VALUES ('running', $1)
RETURNING *;

-- name: GetLatestGCRun :one
SELECT * FROM gc_runs ORDER BY id DESC LIMIT 1;

-- name: UpdateGCRunProgress :exec
UPDATE gc_runs SET phase = $2, stats = $3, updated_at = now()
WHERE id = $1;

-- name: FinishGCRun :exec
UPDATE gc_runs
SET state = $2, phase = '', stats = $3, error = $4, updated_at = now(), finished_at = now()
WHERE id = $1;

-- name: FailInterruptedGCRuns :exec
-- Caller holds the GC lock, so any "running" row is from a dead process.
UPDATE gc_runs
SET state = 'failed', error = 'interrupted', updated_at = now(), finished_at = now()
WHERE state = 'running';
