-- name: InsertPendingClosure :one
INSERT INTO pending_closures (started_at, key)
VALUES (timezone('UTC', now()), $1)
RETURNING *;

-- name: InsertPush :one
INSERT INTO pending_closures (started_at, key, roots)
VALUES (timezone('UTC', now()), $1, $2)
RETURNING *;

-- name: CommitPush :exec
SELECT commit_push($1::bigint);

-- name: InsertPendingObjects :copyfrom
INSERT INTO pending_objects (pending_closure_id, key, refs, size) VALUES ($1, $2, $3, $4);

-- name: GetObjectStats :one
SELECT object_count, total_bytes FROM object_stats WHERE id;

-- name: CountPendingClosures :one
SELECT count(*) FROM pending_closures;

-- name: DeletePendingClosure :exec
-- Drop a pending closure this server could not hand to the client. Its
-- pending objects and multipart uploads cascade.
DELETE FROM pending_closures WHERE id = $1;

-- name: CountPendingObjects :one
SELECT count(*) FROM pending_objects WHERE pending_closure_id = $1;

-- name: GetPendingObjectKeys :many
SELECT key FROM pending_objects
WHERE pending_closure_id = $1;

-- name: GetExistingObjects :many
-- Waits for a sweep holding a row, so the push's upload cannot land before the
-- sweep's S3 delete.
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
WHERE key = any($1::varchar [])
FOR KEY SHARE OF o;

-- name: GetPresentObjects :many
-- GC-marked objects may vanish from S3 any moment, so they count as absent.
SELECT key FROM objects
WHERE key = any($1::varchar []) AND deleted_at IS NULL;

-- name: TouchPresentClosures :many
-- Rows younger than ten minutes keep their age, since GC cuts off by days.
-- Locking in byte order stops overlapping callers from deadlocking, and waits
-- for a GC delete in flight.
WITH live AS (
    SELECT
        c.key,
        c.updated_at
    FROM closures AS c
    INNER JOIN objects AS o ON o.key = c.key
    WHERE c.key = any($1::varchar []) AND o.deleted_at IS NULL
    ORDER BY c.key COLLATE "C"
    FOR UPDATE OF c
),

touched AS (
    UPDATE closures AS c
    SET updated_at = timezone('UTC', now())
    FROM live
    WHERE c.key = live.key AND live.updated_at < timezone('UTC', now()) - interval '10 minutes'
)

SELECT key FROM live;

-- name: CommitPendingClosure :exec
SELECT commit_pending_closure($1::bigint);

-- name: RegisterCompletedObject :exec
-- Record an object as soon as its upload completes so later closures don't
-- re-offer it if this closure never commits. Only while a pending row exists:
-- that row keeps the sweep away from the object. Conflict handling matches
-- commit_pending_closure: merge refs, keep a known size, resurrect tombstones.
INSERT INTO objects (key, refs, size)
SELECT sqlc.arg(key)::varchar, sqlc.arg(refs)::varchar [], sqlc.arg(size)
WHERE EXISTS (SELECT 1 FROM pending_objects WHERE key = sqlc.arg(key)::varchar)
ON CONFLICT (key) DO UPDATE SET
    refs = (
        SELECT ARRAY(
            SELECT DISTINCT unnest(objects.refs || excluded.refs)
        )
    ),
    size = coalesce(objects.size, excluded.size),
    deleted_at = NULL,
    first_deleted_at = NULL;

-- name: GetPendingObject :one
SELECT refs, size FROM pending_objects
WHERE pending_closure_id = $1 AND key = $2;

-- name: GetPendingObjectByKey :one
-- Any pending closure's row for this key; used to recover refs/size when the
-- upload is registered outside closure commit.
SELECT refs, size FROM pending_objects
WHERE key = $1
LIMIT 1;

-- name: CleanupPendingClosures :execrows
-- Pass the cutoff that GetOldMultipartUploads used. Closures in keep are left
-- alone because one of their uploads could not be aborted.
WITH cutoff_time AS (
    SELECT sqlc.arg(cutoff)::timestamp AS time
),

-- Skips closures being committed, which hold their row.
old_closures AS (
    SELECT id
    FROM pending_closures, cutoff_time
    WHERE started_at < cutoff_time.time
      AND NOT (id = any(sqlc.arg(keep)::bigint []))
    FOR UPDATE OF pending_closures SKIP LOCKED
),

-- Insert pending objects into objects table if they don't already exist
-- We mark them as deleted so they can be cleaned up later.
-- Byte order avoids deadlocks and is faster.
inserted_objects AS (
    INSERT INTO objects (key, refs, deleted_at, first_deleted_at)
    SELECT
        po.key,
        po.refs,
        cutoff_time.time,
        cutoff_time.time
    FROM pending_objects AS po
    JOIN old_closures oc ON po.pending_closure_id = oc.id, cutoff_time
    ORDER BY po.key COLLATE "C"
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

-- name: LockOldClosures :many
-- Lock the old closures that have no pin until the transaction ends.
-- Closures that a pin request is locking are skipped. Waiting for them would
-- delete a closure right after it got pinned.
SELECT c.key FROM closures AS c
WHERE c.updated_at < $1
  AND c.key NOT IN (SELECT narinfo_key FROM pins)
ORDER BY c.key COLLATE "C"
FOR UPDATE SKIP LOCKED;

-- name: DeleteUnpinnedClosures :execrows
-- Delete the locked closures unless they are pinned by now. This statement
-- sees pins that committed after the lock query ran. Without the check, such
-- a pin would make the delete fail on the foreign key.
DELETE FROM closures
WHERE key = any($1::varchar [])
  AND key NOT IN (SELECT narinfo_key FROM pins);

-- name: DeleteObjects :exec
DELETE FROM objects
WHERE key = any($1::varchar []);

-- name: InsertMultipartUpload :exec
INSERT INTO multipart_uploads (pending_closure_id, object_key, upload_id)
VALUES ($1, $2, $3);

-- name: GetOldMultipartUploads :many
SELECT upload_id, object_key, pending_closure_id
FROM multipart_uploads mu
JOIN pending_closures pc ON mu.pending_closure_id = pc.id
WHERE pc.started_at < sqlc.arg(cutoff)::timestamp;

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
        AND o.deleted_at IS NULL  -- Only mark fresh objects
    ORDER BY o.key COLLATE "C"  -- lock in key order, like commit_pending_closure
    FOR UPDATE
)
UPDATE objects
SET
    deleted_at = ct.now,
    first_deleted_at = COALESCE(first_deleted_at, ct.now)
FROM stale_objects, ct
WHERE objects.key = stale_objects.key;

-- name: LockObjectsReadyForDeletion :many
-- Tombstoned objects past the grace period, row-locked until the caller's
-- transaction ends. Rows a push transaction holds are skipped; the push is
-- about to treat them as present. Keyset-paginated on key.
SELECT key
FROM objects
WHERE first_deleted_at IS NOT NULL
  AND deleted_at IS NOT NULL
  AND first_deleted_at <= timezone('UTC', now()) - interval '1 second' * sqlc.arg(grace_period_seconds)::int
  AND key > sqlc.arg(after_key)::varchar
  AND NOT EXISTS (
      SELECT 1
      FROM pending_objects AS po
      WHERE po.key = objects.key
  )
ORDER BY key
LIMIT sqlc.arg(limit_count)
FOR UPDATE SKIP LOCKED;

-- name: GetKeysNotPending :many
-- Rechecks on a fresh snapshot, after the sweep holds the rows: a push may
-- have committed its pending rows since the sweep's query took its snapshot.
SELECT key FROM objects
WHERE key = any(sqlc.arg(keys)::varchar [])
  AND NOT EXISTS (SELECT 1 FROM pending_objects AS po WHERE po.key = objects.key);

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
