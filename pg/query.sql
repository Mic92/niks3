-- name: UpsertClosure :exec
INSERT INTO closures (key, updated_at)
VALUES ($1, $2)
ON CONFLICT (key)
DO UPDATE SET updated_at = $2;

-- name: InsertUpload :one
INSERT INTO uploads (started_at, closure_key) VALUES ($1, $2) RETURNING id;

-- name: UpsertObject :exec
INSERT INTO objects (key, reference_count)
VALUES ($1, 1)
ON CONFLICT (key)
DO UPDATE SET reference_count = objects.reference_count + 1;

-- name: InsertClosures :copyfrom
INSERT INTO closure_objects (closure_key, object_key) VALUES ($1, $2);

-- name: DeleteUpload :exec
DELETE FROM uploads WHERE id = $1;
