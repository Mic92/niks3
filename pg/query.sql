-- name: UpsertClosure :exec
INSERT INTO closures (nar_hash, updated_at)
VALUES ($1, $2)
ON CONFLICT (nar_hash)
DO UPDATE SET updated_at = $2;

-- name: InsertUpload :one
INSERT INTO uploads (started_at, closure_nar_hash) VALUES ($1, $2) RETURNING id;

-- name: UpsertObject :exec
INSERT INTO objects (nar_hash, reference_count)
VALUES ($1, 1)
ON CONFLICT (nar_hash)
DO UPDATE SET reference_count = objects.reference_count + 1;

-- name: InsertClosures :copyfrom
INSERT INTO closure_objects (closure_nar_hash, nar_hash) VALUES ($1, $2);

-- name: DeleteUpload :exec
DELETE FROM uploads WHERE id = $1;