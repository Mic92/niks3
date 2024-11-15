-- name: InsertPendingClosure :one
INSERT INTO pending_closures (started_at, key) VALUES ($1, $2) RETURNING id;

-- name: InsertPendingObjects :copyfrom
INSERT INTO pending_objects (pending_closure_id, key) VALUES ($1, $2);

-- name: GetExistingObjects :many
SELECT key FROM objects WHERE key = ANY($1::varchar[]);

-- name: CommitPendingClosure :exec
SELECT commit_pending_closure($1::bigint);

-- name: GetClosure :one
SELECT updated_at FROM closures WHERE key = $1 LIMIT 1;

-- name: GetClosureObjects :many
SELECT object_key FROM closure_objects WHERE closure_key = $1;

-- name: DeleteClosures :exec
DELETE FROM closures where updated_at < $1;

-- name: GetStaleObjects :many
SELECT FROM objects WHERE key NOT IN (SELECT object_key FROM closure_objects);
