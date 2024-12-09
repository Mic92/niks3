// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: query.sql

package pg

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

const cleanupPendingClosures = `-- name: CleanupPendingClosures :exec
WITH cutoff_time AS (
    SELECT timezone('UTC', NOW()) - interval '1 second' * $1 AS time
),
old_closures AS (
    SELECT id
    FROM pending_closures, cutoff_time
    WHERE started_at < cutoff_time.time
),
inserted_objects AS (
    INSERT INTO objects (key, deleted_at)
    SELECT po.key, cutoff_time.time
    FROM pending_objects as po
    JOIN old_closures oc ON po.pending_closure_id = oc.id, cutoff_time
    ON CONFLICT (key) DO NOTHING
    RETURNING key
),
deleted_pending_objects AS (
    DELETE FROM pending_objects
    USING old_closures
    WHERE pending_objects.pending_closure_id = old_closures.id
    RETURNING pending_closure_id
)
DELETE FROM pending_closures
USING old_closures
WHERE pending_closures.id = old_closures.id
`

// Insert pending objects into objects table if they don't already exist
// We mark them as deleted so they can be cleaned up later
// Delete pending objects that were inserted into the objects table
// Delete pending closures older than the specified interval
// This will cascade to pending_objects
func (q *Queries) CleanupPendingClosures(ctx context.Context, dollar_1 interface{}) error {
	_, err := q.db.Exec(ctx, cleanupPendingClosures, dollar_1)
	return err
}

const commitPendingClosure = `-- name: CommitPendingClosure :exec
SELECT commit_pending_closure($1::bigint)
`

func (q *Queries) CommitPendingClosure(ctx context.Context, dollar_1 int64) error {
	_, err := q.db.Exec(ctx, commitPendingClosure, dollar_1)
	return err
}

const deleteClosures = `-- name: DeleteClosures :exec
DELETE FROM closures WHERE updated_at < $1
`

func (q *Queries) DeleteClosures(ctx context.Context, updatedAt pgtype.Timestamp) error {
	_, err := q.db.Exec(ctx, deleteClosures, updatedAt)
	return err
}

const deleteObjects = `-- name: DeleteObjects :exec
DELETE FROM objects WHERE key = any($1::varchar [])
`

func (q *Queries) DeleteObjects(ctx context.Context, dollar_1 []string) error {
	_, err := q.db.Exec(ctx, deleteObjects, dollar_1)
	return err
}

const getClosure = `-- name: GetClosure :one
SELECT updated_at FROM closures WHERE key = $1 LIMIT 1
`

func (q *Queries) GetClosure(ctx context.Context, key string) (pgtype.Timestamp, error) {
	row := q.db.QueryRow(ctx, getClosure, key)
	var updated_at pgtype.Timestamp
	err := row.Scan(&updated_at)
	return updated_at, err
}

const getClosureObjects = `-- name: GetClosureObjects :many
SELECT object_key FROM closure_objects WHERE closure_key = $1
`

func (q *Queries) GetClosureObjects(ctx context.Context, closureKey string) ([]string, error) {
	rows, err := q.db.Query(ctx, getClosureObjects, closureKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []string
	for rows.Next() {
		var object_key string
		if err := rows.Scan(&object_key); err != nil {
			return nil, err
		}
		items = append(items, object_key)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getExistingObjects = `-- name: GetExistingObjects :many
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
WHERE key = any($1::varchar [])
`

type GetExistingObjectsRow struct {
	Key       string      `json:"key"`
	DeletedAt interface{} `json:"deleted_at"`
}

func (q *Queries) GetExistingObjects(ctx context.Context, dollar_1 []string) ([]GetExistingObjectsRow, error) {
	rows, err := q.db.Query(ctx, getExistingObjects, dollar_1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetExistingObjectsRow
	for rows.Next() {
		var i GetExistingObjectsRow
		if err := rows.Scan(&i.Key, &i.DeletedAt); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const insertPendingClosure = `-- name: InsertPendingClosure :one
INSERT INTO pending_closures (started_at, key)
VALUES (timezone('UTC', now()), $1)
RETURNING id, key, started_at
`

func (q *Queries) InsertPendingClosure(ctx context.Context, key string) (PendingClosure, error) {
	row := q.db.QueryRow(ctx, insertPendingClosure, key)
	var i PendingClosure
	err := row.Scan(&i.ID, &i.Key, &i.StartedAt)
	return i, err
}

type InsertPendingObjectsParams struct {
	PendingClosureID int64  `json:"pending_closure_id"`
	Key              string `json:"key"`
}

const markObjectsAsActive = `-- name: MarkObjectsAsActive :exec
UPDATE objects SET deleted_at = NULL WHERE key = any($1::varchar [])
`

func (q *Queries) MarkObjectsAsActive(ctx context.Context, dollar_1 []string) error {
	_, err := q.db.Exec(ctx, markObjectsAsActive, dollar_1)
	return err
}

const markObjectsForDeletion = `-- name: MarkObjectsForDeletion :many
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
RETURNING objects.key
`

func (q *Queries) MarkObjectsForDeletion(ctx context.Context, limit int32) ([]string, error) {
	rows, err := q.db.Query(ctx, markObjectsForDeletion, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		items = append(items, key)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
