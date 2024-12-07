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
SELECT cleanup_pending_closures($1::int)
`

func (q *Queries) CleanupPendingClosures(ctx context.Context, dollar_1 int32) error {
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
SELECT
    key,
    CASE WHEN deleted_at IS NULL THEN NULL ELSE timezone('UTC', now()) - deleted_at END AS deleted_at
FROM objects WHERE key = any($1::varchar [])
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
WITH stale_objects AS (
    SELECT o.key FROM objects AS o
    WHERE
        o.key NOT IN (
            SELECT co.object_key
            FROM closure_objects AS co
            WHERE co.object_key = o.key
        ) AND (
            o.key NOT IN (
                SELECT po.key FROM pending_objects AS po WHERE po.key = o.key
            )
        ) AND (
            o.deleted_at IS NULL
            -- Re-uploads are allowed after 30s, so we give it a 1h grace period
            OR o.deleted_at < timezone('UTC', now()) - interval '1 hour'
        )
    FOR UPDATE
    LIMIT $1
)

UPDATE objects SET deleted_at = timezone('UTC', now())
FROM stale_objects
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
