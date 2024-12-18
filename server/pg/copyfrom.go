// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: copyfrom.go

package pg

import (
	"context"
)

// iteratorForInsertPendingObjects implements pgx.CopyFromSource.
type iteratorForInsertPendingObjects struct {
	rows                 []InsertPendingObjectsParams
	skippedFirstNextCall bool
}

func (r *iteratorForInsertPendingObjects) Next() bool {
	if len(r.rows) == 0 {
		return false
	}
	if !r.skippedFirstNextCall {
		r.skippedFirstNextCall = true
		return true
	}
	r.rows = r.rows[1:]
	return len(r.rows) > 0
}

func (r iteratorForInsertPendingObjects) Values() ([]interface{}, error) {
	return []interface{}{
		r.rows[0].PendingClosureID,
		r.rows[0].Key,
	}, nil
}

func (r iteratorForInsertPendingObjects) Err() error {
	return nil
}

func (q *Queries) InsertPendingObjects(ctx context.Context, arg []InsertPendingObjectsParams) (int64, error) {
	return q.db.CopyFrom(ctx, []string{"pending_objects"}, []string{"pending_closure_id", "key"}, &iteratorForInsertPendingObjects{rows: arg})
}
