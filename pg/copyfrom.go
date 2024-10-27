// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: copyfrom.go

package pg

import (
	"context"
)

// iteratorForInsertClosures implements pgx.CopyFromSource.
type iteratorForInsertClosures struct {
	rows                 []InsertClosuresParams
	skippedFirstNextCall bool
}

func (r *iteratorForInsertClosures) Next() bool {
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

func (r iteratorForInsertClosures) Values() ([]interface{}, error) {
	return []interface{}{
		r.rows[0].ClosureNarHash,
		r.rows[0].NarHash,
	}, nil
}

func (r iteratorForInsertClosures) Err() error {
	return nil
}

func (q *Queries) InsertClosures(ctx context.Context, arg []InsertClosuresParams) (int64, error) {
	return q.db.CopyFrom(ctx, []string{"closure_objects"}, []string{"closure_nar_hash", "nar_hash"}, &iteratorForInsertClosures{rows: arg})
}