// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0

package pg

import (
	"github.com/jackc/pgx/v5/pgtype"
)

type Closure struct {
	Key       string           `json:"key"`
	UpdatedAt pgtype.Timestamp `json:"updated_at"`
}

type ClosureObject struct {
	ClosureKey string `json:"closure_key"`
	ObjectKey  string `json:"object_key"`
}

type Object struct {
	Key       string           `json:"key"`
	DeletedAt pgtype.Timestamp `json:"deleted_at"`
}

type PendingClosure struct {
	ID        int64            `json:"id"`
	Key       string           `json:"key"`
	StartedAt pgtype.Timestamp `json:"started_at"`
}

type PendingObject struct {
	PendingClosureID int64  `json:"pending_closure_id"`
	Key              string `json:"key"`
}