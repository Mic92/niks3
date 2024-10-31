package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Mic92/niks3/pg"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PendingClosureResponse struct {
	ID        string    `json:"id"`
	StartedAt time.Time `json:"started_at"`
}

func rollbackOnError(ctx context.Context, tx *pgx.Tx, err *error, committed *bool) {
	if p := recover(); p != nil && !*committed {
		if err := (*tx).Rollback(ctx); err != nil {
			slog.Error("failed to rollback transaction", "error", err)
		}

		panic(p) // re-throw after Rollback
	} else if err != nil && !*committed {
		if err := (*tx).Rollback(ctx); err != nil {
			slog.Error("failed to rollback transaction", "error", err)
		}
	}
}

func createPendingClosure(
	ctx context.Context,
	pool *pgxpool.Pool,
	closureKey string,
	storePathSet map[string]bool,
) (*PendingClosureResponse, error) {
	now := time.Now().UTC()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	committed := false

	defer rollbackOnError(ctx, &tx, &err, &committed)

	queries := pg.New(tx)

	var pendingClosureID int64

	if pendingClosureID, err = queries.InsertPendingClosure(ctx, pg.InsertPendingClosureParams{
		StartedAt: pgtype.Timestamp{
			Time:  now,
			Valid: true,
		},
		Key: closureKey,
	}); err != nil {
		return nil, fmt.Errorf("failed to insert pending closure: %w", err)
	}

	keys := make([]string, 0, len(storePathSet))
	for key := range storePathSet {
		keys = append(keys, key)
	}

	existingObjects, err := queries.GetExistingObjects(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing objects: %w", err)
	}

	for _, existingObject := range existingObjects {
		delete(storePathSet, existingObject)
	}

	pendingObjects := make([]pg.InsertPendingObjectsParams, 0, len(storePathSet))

	for objectKey := range storePathSet {
		pendingObjects = append(pendingObjects, pg.InsertPendingObjectsParams{
			PendingClosureID: pendingClosureID,
			Key:              objectKey,
		})
	}

	if _, err = queries.InsertPendingObjects(ctx, pendingObjects); err != nil {
		return nil, fmt.Errorf("failed to insert pending objects: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &PendingClosureResponse{
		ID:        fmt.Sprintf("%d", pendingClosureID),
		StartedAt: now,
	}, nil
}

func commitPendingClosure(ctx context.Context, pool *pgxpool.Pool, pendingClosureID int64) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	committed := false

	defer rollbackOnError(ctx, &tx, &err, &committed)
	queries := pg.New(tx)

	if err = queries.CommitPendingClosure(ctx, pendingClosureID); err != nil {
		return fmt.Errorf("failed to commit pending closure: %w", err)
	}

	committed = true

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
