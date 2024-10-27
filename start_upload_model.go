package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Mic92/niks3/pg"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type UploadResponse struct {
	ID        string    `json:"id"`
	StartedAt time.Time `json:"started_at"`
}

func StartUpload(ctx context.Context, pool *pgxpool.Pool, closureKey string, storePathSet map[string]bool) (*UploadResponse, error) {
	now := time.Now().UTC()

	// See https://github.com/jackc/pgx/issues/2050 for why we have to do this
	timestamp := pgtype.Timestamp{
		Time:  now,
		Valid: true,
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	defer func() {
		if err == nil {
			if err = tx.Commit(ctx); err != nil {
				slog.Error("failed to commit transaction", "error", err)
			}
		} else {
			if err = tx.Rollback(ctx); err != nil {
				slog.Error("failed to rollback transaction", "error", err)
			}
		}
	}()

	queries := pg.New(tx)

	var uploadID int64

	// upsert closure
	if err = queries.UpsertClosure(ctx, pg.UpsertClosureParams{
		NarHash:   closureKey,
		UpdatedAt: timestamp,
	}); err != nil {
		return nil, fmt.Errorf("failed to upsert closure: %w", err)
	}

	if uploadID, err = queries.InsertUpload(ctx, pg.InsertUploadParams{
		StartedAt:      timestamp,
		ClosureNarHash: closureKey,
	}); err != nil {
		return nil, fmt.Errorf("failed to insert upload: %w", err)
	}

	for path := range storePathSet {
		if err = queries.UpsertObject(ctx, path); err != nil {
			return nil, fmt.Errorf("failed to upsert object: %w", err)
		}
	}

	closures := make([]pg.InsertClosuresParams, 0, len(storePathSet))
	for storePath := range storePathSet {
		closures = append(closures, pg.InsertClosuresParams{
			NarHash:        storePath,
			ClosureNarHash: closureKey,
		})
	}

	if _, err = queries.InsertClosures(ctx, closures); err != nil {
		return nil, fmt.Errorf("failed to insert closures: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return &UploadResponse{
		// use string to avoid json marshalling issues
		ID:        fmt.Sprintf("%d", uploadID),
		StartedAt: now,
	}, nil
}
