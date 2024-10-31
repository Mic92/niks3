package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Mic92/niks3/pg"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ClosureResponse struct {
	Key       string    `json:"id"`
	UpdatedAt time.Time `json:"updated_at"`
	Objects   []string  `json:"objects"`
}

func getClosure(ctx context.Context, pool *pgxpool.Pool, closureKey string) (*ClosureResponse, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	defer conn.Release()

	queries := pg.New(conn)

	closure, err := queries.GetClosure(ctx, closureKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get closure: %w", err)
	}

	objects, err := queries.GetClosureObjects(ctx, closureKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get closure objects: %w", err)
	}

	return &ClosureResponse{
		Key:       closureKey,
		UpdatedAt: closure.Time,
		Objects:   objects,
	}, nil
}
