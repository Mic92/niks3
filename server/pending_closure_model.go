package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/Mic92/niks3/server/pg"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	maxSignedURLDuration = time.Duration(5) * time.Hour
)

type PendingObject struct {
	PresignedURL string `json:"presigned_url"`
}

type PendingClosureResponse struct {
	ID             string                   `json:"id"`
	StartedAt      time.Time                `json:"started_at"`
	PendingObjects map[string]PendingObject `json:"pending_objects"`
}

type PendingClosure struct {
	id             int64
	startedAt      time.Time
	pendingObjects []pg.InsertPendingObjectsParams
	deletedObjects []string
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

func waitForDeletion(ctx context.Context, pool *pgxpool.Pool, inflightPaths []string) (map[string]bool, error) {
	queries := pg.New(pool)

	missingObjects := make(map[string]bool, len(inflightPaths))
	for _, objectKey := range inflightPaths {
		missingObjects[objectKey] = true
	}

	for len(inflightPaths) > 0 {
		time.Sleep(time.Duration(1) * time.Second)

		existingObjects, err := queries.GetExistingObjects(ctx, inflightPaths)
		if err != nil {
			return nil, fmt.Errorf("failed to get existing objects: %w", err)
		}

		// reset inflightPaths
		inflightPaths = inflightPaths[:0]

		for _, existingObject := range existingObjects {
			deletedAt, ok := existingObject.DeletedAt.(pgtype.Interval)
			if !ok {
				return nil, fmt.Errorf("deleted_at is not set for object: %s", existingObject.Key)
			}

			if deletedAt.Months == 0 && deletedAt.Days == 0 && deletedAt.Microseconds < 1000*1000*30 {
				inflightPaths = append(inflightPaths, existingObject.Key)
			} else {
				delete(missingObjects, existingObject.Key)
			}
		}
	}

	return missingObjects, nil
}

func createPendingClosureInner(
	ctx context.Context,
	pool *pgxpool.Pool,
	closureKey string,
	objectsWithRefs map[string][]string,
) (*PendingClosure, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	committed := false

	defer rollbackOnError(ctx, &tx, &err, &committed)

	queries := pg.New(tx)

	var pendingClosure pg.PendingClosure

	if pendingClosure, err = queries.InsertPendingClosure(ctx, closureKey); err != nil {
		return nil, fmt.Errorf("failed to insert pending closure: %w", err)
	}

	keys := make([]string, 0, len(objectsWithRefs))
	for k := range objectsWithRefs {
		keys = append(keys, k)
	}

	existingObjects, err := queries.GetExistingObjects(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing objects: %w", err)
	}

	deletedObjects := make([]string, 0, len(existingObjects))

	for _, existingObject := range existingObjects {
		if existingObject.DeletedAt != nil {
			deletedObjects = append(deletedObjects, existingObject.Key)
		} else {
			delete(objectsWithRefs, existingObject.Key)
		}
	}

	pendingObjects := make([]pg.InsertPendingObjectsParams, 0, len(objectsWithRefs))

	for objectKey, refs := range objectsWithRefs {
		pendingObjects = append(pendingObjects, pg.InsertPendingObjectsParams{
			PendingClosureID: pendingClosure.ID,
			Key:              objectKey,
			Refs:             refs,
		})
	}

	if _, err = queries.InsertPendingObjects(ctx, pendingObjects); err != nil {
		return nil, fmt.Errorf("failed to insert pending objects: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	committed = true

	return &PendingClosure{
		id:             pendingClosure.ID,
		startedAt:      pendingClosure.StartedAt.Time,
		pendingObjects: pendingObjects,
		deletedObjects: deletedObjects,
	}, nil
}

func (s *Service) makePendingObject(ctx context.Context, objectKey string) (PendingObject, error) {
	// TODO: multi-part uploads
	presignedURL, err := s.MinioClient.PresignedPutObject(ctx,
		s.Bucket,
		objectKey,
		maxSignedURLDuration)
	if err != nil {
		return PendingObject{}, fmt.Errorf("failed to create presigned URL: %w", err)
	}

	return PendingObject{
		PresignedURL: presignedURL.String(),
	}, nil
}

func (s *Service) createPendingClosure(
	ctx context.Context,
	pool *pgxpool.Pool,
	closureKey string,
	objectsWithRefs map[string][]string,
) (*PendingClosureResponse, error) {
	pendingClosure, err := createPendingClosureInner(ctx, pool, closureKey, objectsWithRefs)
	if err != nil {
		return nil, err
	}

	pendingObjects := make(map[string]PendingObject, len(pendingClosure.pendingObjects)+len(pendingClosure.deletedObjects))

	for _, pendingObject := range pendingClosure.pendingObjects {
		po, err := s.makePendingObject(ctx, pendingObject.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to create pending object: %w", err)
		}

		pendingObjects[pendingObject.Key] = po
	}

	if len(pendingClosure.deletedObjects) > 0 {
		slog.Info("Found objects not yet deleted. Waiting for deletion",
			"pending_objects", len(pendingClosure.deletedObjects))

		missingObjects, err := waitForDeletion(ctx, pool, pendingClosure.deletedObjects)
		if err != nil {
			return nil, err
		}

		pendingObjectsParams := make([]pg.InsertPendingObjectsParams, 0, len(missingObjects))
		for objectKey := range missingObjects {
			pendingObjectsParams = append(pendingObjectsParams, pg.InsertPendingObjectsParams{
				PendingClosureID: pendingClosure.id,
				Key:              objectKey,
				Refs:             []string{}, // Deleted objects being re-uploaded don't have refs info here
			})
		}

		queries := pg.New(pool)

		if _, err = queries.InsertPendingObjects(ctx, pendingObjectsParams); err != nil {
			return nil, fmt.Errorf("failed to insert pending objects: %w", err)
		}

		for _, pendingObject := range pendingObjectsParams {
			po, err := s.makePendingObject(ctx, pendingObject.Key)
			if err != nil {
				return nil, fmt.Errorf("failed to create pending object: %w", err)
			}

			pendingObjects[pendingObject.Key] = po
		}
	}

	return &PendingClosureResponse{
		ID:             strconv.FormatInt(pendingClosure.id, 10),
		StartedAt:      pendingClosure.startedAt,
		PendingObjects: pendingObjects,
	}, nil
}

var errPendingClosureNotFound = errors.New("not found")

func commitPendingClosure(ctx context.Context, pool *pgxpool.Pool, pendingClosureID int64) error {
	if err := pg.New(pool).CommitPendingClosure(ctx, pendingClosureID); err != nil {
		msg := "Closure does not exist:"

		var pgError *pgconn.PgError

		ok := errors.As(err, &pgError)
		if ok && strings.Contains(pgError.Message, msg) {
			return fmt.Errorf("failed to commit pending closure: %w", errPendingClosureNotFound)
		}

		return fmt.Errorf("failed to commit pending closure: %w", err)
	}

	return nil
}

func cleanupPendingClosures(ctx context.Context, pool *pgxpool.Pool, duration time.Duration) error {
	seconds := int32(duration.Seconds())
	if err := pg.New(pool).CleanupPendingClosures(ctx, seconds); err != nil {
		return fmt.Errorf("failed to cleanup pending closure: %w", err)
	}

	return nil
}
