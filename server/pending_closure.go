package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Mic92/niks3/server/pg"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"
)

const (
	maxSignedURLDuration = time.Duration(5) * time.Hour
)

type PendingObject struct {
	Type          string               `json:"type"`                     // Object type (narinfo, listing, build_log, realisation, nar)
	PresignedURL  string               `json:"presigned_url,omitempty"`  // For small files (listing, build_log, realisation)
	MultipartInfo *MultipartUploadInfo `json:"multipart_info,omitempty"` // For large files (nar)
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
		if rbErr := (*tx).Rollback(ctx); rbErr != nil {
			slog.Error("failed to rollback transaction", "error", rbErr)
		}

		panic(p) // re-throw after Rollback
	} else if *err != nil && !*committed {
		if rbErr := (*tx).Rollback(ctx); rbErr != nil {
			slog.Error("failed to rollback transaction", "error", rbErr)
		}
	}
}

// checkS3ObjectsExist checks which of the given object keys exist in S3 using a worker pool.
// Returns a map of keys that are missing from S3 and any S3 error encountered.
// If an S3 error occurs, returns immediately with partial results.
func (s *Service) checkS3ObjectsExist(ctx context.Context, objectKeys []string) (map[string]bool, error) {
	if len(objectKeys) == 0 {
		return make(map[string]bool), nil
	}

	missingObjects := make(map[string]bool)
	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(s.S3Concurrency)

	for _, key := range objectKeys {
		g.Go(func() error {
			_, err := s.MinioClient.StatObject(ctx, s.Bucket, key, minio.StatObjectOptions{})
			if err != nil {
				errResp := minio.ToErrorResponse(err)
				if errResp.Code == minio.NoSuchKey {
					mu.Lock()
					missingObjects[key] = true
					mu.Unlock()
					return nil
				}
				// Return error to cancel the group
				return fmt.Errorf("failed to check S3 object %q: %w", key, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return missingObjects, err
	}

	for key := range missingObjects {
		slog.Info("Object in database but missing from S3", "key", key)
	}

	return missingObjects, nil
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
			deletedAt := existingObject.DeletedAt
			if !deletedAt.Valid {
				// Object became active again (resurrected by another pending closure);
				// do not block the flow.
				slog.Debug("object became active during wait", "key", existingObject.Key)
				delete(missingObjects, existingObject.Key)

				continue
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
	objectsMap map[string]objectWithRefs,
	s *Service,
	verifyS3 bool,
) (*PendingClosure, error) {
	if !strings.HasSuffix(closureKey, ".narinfo") {
		return nil, fmt.Errorf("closure key must end with .narinfo: %s", closureKey)
	}

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

	keys := make([]string, 0, len(objectsMap))
	for k := range objectsMap {
		keys = append(keys, k)
	}

	existingObjects, err := queries.GetExistingObjects(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing objects: %w", err)
	}

	deletedObjects := make([]string, 0, len(existingObjects))
	keysToVerifyInS3 := make([]string, 0, len(existingObjects))
	// Keep track of "existing" objects before we delete them from the map
	existingObjectsMap := make(map[string]objectWithRefs)

	for _, existingObject := range existingObjects {
		if existingObject.DeletedAt.Valid {
			deletedObjects = append(deletedObjects, existingObject.Key)
		} else {
			// Track keys that DB says exist (to verify in S3)
			keysToVerifyInS3 = append(keysToVerifyInS3, existingObject.Key)
			// Save the object info before deleting from map
			existingObjectsMap[existingObject.Key] = objectsMap[existingObject.Key]
			delete(objectsMap, existingObject.Key)
		}
	}

	// Verify that objects the DB says exist actually exist in S3 (if requested)
	if verifyS3 && len(keysToVerifyInS3) > 0 {
		missingFromS3, err := s.checkS3ObjectsExist(ctx, keysToVerifyInS3)
		if err != nil {
			return nil, fmt.Errorf("failed to verify objects in S3: %w", err)
		}

		if len(missingFromS3) > 0 {
			slog.Warn("Found objects in DB but missing from S3, will re-upload",
				"count", len(missingFromS3))
			// Add missing objects back to objectsMap so they get uploaded
			for missingKey := range missingFromS3 {
				if obj, ok := existingObjectsMap[missingKey]; ok {
					objectsMap[missingKey] = obj
				}
			}
		}
	}

	pendingObjects := make([]pg.InsertPendingObjectsParams, 0, len(objectsMap))

	for objectKey, obj := range objectsMap {
		pendingObjects = append(pendingObjects, pg.InsertPendingObjectsParams{
			PendingClosureID: pendingClosure.ID,
			Key:              objectKey,
			Refs:             obj.Refs,
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

// createPendingObjects generates presigned URLs or multipart upload info for pending objects.
// Presigned URLs are generated synchronously (no network call, just local signing).
// Multipart uploads are parallelized since they require S3 network calls.
func (s *Service) createPendingObjects(
	ctx context.Context,
	pendingClosureID int64,
	pendingObjectsParams []pg.InsertPendingObjectsParams,
	objectsMap map[string]objectWithRefs,
	result map[string]PendingObject,
) error {
	if len(pendingObjectsParams) == 0 {
		return nil
	}

	// Collect NAR objects that need multipart uploads (require S3 calls)
	type narTask struct {
		key     string
		narSize uint64
	}

	var narTasks []narTask

	// Process non-NAR objects synchronously (presigned URLs are just local signing, no network)
	for _, pendingObject := range pendingObjectsParams {
		obj := objectsMap[pendingObject.Key]

		if obj.Type == "nar" {
			var narSize uint64
			if obj.NarSize != nil {
				narSize = *obj.NarSize
			}

			narTasks = append(narTasks, narTask{key: pendingObject.Key, narSize: narSize})

			continue
		}

		po, err := s.makePresignedURL(ctx, pendingObject.Key, obj.Type)
		if err != nil {
			return fmt.Errorf("failed to create presigned URL %q: %w", pendingObject.Key, err)
		}

		result[pendingObject.Key] = po
	}

	// Process NAR objects in parallel (multipart uploads require S3 network calls)
	if len(narTasks) == 0 {
		return nil
	}

	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(s.S3Concurrency)

	for _, task := range narTasks {
		g.Go(func() error {
			po, err := s.createMultipartUpload(ctx, pendingClosureID, task.key, task.narSize)
			if err != nil {
				return fmt.Errorf("failed to create multipart upload %q: %w", task.key, err)
			}

			po.Type = "nar"

			mu.Lock()
			result[task.key] = po
			mu.Unlock()

			return nil
		})
	}

	return g.Wait()
}

func (s *Service) makePresignedURL(ctx context.Context, objectKey string, objectType string) (PendingObject, error) {
	presignedURL, err := s.MinioClient.PresignedPutObject(ctx,
		s.Bucket,
		objectKey,
		maxSignedURLDuration)
	if err != nil {
		return PendingObject{}, fmt.Errorf("failed to create presigned URL: %w", err)
	}

	return PendingObject{
		Type:         objectType,
		PresignedURL: presignedURL.String(),
	}, nil
}

func (s *Service) createPendingClosure(
	ctx context.Context,
	pool *pgxpool.Pool,
	closureKey string,
	objectsMap map[string]objectWithRefs,
	verifyS3 bool,
) (*PendingClosureResponse, error) {
	pendingClosure, err := createPendingClosureInner(ctx, pool, closureKey, objectsMap, s, verifyS3)
	if err != nil {
		return nil, err
	}

	pendingObjects := make(map[string]PendingObject, len(pendingClosure.pendingObjects)+len(pendingClosure.deletedObjects))

	if err := s.createPendingObjects(ctx, pendingClosure.id, pendingClosure.pendingObjects, objectsMap, pendingObjects); err != nil {
		return nil, err
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
			obj := objectsMap[objectKey]
			pendingObjectsParams = append(pendingObjectsParams, pg.InsertPendingObjectsParams{
				PendingClosureID: pendingClosure.id,
				Key:              objectKey,
				Refs:             obj.Refs,
			})
		}

		queries := pg.New(pool)

		if _, err = queries.InsertPendingObjects(ctx, pendingObjectsParams); err != nil {
			return nil, fmt.Errorf("failed to insert pending objects: %w", err)
		}

		if err := s.createPendingObjects(ctx, pendingClosure.id, pendingObjectsParams, objectsMap, pendingObjects); err != nil {
			return nil, err
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
