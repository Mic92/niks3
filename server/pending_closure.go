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
)

const (
	maxSignedURLDuration = time.Duration(5) * time.Hour
	s3CheckBatchSize     = 100
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

	type checkResult struct {
		key     string
		missing bool
		err     error
	}

	// Task channel for S3 checks
	taskChan := make(chan string, len(objectKeys))
	resultChan := make(chan checkResult, len(objectKeys))

	// Track errors
	errChan := make(chan error, 1)

	var errOnce sync.Once

	// Create worker pool (use s3CheckBatchSize as concurrency limit)
	numWorkers := min(s3CheckBatchSize, len(objectKeys))

	var wg sync.WaitGroup

	for range numWorkers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for key := range taskChan {
				_, err := s.MinioClient.StatObject(ctx, s.Bucket, key, minio.StatObjectOptions{})
				if err != nil {
					errResp := minio.ToErrorResponse(err)
					if errResp.Code == "NoSuchKey" {
						resultChan <- checkResult{key: key, missing: true}
					} else {
						// Report error and stop processing
						errOnce.Do(func() {
							errChan <- fmt.Errorf("failed to check S3 object %q: %w", key, err)
						})

						resultChan <- checkResult{key: key, err: err}

						return
					}
				} else {
					resultChan <- checkResult{key: key, missing: false}
				}
			}
		}()
	}

	// Queue all check tasks
	for _, key := range objectKeys {
		taskChan <- key
	}

	close(taskChan)

	// Wait for workers to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	missingObjects := make(map[string]bool)

	for result := range resultChan {
		// Check for errors first
		select {
		case err := <-errChan:
			return missingObjects, err
		default:
		}

		if result.missing {
			missingObjects[result.key] = true
			slog.Info("Object in database but missing from S3", "key", result.key)
		}
	}

	// Final error check
	select {
	case err := <-errChan:
		return missingObjects, err
	default:
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

func (s *Service) createPendingObjects(
	ctx context.Context,
	pendingClosureID int64,
	pendingObjectsParams []pg.InsertPendingObjectsParams,
	objectsMap map[string]objectWithRefs,
	result map[string]PendingObject,
) error {
	for _, pendingObject := range pendingObjectsParams {
		obj := objectsMap[pendingObject.Key]

		var narSize uint64
		if obj.NarSize != nil {
			narSize = *obj.NarSize
		}

		po, err := s.makePendingObject(ctx, pendingClosureID, pendingObject.Key, obj.Type, narSize)
		if err != nil {
			return fmt.Errorf("failed to create pending object: %w", err)
		}

		result[pendingObject.Key] = po
	}

	return nil
}

func (s *Service) makePendingObject(ctx context.Context, pendingClosureID int64, objectKey string, objectType string, narSize uint64) (PendingObject, error) {
	// Small files use simple presigned URL
	switch objectType {
	case "narinfo", "listing", "build_log", "realisation":
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

	case "nar":
		// NAR files (large) use multipart upload
		po, err := s.createMultipartUpload(ctx, pendingClosureID, objectKey, narSize)
		if err != nil {
			return PendingObject{}, err
		}

		po.Type = objectType

		return po, nil

	default:
		return PendingObject{}, fmt.Errorf("unknown object type %q for key: %s", objectType, objectKey)
	}
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
