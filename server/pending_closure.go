package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Mic92/niks3/server/pg"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"
)

const (
	maxSignedURLDuration = time.Duration(5) * time.Hour
)

// optionalSize maps a reported size to a nullable column; nil stays NULL and is
// excluded from byte totals.
func optionalSize(size *uint64) pgtype.Int8 {
	// A size beyond int64 cannot be stored in the BIGINT column; treat it as
	// unknown rather than wrapping around.
	if size == nil || *size > math.MaxInt64 {
		return pgtype.Int8{}
	}

	return pgtype.Int8{Int64: int64(*size), Valid: true}
}

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
	pendingObjects []pg.InsertPendingObjectsParams // objects the client must upload
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
			if err := s.S3RateLimiter.Wait(ctx); err != nil {
				return fmt.Errorf("rate limiter: %w", err)
			}

			_, err := s.MinioClient.StatObject(ctx, s.Bucket, key, minio.StatObjectOptions{})
			if err != nil {
				if isRateLimitError(err) {
					s.S3RateLimiter.RecordThrottle()
				}

				errResp := minio.ToErrorResponse(err)
				if errResp.Code == minio.NoSuchKey {
					mu.Lock()
					missingObjects[key] = true
					mu.Unlock()
					s.S3RateLimiter.RecordSuccess()

					return nil
				}
				// Return error to cancel the group
				return fmt.Errorf("failed to check S3 object %q: %w", key, err)
			}

			s.S3RateLimiter.RecordSuccess()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return missingObjects, fmt.Errorf("check S3 objects: %w", err)
	}

	for key := range missingObjects {
		slog.Info("Object in database but missing from S3", "key", key)
	}

	return missingObjects, nil
}

func createPendingClosureInner(
	ctx context.Context,
	pool *pgxpool.Pool,
	closureKey string,
	roots []string,
	objectsMap map[string]objectWithRefs,
	s *Service,
	verifyS3 bool,
) (*PendingClosure, error) {
	if !strings.HasSuffix(closureKey, ".narinfo") {
		return nil, fmt.Errorf("closure key must end with .narinfo: %s", closureKey)
	}

	if s.testHookBeforePendingInsert != nil {
		s.testHookBeforePendingInsert()
	}

	// Every object of the closure gets a pending_objects row, whether or not
	// it is present already. The row is what shields the object from GC while
	// the push is in flight (MarkStaleObjects and GetObjectsReadyForDeletion
	// skip pending keys) and what makes commit_pending_closure clear a
	// tombstone the object picked up in the meantime.
	//
	// The rows are written before the existence check, not after: an object
	// found present and then tombstoned and swept before its row existed
	// would be committed live with nothing behind it in S3. With the rows in
	// place first, anything the check then sees as live stays live until the
	// closure commits or is cleaned up.
	pendingClosure, allObjects, err := insertPendingClosure(ctx, pool, closureKey, roots, objectsMap)
	if err != nil {
		return nil, err
	}

	uploadObjects, err := s.objectsToUpload(ctx, pool, allObjects, verifyS3)
	if err != nil {
		// Without a response the client never commits this closure; drop it
		// now rather than have its rows shield objects until GC cleans it up.
		if delErr := pg.New(pool).DeletePendingClosure(context.WithoutCancel(ctx), pendingClosure.ID); delErr != nil {
			slog.Warn("failed to drop pending closure after error", "id", pendingClosure.ID, "error", delErr)
		}

		return nil, err
	}

	return &PendingClosure{
		id:             pendingClosure.ID,
		startedAt:      pendingClosure.StartedAt.Time,
		pendingObjects: uploadObjects,
	}, nil
}

// insertPendingClosure records the closure and a pending row for each of its
// objects in one transaction.
func insertPendingClosure(
	ctx context.Context,
	pool *pgxpool.Pool,
	closureKey string,
	roots []string,
	objectsMap map[string]objectWithRefs,
) (pg.PendingClosure, []pg.InsertPendingObjectsParams, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return pg.PendingClosure{}, nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	committed := false

	// rollbackOnError reads err, so every error below must be assigned to it
	// (no := in nested scopes) or the transaction and its connection leak.
	defer rollbackOnError(ctx, &tx, &err, &committed)

	queries := pg.New(tx)

	var pendingClosure pg.PendingClosure

	if roots != nil {
		pendingClosure, err = queries.InsertPush(ctx, pg.InsertPushParams{Key: closureKey, Roots: roots})
	} else {
		pendingClosure, err = queries.InsertPendingClosure(ctx, closureKey)
	}

	if err != nil {
		return pg.PendingClosure{}, nil, fmt.Errorf("failed to insert pending closure: %w", err)
	}

	allObjects := make([]pg.InsertPendingObjectsParams, 0, len(objectsMap))

	for objectKey, obj := range objectsMap {
		allObjects = append(allObjects, pg.InsertPendingObjectsParams{
			PendingClosureID: pendingClosure.ID,
			Key:              objectKey,
			Refs:             obj.Refs,
			Size:             optionalSize(obj.NarSize),
		})
	}

	if _, err = queries.InsertPendingObjects(ctx, allObjects); err != nil {
		return pg.PendingClosure{}, nil, fmt.Errorf("failed to insert pending objects: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return pg.PendingClosure{}, nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	committed = true

	return pendingClosure, allObjects, nil
}

// objectsToUpload returns the pending rows whose objects the client must
// upload: those the database does not consider live and, with verifyS3, those
// the database considers live but S3 does not have.
func (s *Service) objectsToUpload(
	ctx context.Context,
	pool *pgxpool.Pool,
	allObjects []pg.InsertPendingObjectsParams,
	verifyS3 bool,
) ([]pg.InsertPendingObjectsParams, error) {
	keys := make([]string, 0, len(allObjects))
	for _, row := range allObjects {
		keys = append(keys, row.Key)
	}

	// The S3 round trips must not hold a pool connection, or a few verifying
	// pushes starve every other handler; neither query needs a transaction.
	existingObjects, err := pg.New(pool).GetExistingObjects(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing objects: %w", err)
	}

	// Objects the database considers live. Tombstoned objects are excluded:
	// GC may already have removed them from S3, so they are uploaded again.
	present := make(map[string]bool, len(existingObjects))

	for _, existingObject := range existingObjects {
		if !existingObject.DeletedAt.Valid {
			present[existingObject.Key] = true
		}
	}

	// Verify that objects the DB says exist actually exist in S3 (if requested)
	if verifyS3 && len(present) > 0 {
		keysToVerifyInS3 := make([]string, 0, len(present))
		for key := range present {
			keysToVerifyInS3 = append(keysToVerifyInS3, key)
		}

		missingFromS3, err := s.checkS3ObjectsExist(ctx, keysToVerifyInS3)
		if err != nil {
			return nil, fmt.Errorf("failed to verify objects in S3: %w", err)
		}

		if len(missingFromS3) > 0 {
			slog.Warn("Found objects in DB but missing from S3, will re-upload",
				"count", len(missingFromS3))

			for missingKey := range missingFromS3 {
				delete(present, missingKey)
			}
		}
	}

	uploadObjects := make([]pg.InsertPendingObjectsParams, 0, len(allObjects)-len(present))

	for _, row := range allObjects {
		if !present[row.Key] {
			uploadObjects = append(uploadObjects, row)
		}
	}

	return uploadObjects, nil
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

		if obj.Type == objectTypeNar {
			var narSize uint64
			if obj.NarSize != nil {
				narSize = *obj.NarSize
			}

			// Small NARs fall through to a presigned PUT like the other small objects.
			if !useSimpleUpload(narSize) {
				narTasks = append(narTasks, narTask{key: pendingObject.Key, narSize: narSize})

				continue
			}
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

			po.Type = objectTypeNar

			mu.Lock()
			result[task.key] = po
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("create multipart uploads: %w", err)
	}

	return nil
}

func (s *Service) makePresignedURL(ctx context.Context, objectKey string, objectType string) (PendingObject, error) {
	if err := s.S3RateLimiter.Wait(ctx); err != nil {
		return PendingObject{}, fmt.Errorf("rate limiter: %w", err)
	}

	presignedURL, err := s.PresignClient.PresignedPutObject(ctx,
		s.Bucket,
		objectKey,
		maxSignedURLDuration)
	if err != nil {
		if isRateLimitError(err) {
			s.S3RateLimiter.RecordThrottle()
		}

		return PendingObject{}, fmt.Errorf("failed to create presigned URL: %w", err)
	}

	s.S3RateLimiter.RecordSuccess()

	return PendingObject{
		Type:         objectType,
		PresignedURL: presignedURL.String(),
	}, nil
}

func (s *Service) createPendingClosure(
	ctx context.Context,
	pool *pgxpool.Pool,
	closureKey string,
	roots []string,
	objectsMap map[string]objectWithRefs,
	verifyS3 bool,
) (*PendingClosureResponse, error) {
	pendingClosure, err := createPendingClosureInner(ctx, pool, closureKey, roots, objectsMap, s, verifyS3)
	if err != nil {
		return nil, err
	}

	pendingObjects := make(map[string]PendingObject, len(pendingClosure.pendingObjects))

	if err := s.createPendingObjects(ctx, pendingClosure.id, pendingClosure.pendingObjects, objectsMap, pendingObjects); err != nil {
		return nil, err
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
