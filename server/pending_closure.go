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
	if size == nil {
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
				return err
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
		return missingObjects, err
	}

	for key := range missingObjects {
		slog.Info("Object in database but missing from S3", "key", key)
	}

	return missingObjects, nil
}

// registerPendingClosure records the closure and all its objects as
// pending. GC leaves pending keys alone.
func registerPendingClosure(
	ctx context.Context,
	pool *pgxpool.Pool,
	closureKey string,
	objectsMap map[string]objectWithRefs,
) (pg.PendingClosure, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return pg.PendingClosure{}, fmt.Errorf("failed to start transaction: %w", err)
	}

	defer func() { _ = tx.Rollback(ctx) }()

	queries := pg.New(tx)

	pc, err := queries.InsertPendingClosure(ctx, closureKey)
	if err != nil {
		return pc, fmt.Errorf("failed to insert pending closure: %w", err)
	}

	rows := make([]pg.InsertPendingObjectsParams, 0, len(objectsMap))
	for key, obj := range objectsMap {
		rows = append(rows, pg.InsertPendingObjectsParams{
			PendingClosureID: pc.ID,
			Key:              key,
			Refs:             obj.Refs,
			Size:             optionalSize(obj.NarSize),
		})
	}

	if _, err = queries.InsertPendingObjects(ctx, rows); err != nil {
		return pc, fmt.Errorf("failed to insert pending objects: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return pc, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return pc, nil
}

// keysToUpload returns keys without a live row (plus, with verifyS3, live
// rows missing from S3).
func (s *Service) keysToUpload(
	ctx context.Context,
	objectsMap map[string]objectWithRefs,
	verifyS3 bool,
) ([]string, error) {
	keys := make([]string, 0, len(objectsMap))
	for k := range objectsMap {
		keys = append(keys, k)
	}

	live, err := pg.New(s.Pool).GetLiveObjects(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing objects: %w", err)
	}

	present := make(map[string]struct{}, len(live))
	for _, k := range live {
		present[k] = struct{}{}
	}

	if verifyS3 && len(live) > 0 {
		missing, err := s.checkS3ObjectsExist(ctx, live)
		if err != nil {
			return nil, fmt.Errorf("failed to verify objects in S3: %w", err)
		}

		if len(missing) > 0 {
			slog.Warn("Found objects in DB but missing from S3, will re-upload", "count", len(missing))
		}

		for k := range missing {
			delete(present, k)
		}
	}

	upload := keys[:0]

	for _, k := range keys {
		if _, ok := present[k]; !ok {
			upload = append(upload, k)
		}
	}

	return upload, nil
}

// createPendingObjects presigns URLs (local) and opens multipart uploads
// (S3 calls, parallel) for keys.
func (s *Service) createPendingObjects(
	ctx context.Context,
	pendingClosureID int64,
	keys []string,
	objectsMap map[string]objectWithRefs,
) (map[string]PendingObject, error) {
	result := make(map[string]PendingObject, len(keys))

	type narTask struct {
		key     string
		narSize uint64
	}

	var narTasks []narTask

	for _, key := range keys {
		obj := objectsMap[key]

		if obj.Type == "nar" {
			var narSize uint64
			if obj.NarSize != nil {
				narSize = *obj.NarSize
			}

			// Small NARs fall through to a presigned PUT like the other small objects.
			if !useSimpleUpload(narSize) {
				narTasks = append(narTasks, narTask{key: key, narSize: narSize})

				continue
			}
		}

		po, err := s.makePresignedURL(ctx, key, obj.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to create presigned URL %q: %w", key, err)
		}

		result[key] = po
	}

	if len(narTasks) == 0 {
		return result, nil
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

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("creating multipart uploads: %w", err)
	}

	return result, nil
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
	closureKey string,
	objectsMap map[string]objectWithRefs,
	verifyS3 bool,
) (*PendingClosureResponse, error) {
	if !strings.HasSuffix(closureKey, ".narinfo") {
		return nil, fmt.Errorf("closure key must end with .narinfo: %s", closureKey)
	}

	pc, err := registerPendingClosure(ctx, s.Pool, closureKey, objectsMap)
	if err != nil {
		return nil, err
	}

	upload, err := s.keysToUpload(ctx, objectsMap, verifyS3)
	if err != nil {
		return nil, err
	}

	pendingObjects, err := s.createPendingObjects(ctx, pc.ID, upload, objectsMap)
	if err != nil {
		return nil, err
	}

	return &PendingClosureResponse{
		ID:             strconv.FormatInt(pc.ID, 10),
		StartedAt:      pc.StartedAt.Time,
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
