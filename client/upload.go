package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

// ClosureInfo represents a closure with its associated objects.
type ClosureInfo struct {
	NarinfoKey string
	Objects    []ObjectWithRefs
}

// PrepareClosuresResult contains the result of preparing closures.
type PrepareClosuresResult struct {
	Closures          []ClosureInfo
	PathInfoByHash    map[string]*PathInfo
	LogPathsByKey     map[string]string           // Maps log object key -> local log file path
	RealisationsByKey map[string]*RealisationInfo // Maps realisation key -> realisation info
}

// compressWithZstd compresses data using zstd compression.
// Uses the pooled encoder from client.go for efficiency.
func compressWithZstd(data []byte) ([]byte, error) {
	var compressed bytes.Buffer

	encoder, ok := zstdEncoderPool.Get().(*zstd.Encoder)
	if !ok {
		return nil, errors.New("failed to get zstd encoder from pool")
	}
	defer zstdEncoderPool.Put(encoder)

	encoder.Reset(&compressed)

	if _, err := encoder.Write(data); err != nil {
		return nil, fmt.Errorf("compressing data: %w", err)
	}

	if err := encoder.Close(); err != nil {
		return nil, fmt.Errorf("closing zstd encoder: %w", err)
	}

	return compressed.Bytes(), nil
}

// PrepareClosures prepares closures from path info, including NAR, .ls, narinfo, build log, and realisation objects.
// Build logs are automatically discovered for output paths and included by default.
// Realisations are queried for CA derivations and included automatically.
func PrepareClosures(ctx context.Context, pathInfos map[string]*PathInfo) (*PrepareClosuresResult, error) {
	closures := make([]ClosureInfo, 0, len(pathInfos))
	pathInfoByHash := make(map[string]*PathInfo)
	logPathsByKey := make(map[string]string)

	// Query realisations for CA paths
	realisations, err := QueryRealisations(ctx, pathInfos)
	if err != nil {
		// Log warning but don't fail - realisations are optional
		slog.Warn("Failed to query realisations (CA derivations may not upload correctly)", "error", err)

		realisations = make(map[string]*RealisationInfo)
	}

	for storePath, pathInfo := range pathInfos {
		hash, err := GetStorePathHash(storePath)
		if err != nil {
			return nil, fmt.Errorf("getting store path hash: %w", err)
		}

		pathInfoByHash[hash] = pathInfo

		// Extract references as object keys (hash.narinfo)
		var references []string

		for _, ref := range pathInfo.References {
			refHash, err := GetStorePathHash(ref)
			if err != nil {
				return nil, fmt.Errorf("getting reference hash: %w", err)
			}

			// Store reference as object key (hash.narinfo) so GC can follow it
			references = append(references, refHash+".narinfo")
		}

		// NAR file object
		narFilename := hash + ".nar.zst"
		narKey := "nar/" + narFilename

		// .ls file (directory listing with brotli compression)
		lsKey := hash + ".ls"

		// Check if this path has realisation objects
		var realisationKeys []string

		for realisationKey, realisation := range realisations {
			if realisation.OutPath == storePath {
				realisationKeys = append(realisationKeys, realisationKey)
			}
		}

		// Narinfo references both dependencies, its own NAR file, .ls file, and any realisations
		narinfoRefs := make([]string, 0, len(references)+2+len(realisationKeys))
		narinfoRefs = append(narinfoRefs, references...)
		narinfoRefs = append(narinfoRefs, narKey, lsKey)
		narinfoRefs = append(narinfoRefs, realisationKeys...)
		narinfoKey := hash + ".narinfo"

		// Create objects for this closure
		objects := []ObjectWithRefs{
			{
				Key:  narinfoKey,
				Type: ObjectTypeNarinfo,
				Refs: narinfoRefs,
			},
			{
				Key:     narKey,
				Type:    ObjectTypeNAR,
				Refs:    []string{},
				NarSize: &pathInfo.NarSize, // Include NarSize for multipart estimation
			},
			{
				Key:  lsKey,
				Type: ObjectTypeListing,
				Refs: []string{},
			},
		}

		// Check if this path has a deriver (i.e., was built) and has a build log
		if pathInfo.Deriver != nil && *pathInfo.Deriver != "" {
			drvPath := *pathInfo.Deriver

			logPath, err := GetBuildLogPath(drvPath)
			if err != nil {
				slog.Warn("Error checking for build log", "drv_path", drvPath, "store_path", storePath, "error", err)
			} else if logPath != "" {
				// Build log exists - add log object
				drvName := strings.TrimPrefix(drvPath, "/nix/store/")
				logKey := "log/" + drvName

				objects = append(objects, ObjectWithRefs{
					Key:  logKey,
					Type: ObjectTypeBuildLog,
					Refs: []string{}, // Logs don't reference anything
				})

				// Track the log path for later upload
				logPathsByKey[logKey] = logPath

				slog.Info("Found build log for path", "store_path", storePath, "drv_path", drvPath, "log_key", logKey)
			}
		}

		// Add realisation objects for CA derivations
		for _, realisationKey := range realisationKeys {
			objects = append(objects, ObjectWithRefs{
				Key:  realisationKey,
				Type: ObjectTypeRealisation,
				Refs: []string{}, // Realisations don't reference other objects
			})
		}

		closures = append(closures, ClosureInfo{
			NarinfoKey: narinfoKey,
			Objects:    objects,
		})
	}

	return &PrepareClosuresResult{
		Closures:          closures,
		PathInfoByHash:    pathInfoByHash,
		LogPathsByKey:     logPathsByKey,
		RealisationsByKey: realisations,
	}, nil
}

// CreatePendingClosures creates pending closures and returns all pending objects and closure IDs.
func (c *Client) CreatePendingClosures(ctx context.Context, closures []ClosureInfo) (map[string]PendingObject, []string, error) {
	pendingObjects := make(map[string]PendingObject)
	pendingIDs := make([]string, 0, len(closures))

	for _, closure := range closures {
		resp, err := c.CreatePendingClosure(ctx, closure.NarinfoKey, closure.Objects)
		if err != nil {
			return nil, nil, fmt.Errorf("creating pending closure: %w", err)
		}

		pendingIDs = append(pendingIDs, resp.ID)

		// Collect pending objects
		for key, obj := range resp.PendingObjects {
			pendingObjects[key] = obj
		}
	}

	return pendingObjects, pendingIDs, nil
}

type uploadTask struct {
	key  string
	obj  PendingObject
	hash string
}

// UploadPendingObjects uploads all pending objects (NARs, .ls files, narinfos, build logs, and realisations).
// Uses a unified worker pool where:
// - Logs and realisations upload immediately (independent)
// - NARs upload and queue their listings/narinfos when complete
// - All object types share the same worker pool for maximum parallelism.
func (c *Client) UploadPendingObjects(ctx context.Context, pendingObjects map[string]PendingObject, pathInfoByHash map[string]*PathInfo, logPathsByKey map[string]string, realisationsByKey map[string]*RealisationInfo) error {
	// Collect pending objects by type
	pendingByHash := make(map[string]struct {
		narTask     *uploadTask
		lsTask      *uploadTask
		narinfoTask *uploadTask
	})

	var (
		logTasks         []uploadTask
		realisationTasks []uploadTask
	)

	for key, obj := range pendingObjects {
		switch obj.Type {
		case "narinfo":
			hash := strings.TrimSuffix(key, ".narinfo")
			entry := pendingByHash[hash]
			entry.narinfoTask = &uploadTask{key: key, obj: obj, hash: hash}
			pendingByHash[hash] = entry

		case "nar":
			filename := strings.TrimPrefix(key, "nar/")
			hash := strings.TrimSuffix(filename, ".nar.zst")
			entry := pendingByHash[hash]
			entry.narTask = &uploadTask{key: key, obj: obj, hash: hash}
			pendingByHash[hash] = entry

		case "listing":
			hash := strings.TrimSuffix(key, ".ls")
			entry := pendingByHash[hash]
			entry.lsTask = &uploadTask{key: key, obj: obj, hash: hash}
			pendingByHash[hash] = entry

		case "build_log":
			logTasks = append(logTasks, uploadTask{key: key, obj: obj, hash: ""})

		case "realisation":
			realisationTasks = append(realisationTasks, uploadTask{key: key, obj: obj, hash: ""})

		default:
			return fmt.Errorf("unknown object type %q for key: %s", obj.Type, key)
		}
	}

	// Upload all objects with unified worker pool
	return c.uploadAllObjects(ctx, pendingByHash, logTasks, realisationTasks, pathInfoByHash, logPathsByKey, realisationsByKey)
}

// genericUploadTask represents any upload operation in the unified worker pool.
type genericUploadTask struct {
	taskType string // "nar", "listing", "narinfo", "log", "realisation"
	task     uploadTask
	hash     string // For looking up related tasks
}

// uploadAllObjects uploads all objects using a unified worker pool.
// Logs and realisations upload independently, NARs upload with their listings in the same goroutine,
// then narinfos are uploaded separately after all NARs+listings complete.
func (c *Client) uploadAllObjects(ctx context.Context, pendingByHash map[string]struct {
	narTask     *uploadTask
	lsTask      *uploadTask
	narinfoTask *uploadTask
}, logTasks []uploadTask, realisationTasks []uploadTask, pathInfoByHash map[string]*PathInfo, logPathsByKey map[string]string, realisationsByKey map[string]*RealisationInfo,
) error {
	// Shared state for compressed NAR info (protected by mutex for concurrent writes in phase 1)
	var compressedInfoMu sync.Mutex

	compressedInfo := make(map[string]*CompressedFileInfo)

	// Task channel for NAR, log, and realisation uploads
	taskChan := make(chan genericUploadTask, len(pendingByHash)+len(logTasks)+len(realisationTasks))

	// Track errors
	errChan := make(chan error, 1)

	var errOnce sync.Once

	// Determine number of workers
	numWorkers := c.MaxConcurrentNARUploads
	if numWorkers <= 0 {
		numWorkers = len(pendingByHash) + len(logTasks) + len(realisationTasks)
	}

	// Phase 1: Upload NARs (with listings), logs, and realisations in parallel
	var phase1WG sync.WaitGroup
	for range numWorkers {
		phase1WG.Add(1)

		go func() {
			defer phase1WG.Done()

			for task := range taskChan {
				var err error

				switch task.taskType {
				case "nar":
					err = c.uploadNARWithListing(ctx, task, pendingByHash, pathInfoByHash, compressedInfo, &compressedInfoMu)

				case "log":
					err = c.uploadLog(ctx, task.task, logPathsByKey)

				case "realisation":
					err = c.uploadRealisation(ctx, task.task, realisationsByKey)
				}

				if err != nil {
					errOnce.Do(func() {
						errChan <- err
					})
				}
			}
		}()
	}

	// Queue all log tasks
	for _, task := range logTasks {
		taskChan <- genericUploadTask{taskType: "log", task: task}
	}

	// Queue all realisation tasks
	for _, task := range realisationTasks {
		taskChan <- genericUploadTask{taskType: "realisation", task: task}
	}

	// Queue all NAR tasks
	for hash, entry := range pendingByHash {
		if entry.narTask != nil {
			taskChan <- genericUploadTask{taskType: "nar", task: *entry.narTask, hash: hash}
		}
	}

	close(taskChan)

	// Wait for phase 1 to complete
	phase1WG.Wait()

	// Check for errors from phase 1
	select {
	case err := <-errChan:
		if err != nil {
			return err
		}
	default:
	}

	// Phase 2: Upload narinfos for successfully uploaded NARs
	// Create task channel for narinfos
	narinfoTaskChan := make(chan genericUploadTask, len(pendingByHash))
	for hash, entry := range pendingByHash {
		// Only upload narinfo if NAR was successfully uploaded (check compressedInfo)
		if _, ok := compressedInfo[hash]; ok && entry.narinfoTask != nil {
			narinfoTaskChan <- genericUploadTask{taskType: "narinfo", task: *entry.narinfoTask, hash: hash}
		}
	}

	close(narinfoTaskChan)

	var phase2WG sync.WaitGroup
	for range numWorkers {
		phase2WG.Add(1)

		go func() {
			defer phase2WG.Done()

			for task := range narinfoTaskChan {
				if err := c.uploadNarinfo(ctx, task.task, pathInfoByHash[task.hash], compressedInfo[task.hash]); err != nil {
					errOnce.Do(func() {
						errChan <- err
					})
				}
			}
		}()
	}

	phase2WG.Wait()

	// Check for errors from phase 2
	select {
	case err := <-errChan:
		return err
	default:
	}

	return nil
}

// uploadNARWithListing uploads a NAR and its listing.
// Successfully uploaded NARs are stored in compressedInfo for later narinfo uploads.
func (c *Client) uploadNARWithListing(
	ctx context.Context,
	task genericUploadTask,
	pendingByHash map[string]struct {
		narTask     *uploadTask
		lsTask      *uploadTask
		narinfoTask *uploadTask
	},
	pathInfoByHash map[string]*PathInfo,
	compressedInfo map[string]*CompressedFileInfo,
	compressedInfoMu *sync.Mutex,
) error {
	// Upload NAR
	info, err := c.CompressAndUploadNAR(ctx, pathInfoByHash[task.hash].Path, task.task.obj, task.task.key)
	if err != nil {
		return fmt.Errorf("uploading NAR %s: %w", task.task.key, err)
	}

	// Store compressed info for narinfo phase (protected by mutex for concurrent writes)
	compressedInfoMu.Lock()

	compressedInfo[task.hash] = info

	compressedInfoMu.Unlock()

	// Upload listing immediately in same goroutine
	entry := pendingByHash[task.hash]
	if entry.lsTask != nil {
		if err := c.uploadListing(ctx, *entry.lsTask, info); err != nil {
			return err
		}
	}

	return nil
}

// uploadListing uploads a listing file.
func (c *Client) uploadListing(ctx context.Context, task uploadTask, info *CompressedFileInfo) error {
	if info.Listing == nil {
		return fmt.Errorf("listing not found for hash %s", task.hash)
	}

	// Upload listing with brotli compression
	if err := c.UploadListingToPresignedURL(ctx, task.obj.PresignedURL, info.Listing); err != nil {
		return fmt.Errorf("uploading listing %s: %w", task.key, err)
	}

	slog.Info("Uploaded listing", "key", task.key)

	return nil
}

// uploadLog uploads a build log.
func (c *Client) uploadLog(ctx context.Context, task uploadTask, logPathsByKey map[string]string) error {
	// Get the local log path
	logPath, ok := logPathsByKey[task.key]
	if !ok {
		// Log was requested by server but not found locally - this shouldn't happen
		// but we'll log a warning and continue rather than failing the entire upload
		slog.Warn("Build log not found", "key", task.key)

		return nil // Don't fail the entire upload
	}

	// Compress the log to a temporary file
	compressedInfo, err := CompressBuildLog(logPath)
	if err != nil {
		slog.Warn("Failed to compress build log", "key", task.key, "log_path", logPath, "error", err)

		return nil // Don't fail the entire upload
	}

	defer func() {
		if cleanupErr := compressedInfo.Cleanup(); cleanupErr != nil {
			slog.Warn("Failed to cleanup compressed build log", "key", task.key, "error", cleanupErr)
		}
	}()

	// Upload the compressed log
	if err := c.UploadBuildLogToPresignedURL(ctx, task.obj.PresignedURL, compressedInfo); err != nil {
		return fmt.Errorf("uploading build log %s: %w", task.key, err)
	}

	slog.Info("Uploaded build log", "key", task.key)

	return nil
}

// uploadRealisation uploads a realisation (.doi) file for CA derivations.
func (c *Client) uploadRealisation(ctx context.Context, task uploadTask, realisationsByKey map[string]*RealisationInfo) error {
	// Get the realisation info
	realisationInfo, ok := realisationsByKey[task.key]
	if !ok {
		// Realisation was requested by server but not found locally - this shouldn't happen
		// but we'll log a warning and continue rather than failing the entire upload
		slog.Warn("Realisation not found", "key", task.key)

		return nil // Don't fail the entire upload
	}

	// Marshal realisation to JSON
	jsonData, err := json.Marshal(realisationInfo)
	if err != nil {
		return fmt.Errorf("marshaling realisation %s: %w", task.key, err)
	}

	// Compress with zstd (like narinfo)
	compressed, err := compressWithZstd(jsonData)
	if err != nil {
		return fmt.Errorf("compressing realisation %s: %w", task.key, err)
	}

	// Upload with Content-Encoding header
	headers := map[string]string{
		"Content-Encoding": "zstd",
	}

	if err := c.UploadBytesToPresignedURLWithHeaders(ctx, task.obj.PresignedURL, compressed, headers); err != nil {
		return fmt.Errorf("uploading realisation %s: %w", task.key, err)
	}

	slog.Info("Uploaded realisation", "key", task.key)

	return nil
}

// uploadNarinfo uploads a narinfo file.
func (c *Client) uploadNarinfo(ctx context.Context, task uploadTask, pathInfo *PathInfo, info *CompressedFileInfo) error {
	// Generate narinfo content
	narinfoContent := CreateNarinfo(
		pathInfo,
		task.hash+".nar.zst",
		info.Size,
		info.Hash,
	)

	// Upload narinfo with zstd compression
	if err := c.UploadNarinfoToPresignedURL(ctx, task.obj.PresignedURL, []byte(narinfoContent)); err != nil {
		return fmt.Errorf("uploading narinfo %s: %w", task.key, err)
	}

	slog.Info("Uploaded narinfo", "key", task.key)

	return nil
}

// CompletePendingClosures completes all pending closures.
func (c *Client) CompletePendingClosures(ctx context.Context, pendingIDs []string) error {
	for _, id := range pendingIDs {
		if err := c.CompletePendingClosure(ctx, id); err != nil {
			return fmt.Errorf("completing pending closure %s: %w", id, err)
		}
	}

	return nil
}

// PushPaths uploads store paths and their closures to the server.
func (c *Client) PushPaths(ctx context.Context, paths []string) error {
	// Get path info for all paths and their closures
	slog.Info("Getting path info", "count", len(paths))

	pathInfos, err := GetPathInfoRecursive(ctx, paths)
	if err != nil {
		return fmt.Errorf("getting path info: %w", err)
	}

	slog.Info("Found paths in closure", "count", len(pathInfos))

	// Prepare closures
	result, err := PrepareClosures(ctx, pathInfos)
	if err != nil {
		return fmt.Errorf("preparing closures: %w", err)
	}

	if len(result.LogPathsByKey) > 0 {
		slog.Info("Found build logs", "count", len(result.LogPathsByKey))
	}

	if len(result.RealisationsByKey) > 0 {
		slog.Info("Found realisations for CA derivations", "count", len(result.RealisationsByKey))
	}

	// Create pending closures and collect what needs uploading
	pendingObjects, pendingIDs, err := c.CreatePendingClosures(ctx, result.Closures)
	if err != nil {
		return fmt.Errorf("creating pending closures: %w", err)
	}

	slog.Info("Need to upload objects", "pending", len(pendingObjects), "total", len(pathInfos)*3)

	// Upload all pending objects
	startTime := time.Now()

	if err := c.UploadPendingObjects(ctx, pendingObjects, result.PathInfoByHash, result.LogPathsByKey, result.RealisationsByKey); err != nil {
		return fmt.Errorf("uploading objects: %w", err)
	}

	duration := time.Since(startTime)

	slog.Info("Uploaded all objects", "duration", duration)

	// Complete all pending closures
	if err := c.CompletePendingClosures(ctx, pendingIDs); err != nil {
		return fmt.Errorf("completing closures: %w", err)
	}

	slog.Info("Upload completed successfully")

	return nil
}
