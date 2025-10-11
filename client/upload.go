package client

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// ClosureInfo represents a closure with its associated objects.
type ClosureInfo struct {
	NarinfoKey string
	Objects    []ObjectWithRefs
}

// PrepareClosuresResult contains the result of preparing closures.
type PrepareClosuresResult struct {
	Closures       []ClosureInfo
	PathInfoByHash map[string]*PathInfo
	LogPathsByKey  map[string]string // Maps log object key -> local log file path
}

// PrepareClosures prepares closures from path info, including NAR, .ls, narinfo, and build log objects.
// Build logs are automatically discovered for output paths and included by default.
func PrepareClosures(pathInfos map[string]*PathInfo) (*PrepareClosuresResult, error) {
	closures := make([]ClosureInfo, 0, len(pathInfos))
	pathInfoByHash := make(map[string]*PathInfo)
	logPathsByKey := make(map[string]string)

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

		// Narinfo references both dependencies, its own NAR file, and .ls file
		narinfoRefs := make([]string, 0, len(references)+2)
		narinfoRefs = append(narinfoRefs, references...)
		narinfoRefs = append(narinfoRefs, narKey, lsKey)
		narinfoKey := hash + ".narinfo"

		// Create objects for this closure
		objects := []ObjectWithRefs{
			{
				Key:  narinfoKey,
				Refs: narinfoRefs,
			},
			{
				Key:     narKey,
				Refs:    []string{},
				NarSize: &pathInfo.NarSize, // Include NarSize for multipart estimation
			},
			{
				Key:  lsKey,
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
					Refs: []string{}, // Logs don't reference anything
				})

				// Track the log path for later upload
				logPathsByKey[logKey] = logPath

				slog.Info("Found build log for path", "store_path", storePath, "drv_path", drvPath, "log_key", logKey)
			}
		}

		closures = append(closures, ClosureInfo{
			NarinfoKey: narinfoKey,
			Objects:    objects,
		})
	}

	return &PrepareClosuresResult{
		Closures:       closures,
		PathInfoByHash: pathInfoByHash,
		LogPathsByKey:  logPathsByKey,
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
	key   string
	obj   PendingObject
	isNar bool
	hash  string
}

// UploadPendingObjects uploads all pending objects (NARs, .ls files, narinfos, and build logs).
func (c *Client) UploadPendingObjects(ctx context.Context, pendingObjects map[string]PendingObject, pathInfoByHash map[string]*PathInfo, logPathsByKey map[string]string) error {
	// Separate NAR, .ls, narinfo, and log uploads
	var (
		narTasks     []uploadTask
		lsTasks      []uploadTask
		narinfoTasks []uploadTask
		logTasks     []uploadTask
	)

	for key, obj := range pendingObjects {
		if strings.HasSuffix(key, ".narinfo") {
			hash := strings.TrimSuffix(key, ".narinfo")
			narinfoTasks = append(narinfoTasks, uploadTask{
				key:   key,
				obj:   obj,
				isNar: false,
				hash:  hash,
			})

			continue
		}

		if strings.HasPrefix(key, "nar/") && strings.HasSuffix(key, ".nar.zst") {
			// Extract hash from "nar/HASH.nar.zst"
			filename := strings.TrimPrefix(key, "nar/")
			hash := strings.TrimSuffix(filename, ".nar.zst")
			narTasks = append(narTasks, uploadTask{
				key:   key,
				obj:   obj,
				isNar: true,
				hash:  hash,
			})

			continue
		}

		if strings.HasSuffix(key, ".ls") {
			// Extract hash from "HASH.ls"
			hash := strings.TrimSuffix(key, ".ls")
			lsTasks = append(lsTasks, uploadTask{
				key:   key,
				obj:   obj,
				isNar: false,
				hash:  hash,
			})

			continue
		}

		if strings.HasPrefix(key, "log/") {
			logTasks = append(logTasks, uploadTask{
				key:   key,
				obj:   obj,
				isNar: false,
				hash:  "", // Not used for logs
			})
		}
	}

	// Upload all NAR files in parallel
	compressedInfo, err := c.uploadNARs(ctx, narTasks, pathInfoByHash)
	if err != nil {
		return err
	}

	// Upload .ls files in parallel
	if err := c.uploadListings(ctx, lsTasks, compressedInfo); err != nil {
		return err
	}

	// Upload build logs
	if err := c.uploadLogs(ctx, logTasks, logPathsByKey); err != nil {
		return err
	}

	// Upload narinfo files in parallel
	return c.uploadNarinfos(ctx, narinfoTasks, pathInfoByHash, compressedInfo)
}

func (c *Client) uploadNARs(ctx context.Context, tasks []uploadTask, pathInfoByHash map[string]*PathInfo) (map[string]*CompressedFileInfo, error) {
	// Upload NARs in parallel using worker pool pattern
	type narResult struct {
		hash string
		info *CompressedFileInfo
		err  error
	}

	resultChan := make(chan narResult, len(tasks))
	taskChan := make(chan uploadTask, len(tasks))

	var wg sync.WaitGroup

	// Determine number of workers
	// MaxConcurrentNARUploads of 0 means unlimited
	numWorkers := c.MaxConcurrentNARUploads
	if numWorkers <= 0 {
		numWorkers = len(tasks) // Unlimited - create worker per task
	}

	// Create fixed number of worker goroutines
	for range numWorkers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			// Process tasks from channel until it's closed
			for task := range taskChan {
				pathInfo, ok := pathInfoByHash[task.hash]
				if !ok {
					resultChan <- narResult{
						hash: task.hash,
						err:  fmt.Errorf("path info not found for hash %s", task.hash),
					}

					continue
				}

				info, err := c.CompressAndUploadNAR(ctx, pathInfo.Path, task.obj, task.key)
				if err != nil {
					resultChan <- narResult{
						hash: task.hash,
						err:  fmt.Errorf("uploading NAR %s: %w", task.key, err),
					}

					continue
				}

				resultChan <- narResult{
					hash: task.hash,
					info: info,
				}
			}
		}()
	}

	// Send all tasks to the channel
	for _, task := range tasks {
		taskChan <- task
	}

	close(taskChan) // Signal no more tasks

	// Wait for all workers to complete
	wg.Wait()
	close(resultChan)

	// Collect results
	results := make(map[string]*CompressedFileInfo)

	for result := range resultChan {
		if result.err != nil {
			return nil, result.err
		}

		results[result.hash] = result.info
	}

	return results, nil
}

func (c *Client) uploadListings(ctx context.Context, tasks []uploadTask, compressedInfo map[string]*CompressedFileInfo) error {
	for _, task := range tasks {
		// Get compressed info for this NAR (which includes the listing)
		info := compressedInfo[task.hash]
		if info == nil || info.Listing == nil {
			return fmt.Errorf("listing not found for hash %s", task.hash)
		}

		// Upload listing with brotli compression
		if err := c.UploadListingToPresignedURL(ctx, task.obj.PresignedURL, info.Listing); err != nil {
			return fmt.Errorf("uploading listing %s: %w", task.key, err)
		}
	}

	return nil
}

func (c *Client) uploadLogs(ctx context.Context, tasks []uploadTask, logPathsByKey map[string]string) error {
	for _, task := range tasks {
		// Get the local log path
		logPath, ok := logPathsByKey[task.key]
		if !ok {
			// Log was requested by server but not found locally - this shouldn't happen
			// but we'll log a warning and continue rather than failing the entire upload
			slog.Warn("Build log not found", "key", task.key)

			continue
		}

		// Compress the log to a temporary file
		compressedInfo, err := CompressBuildLog(logPath)
		if err != nil {
			slog.Warn("Failed to compress build log", "key", task.key, "log_path", logPath, "error", err)

			continue
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
	}

	return nil
}

func (c *Client) uploadNarinfos(ctx context.Context, tasks []uploadTask, pathInfoByHash map[string]*PathInfo, compressedInfo map[string]*CompressedFileInfo) error {
	for _, task := range tasks {
		pathInfo, ok := pathInfoByHash[task.hash]
		if !ok {
			return fmt.Errorf("path info not found for hash %s", task.hash)
		}

		// Get compressed info for this NAR
		info := compressedInfo[task.hash]
		if info == nil {
			// This is a server bug: server asked us to upload narinfo without uploading the NAR.
			// NAR and narinfo must always be uploaded together as a closure.
			return fmt.Errorf("server inconsistency: asked to upload narinfo %s without uploading corresponding NAR - this is a server bug", task.key)
		}

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
	}

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
	result, err := PrepareClosures(pathInfos)
	if err != nil {
		return fmt.Errorf("preparing closures: %w", err)
	}

	if len(result.LogPathsByKey) > 0 {
		slog.Info("Found build logs", "count", len(result.LogPathsByKey))
	}

	// Create pending closures and collect what needs uploading
	pendingObjects, pendingIDs, err := c.CreatePendingClosures(ctx, result.Closures)
	if err != nil {
		return fmt.Errorf("creating pending closures: %w", err)
	}

	slog.Info("Need to upload objects", "pending", len(pendingObjects), "total", len(pathInfos)*3)

	// Upload all pending objects
	startTime := time.Now()

	if err := c.UploadPendingObjects(ctx, pendingObjects, result.PathInfoByHash, result.LogPathsByKey); err != nil {
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
