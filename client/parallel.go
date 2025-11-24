package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type uploadTask struct {
	key  string
	obj  PendingObject
	hash string
}

// genericUploadTask represents any upload operation in the unified worker pool.
type genericUploadTask struct {
	taskType string // "nar", "listing", "narinfo", "log", "realisation"
	task     uploadTask
	hash     string // For looking up related tasks
}

// pendingObjectsByHash groups related objects by their store path hash.
type pendingObjectsByHash map[string]struct {
	narTask     *uploadTask
	lsTask      *uploadTask
	narinfoTask *uploadTask
}

// UploadContext contains all the context needed for uploading objects.
type UploadContext struct {
	PendingObjects    map[string]PendingObject
	PathInfoByHash    map[string]*PathInfo
	NARKeyToHash      map[string]string
	LogPathsByKey     map[string]string
	RealisationsByKey map[string]*RealisationInfo
}

// UploadPendingObjects uploads all pending objects (NARs, .ls files, build logs, and realisations).
// Returns narinfo metadata for each closure to be signed and uploaded by the server.
// Uses a unified worker pool where:
// - Logs and realisations upload immediately (independent)
// - NARs upload and queue their listings when complete
// - Narinfo metadata is collected (not uploaded) for server-side signing.
func (c *Client) UploadPendingObjects(ctx context.Context, uploadCtx *UploadContext) (map[string]NarinfoMetadata, error) {
	// Collect pending objects by type
	pendingByHash := make(pendingObjectsByHash)

	var (
		logTasks         []uploadTask
		realisationTasks []uploadTask
	)

	for key, obj := range uploadCtx.PendingObjects {
		switch obj.Type {
		case "narinfo":
			hash := strings.TrimSuffix(key, ".narinfo")
			entry := pendingByHash[hash]
			entry.narinfoTask = &uploadTask{key: key, obj: obj, hash: hash}
			pendingByHash[hash] = entry

		case "nar":
			storePathHash, ok := uploadCtx.NARKeyToHash[key]
			if !ok {
				return nil, fmt.Errorf("NAR key %s not found in mapping", key)
			}

			entry := pendingByHash[storePathHash]
			entry.narTask = &uploadTask{key: key, obj: obj, hash: storePathHash}
			pendingByHash[storePathHash] = entry

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
			return nil, fmt.Errorf("unknown object type %q for key: %s", obj.Type, key)
		}
	}

	// Upload all objects with unified worker pool and collect narinfo metadata
	return c.uploadAllObjects(ctx, pendingByHash, logTasks, realisationTasks, uploadCtx)
}

// uploadAllObjects uploads all objects using a unified worker pool.
// Logs and realisations upload independently, NARs upload with their listings in the same goroutine,
// then narinfo metadata is collected for server-side signing.
func (c *Client) uploadAllObjects(ctx context.Context, pendingByHash pendingObjectsByHash, logTasks []uploadTask, realisationTasks []uploadTask, uploadCtx *UploadContext) (map[string]NarinfoMetadata, error) {
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
					err = c.uploadNARWithListing(ctx, task, pendingByHash, uploadCtx.PathInfoByHash, compressedInfo, &compressedInfoMu)

				case "metadata":
					err = c.uploadMetadataOnly(ctx, task.hash, pendingByHash, uploadCtx.PathInfoByHash, compressedInfo, &compressedInfoMu)

				case "log":
					err = c.uploadLog(ctx, task.task, uploadCtx.LogPathsByKey)

				case "realisation":
					err = c.uploadRealisation(ctx, task.task, uploadCtx.RealisationsByKey)
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

	// Queue all NAR tasks and metadata-only tasks
	for hash, entry := range pendingByHash {
		if entry.narTask != nil {
			taskChan <- genericUploadTask{taskType: "nar", task: *entry.narTask, hash: hash}
		} else if entry.narinfoTask != nil {
			// Deduplicated NAR - queue metadata-only task
			taskChan <- genericUploadTask{taskType: "metadata", task: *entry.narinfoTask, hash: hash}
		}
	}

	close(taskChan)

	// Wait for phase 1 to complete
	phase1WG.Wait()

	// Check for errors from phase 1
	select {
	case err := <-errChan:
		if err != nil {
			return nil, err
		}
	default:
	}

	// Phase 2: Collect narinfo metadata for successfully uploaded NARs
	narinfoMetadata := make(map[string]NarinfoMetadata)

	for hash, entry := range pendingByHash {
		// Only collect metadata if we have compressedInfo (NAR uploaded or metadata-only)
		if _, ok := compressedInfo[hash]; !ok || entry.narinfoTask == nil {
			continue
		}

		pathInfo := uploadCtx.PathInfoByHash[hash]
		if pathInfo == nil {
			continue
		}

		// Convert NarHash to Nix32 format for the narinfo
		narHash := pathInfo.NarHash
		if convertedHash, err := ConvertHashToNix32(pathInfo.NarHash); err == nil {
			narHash = convertedHash
		}

		// Use NarHash-based key for URL (content-based deduplication)
		narURL, err := getNARKey(pathInfo.NarHash)
		if err != nil {
			return nil, fmt.Errorf("getting NAR key for %s: %w", pathInfo.Path, err)
		}

		// Create narinfo metadata
		metadata := NarinfoMetadata{
			StorePath:   pathInfo.Path,
			URL:         narURL,
			Compression: "zstd",
			NarHash:     narHash,
			NarSize:     pathInfo.NarSize,
			References:  pathInfo.References,
			Deriver:     pathInfo.Deriver,
			Signatures:  pathInfo.Signatures,
			CA:          pathInfo.CA,
		}

		narinfoMetadata[entry.narinfoTask.key] = metadata
	}

	return narinfoMetadata, nil
}

// uploadMetadataOnly handles metadata-only uploads for deduplicated NARs.
// It generates the listing and uploads .ls file without uploading the NAR.
func (c *Client) uploadMetadataOnly(
	ctx context.Context,
	hash string,
	pendingByHash pendingObjectsByHash,
	pathInfoByHash map[string]*PathInfo,
	compressedInfo map[string]*CompressedFileInfo,
	compressedInfoMu *sync.Mutex,
) error {
	// Get path info
	pathInfo, ok := pathInfoByHash[hash]
	if !ok || pathInfo == nil {
		return fmt.Errorf("missing PathInfo for hash %s", hash)
	}

	// Generate listing from store path (fast directory walk, no NAR serialization)
	listing, err := GenerateListingOnly(pathInfo.Path)
	if err != nil {
		return fmt.Errorf("generating listing for %s: %w", pathInfo.Path, err)
	}

	// Store listing in compressedInfo (protected by mutex)
	compressedInfoMu.Lock()

	compressedInfo[hash] = &CompressedFileInfo{
		Listing: listing,
	}

	compressedInfoMu.Unlock()

	// Upload .ls file if needed
	entry := pendingByHash[hash]
	if entry.lsTask != nil {
		if err := c.uploadListing(ctx, *entry.lsTask, &CompressedFileInfo{Listing: listing}); err != nil {
			return err
		}
	}

	return nil
}
