package client

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
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

	// Determine number of workers
	numWorkers := c.MaxConcurrentNARUploads
	if numWorkers <= 0 {
		numWorkers = len(pendingByHash) + len(logTasks) + len(realisationTasks)
	}

	// Phase 1: Upload NARs (with listings), logs, and realisations in parallel
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(numWorkers)

	// Queue all log tasks
	for _, task := range logTasks {
		g.Go(func() error {
			return c.uploadLog(ctx, task, uploadCtx.LogPathsByKey)
		})
	}

	// Queue all realisation tasks
	for _, task := range realisationTasks {
		g.Go(func() error {
			return c.uploadRealisation(ctx, task, uploadCtx.RealisationsByKey)
		})
	}

	// Queue all NAR tasks and metadata-only tasks
	for hash, entry := range pendingByHash {
		if entry.narTask != nil {
			g.Go(func() error {
				return c.uploadNARWithListing(ctx, genericUploadTask{taskType: "nar", task: *entry.narTask, hash: hash}, pendingByHash, uploadCtx.PathInfoByHash, compressedInfo, &compressedInfoMu)
			})
		} else if entry.narinfoTask != nil {
			// Deduplicated NAR - queue metadata-only task
			g.Go(func() error {
				return c.uploadMetadataOnly(ctx, hash, pendingByHash, uploadCtx.PathInfoByHash, compressedInfo, &compressedInfoMu)
			})
		}
	}

	// Wait for phase 1 to complete
	if err := g.Wait(); err != nil {
		return nil, err
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
		narHash := pathInfo.NarHash.String()
		if convertedHash, err := ConvertHashToNix32(pathInfo.NarHash.String()); err == nil {
			narHash = convertedHash
		}

		// Use NarHash-based key for URL (content-based deduplication)
		narURL, err := getNARKey(pathInfo.NarHash.String())
		if err != nil {
			return nil, fmt.Errorf("getting NAR key for %s: %w", pathInfo.Path, err)
		}

		// Convert CA to string if present
		var caStr *string

		if pathInfo.CA != nil {
			s := pathInfo.CA.String()
			caStr = &s
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
			CA:          caStr,
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
