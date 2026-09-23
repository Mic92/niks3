package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

// getNARKey generates the NAR object key based on content hash (NarHash) for deduplication.
func getNARKey(narHash string) (string, error) {
	// Convert NarHash to Nix32 format and strip "sha256:" prefix for filename
	narHashNix32, err := ConvertHashToNix32(narHash)
	if err != nil {
		return "", fmt.Errorf("converting nar hash: %w", err)
	}

	narHashPart := strings.TrimPrefix(narHashNix32, "sha256:")
	narFilename := narHashPart + ".nar.zst"

	return "nar/" + narFilename, nil
}

// resolveSymlinks resolves any symlinks in the given paths to their actual store paths.
// Resolves symlinks iteratively until reaching a path in the Nix store, then stops.
// This prevents resolving symlinks within the store to subdirectory paths which would break hash extraction.
func resolveSymlinks(paths []string, storeDir string) ([]string, error) {
	resolved := make([]string, 0, len(paths))
	storeDirPrefix := storeDir + "/"

	const maxSymlinkDepth = 255 // Same limit as Go's filepath.EvalSymlinks (allows 255 resolutions, errors on 256th)

	for _, path := range paths {
		currentPath := path

		// Resolve symlinks iteratively until we reach a path in the store
		for i := range maxSymlinkDepth {
			// If we've reached a path in the store, stop resolving
			if strings.HasPrefix(currentPath, storeDirPrefix) {
				break
			}

			// Check if the current path is a symlink
			linkTarget, err := os.Readlink(currentPath)
			if err != nil {
				// Not a symlink or doesn't exist
				if os.IsNotExist(err) {
					return nil, fmt.Errorf("path does not exist: %s: %w", currentPath, err)
				}
				// Not a symlink, use as-is
				break
			}

			// Make the link target absolute if it's relative
			if !filepath.IsAbs(linkTarget) {
				linkTarget = filepath.Join(filepath.Dir(currentPath), linkTarget)
			}

			if i+1 > maxSymlinkDepth {
				return nil, fmt.Errorf("too many symlinks resolving path: %s", path)
			}

			currentPath = linkTarget
		}

		resolved = append(resolved, currentPath)
	}

	return resolved, nil
}

// ResolveStorePath resolves symlinks (e.g. a nix-build ./result link) until
// the path points into the Nix store. Needed wherever a raw user-supplied
// path is sent to the server, which only accepts store paths.
func (c *Client) ResolveStorePath(path string) (string, error) {
	resolved, err := resolveSymlinks([]string{path}, c.storeDir)
	if err != nil {
		return "", err
	}

	return resolved[0], nil
}

// ClosureInfo represents a closure with its associated objects.
type ClosureInfo struct {
	NarinfoKey string
	Objects    []ObjectWithRefs
}

// PrepareClosuresResult contains the result of preparing closures.
type PrepareClosuresResult struct {
	Closures          []ClosureInfo
	PathInfoByHash    map[string]*PathInfo
	NARKeyToHash      map[string]string           // Maps NAR object key -> store path hash
	LogPathsByKey     map[string]string           // Maps log object key -> local log file path
	RealisationsByKey map[string]*RealisationInfo // Maps realisation key -> realisation info
}

// PrepareClosures prepares closures from path info, including NAR, .ls, narinfo, build log, and realisation objects.
// Build logs are automatically discovered for output paths and included by default.
// Realisations are queried for CA derivations and included automatically.
// topLevelPaths specifies which paths are closure roots - one ClosureInfo is created per top-level path.
func PrepareClosures(ctx context.Context, topLevelPaths []string, pathInfos map[string]*PathInfo, nixEnv []string) (*PrepareClosuresResult, error) {
	pathInfoByHash := make(map[string]*PathInfo)
	narKeyToHash := make(map[string]string)
	logPathsByKey := make(map[string]string)

	// Query realisations for CA paths
	realisations, err := QueryRealisations(ctx, pathInfos, nixEnv)
	if err != nil {
		// Log warning but don't fail - realisations are optional
		slog.Warn("Failed to query realisations (CA derivations may not upload correctly)", "error", err)

		realisations = make(map[string]*RealisationInfo)
	}

	// First pass: collect all objects for all paths
	allObjects := make(map[string][]ObjectWithRefs) // storePath -> objects for that path

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

		// NAR file object - use NarHash for content-based deduplication
		narKey, err := getNARKey(pathInfo.NarHash.String())
		if err != nil {
			return nil, fmt.Errorf("getting NAR key: %w", err)
		}

		// Map NAR key back to store path hash for later lookup
		narKeyToHash[narKey] = hash

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
				// Use filepath.Base to get just the derivation filename (works with any store directory)
				drvName := filepath.Base(drvPath)
				logKey := "log/" + drvName

				objects = append(objects, ObjectWithRefs{
					Key:  logKey,
					Type: ObjectTypeBuildLog,
					Refs: []string{}, // Logs don't reference anything
				})

				// Track the log path for later upload
				logPathsByKey[logKey] = logPath

				slog.Debug("Found build log for path", "store_path", storePath, "drv_path", drvPath, "log_key", logKey)
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

		allObjects[storePath] = objects
	}

	// Second pass: compute closure membership for each top-level path
	closureMembership := computeClosureMembership(topLevelPaths, pathInfos)

	// Third pass: create one ClosureInfo per top-level path with only its reachable objects
	closures := make([]ClosureInfo, 0, len(topLevelPaths))

	for _, topLevelPath := range topLevelPaths {
		// Get the narinfo key for this top-level path
		topLevelHash, err := GetStorePathHash(topLevelPath)
		if err != nil {
			return nil, fmt.Errorf("getting top-level path hash: %w", err)
		}

		narinfoKey := topLevelHash + ".narinfo"

		// Collect objects only for paths reachable from this top-level path
		var closureObjects []ObjectWithRefs

		reachable := closureMembership[topLevelPath]

		for storePath, objects := range allObjects {
			if reachable[storePath] {
				closureObjects = append(closureObjects, objects...)
			}
		}

		closures = append(closures, ClosureInfo{
			NarinfoKey: narinfoKey,
			Objects:    closureObjects,
		})
	}

	return &PrepareClosuresResult{
		Closures:          closures,
		PathInfoByHash:    pathInfoByHash,
		NARKeyToHash:      narKeyToHash,
		LogPathsByKey:     logPathsByKey,
		RealisationsByKey: realisations,
	}, nil
}

// computeClosureMembership returns, for each top-level path, the set of store
// paths reachable from it via references.
func computeClosureMembership(topLevelPaths []string, pathInfos map[string]*PathInfo) map[string]map[string]bool {
	closureMembership := make(map[string]map[string]bool) // topLevelPath -> set of reachable paths

	for _, topLevelPath := range topLevelPaths {
		reachable := make(map[string]bool)

		var visit func(string)

		visit = func(path string) {
			if reachable[path] {
				return
			}

			reachable[path] = true

			pathInfo, ok := pathInfos[path]
			if !ok {
				return
			}

			for _, ref := range pathInfo.References {
				visit(ref)
			}
		}
		visit(topLevelPath)
		closureMembership[topLevelPath] = reachable
	}

	return closureMembership
}

// skippedUploads counts store paths (and their uncompressed NAR bytes)
// dropped by filterOversizedClosures.
type skippedUploads struct {
	Paths    uint64
	NarBytes uint64
}

// filterOversizedClosures drops top-level paths whose closure contains a path
// with NarSize larger than maxNarSize. A limit of 0 disables filtering. It returns the kept
// top-level paths, pathInfos pruned to paths still reachable from them, and
// counts of what was skipped. Skipping only warns, never errors, so pushes
// and the build hook keep succeeding under a server-side size policy.
func filterOversizedClosures(topLevelPaths []string, pathInfos map[string]*PathInfo, maxNarSize uint64) ([]string, map[string]*PathInfo, skippedUploads) {
	if maxNarSize == 0 {
		return topLevelPaths, pathInfos, skippedUploads{}
	}

	closureMembership := computeClosureMembership(topLevelPaths, pathInfos)
	kept := make([]string, 0, len(topLevelPaths))

nextClosure:
	for _, topLevelPath := range topLevelPaths {
		for path := range closureMembership[topLevelPath] {
			if info, ok := pathInfos[path]; ok && info.NarSize > maxNarSize {
				slog.Warn("Skipping closure: path exceeds server max NAR size",
					"top_level_path", topLevelPath,
					"oversized_path", path,
					"nar_size", info.NarSize,
					"max_nar_size", maxNarSize)

				continue nextClosure
			}
		}

		kept = append(kept, topLevelPath)
	}

	if len(kept) == len(topLevelPaths) {
		return topLevelPaths, pathInfos, skippedUploads{}
	}

	// Prune pathInfos to paths still reachable from the kept top-level paths.
	prunedInfos := make(map[string]*PathInfo)

	for _, topLevelPath := range kept {
		for path := range closureMembership[topLevelPath] {
			if info, ok := pathInfos[path]; ok {
				prunedInfos[path] = info
			}
		}
	}

	var skipped skippedUploads

	for path, info := range pathInfos {
		if _, ok := prunedInfos[path]; !ok {
			skipped.Paths++
			skipped.NarBytes += info.NarSize
		}
	}

	return kept, prunedInfos, skipped
}

// createPending registers the closures with the server and returns the route
// to sign and complete them on, plus the pending objects keyed by pending ID.
// The server only signs narinfos that are pending for that specific ID, so
// callers must not merge these sets when signing.
//
// A server that announces pushes gets one push for all closures. Older
// servers, and ones that answer 404 or 405, get one pending closure each.
func (c *Client) createPending(ctx context.Context, closures []ClosureInfo) (string, map[string]map[string]PendingObject, error) {
	if cfg, err := c.GetCacheConfig(ctx); err == nil && cfg.Pushes {
		resp, err := c.CreatePush(ctx, closures, c.VerifyS3Integrity)
		if err == nil {
			return "pushes", map[string]map[string]PendingObject{resp.ID: resp.PendingObjects}, nil
		}

		var statusErr *HTTPStatusError
		if !errors.As(err, &statusErr) || (statusErr.StatusCode != http.StatusNotFound && statusErr.StatusCode != http.StatusMethodNotAllowed) {
			return "", nil, fmt.Errorf("creating push: %w", err)
		}

		slog.Warn("Server has no /api/pushes, using pending closures", "error", err)
	}

	pendingByClosureID := make(map[string]map[string]PendingObject, len(closures))

	for _, closure := range closures {
		resp, err := c.CreatePendingClosure(ctx, closure.NarinfoKey, closure.Objects, c.VerifyS3Integrity)
		if err != nil {
			return "", nil, fmt.Errorf("creating pending closure: %w", err)
		}

		pendingByClosureID[resp.ID] = resp.PendingObjects
	}

	return "pending_closures", pendingByClosureID, nil
}

type narinfoTask struct {
	closureID string
	key       string
	meta      NarinfoMetadata
}

// SignAndUploadNarinfos signs narinfos on the server and uploads them to S3 in parallel.
func (c *Client) SignAndUploadNarinfos(ctx context.Context, route string, narinfosByClosureID map[string]map[string]NarinfoMetadata, pendingObjects map[string]PendingObject) error {
	// Collect all narinfo metadata and closure IDs
	var narinfosToSign []narinfoTask

	for closureID, narinfos := range narinfosByClosureID {
		for key, meta := range narinfos {
			narinfosToSign = append(narinfosToSign, narinfoTask{
				closureID: closureID,
				key:       key,
				meta:      meta,
			})
		}
	}

	if len(narinfosToSign) == 0 {
		return nil
	}

	// Sign narinfos for each closure
	signaturesByKey := make(map[string][]string)

	for closureID, narinfos := range narinfosByClosureID {
		signatures, err := c.SignPendingClosure(ctx, route, closureID, narinfos)
		if err != nil {
			return fmt.Errorf("signing narinfos for closure %s: %w", closureID, err)
		}

		maps.Copy(signaturesByKey, signatures)
		c.recordSignatures(narinfos, signatures)
	}

	// Generate, compress, and upload narinfos in parallel
	return c.uploadNarinfosInParallel(ctx, narinfosToSign, signaturesByKey, pendingObjects)
}

// Signatures returns what the server signed the path with during this process's pushes.
func (c *Client) Signatures(path string) []string {
	c.signedMu.Lock()
	defer c.signedMu.Unlock()

	return c.signed[path]
}

func (c *Client) recordSignatures(narinfos map[string]NarinfoMetadata, signatures map[string][]string) {
	c.signedMu.Lock()
	defer c.signedMu.Unlock()

	if c.signed == nil {
		c.signed = make(map[string][]string, len(signatures))
	}

	for key, sigs := range signatures {
		c.signed[narinfos[key].StorePath] = sigs
	}
}

// uploadNarinfosInParallel generates, compresses, and uploads narinfos in parallel.
func (c *Client) uploadNarinfosInParallel(ctx context.Context, narinfos []narinfoTask, signaturesByKey map[string][]string, pendingObjects map[string]PendingObject) error {
	if len(narinfos) == 0 {
		return nil
	}

	slog.Info(fmt.Sprintf("Uploading %d narinfos", len(narinfos)))

	// Determine concurrency limit
	numWorkers := c.MaxConcurrentNARUploads
	if numWorkers <= 0 || numWorkers > len(narinfos) {
		numWorkers = len(narinfos)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(numWorkers)

	for _, task := range narinfos {
		g.Go(func() error {
			// Get signatures for this narinfo
			signatures := signaturesByKey[task.key]

			// Generate narinfo content with signatures
			content := generateNarinfoContent(&task.meta, signatures)

			// Compress narinfo
			compressed, err := CompressNarinfo(content)
			if err != nil {
				return fmt.Errorf("compressing narinfo %s: %w", task.key, err)
			}

			// Get presigned URL from pending objects
			pendingObj, ok := pendingObjects[task.key]
			if !ok || pendingObj.PresignedURL == "" {
				return fmt.Errorf("no presigned URL for narinfo %s", task.key)
			}

			// Upload to S3
			req, err := http.NewRequestWithContext(ctx, http.MethodPut, pendingObj.PresignedURL, bytes.NewReader(compressed))
			if err != nil {
				return fmt.Errorf("creating upload request for %s: %w", task.key, err)
			}

			req.Header.Set("Content-Type", "text/x-nix-narinfo")
			req.Header.Set("Content-Encoding", "zstd")

			resp, err := c.DoS3Request(ctx, req)
			if err != nil {
				return fmt.Errorf("uploading narinfo %s: %w", task.key, err)
			}

			deferCloseBody(resp)

			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return fmt.Errorf("uploading narinfo %s: unexpected status %d", task.key, resp.StatusCode)
			}

			c.RegisterUploadedObject(ctx, task.key)
			slog.Debug("Uploaded narinfo", "key", task.key, "size", len(compressed))

			return nil
		})
	}

	return g.Wait() //nolint:wrapcheck // errgroup returns the first task's already-wrapped error
}

// PushPaths uploads store paths and their closures to the server.
// It returns the full list of store paths that were part of the uploaded
// closures (including transitive dependencies), which callers can use to
// prune queues of dependency paths that no longer need separate uploads.
func (c *Client) PushPaths(ctx context.Context, paths []string) ([]string, error) {
	startTime := time.Now()

	// Resolve symlinks to actual store paths
	resolvedPaths, err := resolveSymlinks(paths, c.storeDir)
	if err != nil {
		return nil, fmt.Errorf("resolving symlinks: %w", err)
	}

	slog.Debug("Resolved paths", "original", paths, "resolved", resolvedPaths)

	// Skip cached closures before the local closure walk.
	if present, err := c.Present(ctx, resolvedPaths); err != nil {
		slog.Debug("Present check unavailable, pushing everything", "error", err)
	} else if len(present) > 0 {
		missing := resolvedPaths[:0:0]

		for _, p := range resolvedPaths {
			if !present[p] {
				missing = append(missing, p)
			}
		}

		if len(missing) == 0 {
			slog.Info(fmt.Sprintf("All %d paths already cached", len(resolvedPaths)))

			return resolvedPaths, nil
		}

		resolvedPaths = missing
	}

	// Get path info for all paths and their closures
	slog.Debug("Getting path info", "count", len(resolvedPaths))

	pathInfos, err := GetPathInfoRecursive(ctx, resolvedPaths, c.NixEnv)
	if err != nil {
		return nil, fmt.Errorf("getting path info: %w", err)
	}

	slog.Debug("Found paths in closure", "count", len(pathInfos))

	// Collect all closure paths to return to the caller. This includes paths
	// from closures skipped by the size filter below, so callers (e.g. the
	// hook queue) treat them as handled instead of retrying forever.
	closurePaths := make([]string, 0, len(pathInfos))
	for storePath := range pathInfos {
		closurePaths = append(closurePaths, storePath)
	}

	// Skip closures containing paths larger than the server's max NAR size.
	var maxNarSize uint64

	if cfg, err := c.GetCacheConfig(ctx); err != nil {
		slog.Warn("Failed to fetch cache config, uploading without size limit", "error", err)
	} else {
		maxNarSize = cfg.MaxNarSize
	}

	var skipped skippedUploads

	resolvedPaths, pathInfos, skipped = filterOversizedClosures(resolvedPaths, pathInfos, maxNarSize)
	c.ReportSkippedUploads(ctx, skipped.Paths, skipped.NarBytes)

	if len(resolvedPaths) == 0 {
		slog.Warn("All closures skipped by server max NAR size, nothing to upload")

		return closurePaths, nil
	}

	// Prepare closures - one per top-level path
	result, err := PrepareClosures(ctx, resolvedPaths, pathInfos, c.NixEnv)
	if err != nil {
		return nil, fmt.Errorf("preparing closures: %w", err)
	}

	if len(result.LogPathsByKey) > 0 {
		slog.Debug("Found build logs", "count", len(result.LogPathsByKey))
	}

	if len(result.RealisationsByKey) > 0 {
		slog.Debug("Found realisations for CA derivations", "count", len(result.RealisationsByKey))
	}

	// Create pending closures and collect what needs uploading
	route, pendingByClosureID, err := c.createPending(ctx, result.Closures)
	if err != nil {
		return nil, fmt.Errorf("creating pending closures: %w", err)
	}

	pendingObjects := make(map[string]PendingObject)
	for _, objs := range pendingByClosureID {
		maps.Copy(pendingObjects, objs)
	}

	// Calculate how many paths are already cached vs need uploading
	// Count NAR objects in pendingObjects (each NAR corresponds to one store path)
	newPaths := 0

	for key := range pendingObjects {
		if strings.HasPrefix(key, "nar/") {
			newPaths++
		}
	}

	cachedPaths := len(pathInfos) - newPaths

	slog.Info(fmt.Sprintf("Uploading %d paths to %s (%d already cached)", newPaths, c.baseURL.Hostname(), cachedPaths))
	slog.Debug("Need to upload objects", "pending", len(pendingObjects), "closures", len(pendingByClosureID))

	// Upload all pending objects and collect narinfo metadata
	narinfoMetadata, err := c.UploadPendingObjects(ctx, &UploadContext{
		PendingObjects:    pendingObjects,
		PathInfoByHash:    result.PathInfoByHash,
		NARKeyToHash:      result.NARKeyToHash,
		LogPathsByKey:     result.LogPathsByKey,
		RealisationsByKey: result.RealisationsByKey,
	})
	if err != nil {
		return nil, fmt.Errorf("uploading objects: %w", err)
	}

	slog.Debug("Uploaded all objects", "narinfos", len(narinfoMetadata))

	narinfosByClosureID := make(map[string]map[string]NarinfoMetadata)

	for id, closurePending := range pendingByClosureID {
		closureNarinfos := make(map[string]NarinfoMetadata)

		for key := range closurePending {
			if meta, ok := narinfoMetadata[key]; ok {
				closureNarinfos[key] = meta
			}
		}

		narinfosByClosureID[id] = closureNarinfos
	}

	// Sign narinfos for each closure and upload them
	if err := c.SignAndUploadNarinfos(ctx, route, narinfosByClosureID, pendingObjects); err != nil {
		return nil, fmt.Errorf("signing and uploading narinfos: %w", err)
	}

	// Complete all pending closures (all objects including narinfos are now uploaded)
	for id := range pendingByClosureID {
		if err := c.CompletePendingClosure(ctx, route, id); err != nil {
			return nil, fmt.Errorf("completing pending closure %s: %w", id, err)
		}
	}

	duration := time.Since(startTime)
	slog.Info(fmt.Sprintf("Upload complete. (%s)", duration.Round(time.Millisecond)))

	return closurePaths, nil
}
