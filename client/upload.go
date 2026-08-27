package client

import (
	"bytes"
	"context"
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

// narKey is the content-addressed object key for a NAR.
func narKey(narHash Hash) string {
	return "nar/" + narHash.BareNix32() + ".nar.zst"
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

		top, err := toplevelStorePath(currentPath, storeDir)
		if err != nil {
			return nil, err
		}

		resolved = append(resolved, top)
	}

	return resolved, nil
}

// toplevelStorePath reduces a path inside the store to its store object,
// e.g. /nix/store/<hash>-foo/bin/foo -> /nix/store/<hash>-foo.
func toplevelStorePath(path, storeDir string) (string, error) {
	rel, err := filepath.Rel(storeDir, filepath.Clean(path))
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("not a store path: %s", path)
	}

	name, _, _ := strings.Cut(rel, string(filepath.Separator))

	return filepath.Join(storeDir, name), nil
}

// StoreDir returns the Nix store directory, honouring NixEnv.
func (c *Client) StoreDir(ctx context.Context) (string, error) {
	c.storeDirOnce.Do(func() {
		c.storeDir, c.storeDirErr = GetStoreDir(ctx, c.NixEnv)
	})

	if c.storeDirErr != nil {
		return "", fmt.Errorf("getting store directory: %w", c.storeDirErr)
	}

	return c.storeDir, nil
}

// ResolveStorePath resolves symlinks (e.g. a nix-build ./result link) until
// the path points into the Nix store. Needed wherever a raw user-supplied
// path is sent to the server, which only accepts store paths.
func (c *Client) ResolveStorePath(ctx context.Context, path string) (string, error) {
	storeDir, err := c.StoreDir(ctx)
	if err != nil {
		return "", err
	}

	resolved, err := resolveSymlinks([]string{path}, storeDir)
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

	realisations, err := QueryRealisations(ctx, pathInfos, nixEnv)
	if err != nil {
		slog.Warn("Failed to query realisations (CA derivations may not upload correctly)", "error", err)

		realisations = map[string][]RealisationInfo{}
	}

	realisationsByKey := make(map[string]*RealisationInfo)

	// First pass: collect all objects for all paths
	allObjects := make(map[string][]ObjectWithRefs) // storePath -> objects for that path

	for storePath, pathInfo := range pathInfos {
		hash, err := GetStorePathHash(storePath)
		if err != nil {
			return nil, fmt.Errorf("getting store path hash: %w", err)
		}

		pathInfoByHash[hash] = pathInfo

		narKey := narKey(pathInfo.NarHash)
		narKeyToHash[narKey] = hash

		// The narinfo references all siblings so GC keeps them together.
		siblings := []ObjectWithRefs{
			{Key: narKey, Type: ObjectTypeNAR, Refs: []string{}, NarSize: &pathInfo.NarSize},
			{Key: hash + ".ls", Type: ObjectTypeListing, Refs: []string{}},
		}

		if pathInfo.Deriver != nil && *pathInfo.Deriver != "" {
			drvPath := *pathInfo.Deriver

			logPath, err := GetBuildLogPath(drvPath)
			if err != nil {
				slog.Warn("Error checking for build log", "drv_path", drvPath, "store_path", storePath, "error", err)
			} else if logPath != "" {
				logKey := "log/" + filepath.Base(drvPath)
				siblings = append(siblings, ObjectWithRefs{Key: logKey, Type: ObjectTypeBuildLog, Refs: []string{}})
				logPathsByKey[logKey] = logPath
			}
		}

		for i := range realisations[storePath] {
			r := &realisations[storePath][i]
			siblings = append(siblings, ObjectWithRefs{Key: r.Key(), Type: ObjectTypeRealisation, Refs: []string{}})
			realisationsByKey[r.Key()] = r
		}

		narinfoRefs := make([]string, 0, len(pathInfo.References)+len(siblings))

		for _, ref := range pathInfo.References {
			refHash, err := GetStorePathHash(ref)
			if err != nil {
				return nil, fmt.Errorf("getting reference hash: %w", err)
			}

			narinfoRefs = append(narinfoRefs, refHash+".narinfo")
		}

		for _, o := range siblings {
			narinfoRefs = append(narinfoRefs, o.Key)
		}

		allObjects[storePath] = append(
			[]ObjectWithRefs{{Key: hash + ".narinfo", Type: ObjectTypeNarinfo, Refs: narinfoRefs}},
			siblings...)
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
		RealisationsByKey: realisationsByKey,
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

// PendingClosures is the merged result of registering several closures.
type PendingClosures struct {
	Objects map[string]PendingObject
	// OwnerByKey is the closure that offered the key and may sign it.
	OwnerByKey map[string]string
	IDs        []string
}

// CreatePendingClosures registers each closure with the server.
func (c *Client) CreatePendingClosures(ctx context.Context, closures []ClosureInfo) (*PendingClosures, error) {
	pc := &PendingClosures{Objects: map[string]PendingObject{}, OwnerByKey: map[string]string{}}

	for _, closure := range closures {
		resp, err := c.CreatePendingClosure(ctx, closure.NarinfoKey, closure.Objects, c.VerifyS3Integrity)
		if err != nil {
			return nil, fmt.Errorf("creating pending closure: %w", err)
		}

		pc.IDs = append(pc.IDs, resp.ID)

		for key, obj := range resp.PendingObjects {
			if _, seen := pc.Objects[key]; !seen {
				pc.Objects[key] = obj
				pc.OwnerByKey[key] = resp.ID
			}
		}
	}

	return pc, nil
}

type narinfoTask struct {
	closureID string
	key       string
	meta      NarinfoMetadata
}

// SignAndUploadNarinfos signs narinfos on the server and uploads them to S3 in parallel.
func (c *Client) SignAndUploadNarinfos(ctx context.Context, narinfosByClosureID map[string]map[string]NarinfoMetadata, pendingObjects map[string]PendingObject) error {
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
		signatures, err := c.SignPendingClosure(ctx, closureID, narinfos)
		if err != nil {
			return fmt.Errorf("signing narinfos for closure %s: %w", closureID, err)
		}

		maps.Copy(signaturesByKey, signatures)
	}

	// Generate, compress, and upload narinfos in parallel
	return c.uploadNarinfosInParallel(ctx, narinfosToSign, signaturesByKey, pendingObjects)
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

			if err := resp.Body.Close(); err != nil {
				slog.Warn("Failed to close response body", "error", err)
			}

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
	storeDir, err := c.StoreDir(ctx)
	if err != nil {
		return nil, err
	}

	resolvedPaths, err := resolveSymlinks(paths, storeDir)
	if err != nil {
		return nil, fmt.Errorf("resolving symlinks: %w", err)
	}

	slog.Debug("Resolved paths", "original", paths, "resolved", resolvedPaths)

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
	// Fetched per push so the long-running hook picks up config changes.
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

	pending, err := c.CreatePendingClosures(ctx, result.Closures)
	if err != nil {
		return nil, fmt.Errorf("creating pending closures: %w", err)
	}

	pendingObjects := pending.Objects

	newPaths := 0

	for key := range pendingObjects {
		if strings.HasPrefix(key, "nar/") {
			newPaths++
		}
	}

	cachedPaths := len(pathInfos) - newPaths

	slog.Info(fmt.Sprintf("Uploading %d paths to %s (%d already cached)", newPaths, c.baseURL.Hostname(), cachedPaths))
	slog.Debug("Need to upload objects", "pending", len(pendingObjects), "closures", len(pending.IDs))

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

	for key, meta := range narinfoMetadata {
		owner := pending.OwnerByKey[key]
		if narinfosByClosureID[owner] == nil {
			narinfosByClosureID[owner] = map[string]NarinfoMetadata{}
		}

		narinfosByClosureID[owner][key] = meta
	}

	if err := c.SignAndUploadNarinfos(ctx, narinfosByClosureID, pendingObjects); err != nil {
		return nil, fmt.Errorf("signing and uploading narinfos: %w", err)
	}

	for _, id := range pending.IDs {
		if err := c.CompletePendingClosure(ctx, id); err != nil {
			return nil, fmt.Errorf("completing pending closure %s: %w", id, err)
		}
	}

	duration := time.Since(startTime)
	slog.Info(fmt.Sprintf("Upload complete. (%s)", duration.Round(time.Millisecond)))

	return closurePaths, nil
}
