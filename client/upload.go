package client

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// resolveSymlinks resolves any symlinks in the given paths to their actual store paths.
func resolveSymlinks(paths []string) ([]string, error) {
	resolved := make([]string, 0, len(paths))
	for _, path := range paths {
		// Evaluate symlinks to get the actual target
		realPath, err := filepath.EvalSymlinks(path)
		if err != nil {
			// If it's not a symlink or doesn't exist, try to use it as-is
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("path does not exist: %s: %w", path, err)
			}
			// For other errors, still try the original path
			resolved = append(resolved, path)
		} else {
			resolved = append(resolved, realPath)
		}
	}

	return resolved, nil
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
	LogPathsByKey     map[string]string           // Maps log object key -> local log file path
	RealisationsByKey map[string]*RealisationInfo // Maps realisation key -> realisation info
}

// PrepareClosures prepares closures from path info, including NAR, .ls, narinfo, build log, and realisation objects.
// Build logs are automatically discovered for output paths and included by default.
// Realisations are queried for CA derivations and included automatically.
// topLevelPaths specifies which paths are closure roots - one ClosureInfo is created per top-level path.
func PrepareClosures(ctx context.Context, topLevelPaths []string, pathInfos map[string]*PathInfo) (*PrepareClosuresResult, error) {
	pathInfoByHash := make(map[string]*PathInfo)
	logPathsByKey := make(map[string]string)

	// Query realisations for CA paths
	realisations, err := QueryRealisations(ctx, pathInfos)
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
	// Build a map of which paths are reachable from each top-level path
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
		LogPathsByKey:     logPathsByKey,
		RealisationsByKey: realisations,
	}, nil
}

// CreatePendingClosures creates pending closures and returns all pending objects and closure ID to narinfo key mapping.
func (c *Client) CreatePendingClosures(ctx context.Context, closures []ClosureInfo) (map[string]PendingObject, map[string]string, error) {
	pendingObjects := make(map[string]PendingObject)
	closureIDToNarinfoKey := make(map[string]string) // Maps closure ID -> narinfo key

	for _, closure := range closures {
		resp, err := c.CreatePendingClosure(ctx, closure.NarinfoKey, closure.Objects)
		if err != nil {
			return nil, nil, fmt.Errorf("creating pending closure: %w", err)
		}

		closureIDToNarinfoKey[resp.ID] = closure.NarinfoKey

		// Collect pending objects
		for key, obj := range resp.PendingObjects {
			pendingObjects[key] = obj
		}
	}

	return pendingObjects, closureIDToNarinfoKey, nil
}

// CompletePendingClosures completes all pending closures with their respective narinfo metadata.
// The narinfosByClosureID map should contain per-closure narinfo metadata (closure ID -> narinfo map).
func (c *Client) CompletePendingClosures(ctx context.Context, narinfosByClosureID map[string]map[string]NarinfoMetadata) error {
	for id, narinfos := range narinfosByClosureID {
		if err := c.CompletePendingClosure(ctx, id, narinfos); err != nil {
			return fmt.Errorf("completing pending closure %s: %w", id, err)
		}
	}

	return nil
}

// PushPaths uploads store paths and their closures to the server.
func (c *Client) PushPaths(ctx context.Context, paths []string) error {
	startTime := time.Now()

	// Resolve symlinks to actual store paths
	resolvedPaths, err := resolveSymlinks(paths)
	if err != nil {
		return fmt.Errorf("resolving symlinks: %w", err)
	}

	slog.Debug("Resolved paths", "original", paths, "resolved", resolvedPaths)

	// Get path info for all paths and their closures
	slog.Debug("Getting path info", "count", len(resolvedPaths))

	pathInfos, err := GetPathInfoRecursive(ctx, resolvedPaths)
	if err != nil {
		return fmt.Errorf("getting path info: %w", err)
	}

	slog.Debug("Found paths in closure", "count", len(pathInfos))

	// Prepare closures - one per top-level path
	result, err := PrepareClosures(ctx, resolvedPaths, pathInfos)
	if err != nil {
		return fmt.Errorf("preparing closures: %w", err)
	}

	if len(result.LogPathsByKey) > 0 {
		slog.Debug("Found build logs", "count", len(result.LogPathsByKey))
	}

	if len(result.RealisationsByKey) > 0 {
		slog.Debug("Found realisations for CA derivations", "count", len(result.RealisationsByKey))
	}

	// Create pending closures and collect what needs uploading
	pendingObjects, closureIDToNarinfoKey, err := c.CreatePendingClosures(ctx, result.Closures)
	if err != nil {
		return fmt.Errorf("creating pending closures: %w", err)
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
	slog.Debug("Need to upload objects", "pending", len(pendingObjects), "closures", len(closureIDToNarinfoKey))

	// Upload all pending objects and collect narinfo metadata
	narinfoMetadata, err := c.UploadPendingObjects(ctx, pendingObjects, result.PathInfoByHash, result.LogPathsByKey, result.RealisationsByKey)
	if err != nil {
		return fmt.Errorf("uploading objects: %w", err)
	}

	slog.Debug("Uploaded all objects", "narinfos", len(narinfoMetadata))

	// Build a quick lookup map: narinfo key -> closure
	closureByNarinfoKey := make(map[string]ClosureInfo)
	for _, closure := range result.Closures {
		closureByNarinfoKey[closure.NarinfoKey] = closure
	}

	// Build per-closure narinfo maps for completion
	// Only include narinfos for objects that belong to each specific closure
	narinfosByClosureID := make(map[string]map[string]NarinfoMetadata)

	for id, topLevelNarinfoKey := range closureIDToNarinfoKey {
		closure := closureByNarinfoKey[topLevelNarinfoKey]
		closureNarinfos := make(map[string]NarinfoMetadata)

		// Add only narinfos for objects in this closure
		for _, obj := range closure.Objects {
			if obj.Type == ObjectTypeNarinfo {
				if meta, ok := narinfoMetadata[obj.Key]; ok {
					closureNarinfos[obj.Key] = meta
				}
			}
		}

		narinfosByClosureID[id] = closureNarinfos
	}

	// Complete all pending closures with their respective narinfo metadata for server-side signing
	if err := c.CompletePendingClosures(ctx, narinfosByClosureID); err != nil {
		return fmt.Errorf("completing closures: %w", err)
	}

	duration := time.Since(startTime)
	slog.Info(fmt.Sprintf("Upload complete. (%s)", duration.Round(time.Millisecond)))

	return nil
}
