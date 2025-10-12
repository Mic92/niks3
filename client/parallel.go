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
