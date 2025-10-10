package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Mic92/niks3/client"
)

func main() {
	if err := run(); err != nil {
		slog.Error("Fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	// Define flags
	pushCmd := flag.NewFlagSet("push", flag.ExitOnError)
	serverURL := pushCmd.String("server-url", os.Getenv("NIKS3_SERVER_URL"), "Server URL (can also use NIKS3_SERVER_URL env var)")
	authToken := pushCmd.String("auth-token", os.Getenv("NIKS3_AUTH_TOKEN"), "Auth token (can also use NIKS3_AUTH_TOKEN env var)")
	maxConcurrent := pushCmd.Int("max-concurrent-uploads", 30, "Maximum concurrent uploads")

	// Parse command
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: niks3 push [flags] <store-paths...>")
		fmt.Fprintln(os.Stderr, "\nCommands:")
		fmt.Fprintln(os.Stderr, "  push    Upload paths to S3-compatible binary cache")

		return errors.New("no command provided")
	}

	switch os.Args[1] {
	case "push":
		if err := pushCmd.Parse(os.Args[2:]); err != nil {
			return fmt.Errorf("parsing flags: %w", err)
		}

		if *serverURL == "" {
			return errors.New("server URL is required (use --server-url or NIKS3_SERVER_URL env var)")
		}

		if *authToken == "" {
			return errors.New("auth token is required (use --auth-token or NIKS3_AUTH_TOKEN env var)")
		}

		paths := pushCmd.Args()
		if len(paths) == 0 {
			return errors.New("at least one store path is required")
		}

		return pushCommand(*serverURL, *authToken, paths, *maxConcurrent)

	default:
		return fmt.Errorf("unknown command: %s", os.Args[1])
	}
}

func pushCommand(serverURL, authToken string, paths []string, maxConcurrent int) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if maxConcurrent < 1 {
		maxConcurrent = 1
	}

	// Create client
	c, err := client.NewClient(serverURL, authToken)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	// Get path info for all paths and their closures
	slog.Info("Getting path info", "count", len(paths))

	pathInfos, err := client.GetPathInfoRecursive(ctx, paths)
	if err != nil {
		return fmt.Errorf("getting path info: %w", err)
	}

	slog.Info("Found paths in closure", "count", len(pathInfos))

	// Prepare closures
	closures, pathInfoByHash, err := prepareClosures(pathInfos)
	if err != nil {
		return fmt.Errorf("preparing closures: %w", err)
	}

	// Create pending closures and collect what needs uploading
	pendingObjects, pendingIDs, err := createPendingClosures(ctx, c, closures)
	if err != nil {
		return fmt.Errorf("creating pending closures: %w", err)
	}

	slog.Info("Need to upload objects", "pending", len(pendingObjects), "total", len(pathInfos)*2)

	// Upload all pending objects with maximum parallelism
	if err := uploadPendingObjects(ctx, c, pendingObjects, pathInfoByHash, maxConcurrent); err != nil {
		return fmt.Errorf("uploading objects: %w", err)
	}

	// Complete all pending closures
	if err := completeClosures(ctx, c, pendingIDs); err != nil {
		return fmt.Errorf("completing closures: %w", err)
	}

	slog.Info("Upload completed successfully")

	return nil
}

type closureInfo struct {
	narinfoKey string
	objects    []client.ObjectWithRefs
}

func prepareClosures(pathInfos map[string]*client.PathInfo) ([]closureInfo, map[string]*client.PathInfo, error) {
	closures := make([]closureInfo, 0, len(pathInfos))
	pathInfoByHash := make(map[string]*client.PathInfo)

	for storePath, pathInfo := range pathInfos {
		hash, err := client.GetStorePathHash(storePath)
		if err != nil {
			return nil, nil, fmt.Errorf("getting store path hash: %w", err)
		}

		pathInfoByHash[hash] = pathInfo

		// Extract references as object keys (hash.narinfo)
		var references []string

		for _, ref := range pathInfo.References {
			refHash, err := client.GetStorePathHash(ref)
			if err != nil {
				return nil, nil, fmt.Errorf("getting reference hash: %w", err)
			}

			// Store reference as object key (hash.narinfo) so GC can follow it
			references = append(references, refHash+".narinfo")
		}

		// NAR file object
		narFilename := hash + ".nar.zst"
		narKey := "nar/" + narFilename

		// Narinfo references both dependencies and its own NAR file
		narinfoRefs := make([]string, len(references), len(references)+1)
		copy(narinfoRefs, references)
		narinfoRefs = append(narinfoRefs, narKey)
		narinfoKey := hash + ".narinfo"

		// Create objects for this closure
		objects := []client.ObjectWithRefs{
			{
				Key:  narinfoKey,
				Refs: narinfoRefs,
			},
			{
				Key:     narKey,
				Refs:    []string{},
				NarSize: &pathInfo.NarSize, // Include NarSize for multipart estimation
			},
		}

		closures = append(closures, closureInfo{
			narinfoKey: narinfoKey,
			objects:    objects,
		})
	}

	return closures, pathInfoByHash, nil
}

func createPendingClosures(ctx context.Context, c *client.Client, closures []closureInfo) (map[string]client.PendingObject, []string, error) {
	slog.Info("Checking which objects need uploading...")

	pendingObjects := make(map[string]client.PendingObject)
	pendingIDs := make([]string, 0, len(closures))

	for _, closure := range closures {
		resp, err := c.CreatePendingClosure(ctx, closure.narinfoKey, closure.objects)
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
	obj   client.PendingObject
	isNar bool
	hash  string
}

func uploadPendingObjects(ctx context.Context, c *client.Client, pendingObjects map[string]client.PendingObject, pathInfoByHash map[string]*client.PathInfo, maxConcurrent int) error {
	// Separate NAR and narinfo uploads
	var narTasks []uploadTask

	var narinfoTasks []uploadTask

	for key, obj := range pendingObjects {
		if strings.HasSuffix(key, ".narinfo") {
			hash := key[:len(key)-8]
			narinfoTasks = append(narinfoTasks, uploadTask{
				key:   key,
				obj:   obj,
				isNar: false,
				hash:  hash,
			})
		} else if strings.HasPrefix(key, "nar/") {
			// Extract hash from "nar/HASH.nar.zst"
			filename := key[4:]
			if strings.HasSuffix(filename, ".nar.zst") {
				hash := filename[:len(filename)-8]
				narTasks = append(narTasks, uploadTask{
					key:   key,
					obj:   obj,
					isNar: true,
					hash:  hash,
				})
			}
		}
	}

	// Upload all NAR files in parallel and collect results
	slog.Info("Uploading NAR files", "count", len(narTasks), "max_concurrent", maxConcurrent)

	compressedInfo, err := uploadNARs(ctx, c, narTasks, pathInfoByHash, maxConcurrent)
	if err != nil {
		return err
	}

	// Upload narinfo files in parallel
	slog.Info("Uploading narinfo files", "count", len(narinfoTasks), "max_concurrent", maxConcurrent)

	return uploadNarinfos(ctx, c, narinfoTasks, pathInfoByHash, compressedInfo, maxConcurrent)
}

func uploadNARs(ctx context.Context, c *client.Client, tasks []uploadTask, pathInfoByHash map[string]*client.PathInfo, maxConcurrent int) (map[string]*client.CompressedFileInfo, error) {
	results := make(map[string]*client.CompressedFileInfo)

	var resultsMu sync.Mutex

	// Create semaphore for concurrency control
	sem := make(chan struct{}, maxConcurrent)

	var wg sync.WaitGroup

	errChan := make(chan error, len(tasks))

	startTime := time.Now()

	for _, task := range tasks {
		wg.Add(1)

		go func(t uploadTask) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}

			defer func() { <-sem }()

			pathInfo, ok := pathInfoByHash[t.hash]
			if !ok {
				errChan <- fmt.Errorf("path info not found for hash %s", t.hash)

				return
			}

			slog.Info("Uploading NAR", "path", pathInfo.Path)

			// Pass the PendingObject (contains multipart info)
			info, err := c.CompressAndUploadNAR(ctx, pathInfo.Path, t.obj, t.key)
			if err != nil {
				errChan <- fmt.Errorf("uploading NAR %s: %w", t.key, err)

				return
			}

			resultsMu.Lock()

			results[t.hash] = info

			resultsMu.Unlock()

			slog.Info("Successfully uploaded NAR", "path", pathInfo.Path, "bytes", info.Size)
		}(task)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	if len(errChan) > 0 {
		return nil, <-errChan
	}

	duration := time.Since(startTime)
	slog.Info("Uploaded NARs", "count", len(tasks), "duration", duration)

	return results, nil
}

func uploadNarinfos(ctx context.Context, c *client.Client, tasks []uploadTask, pathInfoByHash map[string]*client.PathInfo, compressedInfo map[string]*client.CompressedFileInfo, maxConcurrent int) error {
	// Create semaphore for concurrency control
	sem := make(chan struct{}, maxConcurrent)

	var wg sync.WaitGroup

	errChan := make(chan error, len(tasks))

	startTime := time.Now()

	for _, task := range tasks {
		wg.Add(1)

		go func(t uploadTask) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}

			defer func() { <-sem }()

			pathInfo, ok := pathInfoByHash[t.hash]
			if !ok {
				errChan <- fmt.Errorf("path info not found for hash %s", t.hash)

				return
			}

			slog.Info("Uploading narinfo", "path", pathInfo.Path)

			// Get compressed info for this NAR
			info := compressedInfo[t.hash]
			if info == nil {
				// This is a server bug: server asked us to upload narinfo without uploading the NAR.
				// NAR and narinfo must always be uploaded together as a closure.
				errChan <- fmt.Errorf("server inconsistency: asked to upload narinfo %s without uploading corresponding NAR - this is a server bug", t.key)

				return
			}

			// Generate narinfo content
			narinfoContent := client.CreateNarinfo(
				pathInfo,
				t.hash+".nar.zst",
				info.Size,
				info.Hash,
			)

			// Upload narinfo (uses simple presigned URL)
			if err := c.UploadBytesToPresignedURL(ctx, t.obj.PresignedURL, []byte(narinfoContent)); err != nil {
				errChan <- fmt.Errorf("uploading narinfo %s: %w", t.key, err)

				return
			}

			slog.Info("Successfully uploaded narinfo", "path", pathInfo.Path)
		}(task)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	if len(errChan) > 0 {
		return <-errChan
	}

	duration := time.Since(startTime)
	slog.Info("Uploaded narinfos", "count", len(tasks), "duration", duration)

	return nil
}

func completeClosures(ctx context.Context, c *client.Client, pendingIDs []string) error {
	slog.Info("Completing pending closures", "count", len(pendingIDs))

	for _, id := range pendingIDs {
		if err := c.CompletePendingClosure(ctx, id); err != nil {
			return fmt.Errorf("completing pending closure %s: %w", id, err)
		}
	}

	return nil
}
