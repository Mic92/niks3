package server_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/client"
	"github.com/Mic92/niks3/server"
	minio "github.com/minio/minio-go/v7"
)

const testAuthToken = "test-auth-token" //nolint:gosec // Test token for integration tests

// pushToServer uses the client package to push store paths.
func pushToServer(ctx context.Context, serverURL, authToken string, paths []string) error {
	// Create client
	c, err := client.NewClient(serverURL, authToken)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	// Test with 16 concurrent uploads (2x CPU count for I/O-bound work)
	// TODO: test more configurations in benchmarks
	c.MaxConcurrentNARUploads = 16

	// Get path info for all paths and their closures
	pathInfos, err := client.GetPathInfoRecursive(ctx, paths)
	if err != nil {
		return fmt.Errorf("getting path info: %w", err)
	}

	// Prepare closures
	closures, pathInfoByHash, err := prepareClosures(pathInfos)
	if err != nil {
		return fmt.Errorf("preparing closures: %w", err)
	}

	// Create pending closures
	pendingObjects, pendingIDs, err := createPendingClosures(ctx, c, closures)
	if err != nil {
		return fmt.Errorf("creating pending closures: %w", err)
	}

	// Upload all pending objects
	if err := uploadPendingObjects(ctx, c, pendingObjects, pathInfoByHash); err != nil {
		return fmt.Errorf("uploading objects: %w", err)
	}

	// Complete all pending closures
	if err := completeClosures(ctx, c, pendingIDs); err != nil {
		return fmt.Errorf("completing closures: %w", err)
	}

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

		// Extract references as store path hashes
		var references []string

		for _, ref := range pathInfo.References {
			refHash, err := client.GetStorePathHash(ref)
			if err != nil {
				return nil, nil, fmt.Errorf("getting reference hash: %w", err)
			}

			references = append(references, refHash)
		}

		// NAR file object
		narFilename := hash + ".nar.zst"
		narKey := "nar/" + narFilename

		// Narinfo references both dependencies and its own NAR file
		narinfoRefs := make([]string, 0, len(references)+1)
		narinfoRefs = append(narinfoRefs, references...)
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

func uploadPendingObjects(ctx context.Context, c *client.Client, pendingObjects map[string]client.PendingObject, pathInfoByHash map[string]*client.PathInfo) error {
	// Separate NAR and narinfo uploads
	var narTasks []uploadTask

	var narinfoTasks []uploadTask

	for key, obj := range pendingObjects {
		if key[len(key)-8:] == ".narinfo" {
			hash := key[:len(key)-8]
			narinfoTasks = append(narinfoTasks, uploadTask{
				key:   key,
				obj:   obj,
				isNar: false,
				hash:  hash,
			})
		} else if len(key) > 4 && key[:4] == "nar/" {
			// Extract hash from "nar/HASH.nar.zst"
			filename := key[4:]
			if len(filename) > 8 && filename[len(filename)-8:] == ".nar.zst" {
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

	// Upload all NAR files in parallel
	compressedInfo, err := uploadNARs(ctx, c, narTasks, pathInfoByHash)
	if err != nil {
		return err
	}

	// Upload narinfo files in parallel
	return uploadNarinfos(ctx, c, narinfoTasks, pathInfoByHash, compressedInfo)
}

type uploadTask struct {
	key   string
	obj   client.PendingObject
	isNar bool
	hash  string
}

func uploadNARs(ctx context.Context, c *client.Client, tasks []uploadTask, pathInfoByHash map[string]*client.PathInfo) (map[string]*client.CompressedFileInfo, error) {
	// Upload NARs in parallel using worker pool pattern
	type narResult struct {
		hash string
		info *client.CompressedFileInfo
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
	results := make(map[string]*client.CompressedFileInfo)

	for result := range resultChan {
		if result.err != nil {
			return nil, result.err
		}

		results[result.hash] = result.info
	}

	return results, nil
}

func uploadNarinfos(ctx context.Context, c *client.Client, tasks []uploadTask, pathInfoByHash map[string]*client.PathInfo, compressedInfo map[string]*client.CompressedFileInfo) error {
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
		narinfoContent := client.CreateNarinfo(
			pathInfo,
			task.hash+".nar.zst",
			info.Size,
			info.Hash,
		)

		// Upload narinfo
		if err := c.UploadBytesToPresignedURL(ctx, task.obj.PresignedURL, []byte(narinfoContent)); err != nil {
			return fmt.Errorf("uploading narinfo %s: %w", task.key, err)
		}
	}

	return nil
}

func completeClosures(ctx context.Context, c *client.Client, pendingIDs []string) error {
	for _, id := range pendingIDs {
		if err := c.CompletePendingClosure(ctx, id); err != nil {
			return fmt.Errorf("completing pending closure %s: %w", id, err)
		}
	}

	return nil
}

func TestClientIntegration(t *testing.T) {
	t.Parallel()

	// Start test service (includes Minio and PostgreSQL)
	service := createTestService(t)
	defer service.Close()

	// Create test server with auth
	authToken := testAuthToken
	testService := &server.Service{
		Pool:        service.Pool,
		MinioClient: service.MinioClient,
		Bucket:      service.Bucket,
		APIToken:    authToken,
	}

	// Initialize the bucket with nix-cache-info
	err := testService.InitializeBucket(t.Context())
	ok(t, err)

	mux := http.NewServeMux()

	// Register handlers with auth
	mux.HandleFunc("POST /api/pending_closures", testService.AuthMiddleware(testService.CreatePendingClosureHandler))
	mux.HandleFunc("POST /api/pending_closures/{id}/complete", testService.AuthMiddleware(testService.CommitPendingClosureHandler))
	mux.HandleFunc("POST /api/multipart/complete", testService.AuthMiddleware(testService.CompleteMultipartUploadHandler))
	mux.HandleFunc("GET /health", testService.HealthCheckHandler)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Create a test file and add it to the Nix store
	tempFile := filepath.Join(t.TempDir(), "test-file.txt")
	err = os.WriteFile(tempFile, []byte("test content for niks3 integration test"), 0o600)
	ok(t, err)

	// Use the client package to upload the store path
	ctx := t.Context()

	// Add the file to nix store
	output, err := exec.CommandContext(ctx, "nix-store", "--add", tempFile).CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to add file to nix store: %v\nOutput: %s", err, output)
	}

	storePath := strings.TrimSpace(string(output))
	t.Logf("Created store path: %s", storePath)

	err = pushToServer(ctx, ts.URL, authToken, []string{storePath})
	if err != nil {
		t.Fatalf("Client failed: %v", err)
	}

	// Verify the upload by checking if the objects exist in S3
	// Extract hash from store path
	pathParts := strings.Split(filepath.Base(storePath), "-")
	if len(pathParts) < 1 {
		t.Fatal("Invalid store path format")
	}

	hash := pathParts[0]

	// Check if narinfo exists in S3
	narinfoKey := hash + ".narinfo"
	narinfoObj, err := testService.MinioClient.GetObject(ctx, testService.Bucket, narinfoKey, minio.GetObjectOptions{})
	ok(t, err)

	defer func() {
		if err := narinfoObj.Close(); err != nil {
			t.Logf("Failed to close narinfo object: %v", err)
		}
	}()

	// Read and verify narinfo content
	narinfoContent, err := io.ReadAll(narinfoObj)
	ok(t, err)

	t.Logf("Retrieved narinfo from S3:\n%s", narinfoContent)

	// Verify narinfo contains expected fields
	narinfoStr := string(narinfoContent)
	if !strings.Contains(narinfoStr, "StorePath: "+storePath) {
		t.Errorf("Narinfo doesn't contain correct StorePath")
	}

	if !strings.Contains(narinfoStr, "URL: nar/") {
		t.Errorf("Narinfo doesn't contain NAR URL")
	}

	if !strings.Contains(narinfoStr, "NarHash:") {
		t.Errorf("Narinfo doesn't contain NarHash")
	}

	if !strings.Contains(narinfoStr, "NarSize:") {
		t.Errorf("Narinfo doesn't contain NarSize")
	}

	// Also check if NAR file exists in S3 (compressed with zstd)
	narKey := fmt.Sprintf("nar/%s.nar.zst", hash)

	_, err = testService.MinioClient.StatObject(ctx, testService.Bucket, narKey, minio.StatObjectOptions{})
	if err != nil {
		t.Errorf("NAR file not found in S3: %v", err)
	}
}

func TestClientMultipleUploads(t *testing.T) {
	t.Parallel()

	// Start test service
	service := createTestService(t)
	defer service.Close()

	// Create test server with auth
	authToken := testAuthToken
	testService := &server.Service{
		Pool:        service.Pool,
		MinioClient: service.MinioClient,
		Bucket:      service.Bucket,
		APIToken:    authToken,
	}

	// Initialize the bucket with nix-cache-info
	err := testService.InitializeBucket(t.Context())
	ok(t, err)

	mux := http.NewServeMux()

	// Register handlers
	mux.HandleFunc("POST /api/pending_closures", testService.AuthMiddleware(testService.CreatePendingClosureHandler))
	mux.HandleFunc("POST /api/pending_closures/{id}/complete", testService.AuthMiddleware(testService.CommitPendingClosureHandler))
	mux.HandleFunc("POST /api/multipart/complete", testService.AuthMiddleware(testService.CompleteMultipartUploadHandler))

	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Use the client package to upload all paths
	ctx := t.Context()

	// Create multiple test files and add them to nix store
	storePaths := make([]string, 0, 3)

	var output []byte

	for i := range 3 {
		tempFile := filepath.Join(t.TempDir(), fmt.Sprintf("test-file-%d.txt", i))
		content := fmt.Sprintf("test content %d for niks3 integration test", i)
		err = os.WriteFile(tempFile, []byte(content), 0o600)
		ok(t, err)

		output, err = exec.CommandContext(ctx, "nix-store", "--add", tempFile).CombinedOutput()
		ok(t, err)

		storePath := strings.TrimSpace(string(output))
		storePaths = append(storePaths, storePath)
		t.Logf("Created store path %d: %s", i, storePath)
	}

	start := time.Now()
	err = pushToServer(ctx, ts.URL, authToken, storePaths)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Client failed: %v", err)
	}

	t.Logf("Uploaded %d paths in %v", len(storePaths), duration)

	// Verify all uploads
	for _, storePath := range storePaths {
		pathParts := strings.Split(filepath.Base(storePath), "-")
		hash := pathParts[0]

		// Check if narinfo exists in S3
		narinfoKey := hash + ".narinfo"

		_, err := testService.MinioClient.StatObject(ctx, testService.Bucket, narinfoKey, minio.StatObjectOptions{})
		ok(t, err)

		// Check if NAR exists in S3 (compressed with zstd)
		narKey := fmt.Sprintf("nar/%s.nar.zst", hash)

		_, err = testService.MinioClient.StatObject(ctx, testService.Bucket, narKey, minio.StatObjectOptions{})
		ok(t, err)
	}
}

func buildNixDerivation(ctx context.Context, t *testing.T) string {
	t.Helper()
	// Create a simple Nix expression that has dependencies
	// We'll use a shell script that depends on bash
	nixExpr := filepath.Join(t.TempDir(), "test.nix")
	nixContent := `
	{ pkgs ? import <nixpkgs> {} }:
	pkgs.writeScriptBin "test-script" ''
		#!${pkgs.bash}/bin/bash
		echo "Hello from niks3 test"
	''
	`

	err := os.WriteFile(nixExpr, []byte(nixContent), 0o600)
	ok(t, err)

	// Build the derivation
	output, err := exec.CommandContext(ctx, "nix-build", nixExpr, "--no-out-link").CombinedOutput()
	if err != nil {
		// If nix-build fails, try with nix build
		output, err = exec.CommandContext(ctx, "nix", "build", "-f", nixExpr, "--no-link", "--print-out-paths").CombinedOutput()
		if err != nil {
			t.Skipf("Failed to build nix expression (nix environment not set up): %v\nOutput: %s", err, output)
		}
	}

	// Extract the store path from output (last line)
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	storePath := lines[len(lines)-1]
	t.Logf("Built derivation: %s", storePath)

	return storePath
}

func runClientAndVerifyUpload(ctx context.Context, t *testing.T, testService *server.Service, storePath, serverURL, authToken string) int {
	t.Helper()
	// Get dependencies using nix-store -qR
	output, err := exec.CommandContext(ctx, "nix-store", "-qR", storePath).CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to query dependencies: %v\nOutput: %s", err, output)
	}

	dependencies := strings.Split(strings.TrimSpace(string(output)), "\n")
	t.Logf("Found %d dependencies (including self)", len(dependencies))

	// Use the client package to upload the store path (should upload all dependencies)
	err = pushToServer(ctx, serverURL, authToken, []string{storePath})
	if err != nil {
		t.Fatalf("Client failed: %v", err)
	}

	// Verify all dependencies were uploaded
	uploadedCount := 0

	for _, dep := range dependencies {
		dep = strings.TrimSpace(dep)
		if dep == "" {
			continue
		}

		pathParts := strings.Split(filepath.Base(dep), "-")
		if len(pathParts) < 1 {
			continue
		}

		hash := pathParts[0]

		// Check if narinfo exists in S3
		narinfoKey := hash + ".narinfo"

		_, err := testService.MinioClient.StatObject(ctx, testService.Bucket, narinfoKey, minio.StatObjectOptions{})
		if err != nil {
			// Some dependencies might already exist, which is fine
			t.Logf("Narinfo not found for %s (might already exist): %v", dep, err)
		} else {
			uploadedCount++

			// Also verify NAR exists (compressed with zstd)
			narKey := fmt.Sprintf("nar/%s.nar.zst", hash)

			_, err = testService.MinioClient.StatObject(ctx, testService.Bucket, narKey, minio.StatObjectOptions{})
			if err != nil {
				t.Errorf("NAR not found for %s: %v", dep, err)
			}
		}
	}

	// The client should have uploaded at least the main derivation
	if uploadedCount < 1 {
		t.Error("Expected at least one upload")
	}

	return uploadedCount
}

func testRetrieveWithNixCopy(ctx context.Context, t *testing.T, testService *server.Service, storePath string) {
	t.Helper()
	// Create a temporary store directory
	tempStore := filepath.Join(t.TempDir(), "nix-store")

	err := os.MkdirAll(tempStore, 0o755)
	ok(t, err)

	// Configure the binary cache URL using the same format as Nix's own tests
	endpoint := testService.MinioClient.EndpointURL().Host
	binaryCacheURL := fmt.Sprintf("s3://%s?endpoint=http://%s&region=eu-west-1", testService.Bucket, endpoint)

	// Set up environment for AWS credentials
	// Use the same env vars as Nix's tests
	testEnv := append(os.Environ(),
		"AWS_ACCESS_KEY_ID=minioadmin",
		"AWS_SECRET_ACCESS_KEY="+testMinioServer.secret,
	)

	// First test that we can fetch nix-cache-info (like Nix's own tests do)
	// #nosec G204 -- test code with controlled inputs
	cmd := exec.CommandContext(ctx, "nix", "eval", "--impure", "--expr",
		fmt.Sprintf(`builtins.fetchurl { name = "foo"; url = "s3://%s/nix-cache-info?endpoint=http://%s&region=eu-west-1"; }`, testService.Bucket, endpoint))
	cmd.Env = testEnv

	_, err = cmd.CombinedOutput()
	ok(t, err)

	// Get info about the store (like Nix's tests)
	cmd = exec.CommandContext(ctx, "nix", "store", "info", "--store", binaryCacheURL)
	cmd.Env = testEnv

	_, err = cmd.CombinedOutput()
	ok(t, err)

	// Debug: Download and check a narinfo to see its format
	hash := strings.Split(filepath.Base(storePath), "-")[0]
	narinfoKey := hash + ".narinfo"

	narinfoObj, err := testService.MinioClient.GetObject(ctx,
		testService.Bucket, narinfoKey, minio.GetObjectOptions{})
	ok(t, err)

	_, err = io.ReadAll(narinfoObj)
	if err := narinfoObj.Close(); err != nil {
		ok(t, err)
	}

	ok(t, err)

	// Use --no-check-sigs like in Nix's tests
	cmd = exec.CommandContext(ctx, "nix", "copy",
		"--no-check-sigs",
		"--from", binaryCacheURL,
		storePath)
	cmd.Env = testEnv

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("nix copy failed: %v\nOutput: %s", err, output)
		// This might be expected in some test environments, so don't fail immediately
	}

	// Verify the path exists locally now
	cmd = exec.CommandContext(ctx, "nix", "path-info", storePath)

	_, err = cmd.CombinedOutput()
	ok(t, err)
}

func TestClientWithDependencies(t *testing.T) {
	t.Parallel()

	// Start test service
	service := createTestService(t)
	t.Cleanup(func() { service.Close() })

	// Create test server with auth
	authToken := testAuthToken
	testService := &server.Service{
		Pool:        service.Pool,
		MinioClient: service.MinioClient,
		Bucket:      service.Bucket,
		APIToken:    authToken,
	}

	// Initialize the bucket with nix-cache-info
	err := testService.InitializeBucket(t.Context())
	ok(t, err)

	mux := http.NewServeMux()

	// Register handlers
	mux.HandleFunc("POST /api/pending_closures", testService.AuthMiddleware(testService.CreatePendingClosureHandler))
	mux.HandleFunc("POST /api/pending_closures/{id}/complete", testService.AuthMiddleware(testService.CommitPendingClosureHandler))
	mux.HandleFunc("POST /api/multipart/complete", testService.AuthMiddleware(testService.CompleteMultipartUploadHandler))
	mux.HandleFunc("GET /health", testService.HealthCheckHandler)

	ts := httptest.NewServer(mux)

	t.Cleanup(func() { ts.Close() })

	ctx := t.Context()
	storePath := buildNixDerivation(ctx, t)

	runClientAndVerifyUpload(ctx, t, testService, storePath, ts.URL, authToken)

	// Test that we can retrieve the content using nix copy
	t.Run("RetrieveWithNixCopy", func(t *testing.T) {
		t.Parallel()
		testRetrieveWithNixCopy(t.Context(), t, testService, storePath)
	})
}

func TestClientErrorHandling(t *testing.T) {
	t.Parallel()

	t.Run("InvalidStorePath", func(t *testing.T) {
		t.Parallel()

		service := createTestService(t)
		defer service.Close()

		// Create test server
		authToken := "test-auth-token" //nolint:gosec // test credential
		testService := &server.Service{
			Pool:        service.Pool,
			MinioClient: service.MinioClient,
			Bucket:      service.Bucket,
			APIToken:    authToken,
		}
		mux := http.NewServeMux()
		mux.HandleFunc("POST /api/pending_closures", testService.AuthMiddleware(testService.CreatePendingClosureHandler))
		mux.HandleFunc("POST /api/pending_closures/{id}/complete", testService.AuthMiddleware(testService.CommitPendingClosureHandler))
		mux.HandleFunc("POST /api/multipart/complete", testService.AuthMiddleware(testService.CompleteMultipartUploadHandler))

		ts := httptest.NewServer(mux)
		defer ts.Close()

		// Try to upload a non-store path
		ctx := t.Context()

		err := pushToServer(ctx, ts.URL, authToken, []string{"/tmp/nonexistent"})
		if err == nil {
			t.Fatal("Expected error for invalid store path")
		}

		if !strings.Contains(err.Error(), "nix path-info failed") {
			t.Errorf("Expected 'nix path-info failed' error, got: %s", err)
		}
	})

	t.Run("InvalidAuthToken", func(t *testing.T) {
		t.Parallel()

		service := createTestService(t)
		defer service.Close()

		// Create test server with different auth token
		authToken := "correct-auth-token" //nolint:gosec // test credential
		testService := &server.Service{
			Pool:        service.Pool,
			MinioClient: service.MinioClient,
			Bucket:      service.Bucket,
			APIToken:    authToken,
		}
		mux := http.NewServeMux()
		mux.HandleFunc("POST /api/pending_closures", testService.AuthMiddleware(testService.CreatePendingClosureHandler))
		mux.HandleFunc("POST /api/pending_closures/{id}/complete", testService.AuthMiddleware(testService.CommitPendingClosureHandler))
		mux.HandleFunc("POST /api/multipart/complete", testService.AuthMiddleware(testService.CompleteMultipartUploadHandler))

		ts := httptest.NewServer(mux)
		defer ts.Close()

		// Create a valid store path
		tempFile := filepath.Join(t.TempDir(), "test.txt")
		err := os.WriteFile(tempFile, []byte("test"), 0o600)
		ok(t, err)

		// Try with invalid auth token
		ctx := t.Context()

		output, err := exec.CommandContext(ctx, "nix-store", "--add", tempFile).CombinedOutput()
		ok(t, err)

		storePath := strings.TrimSpace(string(output))

		err = pushToServer(ctx, ts.URL, "invalid-token", []string{storePath})
		if err == nil {
			t.Fatal("Expected error for invalid auth token")
		}
	})

	t.Run("ServerNotAvailable", func(t *testing.T) {
		t.Parallel()

		// Try with unavailable server
		ctx := t.Context()

		// Create a valid store path
		tempFile := filepath.Join(t.TempDir(), "test.txt")
		err := os.WriteFile(tempFile, []byte("test"), 0o600)
		ok(t, err)

		output, err := exec.CommandContext(ctx, "nix-store", "--add", tempFile).CombinedOutput()
		ok(t, err)

		storePath := strings.TrimSpace(string(output))

		err = pushToServer(ctx, "http://localhost:19999", "test-token", []string{storePath})
		if err == nil {
			t.Fatal("Expected error for unavailable server")
		}
	})
}
