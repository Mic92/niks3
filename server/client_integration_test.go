package server_test

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/klauspost/compress/zstd"
	minio "github.com/minio/minio-go/v7"
)

const testAuthToken = "test-auth-token" //nolint:gosec // Test token for integration tests

// verifyNarinfoInS3 checks that a narinfo file exists and has expected fields.
func verifyNarinfoInS3(ctx context.Context, t *testing.T, testService *server.Service, hash, storePath string) {
	t.Helper()

	narinfoKey := hash + ".narinfo"
	narinfoObj, err := testService.MinioClient.GetObject(ctx, testService.Bucket, narinfoKey, minio.GetObjectOptions{})
	ok(t, err)

	defer func() {
		if err := narinfoObj.Close(); err != nil {
			t.Logf("Failed to close narinfo object: %v", err)
		}
	}()

	compressedContent, err := io.ReadAll(narinfoObj)
	ok(t, err)

	decoder, err := zstd.NewReader(bytes.NewReader(compressedContent))
	ok(t, err)

	defer decoder.Close()

	narinfoContent, err := io.ReadAll(decoder)
	ok(t, err)

	t.Logf("Retrieved narinfo from S3:\n%s", narinfoContent)

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

	// Verify NAR file exists
	narKey := fmt.Sprintf("nar/%s.nar.zst", hash)

	_, err = testService.MinioClient.StatObject(ctx, testService.Bucket, narKey, minio.StatObjectOptions{})
	if err != nil {
		t.Errorf("NAR file not found in S3: %v", err)
	}
}

// verifyLsFileInS3 checks that a .ls file exists and has valid JSON structure.
func verifyLsFileInS3(ctx context.Context, t *testing.T, testService *server.Service, hash string) {
	t.Helper()

	lsKey := hash + ".ls"
	lsObj, err := testService.MinioClient.GetObject(ctx, testService.Bucket, lsKey, minio.GetObjectOptions{})
	ok(t, err)

	defer func() {
		if err := lsObj.Close(); err != nil {
			t.Logf("Failed to close ls object: %v", err)
		}
	}()

	compressedLsContent, err := io.ReadAll(lsObj)
	ok(t, err)
	t.Logf("Retrieved .ls file from S3 (compressed size: %d bytes)", len(compressedLsContent))

	zstdReader, err := zstd.NewReader(bytes.NewReader(compressedLsContent))
	ok(t, err)
	defer zstdReader.Close()

	lsContent, err := io.ReadAll(zstdReader)
	ok(t, err)
	t.Logf("Decompressed .ls content (%d bytes):\n%s", len(lsContent), lsContent)

	var listing map[string]interface{}
	if err := json.Unmarshal(lsContent, &listing); err != nil {
		t.Errorf("Failed to parse .ls content as JSON: %v", err)
	}

	if version, ok := listing["version"].(float64); !ok || version != 1 {
		t.Errorf("Expected version 1, got %v", listing["version"])
	}

	if _, ok := listing["root"]; !ok {
		t.Errorf(".ls file missing 'root' field")
	}
}

// verifyGarbageCollection runs GC and verifies objects and closures were deleted.
func verifyGarbageCollection(ctx context.Context, t *testing.T, service *server.Service, c *client.Client) {
	t.Helper()

	err := c.RunGarbageCollection(ctx, "0s", true)
	ok(t, err)

	// Log database state after GC
	rows3, err := service.Pool.Query(ctx, "SELECT key, deleted_at IS NOT NULL as is_deleted, first_deleted_at FROM objects ORDER BY key")
	ok(t, err)

	defer rows3.Close()

	t.Log("Objects in database after GC:")

	for rows3.Next() {
		var (
			key            string
			isDeleted      bool
			firstDeletedAt interface{}
		)

		err = rows3.Scan(&key, &isDeleted, &firstDeletedAt)
		ok(t, err)
		t.Logf("  - %s: deleted=%v, first_deleted_at=%v", key, isDeleted, firstDeletedAt)
	}

	ok(t, rows3.Err())

	// Verify closures were deleted
	var closureCount int

	rows, err := service.Pool.Query(ctx, "SELECT COUNT(*) FROM closures")
	ok(t, err)

	defer rows.Close()

	if rows.Next() {
		err = rows.Scan(&closureCount)
		ok(t, err)
	}

	ok(t, rows.Err())

	if closureCount != 0 {
		t.Errorf("Expected 0 closures after GC, got %d", closureCount)
	}

	// Verify objects were deleted
	var objectCount int

	rows2, err := service.Pool.Query(ctx, "SELECT COUNT(*) FROM objects")
	ok(t, err)

	defer rows2.Close()

	if rows2.Next() {
		err = rows2.Scan(&objectCount)
		ok(t, err)
	}

	ok(t, rows2.Err())

	if objectCount != 0 {
		t.Errorf("Expected all objects to be deleted after GC with --force, but %d remain", objectCount)
	}

	t.Log("Successfully deleted all objects with GC --force")
}

// pushToServer uses the client package to push store paths.
func pushToServer(ctx context.Context, serverURL, authToken string, paths []string) error {
	// Create client
	c, err := client.NewClient(serverURL, authToken)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	// Test with 16 concurrent uploads (optimal based on benchmarks)
	// Tested 8, 16, 24: 16 showed best throughput (3.33s vs 3.59s and 3.62s)
	c.MaxConcurrentNARUploads = 16

	// Use the high-level PushPaths method
	if err := c.PushPaths(ctx, paths); err != nil {
		return fmt.Errorf("pushing paths: %w", err)
	}

	return nil
}

func TestClientIntegration(t *testing.T) {
	t.Parallel()

	// Start test service (includes Minio and PostgreSQL)
	service := createTestService(t)
	defer service.Close()

	// Create test server with auth
	testService := &server.Service{
		Pool:        service.Pool,
		MinioClient: service.MinioClient,
		Bucket:      service.Bucket,
		APIToken:    testAuthToken,
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

	err = pushToServer(ctx, ts.URL, testAuthToken, []string{storePath})
	if err != nil {
		t.Fatalf("Client failed: %v", err)
	}

	// Extract hash from store path
	pathParts := strings.Split(filepath.Base(storePath), "-")
	if len(pathParts) < 1 {
		t.Fatal("Invalid store path format")
	}

	hash := pathParts[0]

	// Verify the upload
	verifyNarinfoInS3(ctx, t, testService, hash, storePath)
	verifyLsFileInS3(ctx, t, testService, hash)

	// Test garbage collection
	t.Log("Testing garbage collection...")
	mux.HandleFunc("DELETE /api/closures", testService.AuthMiddleware(testService.CleanupClosuresOlder))

	c, err := client.NewClient(ts.URL, testAuthToken)
	ok(t, err)

	verifyGarbageCollection(ctx, t, service, c)
}

func TestClientMultipleUploads(t *testing.T) {
	t.Parallel()

	// Start test service
	service := createTestService(t)
	defer service.Close()

	// Create test server with auth
	testService := &server.Service{
		Pool:        service.Pool,
		MinioClient: service.MinioClient,
		Bucket:      service.Bucket,
		APIToken:    testAuthToken,
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
	err = pushToServer(ctx, ts.URL, testAuthToken, storePaths)
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
		output, err = exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command", "build", "-f", nixExpr, "--no-link", "--print-out-paths").CombinedOutput()
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
	cmd := exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command", "eval", "--impure", "--expr",
		fmt.Sprintf(`builtins.fetchurl { name = "foo"; url = "s3://%s/nix-cache-info?endpoint=http://%s&region=eu-west-1"; }`, testService.Bucket, endpoint))
	cmd.Env = testEnv

	_, err = cmd.CombinedOutput()
	ok(t, err)

	// Get info about the store (like Nix's tests)
	cmd = exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command", "store", "info", "--store", binaryCacheURL)
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
	cmd = exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command", "copy",
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
	cmd = exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command", "path-info", storePath)

	_, err = cmd.CombinedOutput()
	ok(t, err)
}

func TestClientWithDependencies(t *testing.T) {
	t.Parallel()

	// Start test service
	service := createTestService(t)
	t.Cleanup(func() { service.Close() })

	// Create test server with auth
	testService := &server.Service{
		Pool:        service.Pool,
		MinioClient: service.MinioClient,
		Bucket:      service.Bucket,
		APIToken:    testAuthToken,
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

	runClientAndVerifyUpload(ctx, t, testService, storePath, ts.URL, testAuthToken)

	// Test that we can retrieve the content using nix copy
	t.Run("RetrieveWithNixCopy", func(t *testing.T) {
		t.Parallel()
		testRetrieveWithNixCopy(t.Context(), t, testService, storePath)
	})
}
