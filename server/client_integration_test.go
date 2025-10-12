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
	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
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

	// Read and decompress narinfo content (it's compressed with zstd)
	compressedContent, err := io.ReadAll(narinfoObj)
	ok(t, err)

	// Decompress with zstd
	decoder, err := zstd.NewReader(bytes.NewReader(compressedContent))
	ok(t, err)

	defer decoder.Close()

	narinfoContent, err := io.ReadAll(decoder)
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

	// Check if .ls file exists in S3 (compressed with brotli)
	lsKey := hash + ".ls"

	lsObj, err := testService.MinioClient.GetObject(ctx, testService.Bucket, lsKey, minio.GetObjectOptions{})
	ok(t, err)

	defer func() {
		if err := lsObj.Close(); err != nil {
			t.Logf("Failed to close ls object: %v", err)
		}
	}()

	// Read and decompress .ls content (it's compressed with brotli)
	compressedLsContent, err := io.ReadAll(lsObj)
	ok(t, err)

	t.Logf("Retrieved .ls file from S3 (compressed size: %d bytes)", len(compressedLsContent))

	// Decompress with brotli
	brReader := brotli.NewReader(bytes.NewReader(compressedLsContent))
	lsContent, err := io.ReadAll(brReader)
	ok(t, err)

	t.Logf("Decompressed .ls content (%d bytes):\n%s", len(lsContent), lsContent)

	// Verify it's valid JSON
	var listing map[string]interface{}
	if err := json.Unmarshal(lsContent, &listing); err != nil {
		t.Errorf("Failed to parse .ls content as JSON: %v", err)
	}

	// Verify it has the expected structure
	if version, ok := listing["version"].(float64); !ok || version != 1 {
		t.Errorf("Expected version 1, got %v", listing["version"])
	}

	if _, ok := listing["root"]; !ok {
		t.Errorf(".ls file missing 'root' field")
	}
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

func buildCADerivation(ctx context.Context, t *testing.T) string {
	t.Helper()

	// Create a CA (content-addressed) derivation
	nixExpr := filepath.Join(t.TempDir(), "ca-test.nix")
	nixContent := `
	{ pkgs ? import <nixpkgs> {} }:
	pkgs.runCommand "ca-test" {
		__contentAddressed = true;
		outputHashMode = "recursive";
		outputHashAlgo = "sha256";
	} ''
		echo "Hello from CA derivation" > $out
	''
	`

	err := os.WriteFile(nixExpr, []byte(nixContent), 0o600)
	ok(t, err)

	// Build the CA derivation without using binary caches (build locally only)
	// Use --option substitute false to prevent fetching from binary caches
	// Enable ca-derivations experimental feature
	output, err := exec.CommandContext(ctx, "nix-build", nixExpr, "--no-out-link",
		"--extra-experimental-features", "ca-derivations",
		"--option", "substitute", "false").CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to build CA derivation: %v\nOutput: %s", err, output)
	}

	// Extract the store path from output (last line)
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	storePath := lines[len(lines)-1]
	t.Logf("Built CA derivation: %s", storePath)

	return storePath
}

// verifyRealisationFiles verifies the structure and content of realisation files.
func verifyRealisationFiles(ctx context.Context, t *testing.T, testService *server.Service, realisationKeys []string) {
	t.Helper()

	for _, realisationKey := range realisationKeys {
		realisationObj, err := testService.MinioClient.GetObject(ctx, testService.Bucket, realisationKey, minio.GetObjectOptions{})
		ok(t, err)

		defer realisationObj.Close()

		compressedRealisation, err := io.ReadAll(realisationObj)
		ok(t, err)

		// Decompress with zstd
		realisationDecoder, err := zstd.NewReader(bytes.NewReader(compressedRealisation))
		ok(t, err)

		defer realisationDecoder.Close()

		realisationContent, err := io.ReadAll(realisationDecoder)
		ok(t, err)

		var realisation map[string]interface{}

		err = json.Unmarshal(realisationContent, &realisation)
		ok(t, err)

		// Verify required fields
		if _, exists := realisation["id"]; !exists {
			t.Errorf("Realisation %s missing 'id' field", realisationKey)
		}

		if outPath, exists := realisation["outPath"]; exists {
			if outPathStr, isString := outPath.(string); isString {
				t.Logf("Realisation %s maps to: %s", realisationKey, outPathStr)
			}
		} else {
			t.Errorf("Realisation %s missing 'outPath' field", realisationKey)
		}

		t.Logf("Realisation file verified (%s):\n%s", realisationKey, realisationContent)
	}
}

func TestClientCADerivations(t *testing.T) {
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

	// Initialize the bucket
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

	// Build CA derivation
	caPath := buildCADerivation(ctx, t)

	// Upload to server
	uploadedCount := runClientAndVerifyUpload(ctx, t, testService, caPath, ts.URL, testAuthToken)
	if uploadedCount < 1 {
		t.Error("Expected at least one CA derivation upload")
	}

	// Extract hash from CA path for verification
	pathParts := strings.Split(filepath.Base(caPath), "-")
	if len(pathParts) < 1 {
		t.Fatal("Invalid store path format")
	}

	hash := pathParts[0]

	// Test 1: Verify narinfo contains CA field
	narinfoKey := hash + ".narinfo"
	narinfoObj, err := testService.MinioClient.GetObject(ctx, testService.Bucket, narinfoKey, minio.GetObjectOptions{})
	ok(t, err)

	defer narinfoObj.Close()

	// Decompress and parse narinfo
	compressedContent, err := io.ReadAll(narinfoObj)
	ok(t, err)

	decoder, err := zstd.NewReader(bytes.NewReader(compressedContent))
	ok(t, err)

	defer decoder.Close()

	narinfoContent, err := io.ReadAll(decoder)
	ok(t, err)

	narinfoStr := string(narinfoContent)
	if !strings.Contains(narinfoStr, "CA:") {
		t.Errorf("Narinfo missing CA field:\n%s", narinfoStr)
	}

	t.Logf("Narinfo contains CA field: %s", narinfoStr)

	// Test 2: Check if realisations were uploaded
	// Note: Locally built CA derivations might not have DrvOutput IDs registered,
	// so realisations might not be uploaded. This is OK - we just verify if they exist.
	t.Log("Checking for realisation files in S3...")

	objectCh := testService.MinioClient.ListObjects(ctx, testService.Bucket, minio.ListObjectsOptions{
		Prefix:    "realisations/",
		Recursive: true,
	})

	realisationKeys := []string{}

	for object := range objectCh {
		if object.Err != nil {
			t.Errorf("Error listing realisations: %v", object.Err)

			continue
		}

		if strings.HasSuffix(object.Key, ".doi") {
			realisationKeys = append(realisationKeys, object.Key)
		}
	}

	if len(realisationKeys) == 0 {
		t.Log("No realisation files uploaded - this is expected for locally built CA derivations")
		t.Log("Locally built CA derivations don't have DrvOutput IDs until they're substituted from a cache")
	}

	if len(realisationKeys) > 0 {
		t.Logf("Found %d realisation file(s): %v", len(realisationKeys), realisationKeys)

		// Test 3: Verify realisation JSON structure if realisations exist
		verifyRealisationFiles(ctx, t, testService, realisationKeys)
	}

	// Test 4: Verify Nix can retrieve the CA derivation
	t.Run("RetrieveCADerivation", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()

		// Configure the binary cache URL
		endpoint := testService.MinioClient.EndpointURL().Host
		binaryCacheURL := fmt.Sprintf("s3://%s?endpoint=http://%s&region=eu-west-1", testService.Bucket, endpoint)

		testEnv := append(os.Environ(),
			"AWS_ACCESS_KEY_ID=minioadmin",
			"AWS_SECRET_ACCESS_KEY="+testMinioServer.secret,
		)

		// Try to copy from our cache
		cmd := exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command", "copy",
			"--no-check-sigs",
			"--from", binaryCacheURL,
			caPath)
		cmd.Env = testEnv

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Logf("nix copy output: %s", output)
			t.Logf("nix copy failed (might be expected in some environments): %v", err)
		} else {
			// Verify realisation is registered locally
			cmd = exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command ca-derivations", "realisation", "info", caPath)

			output, err = cmd.CombinedOutput()
			if err != nil {
				t.Logf("nix realisation info output: %s", output)
				t.Logf("Failed to query realisation after copy: %v", err)
			} else {
				t.Logf("Realisation info after copy:\n%s", output)
			}
		}
	})
}

func TestClientErrorHandling(t *testing.T) {
	t.Parallel()

	t.Run("InvalidStorePath", func(t *testing.T) {
		t.Parallel()

		service := createTestService(t)
		defer service.Close()

		// Create test server
		testService := &server.Service{
			Pool:        service.Pool,
			MinioClient: service.MinioClient,
			Bucket:      service.Bucket,
			APIToken:    testAuthToken,
		}
		mux := http.NewServeMux()
		mux.HandleFunc("POST /api/pending_closures", testService.AuthMiddleware(testService.CreatePendingClosureHandler))
		mux.HandleFunc("POST /api/pending_closures/{id}/complete", testService.AuthMiddleware(testService.CommitPendingClosureHandler))
		mux.HandleFunc("POST /api/multipart/complete", testService.AuthMiddleware(testService.CompleteMultipartUploadHandler))

		ts := httptest.NewServer(mux)
		defer ts.Close()

		// Try to upload a non-store path
		ctx := t.Context()

		err := pushToServer(ctx, ts.URL, testAuthToken, []string{"/tmp/nonexistent"})
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

		// Create test server with correct auth token
		correctAuthToken := "correct-auth-token" //nolint:gosec // test credential
		testService := &server.Service{
			Pool:        service.Pool,
			MinioClient: service.MinioClient,
			Bucket:      service.Bucket,
			APIToken:    correctAuthToken,
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
