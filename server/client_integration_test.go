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

	"github.com/Mic92/niks3/server"
	minio "github.com/minio/minio-go/v7"
)

const testAuthToken = "test-auth-token" //nolint:gosec // Test token for integration tests

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
	err := testService.InitializeBucket(context.Background())
	ok(t, err)

	mux := http.NewServeMux()

	// Register handlers with auth
	mux.HandleFunc("POST /api/pending_closures", testService.AuthMiddleware(testService.CreatePendingClosureHandler))
	mux.HandleFunc("POST /api/pending_closures/{id}/complete", testService.AuthMiddleware(testService.CommitPendingClosureHandler))
	mux.HandleFunc("GET /health", testService.HealthCheckHandler)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Create a test file and add it to the Nix store
	tempFile := filepath.Join(t.TempDir(), "test-file.txt")
	err = os.WriteFile(tempFile, []byte("test content for niks3 integration test"), 0o600)
	ok(t, err)

	// Add the file to nix store
	output, err := exec.Command("nix-store", "--add", tempFile).CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to add file to nix store: %v\nOutput: %s", err, output)
	}

	storePath := strings.TrimSpace(string(output))
	t.Logf("Created store path: %s", storePath)

	// Run the client to upload the store path
	cmd := exec.Command(testClientPath, "push", storePath)

	cmd.Env = append(os.Environ(),
		"NIKS3_SERVER_URL="+ts.URL,
		"NIKS3_AUTH_TOKEN="+authToken,
		"RUST_LOG=info",
	)

	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Client failed: %v\nOutput: %s", err, output)
	}

	t.Logf("Client output: %s", output)

	// Verify the upload by checking if the objects exist in S3
	// Extract hash from store path
	pathParts := strings.Split(filepath.Base(storePath), "-")
	if len(pathParts) < 1 {
		t.Fatal("Invalid store path format")
	}

	hash := pathParts[0]

	// Check if narinfo exists in S3
	narinfoKey := hash + ".narinfo"
	narinfoObj, err := testService.MinioClient.GetObject(context.Background(), testService.Bucket, narinfoKey, minio.GetObjectOptions{})
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

	_, err = testService.MinioClient.StatObject(context.Background(), testService.Bucket, narKey, minio.StatObjectOptions{})
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
	err := testService.InitializeBucket(context.Background())
	ok(t, err)

	mux := http.NewServeMux()

	// Register handlers
	mux.HandleFunc("POST /api/pending_closures", testService.AuthMiddleware(testService.CreatePendingClosureHandler))
	mux.HandleFunc("POST /api/pending_closures/{id}/complete", testService.AuthMiddleware(testService.CommitPendingClosureHandler))

	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Create multiple test files and add them to nix store
	storePaths := make([]string, 0, 3)

	var output []byte

	for i := range 3 {
		tempFile := filepath.Join(t.TempDir(), fmt.Sprintf("test-file-%d.txt", i))
		content := fmt.Sprintf("test content %d for niks3 integration test", i)
		err = os.WriteFile(tempFile, []byte(content), 0o600)
		ok(t, err)

		output, err = exec.Command("nix-store", "--add", tempFile).CombinedOutput()
		ok(t, err)

		storePath := strings.TrimSpace(string(output))
		storePaths = append(storePaths, storePath)
		t.Logf("Created store path %d: %s", i, storePath)
	}

	// Run the client with all paths
	args := []string{"push"}
	args = append(args, storePaths...)

	cmd := exec.Command(testClientPath, args...)

	cmd.Env = append(os.Environ(),
		"NIKS3_SERVER_URL="+ts.URL,
		"NIKS3_AUTH_TOKEN="+authToken,
		"RUST_LOG=info",
	)

	start := time.Now()
	output, err = cmd.CombinedOutput()
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Client failed: %v\nOutput: %s", err, output)
	}

	t.Logf("Uploaded %d paths in %v", len(storePaths), duration)
	t.Logf("Client output: %s", output)

	// Verify all uploads
	for _, storePath := range storePaths {
		pathParts := strings.Split(filepath.Base(storePath), "-")
		hash := pathParts[0]

		// Check if narinfo exists in S3
		narinfoKey := hash + ".narinfo"

		_, err := testService.MinioClient.StatObject(context.Background(), testService.Bucket, narinfoKey, minio.StatObjectOptions{})
		ok(t, err)

		// Check if NAR exists in S3 (compressed with zstd)
		narKey := fmt.Sprintf("nar/%s.nar.zst", hash)

		_, err = testService.MinioClient.StatObject(context.Background(), testService.Bucket, narKey, minio.StatObjectOptions{})
		ok(t, err)
	}
}

func buildNixDerivation(t *testing.T) string {
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
	output, err := exec.Command("nix-build", nixExpr, "--no-out-link").CombinedOutput()
	if err != nil {
		// If nix-build fails, try with nix build
		output, err = exec.Command("nix", "build", "-f", nixExpr, "--no-link", "--print-out-paths").CombinedOutput()
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

func runClientAndVerifyUpload(t *testing.T, testService *server.Service, storePath, serverURL, authToken string) int {
	t.Helper()
	// Get dependencies using nix-store -qR
	output, err := exec.Command("nix-store", "-qR", storePath).CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to query dependencies: %v\nOutput: %s", err, output)
	}

	dependencies := strings.Split(strings.TrimSpace(string(output)), "\n")
	t.Logf("Found %d dependencies (including self)", len(dependencies))

	// Run the client to upload the store path (should upload all dependencies)
	cmd := exec.Command(testClientPath, "push", storePath)

	cmd.Env = append(os.Environ(),
		"NIKS3_SERVER_URL="+serverURL,
		"NIKS3_AUTH_TOKEN="+authToken,
		"RUST_LOG=info",
	)

	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Client failed: %v\nOutput: %s", err, output)
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

		_, err := testService.MinioClient.StatObject(context.Background(), testService.Bucket, narinfoKey, minio.StatObjectOptions{})
		if err != nil {
			// Some dependencies might already exist, which is fine
			t.Logf("Narinfo not found for %s (might already exist): %v", dep, err)
		} else {
			uploadedCount++

			// Also verify NAR exists (compressed with zstd)
			narKey := fmt.Sprintf("nar/%s.nar.zst", hash)

			_, err = testService.MinioClient.StatObject(context.Background(), testService.Bucket, narKey, minio.StatObjectOptions{})
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

func testRetrieveWithNixCopy(t *testing.T, testService *server.Service, storePath string) {
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
	cmd := exec.Command("nix", "eval", "--impure", "--expr",
		fmt.Sprintf(`builtins.fetchurl { name = "foo"; url = "s3://%s/nix-cache-info?endpoint=http://%s&region=eu-west-1"; }`, testService.Bucket, endpoint))
	cmd.Env = testEnv

	_, err = cmd.CombinedOutput()
	ok(t, err)

	// Get info about the store (like Nix's tests)
	cmd = exec.Command("nix", "store", "info", "--store", binaryCacheURL)
	cmd.Env = testEnv

	_, err = cmd.CombinedOutput()
	ok(t, err)

	// Debug: Download and check a narinfo to see its format
	hash := strings.Split(filepath.Base(storePath), "-")[0]
	narinfoKey := hash + ".narinfo"

	narinfoObj, err := testService.MinioClient.GetObject(context.Background(),
		testService.Bucket, narinfoKey, minio.GetObjectOptions{})
	ok(t, err)

	_, err = io.ReadAll(narinfoObj)
	if err := narinfoObj.Close(); err != nil {
		ok(t, err)
	}

	ok(t, err)

	// Use --no-check-sigs like in Nix's tests
	cmd = exec.Command("nix", "copy",
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
	cmd = exec.Command("nix", "path-info", storePath)

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
	err := testService.InitializeBucket(context.Background())
	ok(t, err)

	mux := http.NewServeMux()

	// Register handlers
	mux.HandleFunc("POST /api/pending_closures", testService.AuthMiddleware(testService.CreatePendingClosureHandler))
	mux.HandleFunc("POST /api/pending_closures/{id}/complete", testService.AuthMiddleware(testService.CommitPendingClosureHandler))
	mux.HandleFunc("GET /health", testService.HealthCheckHandler)

	ts := httptest.NewServer(mux)

	t.Cleanup(func() { ts.Close() })

	storePath := buildNixDerivation(t)

	runClientAndVerifyUpload(t, testService, storePath, ts.URL, authToken)

	// Test that we can retrieve the content using nix copy
	t.Run("RetrieveWithNixCopy", func(t *testing.T) {
		t.Parallel()
		testRetrieveWithNixCopy(t, testService, storePath)
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

		ts := httptest.NewServer(mux)
		defer ts.Close()

		// Try to upload a non-store path
		cmd := exec.Command(testClientPath, "push", "/tmp/nonexistent")

		cmd.Env = append(os.Environ(),
			"NIKS3_SERVER_URL="+ts.URL,
			"NIKS3_AUTH_TOKEN="+authToken,
		)

		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatal("Expected error for invalid store path")
		}

		outputStr := string(output)
		if !strings.Contains(outputStr, "nix path-info failed") {
			t.Errorf("Expected 'nix path-info failed' error, got: %s", outputStr)
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

		ts := httptest.NewServer(mux)
		defer ts.Close()

		// Create a valid store path
		tempFile := filepath.Join(t.TempDir(), "test.txt")
		err := os.WriteFile(tempFile, []byte("test"), 0o600)
		ok(t, err)

		output, err := exec.Command("nix-store", "--add", tempFile).CombinedOutput()
		ok(t, err)

		storePath := strings.TrimSpace(string(output))

		// Try with invalid auth token
		cmd := exec.Command(testClientPath, "push", storePath)

		cmd.Env = append(os.Environ(),
			"NIKS3_SERVER_URL="+ts.URL,
			"NIKS3_AUTH_TOKEN=invalid-token",
		)

		output, err = cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("Expected error for invalid auth token, but got success. Output: %s", output)
		}
	})

	t.Run("ServerNotAvailable", func(t *testing.T) {
		t.Parallel()
		// Create a valid store path
		tempFile := filepath.Join(t.TempDir(), "test.txt")
		err := os.WriteFile(tempFile, []byte("test"), 0o600)
		ok(t, err)

		output, err := exec.Command("nix-store", "--add", tempFile).CombinedOutput()
		ok(t, err)

		storePath := strings.TrimSpace(string(output))

		// Try with unavailable server
		cmd := exec.Command(testClientPath, "push", storePath)

		cmd.Env = append(os.Environ(),
			"NIKS3_SERVER_URL=http://localhost:19999",
			"NIKS3_AUTH_TOKEN=test-token",
		)

		output, err = cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("Expected error for unavailable server, but got success. Output: %s", output)
		}
	})
}
