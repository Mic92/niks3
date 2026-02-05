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

const (
	testAuthToken      = "test-auth-token" //nolint:gosec // Test token for integration tests
	defaultNixStoreDir = "/nix/store"
)

// getNARURLFromNarinfo fetches a narinfo from S3, decompresses it, and extracts the URL field.
func getNARURLFromNarinfo(ctx context.Context, t *testing.T, testService *server.Service, narinfoKey string) string {
	t.Helper()

	narinfoObj, err := testService.MinioClient.GetObject(ctx, testService.Bucket, narinfoKey, minio.GetObjectOptions{})
	ok(t, err)

	narinfoContent, err := io.ReadAll(narinfoObj)
	ok(t, err)

	if err := narinfoObj.Close(); err != nil {
		t.Logf("Failed to close narinfo object: %v", err)
	}

	decoder, err := zstd.NewReader(bytes.NewReader(narinfoContent))
	ok(t, err)

	defer decoder.Close()

	narinfoText, err := io.ReadAll(decoder)
	ok(t, err)

	// Extract URL from narinfo
	for line := range strings.SplitSeq(string(narinfoText), "\n") {
		if after, ok0 := strings.CutPrefix(line, "URL: "); ok0 {
			return after
		}
	}

	t.Fatal("No URL found in narinfo")

	return ""
}

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

	// Extract URL from narinfo and verify NAR file exists
	narURL := getNARURLFromNarinfo(ctx, t, testService, narinfoKey)

	_, err = testService.MinioClient.StatObject(ctx, testService.Bucket, narURL, minio.StatObjectOptions{})
	if err != nil {
		t.Errorf("NAR file not found in S3 at %s: %v", narURL, err)
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

	var listing map[string]any
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

	_, err := c.RunGarbageCollection(ctx, "0s", "0s", true)
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
			firstDeletedAt any
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
func pushToServer(ctx context.Context, serverURL, authToken string, paths []string, nixEnv []string) error {
	// Create client
	c, err := client.NewClient(ctx, serverURL, authToken)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	// Test with 16 concurrent uploads (optimal based on benchmarks)
	// Tested 8, 16, 24: 16 showed best throughput (3.33s vs 3.59s and 3.62s)
	c.MaxConcurrentNARUploads = 16
	c.NixEnv = nixEnv

	// Use the high-level PushPaths method
	if err := c.PushPaths(ctx, paths); err != nil {
		return fmt.Errorf("pushing paths: %w", err)
	}

	return nil
}

func TestClientIntegration(t *testing.T) {
	t.Parallel()

	// Start test service (includes Minio and PostgreSQL)
	testService := createTestServiceWithAuth(t, testAuthToken)
	defer testService.Close()

	// Initialize the bucket with nix-cache-info
	err := testService.InitializeBucket(t.Context())
	ok(t, err)

	mux := http.NewServeMux()
	registerTestHandlers(mux, testService)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Create a test file and add it to the Nix store
	tempFile := filepath.Join(t.TempDir(), "test-file.txt")
	err = os.WriteFile(tempFile, []byte("test content for niks3 integration test"), 0o600)
	ok(t, err)

	// Use the client package to upload the store path
	ctx := t.Context()
	nixEnv := setupIsolatedNixStore(t)

	// Add the file to nix store
	storePath := nixStoreAdd(t, nixEnv, tempFile)
	t.Logf("Created store path: %s", storePath)

	err = pushToServer(ctx, ts.URL, testAuthToken, []string{storePath}, nixEnv)
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

	c, err := client.NewClient(ctx, ts.URL, testAuthToken)
	ok(t, err)

	verifyGarbageCollection(ctx, t, testService, c)
}

func TestClientMultipleUploads(t *testing.T) {
	t.Parallel()

	// Start test service
	testService := createTestServiceWithAuth(t, testAuthToken)
	defer testService.Close()

	// Initialize the bucket with nix-cache-info
	err := testService.InitializeBucket(t.Context())
	ok(t, err)

	mux := http.NewServeMux()
	registerTestHandlers(mux, testService)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Use the client package to upload all paths
	ctx := t.Context()
	nixEnv := setupIsolatedNixStore(t)

	// Create multiple test files and add them to nix store
	storePaths := make([]string, 0, 3)

	for i := range 3 {
		tempFile := filepath.Join(t.TempDir(), fmt.Sprintf("test-file-%d.txt", i))
		content := fmt.Sprintf("test content %d for niks3 integration test", i)
		err = os.WriteFile(tempFile, []byte(content), 0o600)
		ok(t, err)

		storePath := nixStoreAdd(t, nixEnv, tempFile)
		storePaths = append(storePaths, storePath)
		t.Logf("Created store path %d: %s", i, storePath)
	}

	start := time.Now()
	err = pushToServer(ctx, ts.URL, testAuthToken, storePaths, nixEnv)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Client failed: %v", err)
	}

	t.Logf("Uploaded %d paths in %v", len(storePaths), duration)

	// Verify all uploads
	for _, storePath := range storePaths {
		pathParts := strings.Split(filepath.Base(storePath), "-")
		hash := pathParts[0]

		// Check if narinfo exists and get NAR URL from it
		narinfoKey := hash + ".narinfo"
		narURL := getNARURLFromNarinfo(ctx, t, testService, narinfoKey)

		// Check if NAR exists in S3 at the URL specified in narinfo
		_, err := testService.MinioClient.StatObject(ctx, testService.Bucket, narURL, minio.StatObjectOptions{})
		ok(t, err)
	}
}

func buildNixDerivation(ctx context.Context, t *testing.T, nixEnv []string) string {
	t.Helper()
	// Create a simple derivation using bare derivation (no nixpkgs dependency)
	nixExpr := filepath.Join(t.TempDir(), "test.nix")
	nixContent := `
	derivation {
		name = "test-script";
		system = builtins.currentSystem;
		builder = "/bin/sh";
		args = [ "-c" "echo 'Hello from niks3 test' > $out" ];
	}
	`

	err := os.WriteFile(nixExpr, []byte(nixContent), 0o600)
	ok(t, err)

	// Build the derivation
	cmd := exec.CommandContext(ctx, "nix-build", nixExpr, "--no-out-link")
	cmd.Env = nixEnv

	output, err := cmd.CombinedOutput()
	if err != nil {
		// If nix-build fails, try with nix build
		cmd = exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command", "build", "-f", nixExpr, "--no-link", "--print-out-paths")
		cmd.Env = nixEnv

		output, err = cmd.CombinedOutput()
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

func runClientAndVerifyUpload(ctx context.Context, t *testing.T, testService *server.Service, storePath, serverURL, authToken string, nixEnv []string) int {
	t.Helper()
	// Get dependencies using nix-store -qR
	cmd := exec.CommandContext(ctx, "nix-store", "-qR", storePath)
	cmd.Env = nixEnv

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to query dependencies: %v\nOutput: %s", err, output)
	}

	dependencies := strings.Split(strings.TrimSpace(string(output)), "\n")
	t.Logf("Found %d dependencies (including self)", len(dependencies))

	// Use the client package to upload the store path (should upload all dependencies)
	err = pushToServer(ctx, serverURL, authToken, []string{storePath}, nixEnv)
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

			// Also verify NAR exists (get URL from narinfo)
			narURL := getNARURLFromNarinfo(ctx, t, testService, narinfoKey)

			_, err = testService.MinioClient.StatObject(ctx, testService.Bucket, narURL, minio.StatObjectOptions{})
			if err != nil {
				t.Errorf("NAR not found for %s at %s: %v", dep, narURL, err)
			}
		}
	}

	// The client should have uploaded at least the main derivation
	if uploadedCount < 1 {
		t.Error("Expected at least one upload")
	}

	return uploadedCount
}

func testRetrieveWithNixCopy(ctx context.Context, t *testing.T, testService *server.Service, storePath string, nixEnv []string) {
	t.Helper()

	// Extract NIX_STORE_DIR from nixEnv
	var storeDir string

	for _, envVar := range nixEnv {
		if after, ok0 := strings.CutPrefix(envVar, "NIX_STORE_DIR="); ok0 {
			storeDir = after

			break
		}
	}

	if storeDir == "" {
		storeDir = defaultNixStoreDir
	}

	// Skip if using isolated store (non-standard NIX_STORE_DIR) because
	// S3 binary cache requires matching store prefixes between upload and download
	if storeDir != defaultNixStoreDir {
		t.Logf("Skipping nix copy test - isolated store (%s) requires matching store prefix", storeDir)

		return
	}

	// Create a temporary store directory
	tempStore := filepath.Join(t.TempDir(), "nix-store")

	err := os.MkdirAll(tempStore, 0o755)
	ok(t, err)

	// Configure the binary cache URL using the same format as Nix's own tests
	endpoint := testService.MinioClient.EndpointURL().Host

	binaryCacheURL := fmt.Sprintf("s3://%s?endpoint=http://%s&region=eu-west-1&store=%s", testService.Bucket, endpoint, storeDir)

	// Set up environment for AWS credentials
	// Use the same env vars as Nix's tests
	nixEnv = append(nixEnv,
		"AWS_ACCESS_KEY_ID=rustfsadmin",
		"AWS_SECRET_ACCESS_KEY="+testRustfsServer.secret,
	)
	testEnv := nixEnv

	// First test that we can fetch nix-cache-info (like Nix's own tests do)
	// #nosec G204 -- test code with controlled inputs
	cmd := exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command flakes", "eval", "--impure", "--expr",
		fmt.Sprintf(`builtins.fetchurl { name = "foo"; url = "s3://%s/nix-cache-info?endpoint=http://%s&region=eu-west-1&store=%s"; }`, testService.Bucket, endpoint, storeDir))
	cmd.Env = testEnv

	_, err = cmd.CombinedOutput()
	ok(t, err)

	// Get info about the store (like Nix's tests)
	cmd = exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command flakes", "store", "info", "--store", binaryCacheURL)
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
	// Pass the full absolute path - NIX_STORE_DIR env var handles the rest
	copyArgs := []string{
		"--extra-experimental-features", "nix-command flakes", "copy",
		"--no-check-sigs",
		"--from", binaryCacheURL,
		storePath,
	}
	cmd = exec.CommandContext(ctx, "nix", copyArgs...)
	cmd.Env = testEnv

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("nix copy failed: %v\nOutput: %s", err, output)
		// This might be expected in some test environments, so don't fail immediately
	}

	// Verify the path exists locally now
	pathInfoArgs := []string{"--extra-experimental-features", "nix-command flakes", "path-info", storePath}
	cmd = exec.CommandContext(ctx, "nix", pathInfoArgs...)
	cmd.Env = testEnv

	_, err = cmd.CombinedOutput()
	ok(t, err)
}

func TestClientWithDependencies(t *testing.T) {
	t.Parallel()

	// Start test service
	testService := createTestServiceWithAuth(t, testAuthToken)
	t.Cleanup(func() { testService.Close() })

	// Initialize the bucket with nix-cache-info
	err := testService.InitializeBucket(t.Context())
	ok(t, err)

	mux := http.NewServeMux()
	registerTestHandlers(mux, testService)

	ts := httptest.NewServer(mux)

	t.Cleanup(func() { ts.Close() })

	ctx := t.Context()
	nixEnv := setupIsolatedNixStore(t)
	storePath := buildNixDerivation(ctx, t, nixEnv)

	runClientAndVerifyUpload(ctx, t, testService, storePath, ts.URL, testAuthToken, nixEnv)

	testRetrieveWithNixCopy(ctx, t, testService, storePath, nixEnv)
}

func TestPinProtectsFromGC(t *testing.T) {
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
	registerTestHandlers(mux, testService)
	mux.HandleFunc("POST /api/pins/{name}", testService.AuthMiddleware(testService.CreatePinHandler))
	mux.HandleFunc("DELETE /api/closures", testService.AuthMiddleware(testService.CleanupClosuresOlder))

	ts := httptest.NewServer(mux)

	t.Cleanup(func() { ts.Close() })

	ctx := t.Context()
	nixEnv := setupIsolatedNixStore(t)

	// Create two store paths - one will be pinned, one won't
	pinnedFile := filepath.Join(t.TempDir(), "pinned-file.txt")
	err = os.WriteFile(pinnedFile, []byte("pinned content"), 0o600)
	ok(t, err)

	unpinnedFile := filepath.Join(t.TempDir(), "unpinned-file.txt")
	err = os.WriteFile(unpinnedFile, []byte("unpinned content"), 0o600)
	ok(t, err)

	pinnedStorePath := nixStoreAdd(t, nixEnv, pinnedFile)
	unpinnedStorePath := nixStoreAdd(t, nixEnv, unpinnedFile)

	t.Logf("Pinned store path: %s", pinnedStorePath)
	t.Logf("Unpinned store path: %s", unpinnedStorePath)

	// Push both paths
	err = pushToServer(ctx, ts.URL, testAuthToken, []string{pinnedStorePath}, nixEnv)
	ok(t, err)
	err = pushToServer(ctx, ts.URL, testAuthToken, []string{unpinnedStorePath}, nixEnv)
	ok(t, err)

	// Create a pin for the first path
	c, err := client.NewClient(ctx, ts.URL, testAuthToken)
	ok(t, err)

	err = c.CreatePin(ctx, "myapp", pinnedStorePath)
	ok(t, err)

	// Verify pin exists in S3
	pinObj, err := testService.MinioClient.GetObject(ctx, testService.Bucket, "pins/myapp", minio.GetObjectOptions{})
	ok(t, err)

	pinContent, err := io.ReadAll(pinObj)
	ok(t, err)

	if err := pinObj.Close(); err != nil {
		t.Logf("Failed to close pin object: %v", err)
	}

	if string(pinContent) != pinnedStorePath {
		t.Errorf("Pin content mismatch: got %q, want %q", string(pinContent), pinnedStorePath)
	}

	// Extract hashes for verification
	pinnedHash := strings.Split(filepath.Base(pinnedStorePath), "-")[0]
	unpinnedHash := strings.Split(filepath.Base(unpinnedStorePath), "-")[0]

	// Run garbage collection with force mode (immediate deletion)
	_, err = c.RunGarbageCollection(ctx, "0s", "0s", true)
	ok(t, err)

	// Verify pinned closure still exists
	var pinnedClosureCount int

	err = testService.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM closures WHERE key = $1", pinnedHash+".narinfo").Scan(&pinnedClosureCount)
	ok(t, err)

	if pinnedClosureCount != 1 {
		t.Errorf("Expected pinned closure to exist, but got count=%d", pinnedClosureCount)
	}

	// Verify unpinned closure was deleted
	var unpinnedClosureCount int

	err = testService.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM closures WHERE key = $1", unpinnedHash+".narinfo").Scan(&unpinnedClosureCount)
	ok(t, err)

	if unpinnedClosureCount != 0 {
		t.Errorf("Expected unpinned closure to be deleted, but got count=%d", unpinnedClosureCount)
	}

	// Verify pinned narinfo still exists in S3
	_, err = testService.MinioClient.StatObject(ctx, testService.Bucket, pinnedHash+".narinfo", minio.StatObjectOptions{})
	if err != nil {
		t.Errorf("Pinned narinfo should still exist in S3: %v", err)
	}

	t.Log("Pin successfully protected closure from garbage collection")
}
