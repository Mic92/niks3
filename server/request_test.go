package server_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/pg"
	minio "github.com/minio/minio-go/v7"
)

func createTestService(tb testing.TB) *server.Service {
	tb.Helper()

	if testPostgresServer == nil {
		tb.Fatal("postgres server not started")
	}

	if testRustfsServer == nil {
		tb.Fatal("rustfs server not started")
	}

	// create database for test
	dbName := "db" + strconv.Itoa(int(testDBCount.Add(1)))
	//nolint:gosec
	command := exec.CommandContext(tb.Context(), "createdb", "-h", testPostgresServer.tempDir, "-U", "postgres", dbName)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	err := command.Run()
	ok(tb, err)

	connectionString := fmt.Sprintf("postgres://?dbname=%s&user=postgres&host=%s", dbName, testPostgresServer.tempDir)

	ctx, cancel := context.WithTimeout(tb.Context(), 10*time.Second)
	defer cancel()

	pool, err := pg.Connect(ctx, connectionString)
	if err != nil {
		ok(tb, err)
	}
	// create bucket for test
	bucketName := "bucket" + strconv.Itoa(int(testBucketCount.Add(1)))
	minioClient := testRustfsServer.Client(tb)

	err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	ok(tb, err)

	return &server.Service{
		Pool:          pool,
		Bucket:        bucketName,
		MinioClient:   minioClient,
		S3Concurrency: 100,
	}
}

func createTestServiceWithAuth(tb testing.TB, token string) *server.Service {
	tb.Helper()
	service := createTestService(tb)
	service.APIToken = token
	return service
}

// setupIsolatedNixStore creates an isolated Nix store environment for a test.
// This prevents tests from interfering with each other when running in parallel.
// Returns environment variables that should be used with exec.Command.
func setupIsolatedNixStore(tb testing.TB) []string {
	tb.Helper()

	// Create a unique temporary directory for this test's Nix store
	testRoot := tb.TempDir()

	// Resolve symlinks to get canonical path (important on macOS where /tmp -> /private/tmp)
	var err error

	testRoot, err = filepath.EvalSymlinks(testRoot)
	ok(tb, err)

	// Set up Nix environment variables pointing to isolated directories
	nixStoreDir := testRoot + "/store"
	nixStateDir := testRoot + "/state"
	nixDataDir := testRoot + "/share"
	nixLogDir := testRoot + "/var/log/nix"
	nixConfDir := testRoot + "/etc"
	xdgCacheHome := testRoot + "/cache"

	// Create required directories
	dirs := []string{
		nixStoreDir,
		nixStateDir + "/nix/profiles",
		nixDataDir,
		nixLogDir + "/drvs",
		nixConfDir,
		xdgCacheHome,
	}

	for _, dir := range dirs {
		err := os.MkdirAll(dir, 0o755)
		ok(tb, err)
	}

	// Build environment with isolated Nix store configuration
	// Start with current environment but override Nix-specific variables
	env := os.Environ()
	nixEnv := []string{
		"NIX_STORE_DIR=" + nixStoreDir,
		"NIX_STATE_DIR=" + nixStateDir,
		"NIX_DATA_DIR=" + nixDataDir,
		"NIX_LOG_DIR=" + nixLogDir,
		"NIX_CONF_DIR=" + nixConfDir,
		"XDG_CACHE_HOME=" + xdgCacheHome,
		"NIX_REMOTE=",
		"_NIX_TEST_NO_SANDBOX=1",
		//nolint:misspell // "substituters" is the correct Nix config option name
		"NIX_CONFIG=substituters =\nconnect-timeout = 0\nsandbox = false",
	}

	// Filter out any existing NIX_* environment variables to avoid conflicts
	var filteredEnv []string

	for _, e := range env {
		if !strings.HasPrefix(e, "NIX_") && !strings.HasPrefix(e, "_NIX_") && !strings.HasPrefix(e, "XDG_CACHE_HOME=") {
			filteredEnv = append(filteredEnv, e)
		}
	}

	return append(filteredEnv, nixEnv...)
}

// nixStoreAdd adds a file to the Nix store and returns its store path.
// This helper properly separates stdout (the path) from stderr (warnings/logs).
func nixStoreAdd(tb testing.TB, nixEnv []string, filePath string) string {
	tb.Helper()

	cmd := exec.CommandContext(tb.Context(), "nix-store", "--add", filePath)
	cmd.Env = nixEnv

	output, err := cmd.Output()
	if err != nil {
		// If Output() fails, get stderr for diagnostics
		exitErr := &exec.ExitError{}
		if errors.As(err, &exitErr) {
			tb.Fatalf("Failed to add file to nix store: %v\nStderr: %s", err, exitErr.Stderr)
		}

		tb.Fatalf("Failed to add file to nix store: %v", err)
	}

	return strings.TrimSpace(string(output))
}

type TestRequest struct {
	method  string
	path    string
	body    []byte
	handler http.HandlerFunc
	// function to checkResponse the response
	checkResponse *func(*testing.T, *httptest.ResponseRecorder)
	header        map[string]string
	pathValues    map[string]string
}

func testRequest(t *testing.T, req *TestRequest) *httptest.ResponseRecorder {
	t.Helper()

	rr := httptest.NewRecorder()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, req.method, req.path, bytes.NewBuffer(req.body))
	for k, v := range req.pathValues {
		httpReq.SetPathValue(k, v)
	}

	for k, v := range req.header {
		httpReq.Header.Set(k, v)
	}

	ok(t, err)
	req.handler.ServeHTTP(rr, httpReq)

	if req.checkResponse != nil {
		(*req.checkResponse)(t, rr)
	} else if rr.Code < 200 || rr.Code >= 300 {
		httpOkDepth(t, rr)
	}

	return rr
}
