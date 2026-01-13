package server_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server"
)

func TestClientErrorHandling(t *testing.T) {
	t.Parallel()

	t.Run("InvalidStorePath", func(t *testing.T) {
		t.Parallel()

		service := createTestService(t)
		defer service.Close()

		// Create test server
		testService := &server.Service{
			Pool:          service.Pool,
			MinioClient:   service.MinioClient,
			Bucket:        service.Bucket,
			APIToken:      testAuthToken,
			S3Concurrency: 100,
		}
		mux := http.NewServeMux()
		registerTestHandlers(mux, testService)

		ts := httptest.NewServer(mux)
		defer ts.Close()

		// Try to upload a non-store path
		ctx := t.Context()

		err := pushToServer(ctx, ts.URL, testAuthToken, []string{"/tmp/nonexistent"}, nil)
		if err == nil {
			t.Fatal("Expected error for invalid store path")
		}

		if !strings.Contains(err.Error(), "path does not exist") {
			t.Errorf("Expected 'path does not exist' error, got: %s", err)
		}
	})

	t.Run("InvalidAuthToken", func(t *testing.T) {
		t.Parallel()

		service := createTestService(t)
		defer service.Close()

		// Create test server with correct auth token
		correctAuthToken := "correct-auth-token" //nolint:gosec // test credential
		testService := &server.Service{
			Pool:          service.Pool,
			MinioClient:   service.MinioClient,
			Bucket:        service.Bucket,
			APIToken:      correctAuthToken,
			S3Concurrency: 100,
		}
		mux := http.NewServeMux()
		registerTestHandlers(mux, testService)

		ts := httptest.NewServer(mux)
		defer ts.Close()

		// Create a valid store path
		tempFile := filepath.Join(t.TempDir(), "test.txt")
		if err := os.WriteFile(tempFile, []byte("test"), 0o600); err != nil {
			ok(t, err)
		}

		// Try with invalid auth token
		ctx := t.Context()
		nixEnv := setupIsolatedNixStore(t)

		storePath := nixStoreAdd(t, nixEnv, tempFile)

		err := pushToServer(ctx, ts.URL, "invalid-token", []string{storePath}, nixEnv)
		if err == nil {
			t.Fatal("Expected error for invalid auth token")
		}
	})

	t.Run("ServerNotAvailable", func(t *testing.T) {
		t.Parallel()

		// Try with unavailable server
		ctx := t.Context()
		nixEnv := setupIsolatedNixStore(t)

		// Create a valid store path
		tempFile := filepath.Join(t.TempDir(), "test.txt")
		err := os.WriteFile(tempFile, []byte("test"), 0o600)
		ok(t, err)

		storePath := nixStoreAdd(t, nixEnv, tempFile)

		err = pushToServer(ctx, "http://localhost:19999", "test-token", []string{storePath}, nixEnv)
		if err == nil {
			t.Fatal("Expected error for unavailable server")
		}
	})
}
