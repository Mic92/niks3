package server_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
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
			Pool:        service.Pool,
			MinioClient: service.MinioClient,
			Bucket:      service.Bucket,
			APIToken:    testAuthToken,
		}
		mux := http.NewServeMux()
		registerTestHandlers(mux, testService)

		ts := httptest.NewServer(mux)
		defer ts.Close()

		// Try to upload a non-store path
		ctx := t.Context()

		err := pushToServer(ctx, ts.URL, testAuthToken, []string{"/tmp/nonexistent"})
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
			Pool:        service.Pool,
			MinioClient: service.MinioClient,
			Bucket:      service.Bucket,
			APIToken:    correctAuthToken,
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

		// Retry nix-store --add to handle transient SQLite errors
		var (
			output []byte
			err    error
		)

		for attempt := 1; attempt <= 3; attempt++ {
			output, err = exec.CommandContext(ctx, "nix-store", "--add", tempFile).CombinedOutput()
			if err == nil {
				break
			}

			if attempt < 3 && strings.Contains(string(output), "database is busy") {
				t.Logf("nix-store --add attempt %d/3 failed (database busy), retrying...", attempt)

				continue
			}

			ok(t, err)
		}

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
