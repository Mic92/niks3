package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server"
)

// BenchmarkPythonClosure benchmarks building a Python closure with
// common dependencies and uploading it to S3.
//
// This benchmark measures the end-to-end performance of:
// 1. Building a Python environment with popular packages (if not already built)
// 2. Uploading the entire closure to S3 via the niks3 server
//
// The Python closure is defined in nix/benchmark/python-closure.nix
// and can be built with: nix build .#python-closure.
func BenchmarkPythonClosure(b *testing.B) {
	ctx := context.Background()

	// Find the git repository root
	gitRoot, err := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		b.Fatalf("Failed to find git repository root: %v", err)
	}

	projectRoot := strings.TrimSpace(string(gitRoot))

	// Build the Python closure once before benchmarking
	b.Log("Building Python closure (this may take a while on first run)...")

	cmd := exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command flakes", "build", ".#benchmark-closure", "--print-out-paths", "--no-link")
	cmd.Dir = projectRoot

	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			b.Fatalf("Failed to build Python closure: %v\nStderr: %s", err, exitErr.Stderr)
		}

		b.Fatalf("Failed to build Python closure: %v", err)
	}

	closurePath := strings.TrimSpace(string(output))
	b.Logf("Built closure: %s", closurePath)

	// Get closure size for reporting
	sizeOutput, err := exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command", "path-info", "-Sh", closurePath).CombinedOutput()
	if err != nil {
		b.Logf("Warning: Could not determine closure size: %v", err)
	} else {
		b.Logf("Closure size: %s", strings.TrimSpace(string(sizeOutput)))
	}

	// Reset the timer to exclude setup time
	b.ResetTimer()

	// Run the benchmark
	for range b.N {
		// Start fresh services for each iteration (don't count setup time)
		b.StopTimer()
		service := createTestService(b)

		// Create test server with auth
		testService := &server.Service{
			Pool:        service.Pool,
			MinioClient: service.MinioClient,
			Bucket:      service.Bucket,
			APIToken:    testAuthToken,
		}

		// Initialize the bucket with nix-cache-info
		err := testService.InitializeBucket(ctx)
		if err != nil {
			b.Fatalf("Failed to initialize bucket: %v", err)
		}

		mux := http.NewServeMux()
		registerTestHandlers(mux, testService)

		ts := httptest.NewServer(mux)

		b.StartTimer()

		// Upload the closure to S3
		err = pushToServer(ctx, ts.URL, testAuthToken, []string{closurePath})
		if err != nil {
			b.Fatalf("Failed to push closure: %v", err)
		}

		// Clean up (don't count cleanup time)
		b.StopTimer()
		ts.Close()
		service.Close()
		b.StartTimer()
	}
}
