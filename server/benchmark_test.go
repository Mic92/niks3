package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
)

// benchmarkClosures lists the flake attributes (see nix/benchmark) used as
// upload workloads: many small/medium NARs vs. one large NAR.
var benchmarkClosures = []struct { //nolint:gochecknoglobals // benchmark table
	name string
	attr string
}{
	{name: "PythonClosure", attr: ".#benchmark-closure"},
	{name: "DiskImage", attr: ".#benchmark-disk-image"},
}

// BenchmarkUploadClosure measures uploading pre-built closures to S3 via the
// niks3 server. Building the closure with nix is not timed.
func BenchmarkUploadClosure(b *testing.B) {
	ctx := context.Background()

	// Find the git repository root
	gitRoot, err := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		b.Fatalf("Failed to find git repository root: %v", err)
	}

	projectRoot := strings.TrimSpace(string(gitRoot))

	for _, bc := range benchmarkClosures {
		b.Run(bc.name, func(b *testing.B) {
			benchmarkUploadClosure(ctx, b, projectRoot, bc.attr)
		})
	}
}

func benchmarkUploadClosure(ctx context.Context, b *testing.B, projectRoot, flakeAttr string) {
	b.Helper()

	b.Logf("Building %s (this may take a while on first run)...", flakeAttr)

	cmd := exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command flakes", "build", flakeAttr, "--print-out-paths", "--no-link")
	cmd.Dir = projectRoot

	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			b.Fatalf("Failed to build %s: %v\nStderr: %s", flakeAttr, err, exitErr.Stderr)
		}

		b.Fatalf("Failed to build %s: %v", flakeAttr, err)
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

	// Run the benchmark
	for b.Loop() {
		// Start fresh services for each iteration (don't count setup time)
		b.StopTimer()
		testService := createTestServiceWithAuth(b, testAuthToken)

		// Initialize the bucket with nix-cache-info
		err := testService.InitializeBucket(ctx)
		if err != nil {
			b.Fatalf("Failed to initialize bucket: %v", err)
		}

		mux := http.NewServeMux()
		registerTestHandlers(mux, testService)

		ts := httptest.NewServer(mux)

		b.StartTimer()

		// Upload the closure to S3 (use nil for nixEnv since we're using system Nix store)
		err = pushToServer(ctx, ts.URL, testAuthToken, []string{closurePath}, nil)
		if err != nil {
			b.Fatalf("Failed to push closure: %v", err)
		}

		// Clean up (don't count cleanup time)
		b.StopTimer()
		ts.Close()
		testService.Close()
		b.StartTimer()
	}
}
