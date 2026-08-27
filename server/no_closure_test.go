package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Mic92/niks3/client"
	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/oidc"
)

// pushToServerNoClosure pushes exactly the given paths; each becomes its own gcroot.
func pushToServerNoClosure(ctx context.Context, serverURL, authToken string, paths []string, nixEnv []string) error {
	c, err := client.NewClient(ctx, serverURL, authToken)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	c.MaxConcurrentNARUploads = 16
	c.NixEnv = nixEnv
	c.NoClosure = true

	if _, err := c.PushPaths(ctx, paths); err != nil {
		return fmt.Errorf("pushing paths: %w", err)
	}

	return nil
}

// setupGCTestService starts a service with the handlers a push and a GC run need.
func setupGCTestService(t *testing.T) (*server.Service, string) {
	t.Helper()

	testService := createTestServiceWithAuth(t, testAuthToken)
	t.Cleanup(func() { testService.Close() })

	ok(t, testService.InitializeBucket(t.Context()))

	mux := http.NewServeMux()
	registerTestHandlers(mux, testService)
	mux.HandleFunc("DELETE /api/closures", testService.RequireScope(oidc.ScopeWrite, testService.CleanupClosuresOlder))
	mux.HandleFunc("GET /api/gc/status", testService.RequireScope(oidc.ScopeWrite, testService.GCStatusHandler))

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return testService, ts.URL
}

// storePathHash returns the 32-character hash prefix of a store path.
func storePathHash(storePath string) string {
	return strings.Split(filepath.Base(storePath), "-")[0]
}

// addFile adds content to the isolated store. The result has no references:
// Nix only scans for those in build outputs, not in nix-store --add paths.
func addFile(t *testing.T, nixEnv []string, name, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	ok(t, os.WriteFile(path, []byte(content), 0o600))

	return nixStoreAdd(t, nixEnv, path)
}

// buildDependentDerivations builds app and dep, where app's output embeds
// dep's store path so Nix records a real app -> dep reference.
func buildDependentDerivations(ctx context.Context, t *testing.T, nixEnv []string) (string, string) {
	t.Helper()

	nixExpr := filepath.Join(t.TempDir(), "linked.nix")
	nixContent := `
	rec {
		dep = derivation {
			name = "niks3-dep";
			system = builtins.currentSystem;
			builder = "/bin/sh";
			args = [ "-c" "echo 'dependency payload' > $out" ];
		};
		app = derivation {
			name = "niks3-app";
			system = builtins.currentSystem;
			builder = "/bin/sh";
			args = [ "-c" "echo ${dep} > $out" ];
		};
	}
	`

	ok(t, os.WriteFile(nixExpr, []byte(nixContent), 0o600))

	build := func(attr string) string {
		cmd := exec.CommandContext(ctx, "nix-build", nixExpr, "-A", attr, "--no-out-link")
		cmd.Env = nixEnv

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Skipf("Failed to build %s (nix environment not set up): %v\nOutput: %s", attr, err, output)
		}

		lines := strings.Split(strings.TrimSpace(string(output)), "\n")

		return lines[len(lines)-1]
	}

	// Build dep first so app can embed an already-realised path.
	dep := build("dep")
	app := build("app")

	t.Logf("Built app=%s dep=%s", app, dep)

	return app, dep
}

func countRows(ctx context.Context, t *testing.T, service *server.Service, query string, args ...any) int {
	t.Helper()

	var count int

	ok(t, service.Pool.QueryRow(ctx, query, args...).Scan(&count))

	return count
}

// TestNoClosurePushCreatesIndependentGCRoots covers why --no-closure exists:
// build-only deps are unreachable at once unless each path is its own gcroot.
func TestNoClosurePushCreatesIndependentGCRoots(t *testing.T) {
	t.Parallel()

	testService, serverURL := setupGCTestService(t)

	ctx := t.Context()
	nixEnv := setupIsolatedNixStore(t)

	// Neither references the other: an output and a build-time-only dep.
	output := addFile(t, nixEnv, "output.txt", "build output")
	buildDep := addFile(t, nixEnv, "build-dep.txt", "build-time only dependency")

	ok(t, pushToServerNoClosure(ctx, serverURL, testAuthToken, []string{output, buildDep}, nixEnv))

	outputKey := storePathHash(output) + ".narinfo"
	buildDepKey := storePathHash(buildDep) + ".narinfo"

	// Both paths must be roots, not just uploaded objects.
	if got := countRows(ctx, t, testService, "SELECT COUNT(*) FROM closures"); got != 2 {
		t.Errorf("closure count = %d, want 2 (one gcroot per pushed path)", got)
	}

	for _, key := range []string{outputKey, buildDepKey} {
		if got := countRows(ctx, t, testService, "SELECT COUNT(*) FROM closures WHERE key = $1", key); got != 1 {
			t.Errorf("no closure row for %s", key)
		}
	}

	// Cutoff spares the fresh closures; the build dep must survive on its own gcroot.
	c, err := client.NewClient(ctx, serverURL, testAuthToken)
	ok(t, err)

	_, err = c.RunGarbageCollection(ctx, "1h", "1h", true)
	ok(t, err)

	for _, key := range []string{outputKey, buildDepKey} {
		got := countRows(ctx, t, testService,
			"SELECT COUNT(*) FROM objects WHERE key = $1 AND deleted_at IS NULL", key)
		if got != 1 {
			t.Errorf("narinfo %s did not survive GC", key)
		}
	}
}

// TestNoClosurePushKeepsReferencedObjectsReachable shows why --no-closure is
// safe: a dep stays reachable via a dependent's narinfo refs after its own
// gcroot is gone.
func TestNoClosurePushKeepsReferencedObjectsReachable(t *testing.T) {
	t.Parallel()

	testService, serverURL := setupGCTestService(t)

	ctx := t.Context()
	nixEnv := setupIsolatedNixStore(t)

	app, dep := buildDependentDerivations(ctx, t, nixEnv)

	ok(t, pushToServerNoClosure(ctx, serverURL, testAuthToken, []string{app, dep}, nixEnv))

	appKey := storePathHash(app) + ".narinfo"
	depKey := storePathHash(dep) + ".narinfo"

	// Guard the fixture: without a real reference this test proves nothing.
	var refs []string

	ok(t, testService.Pool.QueryRow(ctx, "SELECT refs FROM objects WHERE key = $1", appKey).Scan(&refs))

	if !slices.Contains(refs, depKey) {
		t.Fatalf("app narinfo refs = %v, want it to contain %s", refs, depKey)
	}

	// Drop dep's gcroot: its objects are now reachable only via app's refs.
	_, err := testService.Pool.Exec(ctx, "DELETE FROM closures WHERE key = $1", depKey)
	ok(t, err)

	c, err := client.NewClient(ctx, serverURL, testAuthToken)
	ok(t, err)

	_, err = c.RunGarbageCollection(ctx, "1h", "1h", true)
	ok(t, err)

	got := countRows(ctx, t, testService,
		"SELECT COUNT(*) FROM objects WHERE key = $1 AND deleted_at IS NULL", depKey)
	if got != 1 {
		t.Errorf("dependency narinfo %s was collected despite being referenced by %s", depKey, appKey)
	}
}
