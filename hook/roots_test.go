//nolint:paralleltest // The tests select a process-wide scratch Nix store.
package hook_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/hook"
)

// failingDrv fails if built.
const failingDrv = `derivation {
  name = "niks3-hook-unbuilt";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "exit 1" ];
}`

// A .drv output is rooted without building it.
func TestQueueRootsDerivationWithoutBuild(t *testing.T) {
	scratch, store, nix := scratchNixStore(t)
	drv := instantiate(t, scratch)

	q, dbPath := openRootedQueue(t, scratch)
	if err := q.Enqueue([]string{drv}); err != nil {
		t.Fatalf("Enqueue %s: %v", drv, err)
	}

	if got := remaining(t, q); !slices.Equal(got, []string{drv}) {
		t.Fatalf("queue: got %v, want [%s]", got, drv)
	}

	if target, err := os.Readlink(queueRoot(dbPath, drv)); err != nil || target != drv {
		t.Fatalf("queue root: target %q, error %v", target, err)
	}

	for output := range strings.FieldsSeq(runNix(t, "nix-store", "--query", "--outputs", drv)) {
		if _, err := os.Lstat(filepath.Join(store, output)); !os.IsNotExist(err) {
			t.Fatalf("output %s exists, so the derivation was built: %v", output, err)
		}
	}

	nix("store", "gc")

	if _, err := os.Stat(filepath.Join(store, drv)); err != nil {
		t.Fatalf("queued derivation collected: %v", err)
	}

	if err := q.Remove([]string{drv}); err != nil {
		t.Fatal(err)
	}

	nix("store", "gc")

	if _, err := os.Stat(filepath.Join(store, drv)); !os.IsNotExist(err) {
		t.Fatalf("uploaded derivation still retained: %v", err)
	}
}

// A bad path does not drop the rest of its batch.
func TestQueueRootsPartialBatch(t *testing.T) {
	scratch, store, nix := scratchNixStore(t)
	valid := nix("store", "add-file", writeTestFile(t, scratch, "valid"))
	drv := instantiate(t, scratch)
	missing := "/nix/store/00000000000000000000000000000000-missing"
	selector := valid + "^out"

	q, dbPath := openRootedQueue(t, scratch)

	err := q.Enqueue([]string{missing, valid, selector, drv})
	if err == nil {
		t.Fatal("Enqueue reported success for paths it did not queue")
	}

	for _, rejected := range []string{missing, selector} {
		if !strings.Contains(err.Error(), rejected) {
			t.Errorf("error does not name %s: %v", rejected, err)
		}

		if _, err := os.Lstat(queueRoot(dbPath, rejected)); !os.IsNotExist(err) {
			t.Errorf("root left for %s: %v", rejected, err)
		}
	}

	if got := remaining(t, q); !slices.Equal(got, []string{valid, drv}) {
		t.Fatalf("queue: got %v, want [%s %s]", got, valid, drv)
	}

	nix("store", "gc")

	for _, path := range []string{valid, drv} {
		if _, err := os.Stat(filepath.Join(store, path)); err != nil {
			t.Fatalf("queued path %s collected: %v", path, err)
		}
	}
}

// Reopening a queue restores a .drv root.
func TestQueueRootsReconcileDerivation(t *testing.T) {
	scratch, _, _ := scratchNixStore(t)
	drv := instantiate(t, scratch)

	q, dbPath := openRootedQueue(t, scratch)
	if err := q.Enqueue([]string{drv}); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(queueRoot(dbPath, drv)); err != nil {
		t.Fatal(err)
	}

	q, err := hook.OpenQueueWithRoots(dbPath)
	if err != nil {
		t.Fatalf("reopening a queue with a derivation: %v", err)
	}

	t.Cleanup(func() { _ = q.Close() })

	if target, err := os.Readlink(queueRoot(dbPath, drv)); err != nil || target != drv {
		t.Fatalf("restored root: target %q, error %v", target, err)
	}
}

// The outputs of a derivation, <drv>^<outputs>, are queued as <drv>^* and
// keep their .drv, unbuilt, until uploaded; a root left over by a crash
// between removal and release is cleaned up on reopening.
func TestQueueRootsDerivationOutputs(t *testing.T) {
	scratch, store, nix := scratchNixStore(t)
	drv := instantiate(t, scratch)
	entry := drv + "^*"

	q, dbPath := openRootedQueue(t, scratch)
	if err := q.Enqueue([]string{drv + "^out"}); err != nil {
		t.Fatalf("Enqueue %s^out: %v", drv, err)
	}

	if got := remaining(t, q); !slices.Equal(got, []string{entry}) {
		t.Fatalf("queue: got %v, want [%s]", got, entry)
	}

	if target, err := os.Readlink(queueRoot(dbPath, entry)); err != nil || target != drv {
		t.Fatalf("queue root: target %q, error %v", target, err)
	}

	for output := range strings.FieldsSeq(runNix(t, "nix-store", "--query", "--outputs", drv)) {
		if _, err := os.Lstat(filepath.Join(store, output)); !os.IsNotExist(err) {
			t.Fatalf("output %s exists, so the derivation was built: %v", output, err)
		}
	}

	nix("store", "gc")

	if _, err := os.Stat(filepath.Join(store, drv)); err != nil {
		t.Fatalf("queued derivation collected: %v", err)
	}

	// Removed, but its root survives as after a crash before release.
	if err := q.Remove([]string{entry}); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.Symlink(drv, queueRoot(dbPath, entry)); err != nil {
		t.Fatal(err)
	}

	q, err := hook.OpenQueueWithRoots(dbPath)
	if err != nil {
		t.Fatalf("reopening with a stale derivation root: %v", err)
	}

	t.Cleanup(func() { _ = q.Close() })

	if _, err := os.Lstat(queueRoot(dbPath, entry)); !os.IsNotExist(err) {
		t.Fatalf("stale root kept: %v", err)
	}
}

func openRootedQueue(t *testing.T, scratch string) (*hook.Queue, string) {
	t.Helper()

	dbPath := filepath.Join(scratch, "queue.db")

	q, err := hook.OpenQueueWithRoots(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = q.Close() })

	return q, dbPath
}

func queueRoot(dbPath, path string) string {
	return filepath.Join(dbPath+".roots", fmt.Sprintf("%x", sha256.Sum256([]byte(path))))
}

func instantiate(t *testing.T, scratch string) string {
	t.Helper()

	expr := filepath.Join(scratch, "unbuilt.nix")
	if err := os.WriteFile(expr, []byte(failingDrv), 0o600); err != nil {
		t.Fatal(err)
	}

	return runNix(t, "nix-instantiate", expr)
}

// scratchNixStore points Nix at a throwaway store; no daemon needed.
func scratchNixStore(t *testing.T) (string, string, func(...string) string) {
	t.Helper()

	if _, err := exec.LookPath("nix"); err != nil {
		t.Skip("nix not available")
	}

	scratch, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	store := filepath.Join(scratch, "store")
	t.Setenv("NIX_REMOTE", "local?root="+store)
	t.Setenv("NIX_STORE_DIR", "/nix/store")
	t.Setenv("NIX_CONFIG", "substituters =\nexperimental-features = nix-command")

	return scratch, store, func(args ...string) string {
		t.Helper()

		return runNix(t, "nix", append([]string{"--extra-experimental-features", "nix-command"}, args...)...)
	}
}

func runNix(t *testing.T, name string, args ...string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)

	out, err := cmd.Output()
	if err != nil {
		var stderr []byte
		if exitErr := (*exec.ExitError)(nil); errors.As(err, &exitErr) {
			stderr = exitErr.Stderr
		}

		t.Fatalf("%s %v: %v\n%s", name, args, err, stderr)
	}

	return strings.TrimSpace(string(out))
}
