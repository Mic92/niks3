//nolint:paralleltest // Tests select a process-wide disposable Nix store.
package hook_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/hook"
)

func TestQueueRoots(t *testing.T) {
	for _, retain := range []bool{false, true} {
		t.Run(strconv.FormatBool(retain), func(t *testing.T) {
			scratch, store, nix := scratchNixStore(t)
			dependency := nix("store", "add-file", writeTestFile(t, scratch, "dependency"))
			path := nix("eval", "--impure", "--raw", "--expr", `builtins.toFile "parent" (builtins.storePath "`+dependency+`")`)
			initiator := filepath.Join(store, "nix/var/nix/gcroots/initiator")
			if err := os.Symlink(path, initiator); err != nil {
				t.Fatal(err)
			}
			nix("store", "gc")
			if _, err := os.Stat(filepath.Join(store, path)); err != nil {
				t.Fatalf("native root control: %v", err)
			}
			open := hook.OpenQueue
			if retain {
				open = hook.OpenQueueWithRoots
			}
			dbPath := filepath.Join(scratch, "queue.db")
			q, err := open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = q.Close() }()
			ln, _, err := hook.GetListener("upload.sock")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = hook.NewServer(ln, q.Enqueue).Serve(ctx)
			}()
			defer func() { cancel(); <-done }()
			if err := hook.SendPaths("upload.sock", []string{path}); err != nil {
				t.Fatal(err)
			}
			if retain {
				rejected := nix("store", "add-file", writeTestFile(t, scratch, "rejected"))
				missing := "/nix/store/00000000000000000000000000000000-missing"
				if err := hook.SendPaths("upload.sock", []string{path, rejected, missing}); err == nil {
					t.Fatal("failed registration acknowledged")
				}
				nix("store", "gc")
				if _, err := os.Stat(filepath.Join(store, rejected)); !os.IsNotExist(err) {
					t.Fatalf("failed admission retained files: %v", err)
				}
			}
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(initiator); err != nil {
				t.Fatal(err)
			}
			nix("store", "gc")
			q, err = open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			if got := remaining(t, q); !slices.Equal(got, []string{path}) {
				t.Fatalf("queue after restart: %v", got)
			}
			for _, p := range []string{path, dependency} {
				_, err := os.Stat(filepath.Join(store, p))
				if (retain && err != nil) || (!retain && !os.IsNotExist(err)) {
					t.Fatalf("retention=%t, path=%s: %v", retain, p, err)
				}
			}
			if err := q.Remove([]string{path}); err != nil {
				t.Fatal(err)
			}
			nix("store", "gc")
			if _, err := os.Stat(filepath.Join(store, path)); !os.IsNotExist(err) {
				t.Fatalf("terminal removal retained files: %v", err)
			}
		})
	}
}

func TestQueueRootRecovery(t *testing.T) {
	scratch, store, nix := scratchNixStore(t)
	payload := writeTestFile(t, scratch, "missing")
	missing := nix("store", "add-file", payload)
	pending := nix("store", "add-file", writeTestFile(t, scratch, "pending"))
	dbPath := filepath.Join(scratch, "queue.db")
	q, err := hook.OpenQueue(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue([]string{missing, pending}); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	nix("store", "delete", missing)
	if q, err := hook.OpenQueueWithRoots(dbPath); err == nil {
		_ = q.Close()
		t.Fatal("startup accepted a missing pending output")
	}
	nix("store", "gc")
	if _, err := os.Stat(filepath.Join(store, pending)); err != nil {
		t.Fatalf("earlier failure prevented later root recovery: %v", err)
	}
	nix("store", "add-file", payload)
	orphan := nix("store", "add-file", writeTestFile(t, scratch, "orphan"))
	root := func(path string) string {
		return filepath.Join(dbPath+".roots", fmt.Sprintf("%x", sha256.Sum256([]byte(path))))
	}
	for _, path := range []string{missing, orphan} {
		if err := os.Symlink(path, root(path)+".tmp-4242-0"); err != nil {
			t.Fatal(err)
		}
	}
	q, err = hook.OpenQueueWithRoots(dbPath)
	if err != nil {
		t.Fatalf("native temporary links block restart: %v", err)
	}
	if got := remaining(t, q); !slices.Equal(got, []string{missing, pending}) {
		t.Fatalf("recovery changed queue ownership: %v", got)
	}
	for _, path := range []string{missing, orphan} {
		if _, err := os.Lstat(root(path) + ".tmp-4242-0"); !os.IsNotExist(err) {
			t.Fatalf("temporary link remains: %v", err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if q, err := hook.OpenQueue(dbPath); err == nil {
		_ = q.Close()
		t.Fatal("retention silently disabled")
	}
	for _, unknown := range []string{filepath.Join(dbPath+".roots", "unknown"), root(pending) + ".tmp-01-0"} {
		if err := os.Symlink(pending, unknown); err != nil {
			t.Fatal(err)
		}
		if q, err := hook.OpenQueueWithRoots(dbPath); err == nil {
			_ = q.Close()
			t.Fatal("unknown root entry accepted")
		}
		if _, err := os.Lstat(unknown); err != nil {
			t.Fatalf("unknown entry removed: %v", err)
		}
		if err := os.Remove(unknown); err != nil {
			t.Fatal(err)
		}
	}
}

func scratchNixStore(t *testing.T) (string, string, func(...string) string) {
	t.Helper()
	if os.Getenv("NIKS3_TEST_NIX_GC") != "1" {
		t.Skip("set NIKS3_TEST_NIX_GC=1 to test with a disposable Nix store")
	}
	scratch, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(scratch)
	store := filepath.Join(scratch, "store")
	t.Setenv("NIX_REMOTE", "local?root="+store)
	t.Setenv("NIX_STORE_DIR", "/nix/store")

	return scratch, store, func(args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		argv := append([]string{"--extra-experimental-features", "nix-command", "--store", "local?root=" + store}, args...)
		out, err := exec.CommandContext(ctx, "nix", argv...).CombinedOutput()
		if err != nil {
			t.Fatalf("nix %v: %v\n%s", args, err, out)
		}

		return strings.TrimSpace(string(out))
	}
}
