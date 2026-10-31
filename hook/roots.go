package hook

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func (q *Queue) openRoots(dbPath string, retain bool) error {
	rootDir := dbPath + ".roots"
	if !retain {
		if _, err := os.Lstat(rootDir); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("queue roots exist or cannot be inspected; reopen with --retain-paths: %s", rootDir)
		}

		return nil
	}
	if err := os.MkdirAll(rootDir, 0o700); err != nil {
		return fmt.Errorf("creating queue roots directory: %w", err)
	}
	var err error
	q.roots, err = filepath.EvalSymlinks(rootDir)
	if err != nil {
		return fmt.Errorf("resolving queue roots directory: %w", err)
	}

	return q.reconcileRoots()
}

func (q *Queue) rootPath(path string) string {
	return filepath.Join(q.roots, fmt.Sprintf("%x", sha256.Sum256([]byte(path))))
}

func (q *Queue) retain(paths []string) error {
	if q.roots == "" {
		return nil
	}
	for _, path := range paths {
		if !isRootTarget(path) {
			return fmt.Errorf("retention requires a store output path, not a derivation or selector: %q", path)
		}
	}
	for _, path := range paths {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		//nolint:gosec // Nix validates output paths passed after "--".
		cmd := exec.CommandContext(ctx, "nix-store", "--realise", "--add-root", q.rootPath(path),
			"--indirect", "--option", "substitute", "false", "--", path)
		cmd.ExtraFiles = []*os.File{q.owner}
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			return fmt.Errorf("registering queue root for %q: %w: %s", path, err, out)
		}
		target, err := os.Readlink(q.rootPath(path))
		if err != nil {
			return fmt.Errorf("reading queue root for %q: %w", path, err)
		}
		if target != path {
			return fmt.Errorf("queue root for %q has unexpected target %q", path, target)
		}
	}

	return nil
}

func (q *Queue) release(paths []string) error {
	if q.roots == "" {
		return nil
	}
	for _, path := range paths {
		if err := os.Remove(q.rootPath(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("removing queue root for %q: %w", path, err)
		}
	}

	return nil
}

func (q *Queue) reconcileRoots() error {
	paths, err := q.FetchBatch(-1)
	if err != nil {
		return err
	}
	var failures []error
	for _, path := range paths {
		if err := q.retain([]string{path}); err != nil {
			failures = append(failures, err)
		}
	}
	if err := errors.Join(failures...); err != nil {
		return fmt.Errorf("restoring queued roots: %w", err)
	}
	pending := make(map[string]bool, len(paths))
	for _, path := range paths {
		pending[q.rootPath(path)] = true
	}
	entries, err := os.ReadDir(q.roots)
	if err != nil {
		return fmt.Errorf("reading queue roots: %w", err)
	}
	for _, entry := range entries {
		root := filepath.Join(q.roots, entry.Name())
		if pending[root] {
			continue
		}
		target, err := os.Readlink(root)
		if err != nil || (q.rootPath(target) != root && !q.isTemporaryRoot(root, target)) {
			return fmt.Errorf("unexpected entry in queue roots directory: %s", root)
		}
		if err := os.Remove(root); err != nil {
			return fmt.Errorf("removing reconciled queue root %q: %w", root, err)
		}
	}

	return nil
}

func (q *Queue) releaseUnqueued(paths []string) error {
	if q.roots == "" {
		return nil
	}
	var failures []error
	for _, path := range paths {
		var queued bool
		err := q.db.QueryRowContext(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM upload_queue WHERE store_path = ?)", path).Scan(&queued)
		if err != nil {
			failures = append(failures, fmt.Errorf("checking root ownership for %q: %w", path, err))

			continue
		}
		if !queued {
			if err := q.release([]string{path}); err != nil {
				failures = append(failures, err)
			}
		}
	}

	return errors.Join(failures...)
}

func isRootTarget(path string) bool {
	storeDir := os.Getenv("NIX_STORE_DIR")
	if storeDir == "" {
		storeDir = "/nix/store"
	}

	return filepath.IsAbs(path) && filepath.Clean(path) == path && filepath.Dir(path) == storeDir &&
		!strings.ContainsAny(path, "!^") && !strings.HasSuffix(path, ".drv")
}

func (q *Queue) isTemporaryRoot(root, target string) bool {
	suffix, ok := strings.CutPrefix(root, q.rootPath(target)+".tmp-")
	if !ok || !isRootTarget(target) {
		return false
	}
	var pid, random int32
	_, err := fmt.Sscanf(suffix, "%d-%d", &pid, &random)

	return err == nil && pid > 0 && random >= 0 && suffix == fmt.Sprintf("%d-%d", pid, random)
}
