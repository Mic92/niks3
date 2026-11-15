package hook

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
	"time"
)

func (q *Queue) openRoots(dbPath string, retain bool) error {
	rootDir := dbPath + ".roots"
	if !retain {
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

// retain roots each path and returns the retained, the failed, and their errors.
func (q *Queue) retain(paths []string) ([]string, []string, []error) {
	if q.roots == "" {
		return paths, nil, nil
	}
	retained := make([]string, 0, len(paths))
	var failed []string
	var failures []error
	for _, path := range paths {
		if err := q.retainPath(path); err != nil {
			failed = append(failed, path)
			failures = append(failures, err)

			continue
		}
		retained = append(retained, path)
	}

	return retained, failed, failures
}

// rootTarget is the store path that keeps a queue entry uploadable: the
// path itself, or the .drv of a derivation's outputs <drv>^*, which the
// upload reads to find their realisations.
func rootTarget(entry string) string {
	drv, _, found := strings.Cut(entry, "^")
	if found && strings.HasSuffix(drv, ".drv") {
		return drv
	}

	return entry
}

// rootNames are the queue roots an entry for target may have.
func (q *Queue) rootNames(target string) []string {
	names := []string{q.rootPath(target)}
	if strings.HasSuffix(target, ".drv") {
		names = append(names, q.rootPath(target+"^*"))
	}

	return names
}

func (q *Queue) retainPath(entry string) error {
	path := rootTarget(entry)
	if !isRootTarget(path) {
		return fmt.Errorf("retention requires a store path or <drv>^*, not %q", entry)
	}
	root := q.rootPath(entry)
	common := []string{"--option", "substitute", "false", "--", path}
	name, args := "nix-store", slices.Concat([]string{"--realise", "--add-root", root, "--indirect"}, common)
	if strings.HasSuffix(path, ".drv") {
		// nix-store --realise would build it; nix build (Nix >= 2.15, Lix)
		// roots the .drv itself. No jobs or builders: older Nix fails instead.
		name, args = "nix", slices.Concat([]string{
			"--extra-experimental-features", "nix-command", "build",
			"--out-link", root, "--max-jobs", "0", "--builders", "",
		}, common)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.ExtraFiles = []*os.File{q.owner}
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("registering queue root for %q: %w: %s", entry, err, out)
	}
	target, err := os.Readlink(root)
	if err != nil {
		return fmt.Errorf("reading queue root for %q: %w", entry, err)
	}
	if target != path {
		return fmt.Errorf("queue root for %q has unexpected target %q", entry, target)
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
	_, _, failures := q.retain(paths)
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
		if err != nil || (!slices.Contains(q.rootNames(target), root) && !q.isTemporaryRoot(root, target)) {
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
		!strings.ContainsAny(path, "!^")
}

func (q *Queue) isTemporaryRoot(root, target string) bool {
	if !isRootTarget(target) {
		return false
	}
	var suffix string
	var ok bool
	for _, name := range q.rootNames(target) {
		if suffix, ok = strings.CutPrefix(root, name+".tmp-"); ok {
			break
		}
	}
	if !ok {
		return false
	}
	var pid, random int32
	_, err := fmt.Sscanf(suffix, "%d-%d", &pid, &random)

	return err == nil && pid > 0 && random >= 0 && suffix == fmt.Sprintf("%d-%d", pid, random)
}
