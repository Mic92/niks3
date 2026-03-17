package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// storeDir is where Nix store paths live. Relocatable stores exist but are
// vanishingly rare in CI, and `niks3 push` assumes /nix/store too.
const storeDir = "/nix/store"

// storescanSnapshot lists current store paths and writes them one-per-line to
// the given file. Called at setup when the runner user is not in trusted-users
// and cannot set post-build-hook.
func storescanSnapshot(path string) error {
	paths, err := listStorePaths()
	if err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating snapshot file: %w", err)
	}

	defer func() { _ = f.Close() }()

	w := bufio.NewWriter(f)
	for _, p := range paths {
		if _, err := fmt.Fprintln(w, p); err != nil {
			return fmt.Errorf("writing snapshot: %w", err)
		}
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("flushing snapshot: %w", err)
	}

	slog.Info("Store snapshot saved", "paths", len(paths), "file", path)

	return nil
}

// storescanDiff reads the snapshot, lists current store paths, computes the
// set difference, and pushes the new paths. Called at post step.
func storescanDiff(snapshotPath, serverURL, audience string, debug bool) error {
	before, err := readSnapshot(snapshotPath)
	if err != nil {
		return err
	}

	after, err := listStorePaths()
	if err != nil {
		return err
	}

	var added []string

	for _, p := range after {
		if _, seen := before[p]; !seen {
			added = append(added, p)
		}
	}

	if len(added) == 0 {
		ghaNoticef("niks3: no new store paths to push")

		return nil
	}

	slog.Info("Pushing new store paths", "count", len(added))

	return ghaGroup(fmt.Sprintf("niks3: pushing %d paths", len(added)), func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()

		if err := ciPushWithOIDC(ctx, serverURL, audience, added, debug); err != nil {
			return err
		}

		ghaNoticef("niks3: pushed %d paths", len(added))

		return nil
	})
}

// listStorePaths returns absolute store paths, filtering out non-path entries
// like .links/ and the few .lock files that can appear at the top level.
func listStorePaths() ([]string, error) {
	entries, err := os.ReadDir(storeDir)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", storeDir, err)
	}

	paths := make([]string, 0, len(entries))

	for _, e := range entries {
		name := e.Name()
		// Store paths are <32-char-base32-hash>-<name>. Anything starting
		// with a dot (.links, .modules) or lacking a dash separator is noise.
		if strings.HasPrefix(name, ".") {
			continue
		}

		if len(name) < 33 || name[32] != '-' {
			continue
		}

		paths = append(paths, filepath.Join(storeDir, name))
	}

	sort.Strings(paths)

	return paths, nil
}

func readSnapshot(path string) (map[string]struct{}, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening snapshot: %w", err)
	}

	defer func() { _ = f.Close() }()

	m := make(map[string]struct{})

	s := bufio.NewScanner(f)
	for s.Scan() {
		m[s.Text()] = struct{}{}
	}

	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("reading snapshot: %w", err)
	}

	return m, nil
}
