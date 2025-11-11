package client

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// GenerateListingOnly creates a directory listing by walking the filesystem
// without serializing the NAR. This is much faster for deduplicated NARs
// where we only need the listing structure.
func GenerateListingOnly(path string) (*NarListing, error) {
	entry, err := generateListingEntry(path)
	if err != nil {
		return nil, err
	}

	return &NarListing{Version: 1, Root: entry}, nil
}

func generateListingEntry(path string) (NarListingEntry, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return NarListingEntry{}, fmt.Errorf("stat %s: %w", path, err)
	}

	mode := info.Mode()
	switch {
	case mode.IsRegular():
		return generateRegularFileListing(path, info)
	case mode.IsDir():
		return generateDirectoryListing(path)
	case mode&os.ModeSymlink != 0:
		return generateSymlinkListing(path)
	default:
		return NarListingEntry{}, fmt.Errorf("unsupported file type for %s: %v", path, mode)
	}
}

func generateRegularFileListing(_ string, info os.FileInfo) (NarListingEntry, error) {
	fileSize := info.Size()
	if fileSize < 0 {
		return NarListingEntry{}, fmt.Errorf("invalid file size: %d", fileSize)
	}

	size := uint64(fileSize)
	entry := NarListingEntry{
		Type: "regular",
		Size: &size,
	}

	// Check if executable
	if info.Mode()&0o111 != 0 {
		executable := true
		entry.Executable = &executable
	}

	return entry, nil
}

func generateDirectoryListing(path string) (NarListingEntry, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return NarListingEntry{}, fmt.Errorf("reading directory %s: %w", path, err)
	}

	// Sort entries by name (same as NAR format)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	entryMap := make(map[string]NarListingEntry)

	for _, entry := range entries {
		name := entry.Name()
		// Strip case hack suffix on macOS (must match NAR serialization)
		narName := stripCaseHackSuffix(name)

		entryPath := filepath.Join(path, name)

		listingEntry, err := generateListingEntry(entryPath)
		if err != nil {
			return NarListingEntry{}, err
		}

		entryMap[narName] = listingEntry
	}

	return NarListingEntry{
		Type:    "directory",
		Entries: entryMap,
	}, nil
}

func generateSymlinkListing(path string) (NarListingEntry, error) {
	target, err := os.Readlink(path)
	if err != nil {
		return NarListingEntry{}, fmt.Errorf("reading symlink %s: %w", path, err)
	}

	return NarListingEntry{
		Type:   "symlink",
		Target: &target,
	}, nil
}
