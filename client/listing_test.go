package client_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Mic92/niks3/client"
)

// TestCaseHackSuffix verifies that case hack suffix handling in NAR
// serialization and listing generation matches nix-store behavior.
// On macOS, the ~nix~case~hack~ suffix is stripped. On other platforms,
// it is preserved as-is.
func TestCaseHackSuffix(t *testing.T) {
	t.Parallel()

	// Create temporary directory structure (same input on all platforms)
	tmpDir := t.TempDir()

	// On macOS, t.TempDir() returns /var/folders/... but /var is a symlink
	// to /private/var, and nix-store --dump fails on paths containing symlinks.
	// Resolve the real path to avoid this issue.
	tmpDir, err := filepath.EvalSymlinks(tmpDir)
	if err != nil {
		t.Fatalf("Failed to resolve symlinks in temp dir: %v", err)
	}

	testDir := filepath.Join(tmpDir, "test")

	err = os.Mkdir(testDir, 0o755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Create files
	caseHackFile := filepath.Join(testDir, "README~nix~case~hack~")
	normalFile := filepath.Join(testDir, "normal.txt")

	for _, f := range []string{caseHackFile, normalFile} {
		if err := os.WriteFile(f, []byte("test"), 0o600); err != nil {
			t.Fatalf("Failed to create file %s: %v", f, err)
		}
	}

	// Generate NAR with our implementation
	var ourNAR bytes.Buffer

	_, err = client.DumpPathWithListing(&ourNAR, testDir)
	if err != nil {
		t.Fatalf("DumpPathWithListing failed: %v", err)
	}

	// Generate NAR with nix-store --dump
	cmd := exec.CommandContext(t.Context(), "nix-store", "--dump", testDir)

	nixNAR, err := cmd.Output()
	if err != nil {
		t.Fatalf("nix-store --dump failed: %v", err)
	}

	// Compare the two NARs byte-for-byte
	if !bytes.Equal(ourNAR.Bytes(), nixNAR) {
		t.Errorf("NAR mismatch: our implementation differs from nix-store\nOur size: %d, Nix size: %d",
			ourNAR.Len(), len(nixNAR))
	}

	// Verify listing structure
	listing, err := client.GenerateListingOnly(testDir)
	if err != nil {
		t.Fatalf("GenerateListingOnly failed: %v", err)
	}

	entries := listing.Root.Entries
	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	// Determine expected name based on platform
	expectedName := "README~nix~case~hack~"
	unexpectedName := "README"

	if runtime.GOOS == "darwin" {
		expectedName = "README"
		unexpectedName = "README~nix~case~hack~"
	}

	// Verify case hack handling matches platform
	if _, ok := entries[expectedName]; !ok {
		t.Errorf("Expected '%s' entry", expectedName)
	}

	if _, ok := entries[unexpectedName]; ok {
		t.Errorf("Should not have '%s' entry", unexpectedName)
	}

	// Normal file should be unchanged on all platforms
	if _, ok := entries["normal.txt"]; !ok {
		t.Error("Expected 'normal.txt' entry")
	}
}
