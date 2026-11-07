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
//
// Only platforms that strip the suffix (macOS) can serialize such a path at
// all: elsewhere the name would reach the NAR intact and no nix client would
// restore it, so the walk refuses instead. TestCaseHackPathIsRefused covers
// that half.
func TestCaseHackSuffix(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != "darwin" {
		t.Skip("case hack suffixes are only stripped where nix enables use-case-hack by default")
	}

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

	if _, ok := entries["README"]; !ok {
		t.Error("Expected 'README' entry")
	}

	if _, ok := entries["README~nix~case~hack~"]; ok {
		t.Error("Should not have 'README~nix~case~hack~' entry")
	}

	// Normal file should be unchanged on all platforms
	if _, ok := entries["normal.txt"]; !ok {
		t.Error("Expected 'normal.txt' entry")
	}
}
