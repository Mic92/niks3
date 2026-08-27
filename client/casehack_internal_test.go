package client

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Not parallel: flips the package-level useCaseHack.
func TestDumpPathCaseHackMatchesNix(t *testing.T) { //nolint:paralleltest
	if _, err := exec.LookPath("nix-store"); err != nil {
		t.Skip("nix-store not available")
	}

	prev := useCaseHack
	useCaseHack = true

	defer func() { useCaseHack = prev }()

	root := filepath.Join(t.TempDir(), "root")
	if err := os.MkdirAll(filepath.Join(root, "sub~nix~case~hack~1"), 0o755); err != nil {
		t.Fatal(err)
	}

	// On-disk order: "Foo~nix~case~hack~1" < "foo" but also "bar" < "Bar~nix~case~hack~2"
	// once stripped, so sorting by on-disk name would be wrong for one of them.
	for name, content := range map[string]string{
		"foo":                   "lower",
		"Foo~nix~case~hack~1":   "upper",
		"bar~nix~case~hack~2":   "b-lower",
		"Bar":                   "b-upper",
		"sub~nix~case~hack~1/x": "x",
		"plain":                 "plain",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	var ours bytes.Buffer
	if _, err := DumpPathWithListing(&ours, root); err != nil {
		t.Fatalf("DumpPathWithListing: %v", err)
	}

	cmd := exec.CommandContext(t.Context(), "nix-store", "--dump", root, "--option", "use-case-hack", "true")
	cmd.Stderr = os.Stderr

	theirs, err := cmd.Output()
	if err != nil {
		t.Fatalf("nix-store --dump: %v", err)
	}

	if !bytes.Equal(ours.Bytes(), theirs) {
		t.Fatalf("NAR mismatch: ours=%d bytes, nix=%d bytes", ours.Len(), len(theirs))
	}

	listing, err := GenerateListingOnly(root)
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"foo", "Foo", "bar", "Bar", "sub", "plain"} {
		if _, ok := listing.Root.Entries[want]; !ok {
			t.Errorf("listing lacks %q: %v", want, listing.Root.Entries)
		}
	}
}
