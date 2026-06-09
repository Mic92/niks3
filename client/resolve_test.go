package client_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Mic92/niks3/client"
)

func TestResolveStorePath(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	storeDir := filepath.Join(tmp, "nix", "store")
	storePath := filepath.Join(storeDir, "abc123-hello")

	if err := os.MkdirAll(storePath, 0o755); err != nil {
		t.Fatal(err)
	}

	// Simulate a ./result symlink as produced by nix-build
	link := filepath.Join(tmp, "result")
	if err := os.Symlink(storePath, link); err != nil {
		t.Fatal(err)
	}

	c := client.NewTestClientWithStoreDir(storeDir)

	resolved, err := c.ResolveStorePath(link)
	if err != nil {
		t.Fatalf("ResolveStorePath(%q): %v", link, err)
	}

	if resolved != storePath {
		t.Errorf("expected %q, got %q", storePath, resolved)
	}

	// A path already in the store is returned unchanged
	resolved, err = c.ResolveStorePath(storePath)
	if err != nil {
		t.Fatalf("ResolveStorePath(%q): %v", storePath, err)
	}

	if resolved != storePath {
		t.Errorf("expected %q, got %q", storePath, resolved)
	}
}
