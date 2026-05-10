package client_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Mic92/niks3/client"
)

// makeMixedTree builds a tree exercising every NAR node kind plus the
// prefetch boundary (small files read ahead, large files streamed).
func makeMixedTree(t *testing.T, root string) {
	t.Helper()

	mkdir := func(p string) {
		t.Helper()

		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", p, err)
		}
	}

	write := func(p string, size int, mode os.FileMode) {
		t.Helper()

		buf := make([]byte, size)
		if _, err := rand.Read(buf); err != nil {
			t.Fatalf("rand: %v", err)
		}

		if err := os.WriteFile(p, buf, mode); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}

	// Empty file, tiny file, file at the prefetch boundary, file above it
	// (streaming path), and an executable.
	write(filepath.Join(root, "empty"), 0, 0o644)
	write(filepath.Join(root, "tiny"), 7, 0o644) // exercises padding
	write(filepath.Join(root, "boundary"), 256*1024, 0o644)
	write(filepath.Join(root, "large"), 256*1024+1, 0o644)
	write(filepath.Join(root, "huge"), 1024*1024+13, 0o644)
	write(filepath.Join(root, "exec"), 100, 0o755)

	mkdir(filepath.Join(root, "empty-dir"))

	// Nested directories with many small files to exercise the prefetch
	// queue ordering.
	for i := range 5 {
		dir := filepath.Join(root, fmt.Sprintf("dir-%d", i))
		mkdir(dir)

		for j := range 20 {
			write(filepath.Join(dir, fmt.Sprintf("f-%02d", j)), 100+i*10+j, 0o644)
		}

		sub := filepath.Join(dir, "sub")
		mkdir(sub)
		write(filepath.Join(sub, "deep"), 50, 0o644)

		if err := os.Symlink("../f-00", filepath.Join(sub, "link")); err != nil {
			t.Fatalf("symlink: %v", err)
		}
	}

	if err := os.Symlink("/nonexistent/target", filepath.Join(root, "abs-link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
}

// TestDumpPathMatchesNix compares our NAR serialization byte-for-byte against
// nix-store --dump on a tree that hits the prefetched and streaming paths.
func TestDumpPathMatchesNix(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("nix-store"); err != nil {
		t.Skip("nix-store not available")
	}

	tmp := t.TempDir()

	tmp, err := filepath.EvalSymlinks(tmp)
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}

	root := filepath.Join(tmp, "root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	makeMixedTree(t, root)

	var ours bytes.Buffer

	listing, err := client.DumpPathWithListing(&ours, root)
	if err != nil {
		t.Fatalf("DumpPathWithListing: %v", err)
	}

	theirs, err := exec.CommandContext(t.Context(), "nix-store", "--dump", root).Output()
	if err != nil {
		t.Fatalf("nix-store --dump: %v", err)
	}

	if !bytes.Equal(ours.Bytes(), theirs) {
		t.Fatalf("NAR mismatch: ours=%d bytes, nix=%d bytes", ours.Len(), len(theirs))
	}

	// Spot-check listing offsets: re-reading the NAR at narOffset must yield
	// the original file contents.
	checkEntry(t, ours.Bytes(), root, "", listing.Root)
}

func checkEntry(t *testing.T, nar []byte, fsRoot, rel string, e client.NarListingEntry) {
	t.Helper()

	switch e.Type {
	case "regular":
		if e.NarOffset == nil || e.Size == nil {
			t.Fatalf("%s: missing offset/size", rel)
		}

		off, size := *e.NarOffset, *e.Size
		if off+size > uint64(len(nar)) {
			t.Fatalf("%s: offset %d + size %d exceeds NAR length %d", rel, off, size, len(nar))
		}

		want, err := os.ReadFile(filepath.Join(fsRoot, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}

		if !bytes.Equal(nar[off:off+size], want) {
			t.Fatalf("%s: NAR contents at offset %d do not match file", rel, off)
		}
	case "directory":
		for name, child := range e.Entries {
			checkEntry(t, nar, fsRoot, filepath.Join(rel, name), child)
		}
	}
}

// TestDumpPathSingleFile covers the root-is-a-regular-file case, which has a
// distinct walk entry point.
func TestDumpPathSingleFile(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("nix-store"); err != nil {
		t.Skip("nix-store not available")
	}

	tmp := t.TempDir()

	tmp, err := filepath.EvalSymlinks(tmp)
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}

	f := filepath.Join(tmp, "single")
	if err := os.WriteFile(f, bytes.Repeat([]byte("x"), 12345), 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	var ours bytes.Buffer
	if _, err := client.DumpPathWithListing(&ours, f); err != nil {
		t.Fatalf("DumpPathWithListing: %v", err)
	}

	theirs, err := exec.CommandContext(t.Context(), "nix-store", "--dump", f).Output()
	if err != nil {
		t.Fatalf("nix-store --dump: %v", err)
	}

	if !bytes.Equal(ours.Bytes(), theirs) {
		t.Fatalf("NAR mismatch: ours=%d bytes, nix=%d bytes", ours.Len(), len(theirs))
	}
}

// failAfterWriter fails the Nth Write call. Used to verify the prefetch
// goroutines shut down cleanly on a writer error mid-stream.
type failAfterWriter struct {
	n int
}

var errSimulated = errors.New("simulated write failure")

func (w *failAfterWriter) Write(p []byte) (int, error) {
	w.n--
	if w.n < 0 {
		return 0, errSimulated
	}

	return len(p), nil
}

// TestDumpPathWriterError verifies DumpPathWithListing returns promptly and
// does not leak goroutines when the destination writer fails.
func TestDumpPathWriterError(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	makeMixedTree(t, tmp)

	for _, n := range []int{0, 1, 5, 50, 200} {
		_, err := client.DumpPathWithListing(&failAfterWriter{n: n}, tmp)
		if err == nil {
			t.Fatalf("n=%d: expected error", n)
		}

		if !errors.Is(err, errSimulated) {
			t.Fatalf("n=%d: expected wrapped errSimulated, got %v", n, err)
		}
	}
}
