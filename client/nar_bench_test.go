package client_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Mic92/niks3/client"
)

func BenchmarkDumpPathLargeClosure(b *testing.B) {
	b.ReportAllocs()

	root := b.TempDir()

	makeBenchmarkTree(b, root)

	for b.Loop() {
		if _, err := client.DumpPathWithListing(io.Discard, root); err != nil {
			b.Fatalf("DumpPathWithListing failed: %v", err)
		}
	}
}

func makeBenchmarkTree(tb testing.TB, root string) {
	tb.Helper()

	const (
		dirLevels       = 3
		dirsPerLevel    = 4
		filesPerDir     = 6
		fileSize        = 32 * 1024 // 32 KiB of deterministic data
		repeatedContent = "nar-bench-content-"
	)

	content := bytes.Repeat([]byte(repeatedContent), fileSize/len(repeatedContent))

	var createLevel func(base string, level int)

	createLevel = func(base string, level int) {
		if level == dirLevels {
			return
		}

		for dirIndex := range dirsPerLevel {
			dirPath := filepath.Join(base, fmt.Sprintf("dir-%d-%d", level, dirIndex))

			if err := os.Mkdir(dirPath, 0o755); err != nil {
				tb.Fatalf("creating directory %q: %v", dirPath, err)
			}

			for fileIndex := range filesPerDir {
				filePath := filepath.Join(dirPath, fmt.Sprintf("file-%d-%d.dat", level, fileIndex))

				if err := os.WriteFile(filePath, content, 0o600); err != nil {
					tb.Fatalf("creating file %q: %v", filePath, err)
				}
			}

			if level == 0 && dirIndex == 0 {
				linkTarget := filepath.Join(dirPath, "file-0-0.dat")
				linkPath := filepath.Join(dirPath, "symlink-to-file")

				if err := os.Symlink(linkTarget, linkPath); err != nil {
					tb.Fatalf("creating symlink %q -> %q: %v", linkPath, linkTarget, err)
				}
			}

			createLevel(dirPath, level+1)
		}
	}

	createLevel(root, 0)
}
