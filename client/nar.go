package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

const (
	narVersionMagic = "nix-archive-1"
	caseHackSuffix  = "~nix~case~hack~"
)

//nolint:gochecknoglobals // useCaseHack is platform-specific runtime constant
var useCaseHack = runtime.GOOS == "darwin"

// stripCaseHackSuffix removes the case hack suffix from filenames on macOS.
func stripCaseHackSuffix(name string) string {
	if !useCaseHack {
		return name
	}

	// Find position of case hack suffix
	if pos := strings.Index(name, caseHackSuffix); pos != -1 {
		return name[:pos]
	}

	return name
}

// writeString writes a length-prefixed string to the NAR with padding.
func writeString(w io.Writer, s string) error {
	// Write length as little-endian u64
	if err := binary.Write(w, binary.LittleEndian, uint64(len(s))); err != nil {
		return fmt.Errorf("writing string length: %w", err)
	}

	// Write string content
	if _, err := io.WriteString(w, s); err != nil {
		return fmt.Errorf("writing string content: %w", err)
	}

	// Pad to 8-byte boundary
	padding := (8 - (len(s) % 8)) % 8
	if padding > 0 {
		if _, err := w.Write(make([]byte, padding)); err != nil {
			return fmt.Errorf("writing padding: %w", err)
		}
	}

	return nil
}

// DumpPath serializes a filesystem path to NAR format.
func DumpPath(w io.Writer, path string) error {
	if err := writeString(w, narVersionMagic); err != nil {
		return err
	}

	if err := writeString(w, "("); err != nil {
		return err
	}

	if err := dumpPathInner(w, path); err != nil {
		return err
	}

	if err := writeString(w, ")"); err != nil {
		return err
	}

	return nil
}

//nolint:gocyclo // NAR format requires this complexity
func dumpPathInner(w io.Writer, path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}

	if err := writeString(w, "type"); err != nil {
		return err
	}

	switch mode := info.Mode(); {
	case mode.IsRegular():
		if err := writeString(w, "regular"); err != nil {
			return err
		}

		// Check if executable (Unix permissions)
		if mode&0o111 != 0 {
			if err := writeString(w, "executable"); err != nil {
				return err
			}

			if err := writeString(w, ""); err != nil {
				return err
			}
		}

		if err := writeString(w, "contents"); err != nil {
			return err
		}

		// Write file size
		//nolint:gosec // File size from os.FileInfo is safe to convert
		fileSize := uint64(info.Size())
		if err := binary.Write(w, binary.LittleEndian, fileSize); err != nil {
			return fmt.Errorf("writing file size: %w", err)
		}

		// Stream file contents
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("opening file %s: %w", path, err)
		}

		defer func() {
			if err := f.Close(); err != nil {
				slog.Error("Failed to close file", "path", path, "error", err)
			}
		}()

		n, err := io.Copy(w, f)
		if err != nil {
			return fmt.Errorf("copying file %s: %w", path, err)
		}
		//nolint:gosec // n from io.Copy is safe to convert
		if uint64(n) != fileSize {
			return fmt.Errorf("file size mismatch for %s: expected %d, copied %d", path, fileSize, n)
		}

		// Pad to 8-byte boundary
		padding := (8 - (fileSize % 8)) % 8
		if padding > 0 {
			if _, err := w.Write(make([]byte, padding)); err != nil {
				return fmt.Errorf("writing padding: %w", err)
			}
		}

	case mode.IsDir():
		if err := writeString(w, "directory"); err != nil {
			return err
		}

		// Read directory entries
		entries, err := os.ReadDir(path)
		if err != nil {
			return fmt.Errorf("reading directory %s: %w", path, err)
		}

		// Sort entries by name (NAR requirement)
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}

		sort.Strings(names)

		for _, name := range names {
			// Strip case hack suffix for NAR serialization
			narName := stripCaseHackSuffix(name)

			if err := writeString(w, "entry"); err != nil {
				return err
			}

			if err := writeString(w, "("); err != nil {
				return err
			}

			if err := writeString(w, "name"); err != nil {
				return err
			}

			if err := writeString(w, narName); err != nil {
				return err
			}

			if err := writeString(w, "node"); err != nil {
				return err
			}

			if err := writeString(w, "("); err != nil {
				return err
			}

			if err := dumpPathInner(w, filepath.Join(path, name)); err != nil {
				return err
			}

			if err := writeString(w, ")"); err != nil {
				return err
			}

			if err := writeString(w, ")"); err != nil {
				return err
			}
		}

	case mode&os.ModeSymlink != 0:
		if err := writeString(w, "symlink"); err != nil {
			return err
		}

		if err := writeString(w, "target"); err != nil {
			return err
		}

		target, err := os.Readlink(path)
		if err != nil {
			return fmt.Errorf("reading symlink %s: %w", path, err)
		}

		if err := writeString(w, target); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unsupported file type for %s: %v", path, mode)
	}

	return nil
}
