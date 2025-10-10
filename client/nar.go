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

//nolint:gochecknoglobals // pre-encoded constants avoid recomputing framing bytes
var (
	zeroPad = [8]byte{}

	narVersionMagicEncoded = encodeStaticString(narVersionMagic)
	openParenEncoded       = encodeStaticString("(")
	closeParenEncoded      = encodeStaticString(")")
	typeEncoded            = encodeStaticString("type")
	regularEncoded         = encodeStaticString("regular")
	executableEncoded      = encodeStaticString("executable")
	emptyEncoded           = encodeStaticString("")
	contentsEncoded        = encodeStaticString("contents")
	directoryEncoded       = encodeStaticString("directory")
	entryEncoded           = encodeStaticString("entry")
	nameEncoded            = encodeStaticString("name")
	nodeEncoded            = encodeStaticString("node")
	symlinkEncoded         = encodeStaticString("symlink")
	targetEncoded          = encodeStaticString("target")
)

// stripCaseHackSuffix removes the case hack suffix from filenames on macOS.
func stripCaseHackSuffix(name string) string {
	if !useCaseHack {
		return name
	}

	// Only strip if case hack suffix is at the end
	if strings.HasSuffix(name, caseHackSuffix) {
		return name[:len(name)-len(caseHackSuffix)]
	}

	return name
}

// writeString writes a length-prefixed string to the NAR with padding.
func writeString(w io.Writer, s string) error {
	if err := writeUint64(w, uint64(len(s))); err != nil {
		return fmt.Errorf("writing string length: %w", err)
	}

	if _, err := io.WriteString(w, s); err != nil {
		return fmt.Errorf("writing string content: %w", err)
	}

	return writePadding(w, len(s))
}

func writePadding(w io.Writer, n int) error {
	padding := (8 - (n % 8)) % 8
	if padding == 0 {
		return nil
	}

	if _, err := w.Write(zeroPad[:padding]); err != nil {
		return fmt.Errorf("writing padding: %w", err)
	}

	return nil
}

func writeSizePadding(w io.Writer, size uint64) error {
	padding := size % 8
	if padding == 0 {
		return nil
	}

	toWrite := 8 - padding

	if _, err := w.Write(zeroPad[:toWrite]); err != nil {
		return fmt.Errorf("writing padding: %w", err)
	}

	return nil
}

func writeUint64(w io.Writer, v uint64) error {
	var buf [8]byte

	binary.LittleEndian.PutUint64(buf[:], v)

	if _, err := w.Write(buf[:]); err != nil {
		return fmt.Errorf("writing uint64: %w", err)
	}

	return nil
}

func writeStatic(w io.Writer, data []byte) error {
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("writing static string: %w", err)
	}

	return nil
}

func encodeStaticString(s string) []byte {
	n := len(s)

	padding := (8 - (n % 8)) % 8

	buf := make([]byte, 8+n+padding)

	binary.LittleEndian.PutUint64(buf[:8], uint64(n))
	copy(buf[8:], s)

	return buf
}

// DumpPath serializes a filesystem path to NAR format.
func DumpPath(w io.Writer, path string) error {
	if err := writeStatic(w, narVersionMagicEncoded); err != nil {
		return err
	}

	if err := writeStatic(w, openParenEncoded); err != nil {
		return err
	}

	if err := dumpPathInner(w, path); err != nil {
		return err
	}

	if err := writeStatic(w, closeParenEncoded); err != nil {
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

	if err := writeStatic(w, typeEncoded); err != nil {
		return err
	}

	switch mode := info.Mode(); {
	case mode.IsRegular():
		if err := writeStatic(w, regularEncoded); err != nil {
			return err
		}

		// Check if executable (Unix permissions)
		if mode&0o111 != 0 {
			if err := writeStatic(w, executableEncoded); err != nil {
				return err
			}

			if err := writeStatic(w, emptyEncoded); err != nil {
				return err
			}
		}

		if err := writeStatic(w, contentsEncoded); err != nil {
			return err
		}

		// Write file size
		//nolint:gosec // File size from os.FileInfo is safe to convert
		fileSize := uint64(info.Size())
		if err := writeUint64(w, fileSize); err != nil {
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
		if err := writeSizePadding(w, fileSize); err != nil {
			return err
		}

	case mode.IsDir():
		if err := writeStatic(w, directoryEncoded); err != nil {
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

			if err := writeStatic(w, entryEncoded); err != nil {
				return err
			}

			if err := writeStatic(w, openParenEncoded); err != nil {
				return err
			}

			if err := writeStatic(w, nameEncoded); err != nil {
				return err
			}

			if err := writeString(w, narName); err != nil {
				return err
			}

			if err := writeStatic(w, nodeEncoded); err != nil {
				return err
			}

			if err := writeStatic(w, openParenEncoded); err != nil {
				return err
			}

			if err := dumpPathInner(w, filepath.Join(path, name)); err != nil {
				return err
			}

			if err := writeStatic(w, closeParenEncoded); err != nil {
				return err
			}

			if err := writeStatic(w, closeParenEncoded); err != nil {
				return err
			}
		}

	case mode&os.ModeSymlink != 0:
		if err := writeStatic(w, symlinkEncoded); err != nil {
			return err
		}

		if err := writeStatic(w, targetEncoded); err != nil {
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
