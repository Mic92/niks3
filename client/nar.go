package client

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
)

const (
	narVersionMagic = "nix-archive-1"
	caseHackSuffix  = "~nix~case~hack~"
)

//nolint:gochecknoglobals // useCaseHack is platform-specific runtime constant
var useCaseHack = runtime.GOOS == "darwin"

var copyBufferPool = sync.Pool{ //nolint:gochecknoglobals
	New: func() any {
		// 128KB buffer for efficient large file reads
		buf := make([]byte, 128*1024)

		return &buf
	},
}

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

func writeUint64(w io.Writer, v uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)

	if _, err := w.Write(buf[:]); err != nil {
		return fmt.Errorf("writing uint64: %w", err)
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

// NarListing represents the directory structure of a NAR archive in Nix's format.
type NarListing struct {
	Version int             `json:"version"`
	Root    NarListingEntry `json:"root"`
}

// NarListingEntry represents a file, directory, or symlink in a NAR listing.
type NarListingEntry struct {
	Type       string                     `json:"type"` // "regular", "directory", "symlink"
	Size       *uint64                    `json:"size,omitempty"`
	Executable *bool                      `json:"executable,omitempty"`
	NarOffset  *uint64                    `json:"narOffset,omitempty"` //nolint:tagliatelle // matches Nix's JSON format
	Entries    map[string]NarListingEntry `json:"entries,omitempty"`
	Target     *string                    `json:"target,omitempty"`
}

// narWriter wraps an io.Writer and tracks the current offset for NAR serialization.
type narWriter struct {
	w      io.Writer
	offset uint64
}

func (nw *narWriter) writeStatic(data []byte) error {
	if _, err := nw.w.Write(data); err != nil {
		return fmt.Errorf("writing static string: %w", err)
	}

	nw.offset += uint64(len(data))

	return nil
}

func (nw *narWriter) writeString(s string) error {
	if err := writeUint64(nw.w, uint64(len(s))); err != nil {
		return err
	}

	nw.offset += 8

	if _, err := io.WriteString(nw.w, s); err != nil {
		return fmt.Errorf("writing string content: %w", err)
	}

	nw.offset += uint64(len(s))

	padding := (8 - (len(s) % 8)) % 8
	if padding > 0 {
		if _, err := nw.w.Write(zeroPad[:padding]); err != nil {
			return fmt.Errorf("writing padding: %w", err)
		}

		// padding is always 0-7, safe to convert to uint64
		nw.offset += uint64(padding) //nolint:gosec // padding is bounded by (8 - (len(s) % 8)) % 8, max value is 7
	}

	return nil
}

func (nw *narWriter) writeFileContents(path string, size uint64) (uint64, error) {
	if err := writeUint64(nw.w, size); err != nil {
		return 0, fmt.Errorf("writing file size: %w", err)
	}

	nw.offset += 8

	contentOffset := nw.offset

	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("opening file %s: %w", path, err)
	}

	defer func() {
		if err := f.Close(); err != nil {
			slog.Error("Failed to close file", "path", path, "error", err)
		}
	}()

	// Use a pooled buffer to reduce syscalls and allocations
	bufPtr, ok := copyBufferPool.Get().(*[]byte)
	if !ok {
		return 0, errors.New("invalid buffer type from pool")
	}
	defer copyBufferPool.Put(bufPtr)

	n, err := io.CopyBuffer(nw.w, f, *bufPtr)
	if err != nil {
		return 0, fmt.Errorf("copying file %s: %w", path, err)
	}

	// n is from io.CopyBuffer which returns int64; size comes from os.FileInfo.Size() which is also int64
	// The conversion is safe as we're comparing values from the same source
	if uint64(n) != size { //nolint:gosec // n and size are both from filesystem operations, safe to compare
		return 0, fmt.Errorf("file size mismatch for %s: expected %d, copied %d", path, size, n)
	}

	nw.offset += size

	padding := (8 - (size % 8)) % 8
	if padding > 0 {
		if _, err := nw.w.Write(zeroPad[:padding]); err != nil {
			return 0, fmt.Errorf("writing padding: %w", err)
		}

		nw.offset += padding
	}

	return contentOffset, nil
}

// DumpPathWithListing serializes a path to NAR format and returns the directory listing.
// The listing is compatible with Nix's .ls format.
func DumpPathWithListing(w io.Writer, path string) (*NarListing, error) {
	nw := &narWriter{w: w, offset: 0}

	if err := nw.writeStatic(narVersionMagicEncoded); err != nil {
		return nil, err
	}

	if err := nw.writeStatic(openParenEncoded); err != nil {
		return nil, err
	}

	entry, err := dumpPathWithListing(nw, path)
	if err != nil {
		return nil, err
	}

	if err := nw.writeStatic(closeParenEncoded); err != nil {
		return nil, err
	}

	return &NarListing{Version: 1, Root: entry}, nil
}

func dumpPathWithListing(nw *narWriter, path string) (NarListingEntry, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return NarListingEntry{}, fmt.Errorf("stat %s: %w", path, err)
	}

	if err := nw.writeStatic(typeEncoded); err != nil {
		return NarListingEntry{}, err
	}

	mode := info.Mode()
	switch {
	case mode.IsRegular():
		return dumpRegularFile(nw, path, info)
	case mode.IsDir():
		return dumpDirectory(nw, path)
	case mode&os.ModeSymlink != 0:
		return dumpSymlink(nw, path)
	default:
		return NarListingEntry{}, fmt.Errorf("unsupported file type for %s: %v", path, mode)
	}
}

func dumpRegularFile(nw *narWriter, path string, info os.FileInfo) (NarListingEntry, error) {
	if err := nw.writeStatic(regularEncoded); err != nil {
		return NarListingEntry{}, err
	}

	isExecutable := info.Mode()&0o111 != 0
	if isExecutable {
		if err := nw.writeStatic(executableEncoded); err != nil {
			return NarListingEntry{}, err
		}

		if err := nw.writeStatic(emptyEncoded); err != nil {
			return NarListingEntry{}, err
		}
	}

	if err := nw.writeStatic(contentsEncoded); err != nil {
		return NarListingEntry{}, err
	}

	// info.Size() returns int64 from os.FileInfo; safe to convert to uint64 as file sizes are non-negative
	fileSize := uint64(info.Size()) //nolint:gosec // file size from os.FileInfo is always non-negative

	contentOffset, err := nw.writeFileContents(path, fileSize)
	if err != nil {
		return NarListingEntry{}, err
	}

	entry := NarListingEntry{
		Type:      "regular",
		Size:      &fileSize,
		NarOffset: &contentOffset,
	}
	if isExecutable {
		entry.Executable = &isExecutable
	}

	return entry, nil
}

func dumpDirectory(nw *narWriter, path string) (NarListingEntry, error) {
	if err := nw.writeStatic(directoryEncoded); err != nil {
		return NarListingEntry{}, err
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return NarListingEntry{}, fmt.Errorf("reading directory %s: %w", path, err)
	}

	// Sort entries by name (NAR requirement)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	listingEntries := make(map[string]NarListingEntry)

	for _, entry := range entries {
		name := entry.Name()
		narName := stripCaseHackSuffix(name)

		if err := nw.writeStatic(entryEncoded); err != nil {
			return NarListingEntry{}, err
		}

		if err := nw.writeStatic(openParenEncoded); err != nil {
			return NarListingEntry{}, err
		}

		if err := nw.writeStatic(nameEncoded); err != nil {
			return NarListingEntry{}, err
		}

		if err := nw.writeString(narName); err != nil {
			return NarListingEntry{}, err
		}

		if err := nw.writeStatic(nodeEncoded); err != nil {
			return NarListingEntry{}, err
		}

		if err := nw.writeStatic(openParenEncoded); err != nil {
			return NarListingEntry{}, err
		}

		childPath := filepath.Join(path, name)

		var childEntry NarListingEntry

		if err := nw.writeStatic(typeEncoded); err != nil {
			return NarListingEntry{}, err
		}

		// Use entry.Type() to avoid an extra stat syscall when possible
		// Fallback to entry.Info() if Type() returns DT_UNKNOWN (can happen on some filesystems)
		var err error

		entryType := entry.Type()

		// Check if we can classify the entry from Type() alone
		if entryType.IsRegular() || entryType.IsDir() || (entryType&os.ModeSymlink != 0) {
			// Type is known, handle normally
			switch {
			case entryType.IsRegular():
				// Regular files need FileInfo for size and permissions
				info, infoErr := entry.Info()
				if infoErr != nil {
					return NarListingEntry{}, fmt.Errorf("getting info for %s: %w", childPath, infoErr)
				}

				childEntry, err = dumpRegularFile(nw, childPath, info)
			case entryType.IsDir():
				// Directories don't need FileInfo, recurse directly
				childEntry, err = dumpDirectory(nw, childPath)
			case entryType&os.ModeSymlink != 0:
				// Symlinks don't need FileInfo
				childEntry, err = dumpSymlink(nw, childPath)
			}
		} else {
			// Type is DT_UNKNOWN or unclassifiable, fall back to Info() to get mode
			info, infoErr := entry.Info()
			if infoErr != nil {
				return NarListingEntry{}, fmt.Errorf("getting info for %s: %w", childPath, infoErr)
			}

			mode := info.Mode()
			switch {
			case mode.IsRegular():
				childEntry, err = dumpRegularFile(nw, childPath, info)
			case mode.IsDir():
				childEntry, err = dumpDirectory(nw, childPath)
			case mode&os.ModeSymlink != 0:
				childEntry, err = dumpSymlink(nw, childPath)
			default:
				err = fmt.Errorf("unsupported file type for %s: %v", childPath, mode)
			}
		}

		if err != nil {
			return NarListingEntry{}, err
		}

		listingEntries[narName] = childEntry

		if err := nw.writeStatic(closeParenEncoded); err != nil {
			return NarListingEntry{}, err
		}

		if err := nw.writeStatic(closeParenEncoded); err != nil {
			return NarListingEntry{}, err
		}
	}

	return NarListingEntry{
		Type:    "directory",
		Entries: listingEntries,
	}, nil
}

func dumpSymlink(nw *narWriter, path string) (NarListingEntry, error) {
	if err := nw.writeStatic(symlinkEncoded); err != nil {
		return NarListingEntry{}, err
	}

	if err := nw.writeStatic(targetEncoded); err != nil {
		return NarListingEntry{}, err
	}

	target, err := os.Readlink(path)
	if err != nil {
		return NarListingEntry{}, fmt.Errorf("reading symlink %s: %w", path, err)
	}

	if err := nw.writeString(target); err != nil {
		return NarListingEntry{}, err
	}

	return NarListingEntry{
		Type:   "symlink",
		Target: &target,
	}, nil
}

// CompressListingWithZstd compresses a NAR listing as JSON with zstd compression.
// This matches Nix's compression approach and provides better performance than brotli.
// It reuses the existing zstdEncoderPool from nar_upload.go.
func CompressListingWithZstd(listing *NarListing) ([]byte, error) {
	// Marshal to JSON
	jsonData, err := json.Marshal(listing)
	if err != nil {
		return nil, fmt.Errorf("marshaling listing to JSON: %w", err)
	}

	// Use the existing zstd pool (defined in metadata_tasks.go)
	return compressWithZstd(jsonData)
}
