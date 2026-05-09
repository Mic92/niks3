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
	"slices"
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

	if strings.HasSuffix(name, caseHackSuffix) {
		return name[:len(name)-len(caseHackSuffix)]
	}

	return name
}

// writeUint64 writes a little-endian uint64 using the narWriter's scratch
// buffer to avoid a heap allocation per call (passing a stack array to an
// io.Writer interface forces it to escape).
func (nw *narWriter) writeUint64(v uint64) error {
	binary.LittleEndian.PutUint64(nw.scratch[:], v)

	if _, err := nw.w.Write(nw.scratch[:]); err != nil {
		return fmt.Errorf("writing uint64: %w", err)
	}

	nw.offset += 8

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
	w       io.Writer
	offset  uint64
	scratch [8]byte // reused for uint64 framing writes
}

func (nw *narWriter) writeStatic(data []byte) error {
	if _, err := nw.w.Write(data); err != nil {
		return fmt.Errorf("writing static string: %w", err)
	}

	nw.offset += uint64(len(data))

	return nil
}

func (nw *narWriter) writeString(s string) error {
	if err := nw.writeUint64(uint64(len(s))); err != nil {
		return err
	}

	if _, err := io.WriteString(nw.w, s); err != nil {
		return fmt.Errorf("writing string content: %w", err)
	}

	nw.offset += uint64(len(s))

	padding := (8 - (len(s) % 8)) % 8
	if padding > 0 {
		if _, err := nw.w.Write(zeroPad[:padding]); err != nil {
			return fmt.Errorf("writing padding: %w", err)
		}

		nw.offset += uint64(padding)
	}

	return nil
}

// writeContentsHeader writes the size framing before file contents and
// returns the NAR offset of the contents.
func (nw *narWriter) writeContentsHeader(size uint64) (uint64, error) {
	if err := nw.writeUint64(size); err != nil {
		return 0, fmt.Errorf("writing file size: %w", err)
	}

	return nw.offset, nil
}

// writeContentsFooter writes the trailing padding after file contents.
func (nw *narWriter) writeContentsFooter(size uint64) error {
	nw.offset += size

	padding := (8 - (size % 8)) % 8
	if padding > 0 {
		if _, err := nw.w.Write(zeroPad[:padding]); err != nil {
			return fmt.Errorf("writing padding: %w", err)
		}

		nw.offset += padding
	}

	return nil
}

// writeFileContentsStreaming reads a file directly into the NAR output. Used
// for large files that are too big to prefetch into memory.
func (nw *narWriter) writeFileContentsStreaming(path string, size uint64) (uint64, error) {
	contentOffset, err := nw.writeContentsHeader(size)
	if err != nil {
		return 0, err
	}

	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("opening file %s: %w", path, err)
	}

	defer func() {
		if err := f.Close(); err != nil {
			slog.Error("Failed to close file", "path", path, "error", err)
		}
	}()

	bufPtr, ok := copyBufferPool.Get().(*[]byte)
	if !ok {
		return 0, errors.New("invalid buffer type from pool")
	}
	defer copyBufferPool.Put(bufPtr)

	// We already know the file size from stat, so copy exactly that many
	// bytes. Plain io.CopyBuffer would issue one extra read syscall just to
	// observe EOF, which doubles the syscall count for small files.
	buf := *bufPtr
	remaining := size

	for remaining > 0 {
		chunk := buf
		if uint64(len(chunk)) > remaining {
			chunk = chunk[:remaining]
		}

		rn, rerr := io.ReadFull(f, chunk)
		if rerr != nil {
			return 0, fmt.Errorf("reading file %s: expected %d more bytes: %w", path, remaining, rerr)
		}

		if _, werr := nw.w.Write(chunk[:rn]); werr != nil {
			return 0, fmt.Errorf("copying file %s: %w", path, werr)
		}

		remaining -= uint64(rn) //nolint:gosec // rn is bounded by len(chunk)
	}

	if err := nw.writeContentsFooter(size); err != nil {
		return 0, err
	}

	return contentOffset, nil
}

// writeFileContentsPrefetched writes already-read file contents from a
// prefetched buffer.
func (nw *narWriter) writeFileContentsPrefetched(pf *prefetchedFile) (uint64, error) {
	<-pf.done

	if pf.err != nil {
		return 0, pf.err
	}

	contentOffset, err := nw.writeContentsHeader(pf.size)
	if err != nil {
		return 0, err
	}

	if _, err := nw.w.Write(pf.data); err != nil {
		return 0, fmt.Errorf("writing file %s: %w", pf.path, err)
	}

	if err := nw.writeContentsFooter(pf.size); err != nil {
		return 0, err
	}

	return contentOffset, nil
}

// narNode is one entry in the pre-walked tree. The walk is metadata-only
// (readdir/lstat/readlink) and cheap; the expensive part — reading file
// contents — is deferred to the write pass where it can be parallelized.
type narNode struct {
	name       string // case-hack-stripped name in parent directory
	path       string // absolute path on disk
	kind       byte   // 'f' regular, 'd' directory, 'l' symlink
	size       uint64 // regular files only
	executable bool   // regular files only
	target     string // symlinks only
	children   []*narNode
}

// DumpPathWithListing serializes a path to NAR format and returns the directory listing.
// The listing is compatible with Nix's .ls format.
//
// Serialization runs in two passes: a fast metadata-only walk builds the tree,
// then the write pass streams it to w while a worker pool prefetches small
// file contents ahead of the writer.
func DumpPathWithListing(w io.Writer, path string) (*NarListing, error) {
	root, err := walkPath(path)
	if err != nil {
		return nil, err
	}

	pf := newPrefetcher(0)

	// The enqueuer feeds files to the prefetcher in the same DFS order the
	// writer consumes them. It blocks when the prefetch queue is full, so it
	// must run concurrently with the writer.
	enqueueDone := make(chan struct{})

	go func() {
		defer close(enqueueDone)

		enqueuePrefetch(pf, root)
		pf.close()
	}()

	// On error, drain the prefetch queue so the enqueuer goroutine unblocks
	// and the worker pool shuts down cleanly.
	drain := func() {
		go func() {
			for f := range pf.queue {
				<-f.done
				pf.release(f)
			}
		}()

		<-enqueueDone
	}

	nw := &narWriter{w: w, offset: 0}

	if err := nw.writeStatic(narVersionMagicEncoded); err != nil {
		drain()

		return nil, err
	}

	if err := nw.writeStatic(openParenEncoded); err != nil {
		drain()

		return nil, err
	}

	entry, err := writeNode(nw, pf, root)
	if err != nil {
		drain()

		return nil, err
	}

	if err := nw.writeStatic(closeParenEncoded); err != nil {
		drain()

		return nil, err
	}

	<-enqueueDone

	return &NarListing{Version: 1, Root: entry}, nil
}

// walkPath builds the narNode tree for a path using only metadata syscalls.
func walkPath(path string) (*narNode, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}

	return walkNode(path, "", info.Mode(), info)
}

// walkNode classifies a single filesystem entry and recurses into
// directories. info may be nil for directories and symlinks (they don't need
// it); the mode tells us which branch to take.
func walkNode(path, name string, mode os.FileMode, info os.FileInfo) (*narNode, error) {
	switch {
	case mode.IsRegular():
		if info == nil {
			return nil, fmt.Errorf("missing file info for %s", path)
		}

		return &narNode{
			name:       name,
			path:       path,
			kind:       'f',
			size:       uint64(info.Size()), //nolint:gosec // file size is non-negative
			executable: info.Mode()&0o111 != 0,
		}, nil

	case mode.IsDir():
		return walkDirectory(path, name)

	case mode&os.ModeSymlink != 0:
		target, err := os.Readlink(path)
		if err != nil {
			return nil, fmt.Errorf("reading symlink %s: %w", path, err)
		}

		return &narNode{name: name, path: path, kind: 'l', target: target}, nil

	default:
		return nil, fmt.Errorf("unsupported file type for %s: %v", path, mode)
	}
}

func walkDirectory(path, name string) (*narNode, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("reading directory %s: %w", path, err)
	}

	// Sort entries by name (NAR requirement)
	slices.SortFunc(entries, func(a, b os.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})

	node := &narNode{name: name, path: path, kind: 'd', children: make([]*narNode, 0, len(entries))}

	for _, entry := range entries {
		entryName := entry.Name()
		narName := stripCaseHackSuffix(entryName)
		childPath := filepath.Join(path, entryName)

		// Use entry.Type() to avoid an extra stat syscall when possible.
		// Fall back to entry.Info() if Type() returns DT_UNKNOWN.
		entryType := entry.Type()

		var (
			info os.FileInfo
			mode os.FileMode
		)

		switch {
		case entryType.IsRegular():
			// Regular files need FileInfo for size and permissions.
			info, err = entry.Info()
			if err != nil {
				return nil, fmt.Errorf("getting info for %s: %w", childPath, err)
			}

			mode = info.Mode()
		case entryType.IsDir() || entryType&os.ModeSymlink != 0:
			mode = entryType
		default:
			// DT_UNKNOWN: fall back to lstat.
			info, err = entry.Info()
			if err != nil {
				return nil, fmt.Errorf("getting info for %s: %w", childPath, err)
			}

			mode = info.Mode()
		}

		child, err := walkNode(childPath, narName, mode, info)
		if err != nil {
			return nil, err
		}

		node.children = append(node.children, child)
	}

	return node, nil
}

// shouldPrefetch reports whether a regular file is read ahead by the worker
// pool. The enqueuer and writer call this independently and must agree, so it
// is a pure function of node metadata.
func shouldPrefetch(n *narNode) bool {
	return n.kind == 'f' && n.size <= prefetchMaxFileSize
}

// enqueuePrefetch walks the node tree in the same DFS order the writer uses
// and registers small regular files with the prefetcher.
func enqueuePrefetch(p *prefetcher, n *narNode) {
	if shouldPrefetch(n) {
		p.enqueue(n.path, n.size)

		return
	}

	for _, c := range n.children {
		enqueuePrefetch(p, c)
	}
}

func writeNode(nw *narWriter, p *prefetcher, n *narNode) (NarListingEntry, error) {
	if err := nw.writeStatic(typeEncoded); err != nil {
		return NarListingEntry{}, err
	}

	switch n.kind {
	case 'f':
		return writeRegularFile(nw, p, n)
	case 'd':
		return writeDirectory(nw, p, n)
	case 'l':
		return writeSymlink(nw, n)
	default:
		return NarListingEntry{}, fmt.Errorf("unknown node kind %q for %s", n.kind, n.path)
	}
}

func writeRegularFile(nw *narWriter, p *prefetcher, n *narNode) (NarListingEntry, error) {
	if err := nw.writeStatic(regularEncoded); err != nil {
		return NarListingEntry{}, err
	}

	if n.executable {
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

	var (
		contentOffset uint64
		err           error
	)

	if shouldPrefetch(n) {
		// The enqueuer feeds files in the same DFS order, so the next item
		// on the queue is always this node's contents.
		prefetched := <-p.queue
		contentOffset, err = nw.writeFileContentsPrefetched(prefetched)
		p.release(prefetched)
	} else {
		contentOffset, err = nw.writeFileContentsStreaming(n.path, n.size)
	}

	if err != nil {
		return NarListingEntry{}, err
	}

	fileSize := n.size

	entry := NarListingEntry{
		Type:      "regular",
		Size:      &fileSize,
		NarOffset: &contentOffset,
	}

	if n.executable {
		isExecutable := true
		entry.Executable = &isExecutable
	}

	return entry, nil
}

func writeDirectory(nw *narWriter, p *prefetcher, n *narNode) (NarListingEntry, error) {
	if err := nw.writeStatic(directoryEncoded); err != nil {
		return NarListingEntry{}, err
	}

	listingEntries := make(map[string]NarListingEntry, len(n.children))

	for _, child := range n.children {
		if err := nw.writeStatic(entryEncoded); err != nil {
			return NarListingEntry{}, err
		}

		if err := nw.writeStatic(openParenEncoded); err != nil {
			return NarListingEntry{}, err
		}

		if err := nw.writeStatic(nameEncoded); err != nil {
			return NarListingEntry{}, err
		}

		if err := nw.writeString(child.name); err != nil {
			return NarListingEntry{}, err
		}

		if err := nw.writeStatic(nodeEncoded); err != nil {
			return NarListingEntry{}, err
		}

		if err := nw.writeStatic(openParenEncoded); err != nil {
			return NarListingEntry{}, err
		}

		childEntry, err := writeNode(nw, p, child)
		if err != nil {
			return NarListingEntry{}, err
		}

		listingEntries[child.name] = childEntry

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

func writeSymlink(nw *narWriter, n *narNode) (NarListingEntry, error) {
	if err := nw.writeStatic(symlinkEncoded); err != nil {
		return NarListingEntry{}, err
	}

	if err := nw.writeStatic(targetEncoded); err != nil {
		return NarListingEntry{}, err
	}

	if err := nw.writeString(n.target); err != nil {
		return NarListingEntry{}, err
	}

	target := n.target

	return NarListingEntry{
		Type:   "symlink",
		Target: &target,
	}, nil
}

// CompressListingWithZstd compresses a NAR listing as JSON with zstd compression.
// This matches Nix's compression approach and provides better performance than brotli.
// It reuses the existing zstdEncoderPool from nar_upload.go.
func CompressListingWithZstd(listing *NarListing) ([]byte, error) {
	jsonData, err := json.Marshal(listing)
	if err != nil {
		return nil, fmt.Errorf("marshaling listing to JSON: %w", err)
	}

	return compressWithZstd(jsonData)
}
