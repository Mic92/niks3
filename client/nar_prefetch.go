package client

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
)

// NAR output order is fixed by the format, so writes must stay sequential.
// The dominant cost of serialization is filesystem syscalls (open/read/close),
// which we overlap by reading small file contents ahead of the writer with a
// bounded worker pool. Large files are not prefetched to keep memory bounded;
// the writer streams them directly.
const (
	// prefetchMaxFileSize caps which files are prefetched. Files above this
	// size are read synchronously by the writer using the streaming path.
	prefetchMaxFileSize = 256 * 1024

	// prefetchQueueDepth bounds how many files are read ahead of the
	// writer. Each in-flight file holds one pooled prefetchMaxFileSize
	// buffer, so worst-case memory is prefetchQueueDepth * prefetchMaxFileSize
	// (32 MiB by default).
	prefetchQueueDepth = 128
)

// prefetchBufPool pools backing arrays sized to prefetchMaxFileSize. Reading
// each file into a fresh allocation would generate gigabytes of garbage on
// large closures; reusing buffers keeps steady-state allocations near zero.
var prefetchBufPool = sync.Pool{ //nolint:gochecknoglobals // sync.Pool should be global
	New: func() any {
		buf := make([]byte, prefetchMaxFileSize)

		return &buf
	},
}

// prefetchedFile holds the fully-read contents of a small file or the error
// from reading it. done is closed once data/err is populated.
type prefetchedFile struct {
	path string
	size uint64
	buf  *[]byte // pooled backing array; data is a slice of *buf
	data []byte
	err  error
	done chan struct{}
}

// prefetcher reads small files ahead of the NAR writer so the writer rarely
// blocks on disk. Files are delivered in the exact order they were enqueued;
// memory is bounded by the queue capacity since each entry holds at most one
// pooled prefetchMaxFileSize buffer.
type prefetcher struct {
	queue   chan *prefetchedFile // ordered, consumed by writer; bounds in-flight files
	work    chan *prefetchedFile // unordered, consumed by readers
	wg      sync.WaitGroup
	closing sync.Once
}

func newPrefetcher(workers int) *prefetcher {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}

	p := &prefetcher{
		queue: make(chan *prefetchedFile, prefetchQueueDepth),
		work:  make(chan *prefetchedFile, prefetchQueueDepth),
	}

	for range workers {
		p.wg.Add(1)

		go p.worker()
	}

	return p
}

// enqueue registers a file for prefetching. Must be called from a single
// goroutine in the order the writer will consume them. Blocks if the queue
// is full, applying backpressure to the enqueuer.
func (p *prefetcher) enqueue(path string, size uint64) *prefetchedFile {
	pf := &prefetchedFile{
		path: path,
		size: size,
		done: make(chan struct{}),
	}

	p.queue <- pf

	p.work <- pf

	return pf
}

// release returns the pooled buffer held by a consumed file.
func (p *prefetcher) release(pf *prefetchedFile) {
	if pf.buf != nil {
		prefetchBufPool.Put(pf.buf)
		pf.buf = nil
	}

	pf.data = nil
}

// close shuts down the worker pool. Safe to call multiple times.
func (p *prefetcher) close() {
	p.closing.Do(func() {
		close(p.work)
		p.wg.Wait()
		close(p.queue)
	})
}

func (p *prefetcher) worker() {
	defer p.wg.Done()

	for pf := range p.work {
		p.readFile(pf)
	}
}

func (p *prefetcher) readFile(pf *prefetchedFile) {
	defer close(pf.done)

	f, err := os.Open(pf.path)
	if err != nil {
		pf.err = fmt.Errorf("opening file %s: %w", pf.path, err)

		return
	}

	defer func() {
		if err := f.Close(); err != nil && pf.err == nil {
			pf.err = fmt.Errorf("closing file %s: %w", pf.path, err)
		}
	}()

	bufPtr, ok := prefetchBufPool.Get().(*[]byte)
	if !ok {
		pf.err = fmt.Errorf("invalid buffer type from pool for %s", pf.path)

		return
	}

	buf := (*bufPtr)[:pf.size]

	n, rerr := io.ReadFull(f, buf)
	if rerr != nil {
		prefetchBufPool.Put(bufPtr)

		pf.err = fmt.Errorf("reading file %s: expected %d bytes, got %d: %w", pf.path, pf.size, n, rerr)

		return
	}

	pf.buf = bufPtr
	pf.data = buf
}
