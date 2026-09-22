package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
)

const (
	DefaultStreamBatchSize = 50
	// DefaultStreamParallel is the number of concurrent pushes. Each push
	// has its own NAR upload pool; a few in parallel keep closure setup of
	// the next batch off the critical path.
	DefaultStreamParallel = 4
	// Single-path retries of a failed batch before the server is blamed.
	streamIsolationProbes = 3

	streamStatusOK    = "ok"
	streamStatusError = "error"
)

type StreamPushFunc func(ctx context.Context, paths []string) ([]string, error)

// StreamRequest is the JSON form of an input line. A build-farm worker sends
// one per finished build so its outputs commit together.
type StreamRequest struct {
	ID    uint64   `json:"id,omitempty"` // echoed in each result so concurrent requests may share paths
	Paths []string `json:"paths"`
}

// A farm worker's QueryValidPaths request names a whole closure on one line.
const maxRequestLine = 64 << 20

type StreamResult struct {
	ID      uint64 `json:"id,omitempty"`
	Path    string `json:"path"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	// What the server signed the path with, so the caller can add them to its own store.
	Signatures []string `json:"signatures,omitempty"`
}

// StreamPusher pushes store paths read line by line from a reader as they
// arrive and writes one JSON StreamResult per input path, so a long-running
// CI driver learns per path when it is cached. While all `parallel` pushes
// are busy, incoming paths accumulate into one batch of up to `batchSize`;
// once a push slot is free, a batch goes out as soon as no further input is
// ready. A line starting with `{` is a StreamRequest and forms a batch of
// its own.
type StreamPusher struct {
	push      StreamPushFunc
	parallel  int
	batchSize int
	// Signatures, if set, fills StreamResult.Signatures for pushed paths.
	Signatures func(path string) []string
}

func NewStreamPusher(push StreamPushFunc, parallel, batchSize int) *StreamPusher {
	if parallel < 1 {
		parallel = 1
	}

	if batchSize < 1 {
		batchSize = DefaultStreamBatchSize
	}

	return &StreamPusher{push: push, parallel: parallel, batchSize: batchSize}
}

// Run returns after EOF on `in` once every path was reported on `out`.
func (s *StreamPusher) Run(ctx context.Context, in io.Reader, out io.Writer) error {
	lines := make(chan string, s.batchSize*s.parallel)
	readErr := make(chan error, 1)

	go func() {
		defer close(lines)

		sc := bufio.NewScanner(in)
		sc.Buffer(nil, maxRequestLine)

		for sc.Scan() {
			if p := strings.TrimSpace(sc.Text()); p != "" {
				lines <- p
			}
		}

		readErr <- sc.Err()
	}()

	var (
		outMu sync.Mutex
		wg    sync.WaitGroup
	)

	enc := json.NewEncoder(out)
	report := func(results []StreamResult) {
		outMu.Lock()
		defer outMu.Unlock()

		for _, r := range results {
			if err := enc.Encode(r); err != nil {
				slog.Error("Failed to write result", "error", err)
			}
		}
	}

	slots := make(chan struct{}, s.parallel)

	// start runs job on a slot the caller already holds.
	start := func(job func() []StreamResult) {
		wg.Go(func() {
			defer func() { <-slots }()

			report(job())
		})
	}

	// submit waits for a slot, then runs job.
	submit := func(job func() []StreamResult) {
		slots <- struct{}{}

		start(job)
	}

	batch := make([]string, 0, s.batchSize)
	takeBatch := func() []string {
		b := batch
		batch = make([]string, 0, s.batchSize)

		return b
	}

	flush := func() {
		if len(batch) > 0 {
			b := takeBatch()

			submit(func() []StreamResult { return s.upload(ctx, b) })
		}
	}

	take := func(line string) {
		if strings.HasPrefix(line, "{") {
			flush()
			submit(func() []StreamResult { return s.uploadRequest(ctx, line) })

			return
		}

		batch = append(batch, line)
		if len(batch) == s.batchSize {
			flush()
		}
	}

loop:
	for {
		if len(batch) == 0 {
			line, ok := <-lines
			if !ok {
				break
			}

			take(line)

			continue
		}

		// A partial batch. Input that is already waiting joins it first, so
		// consecutive lines coalesce regardless of how the reader and this
		// loop are scheduled.
		select {
		case line, ok := <-lines:
			if !ok {
				break loop
			}

			take(line)

			continue
		default:
		}

		// Nothing more ready: send the batch as soon as a slot is free, and
		// keep growing it with whatever arrives until then. Fixing the batch
		// before a slot is free would split input that arrives while all
		// pushes are busy into many small pushes.
		select {
		case line, ok := <-lines:
			if !ok {
				break loop
			}

			take(line)
		case slots <- struct{}{}:
			b := takeBatch()

			start(func() []StreamResult { return s.upload(ctx, b) })
		}
	}

	flush()

	wg.Wait()

	if err := <-readErr; err != nil {
		return fmt.Errorf("reading paths: %w", err)
	}

	return nil
}

func (s *StreamPusher) result(id uint64, path, status, msg string) StreamResult {
	res := StreamResult{ID: id, Path: path, Status: status, Message: msg}
	if status == streamStatusOK && s.Signatures != nil {
		res.Signatures = s.Signatures(path)
	}

	return res
}

// All or nothing: the outputs of one build must not be published partially.
func (s *StreamPusher) uploadRequest(ctx context.Context, line string) []StreamResult {
	var req StreamRequest
	if err := json.Unmarshal([]byte(line), &req); err != nil || len(req.Paths) == 0 {
		return []StreamResult{{ID: 0, Path: line, Status: streamStatusError, Message: "bad request line"}}
	}

	status, msg := streamStatusOK, ""

	if _, err := s.push(ctx, req.Paths); err != nil {
		slog.Error("Upload failed", "error", err, "count", len(req.Paths))

		status, msg = streamStatusError, err.Error()
	}

	results := make([]StreamResult, 0, len(req.Paths))
	for _, p := range req.Paths {
		results = append(results, s.result(req.ID, p, status, msg))
	}

	return results
}

// A failed batch is retried path by path so one bad path only costs itself.
func (s *StreamPusher) upload(ctx context.Context, batch []string) []StreamResult {
	results := make([]StreamResult, 0, len(batch))

	_, err := s.push(ctx, batch)
	if err == nil {
		for _, p := range batch {
			results = append(results, s.result(0, p, streamStatusOK, ""))
		}

		return results
	}

	slog.Error("Upload failed", "error", err, "count", len(batch))

	fail := func(paths []string, err error) {
		for _, p := range paths {
			results = append(results, StreamResult{ID: 0, Path: p, Status: streamStatusError, Message: err.Error()})
		}
	}

	if len(batch) == 1 || ctx.Err() != nil {
		fail(batch, err)

		return results
	}

	succeeded, failures := 0, 0

	for i, p := range batch {
		if succeeded == 0 && failures >= streamIsolationProbes {
			slog.Error("Server seems unavailable, giving up on batch", "untried", len(batch)-i)
			fail(batch[i:], err)

			break
		}

		if _, perr := s.push(ctx, []string{p}); perr != nil {
			fail([]string{p}, perr)

			failures++

			continue
		}

		results = append(results, s.result(0, p, streamStatusOK, ""))
		succeeded++
	}

	return results
}
