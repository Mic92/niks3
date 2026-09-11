package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
)

const (
	DefaultStreamBatchSize = 50
	// Each push has its own NAR upload pool; a few in parallel keep
	// closure setup of the next batch off the critical path.
	DefaultStreamParallel = 4
	// Single-path retries of a failed batch before the server is blamed.
	streamIsolationProbes = 3

	streamStatusOK    = "ok"
	streamStatusError = "error"
	streamStatusStale = "stale"
)

type StreamPushFunc func(ctx context.Context, paths []string, claimToken int64) ([]string, error)

// StreamRequest is the JSON form of an input line. A build-farm worker sends
// one per finished build so its outputs commit together, fenced by the claim.
type StreamRequest struct {
	Paths      []string `json:"paths"`
	ClaimToken int64    `json:"claim_token,omitempty"`
}

type StreamResult struct {
	Path    string `json:"path"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// StreamPusher pushes store paths read line by line from a reader as they
// arrive and writes one JSON StreamResult per input path, so a long-running
// CI driver learns per path when it is cached. While all `parallel` pushes
// are busy, incoming paths accumulate into one batch of up to `batchSize`.
// A line starting with `{` is a StreamRequest and forms a batch of its own.
type StreamPusher struct {
	push      StreamPushFunc
	parallel  int
	batchSize int
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

	var pending string // read while filling a batch but not part of it

	for {
		first := pending
		pending = ""

		if first == "" {
			var ok bool
			if first, ok = <-lines; !ok {
				break
			}
		}

		slots <- struct{}{}

		if strings.HasPrefix(first, "{") {
			wg.Go(func() {
				defer func() { <-slots }()

				report(s.uploadRequest(ctx, first))
			})

			continue
		}

		batch := append(make([]string, 0, s.batchSize), first)

	fill:
		for len(batch) < s.batchSize {
			select {
			case p, ok := <-lines:
				if !ok {
					break fill
				}

				if strings.HasPrefix(p, "{") {
					pending = p

					break fill
				}

				batch = append(batch, p)
			default:
				break fill
			}
		}

		wg.Go(func() {
			defer func() { <-slots }()

			report(s.upload(ctx, batch))
		})
	}

	wg.Wait()

	if err := <-readErr; err != nil {
		return fmt.Errorf("reading paths: %w", err)
	}

	return nil
}

// All or nothing: the outputs of one build must not be published partially,
// and a stale claim means another worker owns them now.
func (s *StreamPusher) uploadRequest(ctx context.Context, line string) []StreamResult {
	var req StreamRequest
	if err := json.Unmarshal([]byte(line), &req); err != nil || len(req.Paths) == 0 {
		return []StreamResult{{Path: line, Status: streamStatusError, Message: "bad request line"}}
	}

	status, msg := streamStatusOK, ""

	if _, err := s.push(ctx, req.Paths, req.ClaimToken); err != nil {
		slog.Error("Upload failed", "error", err, "count", len(req.Paths))

		status, msg = streamStatusError, err.Error()
		if errors.Is(err, ErrStaleClaim) {
			status = streamStatusStale
		}
	}

	results := make([]StreamResult, 0, len(req.Paths))
	for _, p := range req.Paths {
		results = append(results, StreamResult{Path: p, Status: status, Message: msg})
	}

	return results
}

// A failed batch is retried path by path so one bad path only costs itself.
func (s *StreamPusher) upload(ctx context.Context, batch []string) []StreamResult {
	results := make([]StreamResult, 0, len(batch))

	_, err := s.push(ctx, batch, 0)
	if err == nil {
		for _, p := range batch {
			results = append(results, StreamResult{Path: p, Status: streamStatusOK})
		}

		return results
	}

	slog.Error("Upload failed", "error", err, "count", len(batch))

	fail := func(paths []string, err error) {
		for _, p := range paths {
			results = append(results, StreamResult{Path: p, Status: streamStatusError, Message: err.Error()})
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

		if _, perr := s.push(ctx, []string{p}, 0); perr != nil {
			fail([]string{p}, perr)

			failures++

			continue
		}

		results = append(results, StreamResult{Path: p, Status: streamStatusOK})
		succeeded++
	}

	return results
}
