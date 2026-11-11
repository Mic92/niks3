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

	streamSkippedMaxNarSize = "max-nar-size"
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
	// Why an "ok" path is not in the cache after all. Message has the details.
	Skipped string `json:"skipped,omitempty"`
	// What the server signed the path with, so the caller can add them to its own store.
	Signatures []string `json:"signatures,omitempty"`
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
	// Signatures, if set, fills StreamResult.Signatures for pushed paths.
	Signatures func(path string) []string
	// Skipped, if set, tells why a pushed path was left out, or "".
	Skipped func(path string) string
	// testHookTaken runs once Run has taken a line.
	testHookTaken func(line string)
}

func NewStreamPusher(push StreamPushFunc, parallel, batchSize int) *StreamPusher {
	if parallel < 1 {
		parallel = 1
	}

	if batchSize < 1 {
		batchSize = DefaultStreamBatchSize
	}

	return &StreamPusher{push: push, parallel: parallel, batchSize: batchSize, testHookTaken: func(string) {}}
}

type streamJob struct {
	id     uint64
	paths  []string
	whole  bool   // a request line: all or nothing
	reject string // why the line cannot be pushed
}

func parseStreamRequest(line string) streamJob {
	var req StreamRequest
	if err := json.Unmarshal([]byte(line), &req); err != nil || len(req.Paths) == 0 {
		return streamJob{paths: []string{line}, reject: "bad request line"}
	}

	return streamJob{id: req.ID, paths: req.Paths, whole: true}
}

func (j streamJob) failed(msg string) []StreamResult {
	results := make([]StreamResult, 0, len(j.paths))
	for _, p := range j.paths {
		results = append(results, StreamResult{ID: j.id, Path: p, Status: streamStatusError, Message: msg})
	}

	return results
}

// Run returns after EOF on `in`, or once ctx is canceled, when every line it
// has taken was answered on `out`. Once canceled it starts no push.
func (s *StreamPusher) Run(ctx context.Context, in io.Reader, out io.Writer) error {
	lines := make(chan string, s.batchSize*s.parallel)
	readErr := make(chan error, 1)

	go func() {
		defer close(lines)

		sc := bufio.NewScanner(in)
		sc.Buffer(nil, maxRequestLine)

		for sc.Scan() {
			p := strings.TrimSpace(sc.Text())
			if p == "" {
				continue
			}

			select {
			case lines <- p:
			case <-ctx.Done():
				return
			}
		}

		readErr <- sc.Err()
	}()

	var (
		outMu sync.Mutex
		wg    sync.WaitGroup
	)

	enc := json.NewEncoder(out)
	jobs := make(chan streamJob)

	for range s.parallel {
		wg.Go(func() {
			for j := range jobs {
				results := s.answer(ctx, j)

				outMu.Lock()

				for _, r := range results {
					if err := enc.Encode(r); err != nil {
						slog.Error("Failed to write result", "error", err)
					}
				}

				outMu.Unlock()
			}
		})
	}

	s.feed(ctx, lines, jobs)
	close(jobs)
	wg.Wait()

	// The reader may be stuck in a read that nobody ends.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("reading paths: %w", err)
	}

	if err := <-readErr; err != nil {
		return fmt.Errorf("reading paths: %w", err)
	}

	return nil
}

// Paths gather into one batch until a push is idle to receive it.
func (s *StreamPusher) feed(ctx context.Context, lines <-chan string, jobs chan<- streamJob) {
	var batch []string

	flush := func() {
		if len(batch) > 0 {
			jobs <- streamJob{paths: batch}
			batch = nil
		}
	}
	defer flush()

	take := func(line string) {
		defer s.testHookTaken(line)

		if strings.HasPrefix(line, "{") {
			flush()
			jobs <- parseStreamRequest(line)

			return
		}

		batch = append(batch, line)
		if len(batch) == s.batchSize {
			flush()
		}
	}

	for {
		// Input that is already waiting joins the batch first.
		select {
		case line, ok := <-lines:
			if !ok {
				return
			}

			take(line)

			continue
		default:
		}

		var idle chan<- streamJob
		if len(batch) > 0 {
			idle = jobs
		}

		select {
		case line, ok := <-lines:
			if !ok {
				return
			}

			take(line)
		case idle <- streamJob{paths: batch}:
			batch = nil
		case <-ctx.Done():
			return
		}
	}
}

func (s *StreamPusher) answer(ctx context.Context, j streamJob) []StreamResult {
	switch {
	case j.reject != "":
		return j.failed(j.reject)
	case ctx.Err() != nil:
		return j.failed(ctx.Err().Error())
	case j.whole:
		return s.uploadRequest(ctx, j)
	default:
		return s.upload(ctx, j.paths)
	}
}

func (s *StreamPusher) result(id uint64, path, status, msg string) StreamResult {
	res := StreamResult{ID: id, Path: path, Status: status, Message: msg}
	if status == streamStatusOK && s.Signatures != nil {
		res.Signatures = s.Signatures(path)
	}

	if status == streamStatusOK && s.Skipped != nil {
		if why := s.Skipped(path); why != "" {
			res.Skipped, res.Message = streamSkippedMaxNarSize, why
		}
	}

	return res
}

// All or nothing: the outputs of one build must not be published partially.
func (s *StreamPusher) uploadRequest(ctx context.Context, j streamJob) []StreamResult {
	if _, err := s.push(ctx, j.paths); err != nil {
		slog.Error("Upload failed", "error", err, "count", len(j.paths))

		return j.failed(err.Error())
	}

	results := make([]StreamResult, 0, len(j.paths))
	for _, p := range j.paths {
		results = append(results, s.result(j.id, p, streamStatusOK, ""))
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
