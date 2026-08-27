package hook_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/hook"
)

// writeTestFile creates a file simulating an existing store path.
func writeTestFile(t *testing.T, dir, name string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(name), 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

// recordingPush returns a PushFunc that records each batch it receives and a
// getter for the recorded batches.
func recordingPush() (hook.PushFunc, func() [][]string) {
	var mu sync.Mutex

	var pushed [][]string

	push := func(_ context.Context, paths []string) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()

		cp := make([]string, len(paths))
		copy(cp, paths)
		pushed = append(pushed, cp)

		return cp, nil
	}

	batches := func() [][]string {
		mu.Lock()
		defer mu.Unlock()

		return pushed
	}

	return push, batches
}

// startWorker runs a worker in the background and returns a stop function
// that cancels it and waits for the shutdown drain to finish.
func startWorker(t *testing.T, q *hook.Queue, push hook.PushFunc, batchSize int) func() {
	t.Helper()

	notify := make(chan struct{}, 1)
	w := hook.NewWorker(q, push, batchSize, notify)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)

		w.Run(ctx)
	}()

	notify <- struct{}{}

	return func() {
		t.Helper()
		cancel()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for worker to stop")
		}
	}
}

// waitForCount blocks until the queue holds exactly n paths.
func waitForCount(t *testing.T, q *hook.Queue, n int) {
	t.Helper()

	deadline := time.After(5 * time.Second)

	for {
		if count, _ := q.Count(); count == n {
			return
		}

		select {
		case <-deadline:
			count, _ := q.Count()
			t.Fatalf("timeout waiting for queue count %d, have %d", n, count)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// runWorkerUntilDrained runs a worker until the queue is empty, then stops it.
func runWorkerUntilDrained(t *testing.T, q *hook.Queue, push hook.PushFunc, batchSize int) {
	t.Helper()

	stop := startWorker(t, q, push, batchSize)
	waitForCount(t, q, 0)
	stop()
}

// drainWorker runs only the shutdown drain by starting a worker with an
// already-cancelled context.
func drainWorker(t *testing.T, q *hook.Queue, push hook.PushFunc, batchSize int) {
	t.Helper()

	w := hook.NewWorker(q, push, batchSize, make(chan struct{}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})

	go func() {
		defer close(done)

		w.Run(ctx)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for drain to finish")
	}
}

func remaining(t *testing.T, q *hook.Queue) []string {
	t.Helper()

	paths, err := q.FetchBatch(1000)
	if err != nil {
		t.Fatal(err)
	}

	return paths
}

func enqueueFiles(t *testing.T, q *hook.Queue, names ...string) []string {
	t.Helper()

	dir := t.TempDir()
	paths := make([]string, len(names))

	for i, n := range names {
		paths[i] = writeTestFile(t, dir, n)
	}

	if err := q.Enqueue(paths); err != nil {
		t.Fatal(err)
	}

	return paths
}

var errUpload = errors.New("upload failed")

// poisonPush fails any push containing poison and uploads everything else.
func poisonPush(poison string, calls *atomic.Int32) hook.PushFunc {
	return func(_ context.Context, paths []string) ([]string, error) {
		calls.Add(1)

		if slices.Contains(paths, poison) {
			return nil, errUpload
		}

		return paths, nil
	}
}

// A single unuploadable path in a batch must cost only itself on the final
// drain; on an ephemeral CI runner the rest of the queue is otherwise lost.
func TestDrainIsolatesPoisonPath(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)
	paths := enqueueFiles(t, q, "aaa", "bbb", "ccc", "ddd")
	poison := paths[1]

	var calls atomic.Int32

	drainWorker(t, q, poisonPush(poison, &calls), 10)

	if left := remaining(t, q); len(left) != 1 || left[0] != poison {
		t.Errorf("expected only %s left, got %v", poison, left)
	}
}

// During normal operation a permanently failing path at the head of the queue
// must not block paths behind it until the next restart.
func TestRunNotBlockedByPoisonHead(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)
	paths := enqueueFiles(t, q, "aaa", "bbb", "ccc")
	poison := paths[0]

	var calls atomic.Int32

	stop := startWorker(t, q, poisonPush(poison, &calls), 1)
	waitForCount(t, q, 1)
	stop()

	if left := remaining(t, q); len(left) != 1 || left[0] != poison {
		t.Errorf("expected only %s left, got %v", poison, left)
	}
}

// When nothing at all goes through, drain must terminate on its own, keep
// every path for a later start and not retry each path individually forever.
func TestDrainGivesUpWhenServerDown(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)
	paths := enqueueFiles(t, q, "a", "b", "c", "d", "e", "f", "g", "h", "i", "j")

	var calls atomic.Int32

	push := func(_ context.Context, _ []string) ([]string, error) {
		calls.Add(1)

		return nil, errUpload
	}

	drainWorker(t, q, push, 2)

	if left := remaining(t, q); len(left) != len(paths) {
		t.Errorf("expected all %d paths kept, got %d", len(paths), len(left))
	}

	// 3 stalled batches × (1 batch push + 2 single-path retries), then stop.
	if n := calls.Load(); n != 9 {
		t.Errorf("expected drain to back off from a dead server, got %d push calls", n)
	}
}

// A path that failed on its own can still go up later as part of a parent's
// closure and must be removed from the queue then.
func TestFailedPathPrunedByLaterClosure(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)
	paths := enqueueFiles(t, q, "dep", "top", "last")
	dep, top := paths[0], paths[1]

	push := func(_ context.Context, batch []string) ([]string, error) {
		if slices.Contains(batch, top) {
			return []string{dep, top}, nil
		}

		if slices.Contains(batch, dep) {
			return nil, errUpload
		}

		return batch, nil
	}

	drainWorker(t, q, push, 1)

	if left := remaining(t, q); len(left) != 0 {
		t.Errorf("expected empty queue, got %v", left)
	}
}

func TestWorkerUploadsAndRemoves(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)

	dir := t.TempDir()
	p1 := writeTestFile(t, dir, "aaa")
	p2 := writeTestFile(t, dir, "bbb")

	if err := q.Enqueue([]string{p1, p2}); err != nil {
		t.Fatal(err)
	}

	push, batches := recordingPush()
	runWorkerUntilDrained(t, q, push, 10)

	pushed := batches()
	if len(pushed) != 1 {
		t.Fatalf("expected 1 push call, got %d", len(pushed))
	}

	if len(pushed[0]) != 2 {
		t.Errorf("expected 2 paths in batch, got %d", len(pushed[0]))
	}
}

func TestWorkerSkipsGCdPaths(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)

	// Create one real file and one nonexistent path.
	dir := t.TempDir()
	existing := writeTestFile(t, dir, "existing")
	gcedPath := filepath.Join(dir, "nonexistent")

	if err := q.Enqueue([]string{existing, gcedPath}); err != nil {
		t.Fatal(err)
	}

	push, batches := recordingPush()
	runWorkerUntilDrained(t, q, push, 10)

	pushed := batches()
	if len(pushed) != 1 {
		t.Fatalf("expected 1 push call, got %d", len(pushed))
	}

	// Only the existing path should have been pushed.
	if len(pushed[0]) != 1 || pushed[0][0] != existing {
		t.Errorf("expected [%s], got %v", existing, pushed[0])
	}
}

// TestWorkerPrunesClosureDeps verifies that when push returns closure paths
// beyond the batch, those extra paths are also removed from the queue.
func TestWorkerPrunesClosureDeps(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)

	dir := t.TempDir()
	depPath := writeTestFile(t, dir, "dep")
	topPath := writeTestFile(t, dir, "top")

	// Queue both the dependency and the top-level path.
	if err := q.Enqueue([]string{depPath, topPath}); err != nil {
		t.Fatal(err)
	}

	// push is called with batch_size=1, so only "dep" is in the first batch.
	// But it returns both paths as the closure, simulating that "top" was
	// uploaded as a dependency.
	push := func(_ context.Context, _ []string) ([]string, error) {
		// Return the full closure regardless of which paths were requested.
		return []string{depPath, topPath}, nil
	}

	runWorkerUntilDrained(t, q, push, 1)

	// Both paths should have been removed even though only one was in the batch.
	count, err := q.Count()
	if err != nil {
		t.Fatal(err)
	}

	if count != 0 {
		t.Errorf("expected empty queue, got %d remaining", count)
	}
}
