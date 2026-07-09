package hook_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
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

// runWorkerUntilDrained runs a worker for the queue until it is empty (or the
// test times out), then shuts the worker down.
func runWorkerUntilDrained(t *testing.T, q *hook.Queue, push hook.PushFunc, batchSize int) {
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

	deadline := time.After(5 * time.Second)

	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for queue drain")
		default:
		}

		count, _ := q.Count()
		if count == 0 {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	<-done
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
