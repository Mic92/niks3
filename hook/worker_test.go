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

func TestWorkerUploadsAndRemoves(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)
	notify := make(chan struct{}, 1)

	// Create temp files to simulate existing store paths.
	dir := t.TempDir()
	p1 := filepath.Join(dir, "aaa")
	p2 := filepath.Join(dir, "bbb")

	if err := os.WriteFile(p1, []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(p2, []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := q.Enqueue([]string{p1, p2}); err != nil {
		t.Fatal(err)
	}

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

	w := hook.NewWorker(q, push, 10, notify)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		defer close(done)

		w.Run(ctx)
	}()

	// Notify worker that there's work.
	notify <- struct{}{}

	// Wait for the upload to complete.
	deadline := time.After(5 * time.Second)

	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for upload")
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

	mu.Lock()
	defer mu.Unlock()

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
	notify := make(chan struct{}, 1)

	// Create one real file and one nonexistent path.
	dir := t.TempDir()
	existing := filepath.Join(dir, "existing")

	if err := os.WriteFile(existing, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	gcedPath := filepath.Join(dir, "nonexistent")

	if err := q.Enqueue([]string{existing, gcedPath}); err != nil {
		t.Fatal(err)
	}

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

	w := hook.NewWorker(q, push, 10, notify)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		defer close(done)

		w.Run(ctx)
	}()

	notify <- struct{}{}

	// Wait for queue to drain.
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

	mu.Lock()
	defer mu.Unlock()

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
	notify := make(chan struct{}, 1)

	dir := t.TempDir()
	depPath := filepath.Join(dir, "dep")
	topPath := filepath.Join(dir, "top")

	if err := os.WriteFile(depPath, []byte("d"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(topPath, []byte("t"), 0o600); err != nil {
		t.Fatal(err)
	}

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

	w := hook.NewWorker(q, push, 1, notify)

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

	// Both paths should have been removed even though only one was in the batch.
	count, err := q.Count()
	if err != nil {
		t.Fatal(err)
	}

	if count != 0 {
		t.Errorf("expected empty queue, got %d remaining", count)
	}
}
