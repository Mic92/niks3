package hook_test

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/Mic92/niks3/hook"
)

func newTestQueue(t *testing.T) *hook.Queue {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")

	q, err := hook.OpenQueue(dbPath)
	if err != nil {
		t.Fatalf("OpenQueue: %v", err)
	}

	t.Cleanup(func() { _ = q.Close() })

	return q
}

func TestQueueEnqueueAndFetch(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)

	paths := []string{"/nix/store/aaa", "/nix/store/bbb", "/nix/store/ccc"}
	if err := q.Enqueue(paths); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	count, err := q.Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}

	if count != 3 {
		t.Fatalf("expected 3, got %d", count)
	}

	fetched, err := q.FetchBatch(10)
	if err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}

	sort.Strings(fetched)
	sort.Strings(paths)

	if len(fetched) != len(paths) {
		t.Fatalf("expected %d, got %d", len(paths), len(fetched))
	}

	for i := range paths {
		if fetched[i] != paths[i] {
			t.Errorf("path %d: expected %q, got %q", i, paths[i], fetched[i])
		}
	}
}

func TestQueueDeduplication(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)

	if err := q.Enqueue([]string{"/nix/store/aaa", "/nix/store/bbb"}); err != nil {
		t.Fatalf("Enqueue 1: %v", err)
	}

	// Enqueue again with overlap.
	if err := q.Enqueue([]string{"/nix/store/bbb", "/nix/store/ccc"}); err != nil {
		t.Fatalf("Enqueue 2: %v", err)
	}

	count, err := q.Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}

	if count != 3 {
		t.Errorf("expected 3 (deduplicated), got %d", count)
	}
}

func TestQueueRemove(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)

	if err := q.Enqueue([]string{"/nix/store/aaa", "/nix/store/bbb", "/nix/store/ccc"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := q.Remove([]string{"/nix/store/aaa", "/nix/store/ccc"}); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	fetched, err := q.FetchBatch(10)
	if err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}

	if len(fetched) != 1 || fetched[0] != "/nix/store/bbb" {
		t.Errorf("expected [/nix/store/bbb], got %v", fetched)
	}
}

func TestQueueFetchBatchLimit(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)

	paths := []string{"/nix/store/aaa", "/nix/store/bbb", "/nix/store/ccc", "/nix/store/ddd"}
	if err := q.Enqueue(paths); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	fetched, err := q.FetchBatch(2)
	if err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}

	if len(fetched) != 2 {
		t.Errorf("expected 2, got %d", len(fetched))
	}
}

func TestQueueFetchRemoveLifecycle(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)

	if err := q.Enqueue([]string{"/nix/store/aaa", "/nix/store/bbb"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	batch, err := q.FetchBatch(10)
	if err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}

	if len(batch) != 2 {
		t.Fatalf("expected 2, got %d", len(batch))
	}

	// Simulate successful upload.
	if err := q.Remove(batch); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	count, err := q.Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}

	if count != 0 {
		t.Errorf("expected 0 after remove, got %d", count)
	}
}
