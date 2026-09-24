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

	return startWorkerTimeout(t, q, push, batchSize, 0)
}

// startWorkerTimeout is startWorker with a DrainTimeout budget.
func startWorkerTimeout(t *testing.T, q *hook.Queue, push hook.PushFunc, batchSize int, drainTimeout time.Duration) func() {
	t.Helper()

	notify := make(chan struct{}, 1)
	w := hook.NewWorker(q, push, batchSize, notify)
	w.DrainTimeout = drainTimeout

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

	drainWorkerTimeout(t, q, push, batchSize, 0)
}

// drainWorkerTimeout is drainWorker with a DrainTimeout budget.
func drainWorkerTimeout(
	t *testing.T,
	q *hook.Queue,
	push hook.PushFunc,
	batchSize int,
	drainTimeout time.Duration,
) {
	t.Helper()

	w := hook.NewWorker(q, push, batchSize, make(chan struct{}))
	w.DrainTimeout = drainTimeout

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

	// One real file, one nonexistent path, and one store path that is a
	// symlink to a target that does not exist. A build output may be such a
	// symlink; the path exists and must be pushed.
	dir := t.TempDir()
	existing := writeTestFile(t, dir, "existing")
	gcedPath := filepath.Join(dir, "nonexistent")
	dangling := filepath.Join(dir, "dangling")

	if err := os.Symlink(filepath.Join(dir, "no-such-target"), dangling); err != nil {
		t.Fatal(err)
	}

	if err := q.Enqueue([]string{existing, gcedPath, dangling}); err != nil {
		t.Fatal(err)
	}

	push, batches := recordingPush()
	runWorkerUntilDrained(t, q, push, 10)

	pushed := batches()
	if len(pushed) != 1 {
		t.Fatalf("expected 1 push call, got %d", len(pushed))
	}

	// The existing path and the dangling symlink are pushed; only the
	// nonexistent path is dropped as collected.
	want := []string{existing, dangling}
	if !slices.Equal(pushed[0], want) {
		t.Errorf("expected %v, got %v", want, pushed[0])
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

// A path the server already has is left out of the closure push reports,
// which only covers what was uploaded. Batched with a larger closure it must
// still leave the queue, or it is fetched and probed again every round.
func TestWorkerRemovesCachedPathBatchedWithLargerClosure(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)

	dir := t.TempDir()
	cached := writeTestFile(t, dir, "cached")
	top := writeTestFile(t, dir, "top")
	dep1 := filepath.Join(dir, "dep1")
	dep2 := filepath.Join(dir, "dep2")

	if err := q.Enqueue([]string{cached, top}); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32

	// "cached" is already on the server and is not part of the reported
	// closure; "top" brings two dependencies, so the report is larger than
	// the batch without containing all of it.
	push := func(_ context.Context, paths []string) ([]string, error) {
		calls.Add(1)

		if !slices.Contains(paths, top) {
			return nil, nil
		}

		return []string{top, dep1, dep2}, nil
	}

	runWorkerUntilDrained(t, q, push, 10)

	if n := calls.Load(); n != 1 {
		t.Errorf("expected the batch to settle in 1 push, took %d", n)
	}
}

// A hung server must not keep an unsupervised drain alive forever, and the
// interrupted paths must stay queued in their original order.
func TestDrainTimeout(t *testing.T) {
	t.Parallel()

	q := newTestQueue(t)
	paths := enqueueFiles(t, q, "a", "b", "c", "d")

	var calls atomic.Int32

	push := func(ctx context.Context, _ []string) ([]string, error) {
		calls.Add(1)
		<-ctx.Done()

		return nil, ctx.Err()
	}

	start := time.Now()

	drainWorkerTimeout(t, q, push, 2, 200*time.Millisecond)

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("drain ignored its timeout, took %s", elapsed)
	}

	if n := calls.Load(); n != 1 {
		t.Errorf("expected no isolation probes after timeout, got %d pushes", n)
	}

	if left := remaining(t, q); !slices.Equal(left, paths) {
		t.Errorf("expected queue untouched %v, got %v", paths, left)
	}
}

// The timeout can also strike between isolation probes. The probes not yet
// made must not be counted as failures nor have their paths moved to the back
// of the queue: the server, not the paths, ran out of time. That holds
// whether the timeout cuts a probe short or strikes while a probe that then
// succeeds is still running: no further probe may start.
func TestDrainTimeoutDuringIsolation(t *testing.T) {
	t.Parallel()

	const timeout = 200 * time.Millisecond

	for _, tc := range []struct {
		name string
		// probe is the first single-path push.
		probe func(ctx context.Context, batch []string) ([]string, error)
		// left is which of a, b, c, d stay queued, in order.
		left []int
	}{
		{
			name: "probe cut short",
			probe: func(ctx context.Context, _ []string) ([]string, error) {
				<-ctx.Done()

				return nil, ctx.Err()
			},
			left: []int{0, 1, 2, 3},
		},
		{
			name: "probe outlasts the deadline",
			probe: func(_ context.Context, batch []string) ([]string, error) {
				time.Sleep(timeout + 100*time.Millisecond)

				return batch, nil
			},
			left: []int{1, 2, 3},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			q := newTestQueue(t)
			paths := enqueueFiles(t, q, "a", "b", "c", "d")

			var calls atomic.Int32

			push := func(ctx context.Context, batch []string) ([]string, error) {
				n := calls.Add(1)

				switch {
				case len(batch) > 1:
					return nil, errUpload // the batch itself fails at once
				case n == 2:
					return tc.probe(ctx, batch)
				case ctx.Err() != nil:
					return nil, ctx.Err()
				default:
					return batch, nil
				}
			}

			drainWorkerTimeout(t, q, push, 4, timeout)

			if n := calls.Load(); n != 2 {
				t.Errorf("expected the batch and one probe, got %d pushes", n)
			}

			want := make([]string, 0, len(tc.left))
			for _, i := range tc.left {
				want = append(want, paths[i])
			}

			if left := remaining(t, q); !slices.Equal(left, want) {
				t.Errorf("expected queue %v, got %v", want, left)
			}
		})
	}
}

// Shutdown must not abort a push that is in flight: the drain would only
// start the same batch over from scratch, and under a stop timeout a large
// closure then never completes. The drain timeout, counted from the
// shutdown, still bounds a push that hangs.
func TestShutdownFinishesInFlightPush(t *testing.T) {
	t.Parallel()

	t.Run("completes", func(t *testing.T) {
		t.Parallel()

		q := newTestQueue(t)
		enqueueFiles(t, q, "aaa", "bbb")

		var (
			calls   atomic.Int32
			started = make(chan struct{})
			release = make(chan struct{})
			aborted atomic.Bool
		)

		push := func(ctx context.Context, paths []string) ([]string, error) {
			if calls.Add(1) == 1 {
				close(started)
			}

			<-release

			if ctx.Err() != nil {
				aborted.Store(true)

				return nil, ctx.Err()
			}

			return paths, nil
		}

		stop := startWorker(t, q, push, 10)

		<-started

		go func() {
			time.Sleep(100 * time.Millisecond)
			close(release)
		}()

		stop() // cancels while the push is blocked, then waits for the drain

		if aborted.Load() {
			t.Error("shutdown cancelled the push in flight")
		}

		if n := calls.Load(); n != 1 {
			t.Errorf("expected the in-flight push to complete once, got %d pushes", n)
		}

		if left := remaining(t, q); len(left) != 0 {
			t.Errorf("expected empty queue after the push completed, got %v", left)
		}
	})

	// A push that never returns on its own is cancelled DrainTimeout after
	// the shutdown, not after a drain that cannot start until it returns.
	t.Run("hung push bounded by the drain timeout", func(t *testing.T) {
		t.Parallel()

		q := newTestQueue(t)
		paths := enqueueFiles(t, q, "aaa", "bbb")

		var (
			calls   atomic.Int32
			started = make(chan struct{})
		)

		push := func(ctx context.Context, _ []string) ([]string, error) {
			if calls.Add(1) == 1 {
				close(started)
			}

			<-ctx.Done()

			return nil, ctx.Err()
		}

		const drainTimeout = 200 * time.Millisecond

		stop := startWorkerTimeout(t, q, push, 10, drainTimeout)

		<-started

		begin := time.Now()

		stop() // fails the test if the worker is still running after 5s

		if elapsed := time.Since(begin); elapsed < drainTimeout/2 {
			t.Errorf("push in flight was cut off after %v, before the drain timeout", elapsed)
		}

		if n := calls.Load(); n != 1 {
			t.Errorf("expected only the push in flight, got %d pushes", n)
		}

		if left := remaining(t, q); !slices.Equal(left, paths) {
			t.Errorf("expected queue untouched %v, got %v", paths, left)
		}
	})
}

// A batch whose removal from the queue fails is not progress: treating it as
// such spun on the same batch with no backoff and kept the drain from giving
// up. The drain must terminate on its own here rather than run until killed,
// whichever way the paths would have left the queue: dropped as collected,
// settled after the batch pushed, or settled one by one after the batch
// failed and each probe succeeded.
func TestWorkerRemoveFailureIsNotProgress(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		files []string // existing store paths; none means one collected path
		// push fails batches of more than one path when failBatches is set.
		failBatches bool
		// Pushes of three stalled rounds: the batch, plus a probe per path
		// when it fails.
		wantPushes int
	}{
		{name: "collected path", wantPushes: 0},
		{name: "pushed batch", files: []string{"aaa", "bbb"}, wantPushes: 3},
		{name: "isolated paths", files: []string{"aaa", "bbb"}, failBatches: true, wantPushes: 3 * 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			paths := []string{filepath.Join(t.TempDir(), "nonexistent")}
			if tc.files != nil {
				dir := t.TempDir()
				paths = paths[:0]

				for _, name := range tc.files {
					paths = append(paths, writeTestFile(t, dir, name))
				}
			}

			q := readOnlyQueue(t, paths)

			var calls atomic.Int32

			push := func(_ context.Context, batch []string) ([]string, error) {
				calls.Add(1)

				if tc.failBatches && len(batch) > 1 {
					return nil, errUpload
				}

				return batch, nil
			}

			drainWorker(t, q, push, 10)

			if n := calls.Load(); int(n) != tc.wantPushes {
				t.Errorf("drain pushed %d times, want %d: three stalled rounds, then give up", n, tc.wantPushes)
			}

			if left := remaining(t, q); !slices.Equal(left, paths) {
				t.Errorf("queue holds %v, want %v (its removal failed)", left, paths)
			}
		})
	}
}

// readOnlyQueue returns a queue holding paths whose database is read-only:
// reads work, removals fail. It skips the test when that cannot be arranged.
func readOnlyQueue(t *testing.T, paths []string) *hook.Queue {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "queue.db")

	q, err := hook.OpenQueue(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	if err := q.Enqueue(paths); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Chmod(dbPath+suffix, 0o444); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}

	q, err = hook.OpenQueue(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = q.Close() })

	if err := q.Remove(paths[:1]); err == nil {
		t.Skip("queue is writable despite read-only file (running as root?)")
	}

	return q
}

// A stat error other than "not found" is not proof the path is gone, so the
// path stays queued for a later attempt instead of being dropped.
func TestWorkerKeepsPathItCannotStat(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("root can stat anything")
	}

	q := newTestQueue(t)

	locked := filepath.Join(t.TempDir(), "locked")
	if err := os.Mkdir(locked, 0o700); err != nil {
		t.Fatal(err)
	}

	p := writeTestFile(t, locked, "aaa")

	if err := os.Chmod(locked, 0o000); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })

	if err := q.Enqueue([]string{p}); err != nil {
		t.Fatal(err)
	}

	push, batches := recordingPush()
	drainWorker(t, q, push, 10)

	if n := len(batches()); n != 0 {
		t.Errorf("unreadable path was pushed %d times", n)
	}

	count, err := q.Count()
	if err != nil {
		t.Fatal(err)
	}

	if count != 1 {
		t.Errorf("queue holds %d paths, want the unreadable one kept", count)
	}
}
