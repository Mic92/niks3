//nolint:contextcheck // Queue methods use background context internally; SQLite ops are local and fast.
package hook

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"time"
)

const (
	defaultPollInterval = 30 * time.Second
	maxBackoff          = 15 * time.Minute
	initialBackoff      = 1 * time.Second
	defaultBatchSize    = 50
	queueLogInterval    = 30 * time.Second
	// isolationProbes is how many paths of a failed batch are tried on their
	// own before concluding the server, not the paths, is the problem.
	isolationProbes = 3
)

// PushFunc is called by the worker to upload store paths. It returns the
// full set of store paths that were part of the uploaded closures, which
// may be a superset of paths (including transitive dependencies). The
// worker uses this to prune the queue of dependency paths that were
// uploaded as part of a parent closure.
type PushFunc func(ctx context.Context, paths []string) (uploaded []string, err error)

// Worker fetches paths from the queue and uploads them.
type Worker struct {
	queue     *Queue
	push      PushFunc
	batchSize int
	notify    <-chan struct{} // Woken on enqueue.

	DrainTimeout time.Duration // 0 = unbounded
}

// NewWorker creates a Worker that reads from queue and calls push for each batch.
// notify should be a channel that receives a value whenever new paths are enqueued.
func NewWorker(queue *Queue, push PushFunc, batchSize int, notify <-chan struct{}) *Worker {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	return &Worker{
		queue:     queue,
		push:      push,
		batchSize: batchSize,
		notify:    notify,
	}
}

// QueueEmpty reports whether the queue has no pending paths.
func (w *Worker) QueueEmpty() bool {
	count, err := w.queue.Count()
	if err != nil {
		return false // Assume non-empty on error.
	}

	return count == 0
}

// Run processes the queue until ctx is cancelled, then makes one final pass
// over everything still queued (see drain).
//
// Pushes do not run on ctx itself: cancelling a push in flight only to have
// the drain start the same batch over from scratch wastes the work done so
// far, and under systemd's stop timeout can mean the batch never completes.
// Instead cancellation stops the loop after the current step, and
// DrainTimeout, counted from the cancellation, bounds the in-flight push and
// the drain together.
func (w *Worker) Run(ctx context.Context) {
	pushCtx, cancelPush := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelPush()

	stopTimer := context.AfterFunc(ctx, func() {
		if w.DrainTimeout > 0 {
			time.AfterFunc(w.DrainTimeout, cancelPush)
		}
	})
	defer stopTimer()

	backoff := time.Duration(0)

	var lastQueueLog time.Time

	for {
		wait := defaultPollInterval
		if backoff > 0 {
			wait = backoff
		}

		select {
		case <-ctx.Done():
			w.drain(pushCtx)

			return
		case <-time.After(wait):
		case <-w.notify:
		}

		for {
			if ctx.Err() != nil {
				w.drain(pushCtx)

				return
			}

			if time.Since(lastQueueLog) > queueLogInterval {
				if count, err := w.queue.Count(); err == nil && count > 0 {
					slog.Info("Upload queue status", "pending", count)
				}

				lastQueueLog = time.Now()
			}

			batch, progress := w.step(pushCtx)
			if !progress {
				backoff = nextBackoff(backoff)

				break
			}

			backoff = 0

			if len(batch) == 0 {
				break
			}
		}
	}
}

// drain is the last pass before exit. Under systemd whatever is left is picked
// up on the next start, but on an ephemeral CI runner the queue dies with the
// job, so it keeps going past failures. It gives up once a few batches in a
// row make no progress, since that points at the server rather than at
// individual paths; retried paths sort behind untried ones, so little is lost.
//
// Unbounded by default since systemd enforces TimeoutStopSec; DrainTimeout is
// for unsupervised runs and ends ctx (see Run). Either way an external SIGKILL
// remains the backstop.
func (w *Worker) drain(ctx context.Context) {
	stalled := 0

	for stalled < isolationProbes && ctx.Err() == nil {
		batch, progress := w.step(ctx)
		if len(batch) == 0 {
			break
		}

		if progress {
			stalled = 0
		} else {
			stalled++
		}
	}

	if count, err := w.queue.Count(); err == nil && count > 0 {
		slog.Error("Drain finished with paths left in queue", "remaining", count)
	}
}

// step fetches and uploads one batch. It returns the fetched batch (empty when
// the queue is exhausted or unreadable) and whether anything in it could be
// settled.
func (w *Worker) step(ctx context.Context) ([]string, bool) {
	paths, err := w.queue.FetchBatch(w.batchSize)
	if err != nil {
		slog.Error("Failed to fetch batch from queue", "error", err)

		return nil, false
	}

	if len(paths) == 0 {
		return nil, true
	}

	var existing, gced, unreadable []string

	for _, p := range paths {
		// Lstat: a store path may itself be a symlink, and its target need
		// not exist for the path to be valid (or even for it to be a symlink
		// into the store). Following it would drop such a path as collected.
		_, err := os.Lstat(p)

		switch {
		case err == nil:
			existing = append(existing, p)
		case errors.Is(err, fs.ErrNotExist):
			slog.Warn("Store path no longer exists (garbage collected?), removing from queue", "path", p)

			gced = append(gced, p)
		default:
			// Not proof the path is gone (EIO, EACCES, an unmounted store);
			// dropping it would lose the upload. Try again later.
			slog.Warn("Cannot stat store path, will retry later", "path", p, "error", err)

			unreadable = append(unreadable, p)
		}
	}

	w.retry(unreadable)

	// Progress means something left the queue. A failed removal is not
	// progress: counting it as such would spin on the same batch with no
	// backoff and keep the shutdown drain from ever giving up.
	progress := len(gced) > 0 && w.remove(gced)

	if len(existing) > 0 {
		progress = w.upload(ctx, existing) || progress
	}

	return paths, progress
}

// upload pushes a batch; uploaded paths (and their closure) are removed from
// the queue, failed ones moved to its back. If the batch fails as a whole it is
// retried path by path so that a single bad path only costs itself. After
// isolationProbes failures without any success the server is assumed to be the
// problem and the rest is left in place for the next round.
func (w *Worker) upload(ctx context.Context, batch []string) bool {
	slog.Info("Uploading batch", "count", len(batch))

	uploaded, err := w.push(ctx, batch)
	if err == nil {
		_, settled := w.settle(batch, uploaded)

		return settled
	}

	slog.Error("Upload failed", "error", err, "count", len(batch))

	// Cancelled/timed out: not the paths' fault, leave them where they are.
	if ctx.Err() != nil {
		return false
	}

	if len(batch) == 1 {
		w.retry(batch)

		return false
	}

	done := make(map[string]struct{})
	failures := 0

	for i, p := range batch {
		// Cancelled mid-isolation: the remaining probes would all fail at
		// once and, retried, move their paths behind everything else for no
		// fault of their own.
		if ctx.Err() != nil {
			return len(done) > 0
		}

		if _, ok := done[p]; ok {
			continue
		}

		if len(done) == 0 && failures >= isolationProbes {
			slog.Error("Server seems unavailable, giving up on batch", "untried", len(batch)-i)

			return false
		}

		uploaded, err := w.push(ctx, []string{p})
		if err != nil {
			// Same rule as for the batch: a probe cut short by cancellation
			// says nothing about its path.
			if ctx.Err() != nil {
				return len(done) > 0
			}

			slog.Error("Upload failed, will retry later", "error", err, "path", p)
			w.retry([]string{p})

			failures++

			continue
		}

		settled, ok := w.settle([]string{p}, uploaded)
		if !ok {
			continue
		}

		for _, r := range settled {
			done[r] = struct{}{}
		}
	}

	return len(done) > 0
}

// settle removes an uploaded batch and its closure from the queue and returns
// what was removed and whether the removal succeeded. Removing the whole
// closure prunes dependencies that were queued separately but went up as
// part of a parent. The batch is always removed: push reports the closures
// it uploaded, not the paths it found already cached, so a cached path
// batched with a larger closure would otherwise stay queued for good.
func (w *Worker) settle(batch, uploaded []string) ([]string, bool) {
	seen := make(map[string]struct{}, len(batch)+len(uploaded))
	toRemove := make([]string, 0, len(batch)+len(uploaded))

	for _, paths := range [][]string{batch, uploaded} {
		for _, p := range paths {
			if _, dup := seen[p]; dup {
				continue
			}

			seen[p] = struct{}{}
			toRemove = append(toRemove, p)
		}
	}

	return toRemove, w.remove(toRemove)
}

// remove deletes paths from the queue and reports whether it succeeded.
func (w *Worker) remove(paths []string) bool {
	if err := w.queue.Remove(paths); err != nil {
		slog.Error("Failed to remove paths from queue", "error", err, "count", len(paths))

		return false
	}

	return true
}

func (w *Worker) retry(paths []string) {
	if err := w.queue.Retry(paths); err != nil {
		slog.Error("Failed to requeue paths", "error", err, "count", len(paths))
	}
}

func nextBackoff(current time.Duration) time.Duration {
	if current == 0 {
		return initialBackoff
	}

	next := current * 2
	if next > maxBackoff {
		return maxBackoff
	}

	return next
}
