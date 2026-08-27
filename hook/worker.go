//nolint:contextcheck // Queue methods use background context internally; SQLite ops are local and fast.
package hook

import (
	"context"
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
func (w *Worker) Run(ctx context.Context) {
	backoff := time.Duration(0)

	var lastQueueLog time.Time

	for {
		wait := defaultPollInterval
		if backoff > 0 {
			wait = backoff
		}

		select {
		case <-ctx.Done():
			w.drain()

			return
		case <-time.After(wait):
		case <-w.notify:
		}

		for {
			if ctx.Err() != nil {
				w.drain()

				return
			}

			if time.Since(lastQueueLog) > queueLogInterval {
				if count, err := w.queue.Count(); err == nil && count > 0 {
					slog.Info("Upload queue status", "pending", count)
				}

				lastQueueLog = time.Now()
			}

			batch, progress := w.step(ctx)
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
// No timeout: the supervisor (systemd / CI post step) enforces the shutdown
// budget and SIGKILLs on expiry.
func (w *Worker) drain() {
	stalled := 0

	for stalled < isolationProbes {
		batch, progress := w.step(context.Background())
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

	var existing, gced []string

	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			slog.Warn("Store path no longer exists (garbage collected?), removing from queue", "path", p)

			gced = append(gced, p)
		} else {
			existing = append(existing, p)
		}
	}

	w.remove(gced)

	if len(existing) == 0 {
		return paths, true
	}

	return paths, w.upload(ctx, existing)
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
		w.settle(batch, uploaded)

		return true
	}

	slog.Error("Upload failed", "error", err, "count", len(batch))

	if len(batch) == 1 {
		w.retry(batch)

		return false
	}

	done := make(map[string]struct{})
	failures := 0

	for i, p := range batch {
		if _, ok := done[p]; ok {
			continue
		}

		if len(done) == 0 && failures >= isolationProbes {
			slog.Error("Server seems unavailable, giving up on batch", "untried", len(batch)-i)

			return false
		}

		uploaded, err := w.push(ctx, []string{p})
		if err != nil {
			slog.Error("Upload failed, will retry later", "error", err, "path", p)
			w.retry([]string{p})

			failures++

			continue
		}

		for _, r := range w.settle([]string{p}, uploaded) {
			done[r] = struct{}{}
		}
	}

	return len(done) > 0
}

// settle removes an uploaded batch and its closure from the queue and returns
// what was removed. Removing the whole closure prunes dependencies that were
// queued separately but went up as part of a parent.
func (w *Worker) settle(batch, uploaded []string) []string {
	toRemove := batch
	if len(uploaded) > len(batch) {
		toRemove = uploaded
	}

	w.remove(toRemove)

	return toRemove
}

func (w *Worker) remove(paths []string) {
	if err := w.queue.Remove(paths); err != nil {
		slog.Error("Failed to remove paths from queue", "error", err, "count", len(paths))
	}
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
