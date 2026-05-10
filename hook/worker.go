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

// Run processes the queue until ctx is cancelled. It finishes any in-flight
// upload before returning.
func (w *Worker) Run(ctx context.Context) {
	backoff := time.Duration(0)

	var lastQueueLog time.Time

	for {
		// Wait for notification, poll timer, or context cancellation.
		if backoff > 0 {
			select {
			case <-ctx.Done():
				w.drain()

				return
			case <-time.After(backoff):
			case <-w.notify:
			}
		} else {
			select {
			case <-ctx.Done():
				w.drain()

				return
			case <-time.After(defaultPollInterval):
			case <-w.notify:
			}
		}

		// Process all available batches.
		for {
			if ctx.Err() != nil {
				w.drain()

				return
			}

			paths, err := w.queue.FetchBatch(w.batchSize)
			if err != nil {
				slog.Error("Failed to fetch batch from queue", "error", err)

				backoff = nextBackoff(backoff)

				break
			}

			if len(paths) == 0 {
				// Queue is empty — reset backoff and go back to waiting.
				backoff = 0

				break
			}

			// Log queue size periodically.
			if time.Since(lastQueueLog) > queueLogInterval {
				if count, err := w.queue.Count(); err == nil && count > 0 {
					slog.Info("Upload queue status", "pending", count)
				}

				lastQueueLog = time.Now()
			}

			// Check which paths still exist locally.
			var existing []string

			var gcedPaths []string

			for _, p := range paths {
				if _, err := os.Stat(p); err != nil {
					slog.Warn("Store path no longer exists (garbage collected?), removing from queue", "path", p)
					gcedPaths = append(gcedPaths, p)
				} else {
					existing = append(existing, p)
				}
			}

			// Remove GC'd paths from queue.
			if len(gcedPaths) > 0 {
				if err := w.queue.Remove(gcedPaths); err != nil {
					slog.Error("Failed to remove GC'd paths from queue", "error", err)
				}
			}

			if len(existing) == 0 {
				// All paths in this batch were GC'd; fetch next batch.
				continue
			}

			slog.Info("Uploading batch", "count", len(existing))

			uploaded, err := w.push(ctx, existing)
			if err != nil {
				slog.Error("Upload failed", "error", err, "count", len(existing))

				backoff = nextBackoff(backoff)

				break
			}

			// Remove all closure paths from queue, not just the batch.
			// This prunes dependency paths that were uploaded as part of
			// a parent's closure, avoiding redundant uploads later.
			toRemove := existing
			if len(uploaded) > len(existing) {
				toRemove = uploaded
				slog.Debug("Pruning dependency paths from queue",
					"batch", len(existing), "closure", len(uploaded))
			}

			if err := w.queue.Remove(toRemove); err != nil {
				slog.Error("Failed to remove uploaded paths from queue", "error", err)
			}

			backoff = 0
		}
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

// drain processes remaining queue entries with a background context.
func (w *Worker) drain() {
	for {
		paths, err := w.queue.FetchBatch(w.batchSize)
		if err != nil || len(paths) == 0 {
			return
		}

		// Check which paths still exist.
		var existing []string

		var gcedPaths []string

		for _, p := range paths {
			if _, err := os.Stat(p); err != nil {
				gcedPaths = append(gcedPaths, p)
			} else {
				existing = append(existing, p)
			}
		}

		if len(gcedPaths) > 0 {
			_ = w.queue.Remove(gcedPaths)
		}

		if len(existing) == 0 {
			continue
		}

		// No timeout: the supervisor (systemd / CI post step) enforces the
		// shutdown budget and SIGKILLs on expiry.
		uploaded, err := w.push(context.Background(), existing)
		if err != nil {
			slog.Error("Drain upload failed, paths remain in queue for next start", "error", err, "count", len(existing))

			return
		}

		toRemove := existing
		if len(uploaded) > len(existing) {
			toRemove = uploaded
		}

		_ = w.queue.Remove(toRemove)
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
