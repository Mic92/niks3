package client

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"
)

// PushFunc is called by the Listener to push a batch of store paths.
type PushFunc func(ctx context.Context, paths []string) error

// ListenerConfig controls the batching and error behavior of a Listener.
type ListenerConfig struct {
	BatchSize    int
	BatchTimeout time.Duration
	IdleTimeout  time.Duration // 0 = disabled
	MaxErrors    int
	DrainTimeout time.Duration // timeout for final push on shutdown; 0 = 5s default
}

// Listener receives store paths from a unix datagram socket and pushes them in batches.
type Listener struct {
	conn   net.PacketConn
	push   PushFunc
	config ListenerConfig
}

// NewListener creates a new Listener that reads paths from conn and pushes them using push.
func NewListener(conn net.PacketConn, push PushFunc, config ListenerConfig) *Listener {
	return &Listener{
		conn:   conn,
		push:   push,
		config: config,
	}
}

// Run reads store paths from the socket and pushes them in batches.
// It returns nil on context cancellation or idle timeout, or an error after MaxErrors consecutive push failures.
//
// Deduplication is performed within each batch: if the same path arrives multiple times
// before the batch is pushed, it is only pushed once. Paths arriving in separate batches
// are not deduplicated here; the server handles idempotent uploads.
//
// The caller must close the underlying connection after Run returns to ensure the
// readLoop goroutine exits.
func (l *Listener) Run(ctx context.Context) error {
	pathCh := make(chan string, l.config.BatchSize)

	// Start a goroutine to read datagrams from the socket.
	go l.readLoop(ctx, pathCh)

	batch := make(map[string]struct{})
	consecutiveErrors := 0

	var batchTimer *time.Timer
	var batchTimerC <-chan time.Time

	var idleTimer *time.Timer
	var idleTimerC <-chan time.Time

	if l.config.IdleTimeout > 0 {
		idleTimer = time.NewTimer(l.config.IdleTimeout)
		idleTimerC = idleTimer.C
		defer idleTimer.Stop()
	}

	resetBatchTimer := func() {
		if batchTimer != nil {
			batchTimer.Stop()
		}
		batchTimer = time.NewTimer(l.config.BatchTimeout)
		batchTimerC = batchTimer.C
	}

	stopBatchTimer := func() {
		if batchTimer != nil {
			batchTimer.Stop()
			batchTimer = nil
			batchTimerC = nil
		}
	}

	resetIdleTimer := func() {
		if idleTimer != nil {
			idleTimer.Reset(l.config.IdleTimeout)
		}
	}

	pushBatch := func(pushCtx context.Context) error {
		if len(batch) == 0 {
			return nil
		}

		paths := make([]string, 0, len(batch))
		for p := range batch {
			paths = append(paths, p)
		}

		slog.Info("Pushing batch", "count", len(paths))

		if err := l.push(pushCtx, paths); err != nil {
			consecutiveErrors++
			slog.Error("Push failed", "error", err, "consecutive_errors", consecutiveErrors)

			if l.config.MaxErrors > 0 && consecutiveErrors >= l.config.MaxErrors {
				return fmt.Errorf("reached %d consecutive push errors: %w", consecutiveErrors, err)
			}

			// Restart the batch timer to retry the failed batch.
			resetBatchTimer()

			return nil
		}

		consecutiveErrors = 0
		batch = make(map[string]struct{})
		stopBatchTimer()

		return nil
	}

	drainTimeout := l.config.DrainTimeout
	if drainTimeout == 0 {
		drainTimeout = 5 * time.Second
	}

	// drainBatch pushes remaining paths with a fresh context, since the
	// original context may already be cancelled during shutdown.
	drainBatch := func() {
		if len(batch) == 0 {
			return
		}
		drainCtx, drainCancel := context.WithTimeout(context.Background(), drainTimeout)
		defer drainCancel()
		if err := pushBatch(drainCtx); err != nil {
			slog.Error("Drain push failed", "error", err)
		}
	}

	for {
		select {
		case path, ok := <-pathCh:
			if !ok {
				// Channel closed (read loop exited), push remaining and return.
				drainBatch()
				return nil
			}

			batch[path] = struct{}{}
			resetIdleTimer()

			if len(batch) == 1 {
				resetBatchTimer()
			}

			if len(batch) >= l.config.BatchSize {
				if err := pushBatch(ctx); err != nil {
					return err
				}
			}

		case <-batchTimerC:
			if err := pushBatch(ctx); err != nil {
				return err
			}

		case <-idleTimerC:
			slog.Info("Idle timeout reached, shutting down")
			drainBatch()
			return nil

		case <-ctx.Done():
			slog.Info("Context cancelled, shutting down")
			drainBatch()
			return nil
		}
	}
}

// readLoop reads datagrams from the socket and sends path strings to pathCh.
// It closes pathCh when the context is cancelled or an error occurs.
func (l *Listener) readLoop(ctx context.Context, pathCh chan<- string) {
	defer close(pathCh)

	buf := make([]byte, 4096) // Store paths are well under 4KB

	for {
		// Check context before blocking on read.
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Set a read deadline so we can check context periodically.
		if err := l.conn.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
			slog.Error("Failed to set read deadline", "error", err)
			return
		}

		n, _, err := l.conn.ReadFrom(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}

			// If the context is done, this is expected.
			select {
			case <-ctx.Done():
				return
			default:
			}

			slog.Error("Failed to read from socket", "error", err)
			return
		}

		if n > 0 {
			path := string(buf[:n])
			select {
			case pathCh <- path:
			case <-ctx.Done():
				return
			}
		}
	}
}
