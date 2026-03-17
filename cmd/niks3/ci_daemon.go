package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Mic92/niks3/client"
)

const (
	// daemonBatchSize bounds argv length when invoking PushPaths. 32 store
	// paths × ~85 chars ≈ 3KB — far under ARG_MAX, and large enough that
	// PushPaths' internal parallelism (30-way by default) stays saturated.
	daemonBatchSize = 32

	// daemonBatchWait is how long a partial batch waits for more paths
	// before pushing anyway. Post-build-hooks fire in bursts as Nix finishes
	// parallel derivations; 2s catches the burst without adding meaningful
	// latency to the overall job.
	daemonBatchWait = 2 * time.Second
)

// Socket wire protocol commands. Single-word verbs, line-based.
const (
	wireCmdPush = "push" // "push <path> <path>...\n" → "ok\n"
	wireCmdStop = "stop" // "stop\n" → "stats <pushed> <skipped> <queued>\n"
)

// daemonStats tracks push outcomes, reported back on the "stop" command so
// the post-step can emit a ::notice:: summary.
type daemonStats struct {
	mu      sync.Mutex
	queued  int // paths enqueued via "push"
	pushed  int // paths successfully uploaded
	skipped int // paths in a failed batch (cache miss next run, not fatal)
}

func (s *daemonStats) addQueued(n int)  { s.mu.Lock(); s.queued += n; s.mu.Unlock() }
func (s *daemonStats) addPushed(n int)  { s.mu.Lock(); s.pushed += n; s.mu.Unlock() }
func (s *daemonStats) addSkipped(n int) { s.mu.Lock(); s.skipped += n; s.mu.Unlock() }

func (s *daemonStats) snapshot() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.pushed, s.skipped, s.queued
}

// daemonConfig carries everything the batcher needs to construct a fresh
// authenticated client for each batch.
type daemonConfig struct {
	serverURL string
	audience  string
	debug     bool
}

// runCIDaemon starts the upload daemon. It listens on the given Unix socket,
// accepts line-based commands ("push <paths...>", "stop"), and runs a
// background batcher that fetches a fresh OIDC token per batch and uploads
// via client.PushPaths.
//
// Returns when "stop" is received or SIGTERM/SIGINT arrives. The pending
// batch is always flushed before exit.
func runCIDaemon(socket string, cfg daemonConfig) error {
	// Best effort: remove a stale socket from a crashed prior run.
	_ = os.Remove(socket)

	lc := net.ListenConfig{}

	ln, err := lc.Listen(context.Background(), "unix", socket)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", socket, err)
	}

	defer func() { _ = ln.Close() }()
	defer func() { _ = os.Remove(socket) }()

	slog.Info("Niks3 ci daemon started", "socket", socket, "server", cfg.serverURL)

	var (
		stats  daemonStats
		queue  = make(chan []string, 256) // per-hook bundles, not individual paths
		done   = make(chan struct{})      // batcher finished draining
		stopCh = make(chan net.Conn, 1)   // carries the conn to reply to, or nil for SIGTERM
	)

	// Batcher: pulls bundles off the queue, aggregates up to daemonBatchSize
	// paths or daemonBatchWait, pushes. Exits when queue is closed AND drained.
	go func() {
		defer close(done)

		batcher(queue, &stats, cfg)
	}()

	// Signal handling: SIGTERM/SIGINT → drain and exit, same as "stop" but
	// with no client to reply to.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigCh
		slog.Info("Signal received, draining")

		select {
		case stopCh <- nil:
		default:
		}

		_ = ln.Close() // unblocks Accept
	}()

	// Accept loop. Each connection handles exactly one command then closes.
	// Post-build-hooks are short-lived: dial, write, read ack, exit.
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				// Listener closed — stop accepting.
				return
			}

			go handleConn(conn, queue, &stats, stopCh, ln)
		}
	}()

	// Wait for stop (via command or signal), then drain.
	replyConn := <-stopCh

	close(queue)
	<-done

	pushed, skipped, queued := stats.snapshot()
	slog.Info("Niks3 ci daemon drained", "pushed", pushed, "skipped", skipped, "queued", queued)

	if replyConn != nil {
		_, _ = fmt.Fprintf(replyConn, "stats %d %d %d\n", pushed, skipped, queued)
		_ = replyConn.Close()
	}

	return nil
}

// handleConn reads one line command from conn and dispatches it.
// For "push", it enqueues and acks. For "stop", it hands the conn to the
// main goroutine via stopCh (so the reply arrives after drain completes)
// and closes the listener to stop accepting further connections.
func handleConn(conn net.Conn, queue chan<- []string, stats *daemonStats, stopCh chan<- net.Conn, ln net.Listener) {
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		_ = conn.Close()

		return
	}

	fields := strings.Fields(line)
	if len(fields) == 0 {
		_ = conn.Close()

		return
	}

	switch fields[0] {
	case wireCmdPush:
		paths := fields[1:]
		if len(paths) > 0 {
			stats.addQueued(len(paths))

			select {
			case queue <- paths:
			default:
				// Queue full — extremely unlikely (256 buffered bundles),
				// but don't block the hook. Treat as skipped.
				stats.addSkipped(len(paths))
				slog.Warn("Queue full, dropping paths", "count", len(paths))
			}
		}

		_, _ = fmt.Fprintln(conn, "ok")
		_ = conn.Close()

	case wireCmdStop:
		// Hand off the conn; reply goes out after drain.
		// Don't close conn here — main goroutine owns it now.
		select {
		case stopCh <- conn:
			_ = ln.Close() // stop accepting new connections
		default:
			// Already stopping; just ack this late arrival.
			_, _ = fmt.Fprintln(conn, "stats 0 0 0")
			_ = conn.Close()
		}

	default:
		_, _ = fmt.Fprintf(conn, "error unknown command %q\n", fields[0])
		_ = conn.Close()
	}
}

// batcher aggregates path bundles into batches and pushes them. Runs until
// the queue channel is closed AND all buffered bundles have been processed.
func batcher(queue <-chan []string, stats *daemonStats, cfg daemonConfig) {
	var (
		batch []string
		timer *time.Timer // nil when batch is empty
	)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		if err := pushBatch(batch, cfg); err != nil {
			slog.Error("Batch push failed", "paths", len(batch), "error", err)
			stats.addSkipped(len(batch))
		} else {
			slog.Info("Batch pushed", "paths", len(batch))
			stats.addPushed(len(batch))
		}

		batch = nil

		if timer != nil {
			timer.Stop()
			timer = nil
		}
	}

	// timerC returns the timer's channel, or nil if no timer is running.
	// Selecting on a nil channel blocks forever, which is what we want
	// when the batch is empty.
	timerC := func() <-chan time.Time {
		if timer == nil {
			return nil
		}

		return timer.C
	}

	for {
		select {
		case bundle, ok := <-queue:
			if !ok {
				flush()

				return
			}

			if len(batch) == 0 {
				timer = time.NewTimer(daemonBatchWait)
			}

			batch = append(batch, bundle...)

			if len(batch) >= daemonBatchSize {
				flush()
			}

		case <-timerC():
			flush()
		}
	}
}

// pushBatch fetches a fresh OIDC token and uploads the given store paths.
// A fresh token per batch avoids expiry during long jobs (GitHub OIDC tokens
// last roughly 5–10 minutes; VM test jobs can run 30+).
func pushBatch(paths []string, cfg daemonConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	return ciPushWithOIDC(ctx, cfg.serverURL, cfg.audience, paths, cfg.debug)
}

// ciPushWithOIDC fetches a fresh GitHub OIDC token, constructs a client,
// and pushes the given store paths. Shared by daemon batches and storescan.
func ciPushWithOIDC(ctx context.Context, serverURL, audience string, paths []string, debug bool) error {
	src := &client.GitHubOIDCTokenSource{Audience: audience}

	token, err := src.Token(ctx)
	if err != nil {
		return fmt.Errorf("fetching OIDC token: %w", err)
	}

	c, err := client.NewClient(ctx, serverURL, token)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	if debug {
		c.SetDebugHTTP(true)
	}

	if _, err := c.PushPaths(ctx, paths); err != nil {
		return fmt.Errorf("pushing paths: %w", err)
	}

	return nil
}
