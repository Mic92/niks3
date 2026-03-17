package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestDaemonSocketProtocol exercises the push/stop wire protocol end-to-end
// against a real Unix socket. pushBatch is expected to fail (no OIDC env in
// the test), which is fine — we're verifying queueing, drain ordering, and
// stats accounting, not uploads.
func TestDaemonSocketProtocol(t *testing.T) {
	t.Parallel()

	socket := filepath.Join(t.TempDir(), "d.sock")

	daemonErr := make(chan error, 1)

	go func() {
		daemonErr <- runCIDaemon(socket, daemonConfig{
			serverURL: "http://test.invalid",
			audience:  "test",
		})
	}()

	// Wait for the socket to appear. The daemon removes any stale socket
	// then listens; a successful dial means Accept is ready.
	waitForSocket(t, socket)

	// Push two bundles.
	if err := pushToSocket(socket, []string{"/nix/store/aaa-foo", "/nix/store/bbb-bar"}); err != nil {
		t.Fatalf("first push: %v", err)
	}

	if err := pushToSocket(socket, []string{"/nix/store/ccc-baz"}); err != nil {
		t.Fatalf("second push: %v", err)
	}

	// Stop and collect stats.
	stats := stopAndReadStats(t, socket)

	// 3 paths queued. None pushed (no OIDC), all skipped.
	if stats.queued != 3 {
		t.Errorf("queued = %d, want 3", stats.queued)
	}

	if stats.pushed != 0 {
		t.Errorf("pushed = %d, want 0 (no OIDC in test)", stats.pushed)
	}

	if stats.skipped != 3 {
		t.Errorf("skipped = %d, want 3", stats.skipped)
	}

	// Daemon should exit cleanly.
	select {
	case err := <-daemonErr:
		if err != nil {
			t.Errorf("daemon returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not exit after stop")
	}
}

// TestDaemonStopBlocksUntilDrain ensures the "stats" reply arrives only
// AFTER the batcher has processed everything. We push, then immediately
// stop — if stop replied before drain, stats.skipped would be 0.
func TestDaemonStopBlocksUntilDrain(t *testing.T) {
	t.Parallel()

	socket := filepath.Join(t.TempDir(), "d.sock")

	daemonErr := make(chan error, 1)

	go func() {
		daemonErr <- runCIDaemon(socket, daemonConfig{
			serverURL: "http://test.invalid",
			audience:  "test",
		})
	}()

	waitForSocket(t, socket)

	// Push then immediately stop on a separate connection. The reply to
	// stop must reflect the push, proving drain happened first.
	if err := pushToSocket(socket, []string{"/nix/store/xxx-drain"}); err != nil {
		t.Fatalf("push: %v", err)
	}

	stats := stopAndReadStats(t, socket)

	if stats.queued != 1 || stats.skipped != 1 {
		t.Errorf("stats = queued:%d skipped:%d, want queued:1 skipped:1 (drain must precede reply)",
			stats.queued, stats.skipped)
	}

	<-daemonErr
}

type parsedStats struct {
	pushed, skipped, queued int
}

func stopAndReadStats(t *testing.T, socket string) parsedStats {
	t.Helper()

	d := net.Dialer{Timeout: 5 * time.Second}

	conn, err := d.DialContext(t.Context(), "unix", socket)
	if err != nil {
		t.Fatalf("dial for stop: %v", err)
	}

	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	if _, err := fmt.Fprintln(conn, wireCmdStop); err != nil {
		t.Fatalf("write stop: %v", err)
	}

	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read stats: %v", err)
	}

	var s parsedStats

	n, err := fmt.Sscanf(strings.TrimSpace(reply), "stats %d %d %d", &s.pushed, &s.skipped, &s.queued)
	if err != nil || n != 3 {
		t.Fatalf("bad stats reply %q: %v", reply, err)
	}

	return s
}

func waitForSocket(t *testing.T, socket string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	d := net.Dialer{Timeout: 100 * time.Millisecond}

	for time.Now().Before(deadline) {
		conn, err := d.DialContext(context.Background(), "unix", socket)
		if err == nil {
			_ = conn.Close()

			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("socket %s never became dialable", socket)
}
