package server_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
)

// newNotifySocket sets up a unixgram socket, points NOTIFY_SOCKET at it, and
// returns the listening connection.
func newNotifySocket(t *testing.T) *net.UnixConn {
	t.Helper()

	socketPath := filepath.Join(t.TempDir(), "notify.sock")

	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Net: "unixgram", Name: socketPath})
	ok(t, err)

	t.Cleanup(func() { _ = conn.Close() })
	t.Setenv("NOTIFY_SOCKET", socketPath)

	return conn
}

func readNotify(t *testing.T, conn *net.UnixConn, timeout time.Duration) (string, bool) {
	t.Helper()

	ok(t, conn.SetReadDeadline(time.Now().Add(timeout)))

	buf := make([]byte, 64)

	n, err := conn.Read(buf)
	if err != nil {
		return "", false
	}

	return string(buf[:n]), true
}

// TestWatchdogBeatsWhenHealthy verifies runWatchdog sends WATCHDOG=1 while the
// liveness check passes, and stops when its context is cancelled.
//
//nolint:paralleltest // newNotifySocket uses t.Setenv, incompatible with t.Parallel
func TestWatchdogBeatsWhenHealthy(t *testing.T) {
	conn := newNotifySocket(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	healthy := func(context.Context) error { return nil }

	go server.RunWatchdogForTest(ctx, 20*time.Millisecond, healthy)

	got, recv := readNotify(t, conn, 2*time.Second)
	if !recv || got != "WATCHDOG=1" {
		t.Fatalf("expected WATCHDOG=1 heartbeat, got %q (received=%v)", got, recv)
	}
}

// TestWatchdogSkipsWhenUnhealthy verifies runWatchdog withholds the heartbeat
// while the liveness check fails, so systemd's timeout fires.
//
//nolint:paralleltest // newNotifySocket uses t.Setenv, incompatible with t.Parallel
func TestWatchdogSkipsWhenUnhealthy(t *testing.T) {
	conn := newNotifySocket(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	unhealthy := func(context.Context) error { return context.DeadlineExceeded }

	go server.RunWatchdogForTest(ctx, 20*time.Millisecond, unhealthy)

	// Over several intervals we must never receive a heartbeat.
	if got, recv := readNotify(t, conn, 200*time.Millisecond); recv {
		t.Fatalf("expected no heartbeat while unhealthy, got %q", got)
	}
}
