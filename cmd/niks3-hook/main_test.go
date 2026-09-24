package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Mic92/niks3/hook"
)

// TestMain lets a test run the binary's main in a subprocess: signal
// handling is process-wide and can only be observed from outside.
func TestMain(m *testing.M) {
	if os.Getenv("NIKS3_HOOK_TEST_RUN_MAIN") == "1" {
		main()
		os.Exit(0)
	}

	os.Exit(m.Run())
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}

		time.Sleep(20 * time.Millisecond)
	}
}

// The shutdown drain waits for the push in flight, without a bound unless
// --drain-timeout is set. A second SIGINT or SIGTERM during it must end the
// process instead of being swallowed by the handler installed for the first;
// the queue is on disk and loses nothing.
func TestServeSecondSignalEndsDrain(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	storeDir := filepath.Join(dir, "store")
	socketPath := filepath.Join(dir, "hook.sock")
	tokenPath := filepath.Join(dir, "token")
	storePath := filepath.Join(storeDir, "0123456789abcdfghijklmnpqrsvwxyz-hello")

	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		t.Fatal(err)
	}

	for path, content := range map[string]string{storePath: "hello", tokenPath: "token"} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// A server that never answers: the push hangs until the process ends.
	var (
		pushStarted = make(chan struct{})
		startedOnce sync.Once
		release     = make(chan struct{})
	)

	ts := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		startedOnce.Do(func() { close(pushStarted) })

		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(func() { close(release) })

	cmd := exec.CommandContext(context.Background(), os.Args[0], //nolint:gosec // re-running the test binary
		"serve",
		"--server-url", ts.URL,
		"--auth-token-path", tokenPath,
		"--socket", socketPath,
		"--db-path", filepath.Join(dir, "queue.db"),
		"--idle-exit-timeout", "0",
	)
	cmd.Env = append(os.Environ(), "NIKS3_HOOK_TEST_RUN_MAIN=1", "NIX_STORE_DIR="+storeDir)

	var output strings.Builder

	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	exited := make(chan error, 1)

	go func() { exited <- cmd.Wait() }()

	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-exited

		if t.Failed() {
			t.Logf("daemon output:\n%s", output.String())
		}
	})

	waitFor(t, "the socket", func() bool {
		_, err := os.Stat(socketPath)

		return err == nil
	})

	if err := hook.SendPaths(socketPath, []string{storePath}); err != nil {
		t.Fatalf("SendPaths: %v", err)
	}

	select {
	case <-pushStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("the worker never started pushing")
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	// The listener closes once the first signal is handled; the drain is
	// then waiting for the push.
	dialer := net.Dialer{Timeout: time.Second}

	waitFor(t, "the listener to close", func() bool {
		conn, err := dialer.DialContext(t.Context(), "unix", socketPath)
		if err == nil {
			_ = conn.Close()
		}

		return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, os.ErrNotExist)
	})

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	select {
	case <-exited:
		exited <- nil // for the cleanup
	case <-time.After(5 * time.Second):
		t.Fatal("a second SIGTERM did not end the drain")
	}
}
