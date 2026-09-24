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
	"slices"
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

// A send accepted just before shutdown is acknowledged only once its paths
// are queued, so the final drain must start after Serve has returned and
// push them. Starting the drain with the listener close let it find the
// queue empty and finish first, and the process exited with an acknowledged
// path left behind.
func TestServeThenDrainPushesSendAcceptedBeforeShutdown(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	queue, err := hook.OpenQueue(filepath.Join(dir, "queue.db"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = queue.Close() })

	storePath := filepath.Join(dir, "aaa-hello")
	if err := os.WriteFile(storePath, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}

	var (
		mu     sync.Mutex
		pushed []string
	)

	push := func(_ context.Context, paths []string) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()

		pushed = append(pushed, paths...)

		return paths, nil
	}

	notify := make(chan struct{}, 1)
	entered := make(chan struct{})
	release := make(chan struct{})

	// The send's paths are committed to the queue only after shutdown has
	// begun, as with a send accepted just before the listener closed.
	queueFunc := func(paths []string) error {
		close(entered)
		<-release

		if err := queue.Enqueue(paths); err != nil {
			return err //nolint:wrapcheck // test
		}

		select {
		case notify <- struct{}{}:
		default:
		}

		return nil
	}

	socketPath := filepath.Join(dir, "hook.sock")

	lc := net.ListenConfig{}

	ln, err := lc.Listen(t.Context(), "unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}

	srv := hook.NewServer(ln, queueFunc)
	worker := hook.NewWorker(queue, push, 10, notify)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan struct{})

	go func() {
		defer close(done)

		serveThenDrain(ctx, srv, worker)
	}()

	sent := make(chan error, 1)

	go func() { sent <- hook.SendPaths(socketPath, []string{storePath}) }()

	<-entered
	cancel()

	// A drain started by the cancellation finds the queue empty and is
	// done well within this; the fixed drain waits for Serve instead.
	time.Sleep(100 * time.Millisecond)
	close(release)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("serveThenDrain did not return after shutdown")
	}

	if err := <-sent; err != nil {
		t.Fatalf("send was not acknowledged: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if !slices.Contains(pushed, storePath) {
		t.Errorf("acknowledged path was not pushed on shutdown (pushed %v)", pushed)
	}

	if count, err := queue.Count(); err != nil || count != 0 {
		t.Errorf("queue holds %d paths after shutdown (err %v), want 0", count, err)
	}
}
