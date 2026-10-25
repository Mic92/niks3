package hook_test

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Mic92/niks3/hook"
)

// TestServerClientIntegration tests the full server+client flow: multiple
// concurrent clients send paths, the server queues them, and acks each client.
//
//nolint:paralleltest // t.Chdir is incompatible with t.Parallel
func TestServerClientIntegration(t *testing.T) {
	// macOS limits a socket path to 103 bytes, which the sandbox's temp dir exceeds.
	t.Chdir(t.TempDir())

	socketPath := "test.sock"

	lc := net.ListenConfig{}

	ln, err := lc.Listen(context.Background(), "unix", socketPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var mu sync.Mutex

	var allPaths []string

	queueFunc := func(paths []string) error {
		mu.Lock()
		defer mu.Unlock()

		allPaths = append(allPaths, paths...)

		return nil
	}

	srv := hook.NewServer(ln, queueFunc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = srv.Serve(ctx)
	}()

	// Send from multiple concurrent clients.
	var wg sync.WaitGroup

	for i := range 5 {
		wg.Go(func() {
			paths := []string{"/nix/store/path-" + strconv.Itoa(i)}
			if err := hook.SendPaths(socketPath, paths); err != nil {
				t.Errorf("SendPaths %d: %v", i, err)
			}
		})
	}

	wg.Wait()

	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()

	sort.Strings(allPaths)

	expected := []string{
		"/nix/store/path-0",
		"/nix/store/path-1",
		"/nix/store/path-2",
		"/nix/store/path-3",
		"/nix/store/path-4",
	}

	if len(allPaths) != len(expected) {
		t.Fatalf("expected %d paths, got %d: %v", len(expected), len(allPaths), allPaths)
	}

	for i := range expected {
		if allPaths[i] != expected[i] {
			t.Errorf("path %d: expected %q, got %q", i, expected[i], allPaths[i])
		}
	}
}

// TestServerQueueError verifies that queue errors are propagated back to the client.
//
//nolint:paralleltest // t.Chdir is incompatible with t.Parallel
func TestServerQueueError(t *testing.T) {
	// macOS limits a socket path to 103 bytes, which the sandbox's temp dir exceeds.
	t.Chdir(t.TempDir())

	socketPath := "test.sock"

	lc := net.ListenConfig{}

	ln, err := lc.Listen(context.Background(), "unix", socketPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := hook.NewServer(ln, func(_ []string) error {
		return os.ErrPermission
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = srv.Serve(ctx)
	}()

	err = hook.SendPaths(socketPath, []string{"/nix/store/aaa"})
	if err == nil {
		t.Fatal("expected error from queue failure")
	}

	cancel()
	<-done
}

// TestGetListenerSocketActivation tests the systemd socket activation path.
// Uses a subprocess because dup2 to fd 3 conflicts with Go's runtime netpoller.
func TestGetListenerSocketActivation(t *testing.T) { //nolint:paralleltest // t.Setenv incompatible with t.Parallel
	if runtime.GOOS == "darwin" {
		t.Skip("systemd socket activation does not exist on darwin")
	}

	if os.Getenv("GO_TEST_SOCKET_ACTIVATION") == "1" {
		socketPath := os.Getenv("GO_TEST_SOCKET_PATH")

		t.Setenv("LISTEN_PID", strconv.Itoa(os.Getpid()))
		t.Setenv("LISTEN_FDS", "1")

		ln, activated, err := hook.GetListener(socketPath)
		if err != nil {
			t.Fatalf("GetListener: %v", err)
		}

		defer func() { _ = ln.Close() }()

		if !activated {
			t.Fatal("expected activated=true")
		}

		if os.Getenv("LISTEN_PID") != "" {
			t.Error("LISTEN_PID should have been unset")
		}

		if os.Getenv("LISTEN_FDS") != "" {
			t.Error("LISTEN_FDS should have been unset")
		}

		return
	}

	dir := t.TempDir()
	socketPath := filepath.Join(dir, "activated.sock")

	lc := net.ListenConfig{}

	ln, err := lc.Listen(context.Background(), "unix", socketPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ul, ok := ln.(*net.UnixListener)
	if !ok {
		_ = ln.Close()

		t.Fatal("expected *net.UnixListener")
	}

	f, err := ul.File()
	if err != nil {
		_ = ln.Close()

		t.Fatalf("get file: %v", err)
	}

	_ = ln.Close()

	defer func() { _ = f.Close() }()

	cmd := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^TestGetListenerSocketActivation$", "-test.v") //nolint:gosec // test binary

	cmd.Env = append(
		os.Environ(),
		"GO_TEST_SOCKET_ACTIVATION=1",
		"GO_TEST_SOCKET_PATH="+socketPath,
	)
	cmd.ExtraFiles = []*os.File{f}

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("subprocess failed: %v\n%s", err, output)
	}

	t.Log(string(output))
}

// A client that connects and never sends a request must not keep Serve from
// returning on shutdown.
func TestServerStalledClientDoesNotBlockShutdown(t *testing.T) {
	t.Parallel()

	socketPath := testSocketPath(t, "test.sock")

	lc := net.ListenConfig{}

	ln, err := lc.Listen(context.Background(), "unix", socketPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := hook.NewServer(ln, func(_ []string) error { return nil })
	srv.ConnTimeout = 200 * time.Millisecond

	cancel, done := serveInBackground(t, srv)

	dialer := net.Dialer{}

	stalled, err := dialer.DialContext(t.Context(), "unix", socketPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	defer func() { _ = stalled.Close() }()

	// Accepted in order, so the stalled connection is being handled now.
	if err := hook.SendPaths(socketPath, []string{"/nix/store/aaa"}); err != nil {
		t.Fatalf("send after the stalled client: %v", err)
	}

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return while a client held an idle connection")
	}
}

// A persistent Accept error (EMFILE) must not spin, and shutdown must not
// wait out the backoff.
func TestServerBacksOffOnAcceptErrors(t *testing.T) {
	t.Parallel()

	ln := &failingListener{closed: make(chan struct{}), eighth: make(chan struct{})}
	srv := hook.NewServer(ln, func(_ []string) error { return nil })

	cancel, done := serveInBackground(t, srv)

	// The backoff after the eighth failure is 640ms.
	<-ln.eighth
	cancel()

	start := time.Now()
	<-done

	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Errorf("Serve took %v to return: shutdown waited out the backoff", elapsed)
	}

	if n := ln.calls.Load(); n > 9 {
		t.Errorf("%d accept calls, want at most 9: no backoff between failures", n)
	}
}

// testSocketPath avoids t.TempDir, whose name grows with the test's and can
// exceed the 104 bytes macOS allows for a socket path.
func testSocketPath(t *testing.T, name string) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "hook") //nolint:usetesting // t.TempDir is the path that is too long
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return filepath.Join(dir, name)
}

func serveInBackground(t *testing.T, srv *hook.Server) (context.CancelFunc, <-chan struct{}) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = srv.Serve(ctx)
	}()

	return cancel, done
}

// failingListener fails every Accept until it is closed.
type failingListener struct {
	calls     atomic.Int32
	closed    chan struct{}
	closeOnce sync.Once
	eighth    chan struct{}
}

func (l *failingListener) Accept() (net.Conn, error) {
	if l.calls.Add(1) == 8 {
		close(l.eighth)
	}

	select {
	case <-l.closed:
		return nil, net.ErrClosed
	default:
		return nil, syscall.EMFILE
	}
}

func (l *failingListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })

	return nil
}

func (l *failingListener) Addr() net.Addr {
	return &net.UnixAddr{Name: "failing", Net: "unix"}
}
