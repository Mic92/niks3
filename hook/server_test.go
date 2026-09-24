package hook_test

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Mic92/niks3/hook"
)

// TestServerClientIntegration tests the full server+client flow: multiple
// concurrent clients send paths, the server queues them, and acks each client.
func TestServerClientIntegration(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	socketPath := filepath.Join(dir, "test.sock")

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
func TestServerQueueError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	socketPath := filepath.Join(dir, "test.sock")

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

// The socket is writable by the build users. A request must be bounded in
// size, and a path that is not a store path must be refused before it
// reaches the queue, where the worker would stat and push it every round.
func TestServerRefusesOversizedAndNonStoreRequests(t *testing.T) {
	t.Parallel()

	socketPath := filepath.Join(t.TempDir(), "test.sock")

	lc := net.ListenConfig{}

	ln, err := lc.Listen(context.Background(), "unix", socketPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var queued atomic.Int32

	srv := hook.NewServer(ln, func(_ []string) error {
		queued.Add(1)

		return nil
	})
	srv.StoreDir = "/nix/store"
	srv.MaxRequestBytes = 4096

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = srv.Serve(ctx)
	}()

	const hash = "0123456789abcdfghijklmnpqrsvwxyz"

	for _, p := range []string{
		"/etc/shadow",
		"/nix/store",
		"/nix/store/",
		"/nix/store/../../etc/shadow",
		"/nix/store/" + hash + "-hello/bin/sh",
		"nix/store/" + hash + "-hello",
		"/nix/storeX/" + hash + "-hello",
		// Directly below the store but not store paths. Each exists or
		// fails Lstat with something other than "not found", so the
		// worker would keep it queued and retry it for good.
		"/nix/store/.links",
		"/nix/store/aaa-hello",
		"/nix/store/" + hash,
		"/nix/store/" + hash + "-",
		"/nix/store/" + strings.Replace(hash, "a", "e", 1) + "-hello",
		"/nix/store/" + hash + "-hel\x00lo",
		"/nix/store/" + hash + "-hel/lo",
		"/nix/store/" + hash + "-" + strings.Repeat("x", 212),
	} {
		if err := hook.SendPaths(socketPath, []string{p}); err == nil {
			t.Errorf("%q was accepted as a store path", p)
		}
	}

	if err := hook.SendPaths(socketPath, []string{
		"/nix/store/" + hash + "-hello-2.12.1",
		"/nix/store/" + strings.Repeat("0", 32) + "-world.drv",
		"/nix/store/" + strings.Repeat("z", 32) + "-" + strings.Repeat("A+-._?=", 30) + "x",
	}); err != nil {
		t.Errorf("store paths refused: %v", err)
	}

	// A request that streams past the limit is cut off and answered with an
	// error rather than buffered whole.
	huge := make([]string, 0, 200)
	for i := range 200 {
		huge = append(huge, "/nix/store/"+strconv.Itoa(i)+"-"+strings.Repeat("x", 100))
	}

	if err := hook.SendPaths(socketPath, huge); err == nil {
		t.Error("oversized request was accepted")
	}

	cancel()
	<-done

	if n := queued.Load(); n != 1 {
		t.Errorf("queue called %d times, want 1 (the valid request only)", n)
	}
}

// TestGetListenerSocketActivation tests the systemd socket activation path.
// Uses a subprocess because dup2 to fd 3 conflicts with Go's runtime netpoller.
func TestGetListenerSocketActivation(t *testing.T) { //nolint:paralleltest // t.Setenv incompatible with t.Parallel
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

	socketPath := filepath.Join(t.TempDir(), "test.sock")

	lc := net.ListenConfig{}

	ln, err := lc.Listen(context.Background(), "unix", socketPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := hook.NewServer(ln, func(_ []string) error { return nil })
	srv.ConnTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = srv.Serve(ctx)
	}()

	dialer := net.Dialer{}

	stalled, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	defer func() { _ = stalled.Close() }()

	// Connections are accepted in order, so once a later send has been
	// answered the stalled one has been accepted too and its handler is
	// waiting for a request.
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

// A persistent Accept error (EMFILE) must not spin: Serve backs off, up to a
// second, and a shutdown during the backoff ends it at once.
func TestServerBacksOffOnAcceptErrors(t *testing.T) {
	t.Parallel()

	ln := &failingListener{closed: make(chan struct{}), eighth: make(chan struct{})}
	srv := hook.NewServer(ln, func(_ []string) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	serving := time.Now()

	go func() {
		defer close(done)

		_ = srv.Serve(ctx)
	}()

	// 5+10+...+320ms of backoff lie behind the eighth failure, and 640ms
	// ahead of it.
	select {
	case <-ln.eighth:
	case <-time.After(5 * time.Second):
		t.Fatalf("only %d accept calls in 5s", ln.calls.Load())
	}

	if elapsed := time.Since(serving); elapsed < 500*time.Millisecond {
		t.Errorf("eight accept failures within %v: no backoff between them", elapsed)
	}

	cancel()

	start := time.Now()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after shutdown")
	}

	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Errorf("Serve took %v to return: shutdown waited out the backoff", elapsed)
	}

	// The eight failures, and one more Accept once the shutdown ended the
	// backoff.
	if n := ln.calls.Load(); n > 9 {
		t.Errorf("%d accept calls, want at most 9: no backoff between failures", n)
	}
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
