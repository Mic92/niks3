package hook_test

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

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

	srv := hook.NewServer(ln, queueFunc, nil)

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
	}, nil)

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

func TestServerWait(t *testing.T) {
	t.Parallel()

	socketPath := filepath.Join(t.TempDir(), "test.sock")

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", socketPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var queued, pushed []string

	pushErr := errors.New("409 stale claim")
	srv := hook.NewServer(ln, func(paths []string) error {
		queued = append(queued, paths...)

		return nil
	}, func(_ context.Context, paths []string) ([]string, error) {
		pushed = append(pushed, paths...)
		if paths[0] == "/nix/store/bad" {
			return nil, pushErr
		}

		return paths, nil
	})

	go func() { _ = srv.Serve(t.Context()) }()

	if err := hook.Send(socketPath, hook.Request{Paths: []string{"/nix/store/good"}, Wait: true}); err != nil {
		t.Fatalf("wait push: %v", err)
	}

	err = hook.Send(socketPath, hook.Request{Paths: []string{"/nix/store/bad"}, Wait: true})
	if err == nil || !strings.Contains(err.Error(), pushErr.Error()) {
		t.Fatalf("want push error propagated, got %v", err)
	}

	if len(queued) != 0 {
		t.Fatalf("wait requests must bypass the queue, queued=%v", queued)
	}

	if !slices.Equal(pushed, []string{"/nix/store/good", "/nix/store/bad"}) {
		t.Fatalf("pushed = %v", pushed)
	}
}
