package client

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// newTestSocketPair creates a connected unixgram socket pair in a temp directory.
// Returns the listener (server) PacketConn and a send function for writing datagrams.
func newTestSocketPair(t *testing.T) (net.PacketConn, func(string)) {
	t.Helper()

	dir := t.TempDir()
	serverPath := filepath.Join(dir, "server.sock")
	clientPath := filepath.Join(dir, "client.sock")

	serverConn, err := net.ListenPacket("unixgram", serverPath)
	if err != nil {
		t.Fatalf("listen server socket: %v", err)
	}
	t.Cleanup(func() { _ = serverConn.Close() })

	clientConn, err := net.ListenPacket("unixgram", clientPath)
	if err != nil {
		t.Fatalf("listen client socket: %v", err)
	}
	t.Cleanup(func() { _ = clientConn.Close() })

	serverAddr, err := net.ResolveUnixAddr("unixgram", serverPath)
	if err != nil {
		t.Fatalf("resolve server addr: %v", err)
	}

	send := func(path string) {
		_, err := clientConn.WriteTo([]byte(path), serverAddr)
		if err != nil {
			t.Errorf("send datagram: %v", err)
		}
	}

	return serverConn, send
}

func TestBatchSizeTrigger(t *testing.T) {
	conn, send := newTestSocketPair(t)

	var mu sync.Mutex
	var pushed [][]string
	pushDone := make(chan struct{}, 10)

	push := func(_ context.Context, paths []string) error {
		mu.Lock()
		defer mu.Unlock()
		cp := make([]string, len(paths))
		copy(cp, paths)
		pushed = append(pushed, cp)
		pushDone <- struct{}{}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	l := NewListener(conn, push, ListenerConfig{
		BatchSize:    3,
		BatchTimeout: 10 * time.Second,
		MaxErrors:    5,
	})

	go func() {
		send("/nix/store/aaa")
		send("/nix/store/bbb")
		send("/nix/store/ccc")
		<-pushDone
		cancel()
	}()

	if err := l.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(pushed) < 1 {
		t.Fatalf("expected at least 1 push call, got %d", len(pushed))
	}

	if len(pushed[0]) != 3 {
		t.Errorf("expected 3 paths in first batch, got %d", len(pushed[0]))
	}
}

func TestBatchTimeoutTrigger(t *testing.T) {
	conn, send := newTestSocketPair(t)

	var mu sync.Mutex
	var pushed [][]string
	pushDone := make(chan struct{}, 10)

	push := func(_ context.Context, paths []string) error {
		mu.Lock()
		defer mu.Unlock()
		cp := make([]string, len(paths))
		copy(cp, paths)
		pushed = append(pushed, cp)
		pushDone <- struct{}{}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	l := NewListener(conn, push, ListenerConfig{
		BatchSize:    100, // Won't be reached.
		BatchTimeout: 200 * time.Millisecond,
		MaxErrors:    5,
	})

	go func() {
		send("/nix/store/aaa")
		send("/nix/store/bbb")
		<-pushDone // Wait for timeout-triggered push.
		cancel()
	}()

	if err := l.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(pushed) < 1 {
		t.Fatalf("expected at least 1 push call, got %d", len(pushed))
	}

	if len(pushed[0]) != 2 {
		t.Errorf("expected 2 paths in first batch, got %d", len(pushed[0]))
	}
}

func TestIdleTimeout(t *testing.T) {
	conn, _ := newTestSocketPair(t)

	push := func(_ context.Context, _ []string) error {
		t.Fatal("push should not be called")
		return nil
	}

	l := NewListener(conn, push, ListenerConfig{
		BatchSize:    100,
		BatchTimeout: 10 * time.Second,
		IdleTimeout:  200 * time.Millisecond,
		MaxErrors:    5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := l.Run(ctx)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
}

func TestDeduplication(t *testing.T) {
	conn, send := newTestSocketPair(t)

	var mu sync.Mutex
	var pushed [][]string
	pushDone := make(chan struct{}, 10)

	push := func(_ context.Context, paths []string) error {
		mu.Lock()
		defer mu.Unlock()
		cp := make([]string, len(paths))
		copy(cp, paths)
		pushed = append(pushed, cp)
		pushDone <- struct{}{}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	l := NewListener(conn, push, ListenerConfig{
		BatchSize:    10,
		BatchTimeout: 200 * time.Millisecond,
		MaxErrors:    5,
	})

	go func() {
		send("/nix/store/aaa")
		send("/nix/store/aaa")
		send("/nix/store/bbb")
		send("/nix/store/aaa")
		<-pushDone // Wait for timeout-triggered push.
		cancel()
	}()

	if err := l.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(pushed) < 1 {
		t.Fatalf("expected at least 1 push call, got %d", len(pushed))
	}

	paths := pushed[0]
	sort.Strings(paths)

	if len(paths) != 2 {
		t.Errorf("expected 2 deduplicated paths, got %d: %v", len(paths), paths)
	}
}

func TestMaxErrors(t *testing.T) {
	conn, send := newTestSocketPair(t)

	pushErr := fmt.Errorf("simulated push failure")

	push := func(_ context.Context, _ []string) error {
		return pushErr
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	l := NewListener(conn, push, ListenerConfig{
		BatchSize:    1,
		BatchTimeout: 100 * time.Millisecond,
		MaxErrors:    3,
	})

	// Send a single path before Run. The batch timer retries the failed batch
	// until MaxErrors consecutive failures are reached.
	send("/nix/store/path-0")

	runErr := l.Run(ctx)
	if runErr == nil {
		t.Fatal("Run should have returned an error after MaxErrors")
	}
}

func TestBatchTimerRetryAfterFailure(t *testing.T) {
	conn, send := newTestSocketPair(t)

	var mu sync.Mutex
	callCount := 0
	var pushed [][]string
	successDone := make(chan struct{}, 10)

	push := func(_ context.Context, paths []string) error {
		mu.Lock()
		callCount++
		n := callCount
		mu.Unlock()

		// Fail on first push, succeed on subsequent pushes.
		if n == 1 {
			return fmt.Errorf("simulated first push failure")
		}

		mu.Lock()
		cp := make([]string, len(paths))
		copy(cp, paths)
		pushed = append(pushed, cp)
		mu.Unlock()
		successDone <- struct{}{}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	l := NewListener(conn, push, ListenerConfig{
		BatchSize:    100,                    // Won't be reached.
		BatchTimeout: 200 * time.Millisecond, // Timer fires, push fails, timer restarts, push succeeds.
		MaxErrors:    5,
	})

	go func() {
		send("/nix/store/aaa")
		<-successDone // Wait for the retry to succeed.
		cancel()
	}()

	if err := l.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(pushed) < 1 {
		t.Fatalf("expected at least 1 successful push after retry, got %d", len(pushed))
	}

	// The retried batch should contain the original path.
	found := false
	for _, batch := range pushed {
		for _, p := range batch {
			if p == "/nix/store/aaa" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected /nix/store/aaa in retried batch")
	}
}

func TestErrorCountReset(t *testing.T) {
	conn, send := newTestSocketPair(t)

	var mu sync.Mutex
	callCount := 0

	push := func(_ context.Context, _ []string) error {
		mu.Lock()
		callCount++
		n := callCount
		mu.Unlock()

		// Fail on calls 1 and 2, succeed on call 3.
		if n <= 2 {
			return fmt.Errorf("simulated failure %d", n)
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	l := NewListener(conn, push, ListenerConfig{
		BatchSize:    1,
		BatchTimeout: 100 * time.Millisecond,
		MaxErrors:    3, // Would be reached if errors didn't reset.
	})

	go func() {
		send("/nix/store/path-1")
		time.Sleep(200 * time.Millisecond)
		send("/nix/store/path-2")
		time.Sleep(200 * time.Millisecond)
		send("/nix/store/path-3") // This push succeeds, resetting error count.
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	err := l.Run(ctx)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
}

func TestContextCancellation(t *testing.T) {
	conn, send := newTestSocketPair(t)

	var mu sync.Mutex
	var pushed [][]string

	push := func(_ context.Context, paths []string) error {
		mu.Lock()
		defer mu.Unlock()
		cp := make([]string, len(paths))
		copy(cp, paths)
		pushed = append(pushed, cp)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	l := NewListener(conn, push, ListenerConfig{
		BatchSize:    100,
		BatchTimeout: 10 * time.Second,
		MaxErrors:    5,
	})

	go func() {
		send("/nix/store/aaa")
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := l.Run(ctx)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	// The remaining path should have been pushed on shutdown.
	mu.Lock()
	defer mu.Unlock()

	totalPaths := 0
	for _, batch := range pushed {
		totalPaths += len(batch)
	}

	if totalPaths != 1 {
		t.Errorf("expected 1 path pushed on shutdown, got %d", totalPaths)
	}
}

func TestMultipleBatches(t *testing.T) {
	conn, send := newTestSocketPair(t)

	var mu sync.Mutex
	var pushed [][]string
	pushDone := make(chan struct{}, 10)

	push := func(_ context.Context, paths []string) error {
		mu.Lock()
		defer mu.Unlock()
		cp := make([]string, len(paths))
		copy(cp, paths)
		pushed = append(pushed, cp)
		pushDone <- struct{}{}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	l := NewListener(conn, push, ListenerConfig{
		BatchSize:    2,
		BatchTimeout: 10 * time.Second,
		MaxErrors:    5,
	})

	go func() {
		send("/nix/store/aaa")
		send("/nix/store/bbb")
		<-pushDone // Wait for first batch.
		send("/nix/store/ccc")
		send("/nix/store/ddd")
		<-pushDone // Wait for second batch.
		cancel()
	}()

	if err := l.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(pushed) < 2 {
		t.Fatalf("expected at least 2 push calls, got %d", len(pushed))
	}

	for i, batch := range pushed {
		if len(batch) > 2 {
			t.Errorf("batch %d has %d paths, expected at most 2", i, len(batch))
		}
	}
}

func TestZeroDatagram(t *testing.T) {
	conn, send := newTestSocketPair(t)

	var mu sync.Mutex
	var pushed [][]string
	pushDone := make(chan struct{}, 10)

	push := func(_ context.Context, paths []string) error {
		mu.Lock()
		defer mu.Unlock()
		cp := make([]string, len(paths))
		copy(cp, paths)
		pushed = append(pushed, cp)
		pushDone <- struct{}{}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	l := NewListener(conn, push, ListenerConfig{
		BatchSize:    10,
		BatchTimeout: 200 * time.Millisecond,
		MaxErrors:    5,
	})

	go func() {
		send("") // Zero-length datagram — should be silently dropped.
		send("/nix/store/aaa")
		<-pushDone // Wait for timeout-triggered push.
		cancel()
	}()

	if err := l.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(pushed) < 1 {
		t.Fatalf("expected at least 1 push call, got %d", len(pushed))
	}

	// Only the non-empty path should have been pushed.
	if len(pushed[0]) != 1 {
		t.Errorf("expected 1 path (zero-length dropped), got %d: %v", len(pushed[0]), pushed[0])
	}
}

// TestDrainUsesFreshContext verifies that when the context is cancelled,
// the drain push uses a fresh context rather than the cancelled one.
func TestDrainUsesFreshContext(t *testing.T) {
	conn, send := newTestSocketPair(t)

	var mu sync.Mutex
	var pushed [][]string

	push := func(ctx context.Context, paths []string) error {
		if ctx.Err() != nil {
			return fmt.Errorf("push called with cancelled context")
		}
		mu.Lock()
		defer mu.Unlock()
		cp := make([]string, len(paths))
		copy(cp, paths)
		pushed = append(pushed, cp)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	l := NewListener(conn, push, ListenerConfig{
		BatchSize:    100,
		BatchTimeout: 10 * time.Second,
		MaxErrors:    5,
		DrainTimeout: 2 * time.Second,
	})

	go func() {
		send("/nix/store/drain-test")
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := l.Run(ctx)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	totalPaths := 0
	for _, batch := range pushed {
		totalPaths += len(batch)
	}

	if totalPaths != 1 {
		t.Errorf("expected drain to push 1 path with fresh context, got %d", totalPaths)
	}
}
