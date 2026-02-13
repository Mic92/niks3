package main

import (
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestSendPaths(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "test.sock")

	// Create a receiving socket.
	conn, err := net.ListenPacket("unixgram", socketPath)
	if err != nil {
		t.Fatalf("listen socket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err := sendPaths(socketPath, "/nix/store/aaa /nix/store/bbb /nix/store/ccc"); err != nil {
		t.Fatalf("sendPaths failed: %v", err)
	}

	// Read all 3 datagrams.
	buf := make([]byte, 4096)
	var received []string

	for i := 0; i < 3; i++ {
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("set deadline: %v", err)
		}

		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			t.Fatalf("read datagram %d: %v", i, err)
		}

		received = append(received, string(buf[:n]))
	}

	expected := []string{"/nix/store/aaa", "/nix/store/bbb", "/nix/store/ccc"}
	for i, exp := range expected {
		if received[i] != exp {
			t.Errorf("datagram %d: expected %q, got %q", i, exp, received[i])
		}
	}
}

func TestEmptyOutPaths(t *testing.T) {
	// Empty OUT_PATHS returns nil immediately without connecting.
	if err := sendPaths("/nonexistent/socket", ""); err != nil {
		t.Fatalf("sendPaths failed for empty paths: %v", err)
	}
}

func TestMissingSocket(t *testing.T) {
	// Connection error should be reported.
	err := sendPaths("/nonexistent/path/to/socket.sock", "/nix/store/aaa")
	if err == nil {
		t.Fatal("expected error for missing socket, got nil")
	}
}
