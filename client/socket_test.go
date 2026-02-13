package client

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

func TestCreateSocket(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "test.sock")

	conn, activated, err := GetSocket(socketPath)
	if err != nil {
		t.Fatalf("GetSocket failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if activated {
		t.Error("expected activated=false for self-created socket")
	}

	// Verify the socket file exists.
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("socket file does not exist: %v", err)
	}

	// Verify we can send and receive a datagram.
	clientPath := filepath.Join(dir, "client.sock")
	clientConn, err := net.ListenPacket("unixgram", clientPath)
	if err != nil {
		t.Fatalf("listen client socket: %v", err)
	}
	defer func() { _ = clientConn.Close() }()

	serverAddr, err := net.ResolveUnixAddr("unixgram", socketPath)
	if err != nil {
		t.Fatalf("resolve server addr: %v", err)
	}

	msg := []byte("/nix/store/test-path")
	if _, err := clientConn.WriteTo(msg, serverAddr); err != nil {
		t.Fatalf("write datagram: %v", err)
	}

	buf := make([]byte, 4096)
	n, _, err := conn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("read datagram: %v", err)
	}

	if string(buf[:n]) != string(msg) {
		t.Errorf("expected %q, got %q", msg, buf[:n])
	}
}

// TestSocketActivation tests the systemd socket activation path in GetSocket.
// It spawns a subprocess because dup2 to fd 3 conflicts with Go's runtime netpoller.
// The socket is passed to the subprocess as fd 3 via exec.Cmd.ExtraFiles.
func TestSocketActivation(t *testing.T) {
	if os.Getenv("GO_TEST_SOCKET_ACTIVATION") == "1" {
		// Subprocess: fd 3 is a unixgram socket passed by the parent.
		socketPath := os.Getenv("GO_TEST_SOCKET_PATH")
		t.Setenv("LISTEN_PID", strconv.Itoa(os.Getpid()))
		t.Setenv("LISTEN_FDS", "1")

		conn, activated, err := GetSocket(socketPath)
		if err != nil {
			t.Fatalf("GetSocket failed: %v", err)
		}
		defer func() { _ = conn.Close() }()

		if !activated {
			t.Fatal("expected activated=true for socket-activated socket")
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

	// Create a unixgram socket to simulate systemd socket activation.
	conn, err := net.ListenPacket("unixgram", socketPath)
	if err != nil {
		t.Fatalf("listen socket: %v", err)
	}

	// Get a dup'd file descriptor from the connection.
	uc := conn.(*net.UnixConn)
	f, err := uc.File()
	if err != nil {
		_ = conn.Close()
		t.Fatalf("get file: %v", err)
	}
	_ = conn.Close() // Close original connection; f holds a dup'd fd.
	defer func() { _ = f.Close() }()

	// Launch subprocess with the socket on fd 3 via ExtraFiles.
	cmd := exec.Command(os.Args[0], "-test.run=^TestSocketActivation$", "-test.v")
	cmd.Env = append(os.Environ(),
		"GO_TEST_SOCKET_ACTIVATION=1",
		"GO_TEST_SOCKET_PATH="+socketPath,
	)
	cmd.ExtraFiles = []*os.File{f} // ExtraFiles[0] becomes fd 3 in subprocess.

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("subprocess failed: %v\n%s", err, output)
	}
	t.Log(string(output))
}

// TestPIDMismatchFallsThrough verifies that when LISTEN_PID is set but doesn't
// match the current process, GetSocket falls through to creating a new socket.
func TestPIDMismatchFallsThrough(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "test.sock")

	// Set LISTEN_PID to a PID that doesn't match this process.
	t.Setenv("LISTEN_PID", "999999999")
	t.Setenv("LISTEN_FDS", "1")

	conn, activated, err := GetSocket(socketPath)
	if err != nil {
		t.Fatalf("GetSocket failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if activated {
		t.Error("expected activated=false when LISTEN_PID doesn't match")
	}

	// The socket file should have been created (fallthrough path).
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("socket file does not exist: %v", err)
	}
}

func TestCleanupStaleSocket(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "test.sock")

	// Create a stale socket file.
	staleConn, err := net.ListenPacket("unixgram", socketPath)
	if err != nil {
		t.Fatalf("create stale socket: %v", err)
	}
	_ = staleConn.Close()

	// Verify the stale socket file exists.
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("stale socket file does not exist: %v", err)
	}

	// GetSocket should remove the stale file and create a new one.
	conn, activated, err := GetSocket(socketPath)
	if err != nil {
		t.Fatalf("GetSocket failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if activated {
		t.Error("expected activated=false for self-created socket")
	}

	// Verify the new socket works.
	clientPath := filepath.Join(dir, "client.sock")
	clientConn, err := net.ListenPacket("unixgram", clientPath)
	if err != nil {
		t.Fatalf("listen client socket: %v", err)
	}
	defer func() { _ = clientConn.Close() }()

	serverAddr, err := net.ResolveUnixAddr("unixgram", socketPath)
	if err != nil {
		t.Fatalf("resolve server addr: %v", err)
	}

	msg := []byte("/nix/store/test-path")
	if _, err := clientConn.WriteTo(msg, serverAddr); err != nil {
		t.Fatalf("write datagram: %v", err)
	}

	buf := make([]byte, 4096)
	n, _, err := conn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("read datagram: %v", err)
	}

	if string(buf[:n]) != string(msg) {
		t.Errorf("expected %q, got %q", msg, buf[:n])
	}
}
