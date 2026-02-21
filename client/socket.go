package client

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
)

// DefaultSocketPath is the default path for the niks3 upload socket.
// It can be overridden at build time via:
//
//	-ldflags "-X github.com/Mic92/niks3/client.DefaultSocketPath=/custom/path"
var DefaultSocketPath = "/run/niks3/upload-to-cache.sock"

// GetSocket acquires a packet connection for receiving store paths.
//
// It first checks for systemd socket activation (LISTEN_PID + LISTEN_FDS).
// If found, it uses fd 3 via net.FilePacketConn, unsets the env vars, and returns (conn, true, nil).
// Otherwise, it removes any stale socket file and creates a new unixgram listener.
// The bool return value indicates whether the socket was acquired via socket activation;
// callers should not unlink the socket file on exit if true.
func GetSocket(socketPath string) (net.PacketConn, bool, error) {
	// Check for systemd socket activation.
	if listenPID := os.Getenv("LISTEN_PID"); listenPID != "" {
		pid, err := strconv.Atoi(listenPID)
		if err == nil && pid == os.Getpid() {
			listenFDs := os.Getenv("LISTEN_FDS")
			nfds, err := strconv.Atoi(listenFDs)
			if err == nil && nfds >= 1 {
				// fd 3 is the first socket activation fd.
				f := os.NewFile(3, "systemd-socket")
				if f == nil {
					return nil, false, fmt.Errorf("fd 3 is not valid")
				}

				conn, err := net.FilePacketConn(f)
				_ = f.Close()

				if err != nil {
					return nil, false, fmt.Errorf("creating packet conn from fd 3: %w", err)
				}

				// Unset env vars so child processes don't try to use them.
				_ = os.Unsetenv("LISTEN_PID")
				_ = os.Unsetenv("LISTEN_FDS")

				return conn, true, nil
			}
		}
	}

	// No socket activation — create our own socket.
	// Remove stale socket file if it exists.
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, false, fmt.Errorf("removing stale socket %s: %w", socketPath, err)
	}

	conn, err := net.ListenPacket("unixgram", socketPath)
	if err != nil {
		return nil, false, fmt.Errorf("listening on %s: %w", socketPath, err)
	}

	return conn, false, nil
}
