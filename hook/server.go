package hook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
)

// DefaultSocketPath is the default path for the niks3-hook upload socket.
// It can be overridden at build time via:
//
//	-ldflags "-X github.com/Mic92/niks3/hook.DefaultSocketPath=/custom/path"
var DefaultSocketPath = "/run/niks3/upload-to-cache.sock" //nolint:gochecknoglobals // ldflags override

// QueueFunc is called by the server to persist paths. It must return nil on success.
type QueueFunc func(paths []string) error

// Server listens on a unix stream socket and accepts path submissions.
type Server struct {
	listener  net.Listener
	queueFunc QueueFunc
	wg        sync.WaitGroup
}

// NewServer creates a Server that accepts connections on listener and calls
// queueFunc for each batch of paths received.
func NewServer(listener net.Listener, queueFunc QueueFunc) *Server {
	return &Server{
		listener:  listener,
		queueFunc: queueFunc,
	}
}

// Serve accepts connections until ctx is cancelled. It blocks until all
// in-flight connection handlers have finished.
func (s *Server) Serve(ctx context.Context) error {
	// Close the listener when the context is cancelled so Accept returns.
	go func() {
		<-ctx.Done()

		_ = s.listener.Close()
	}()

	var lastErr error

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}

			slog.Error("Accept failed", "error", err)
			lastErr = err

			continue
		}

		s.wg.Go(func() {
			s.handleConn(conn)
		})
	}

	s.wg.Wait()

	return lastErr
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	var req Request
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		slog.Error("Failed to decode request", "error", err)
		writeResponse(conn, Response{Status: "error", Message: "invalid request"})

		return
	}

	if len(req.Paths) == 0 {
		writeResponse(conn, Response{Status: "ok"})

		return
	}

	if err := s.queueFunc(req.Paths); err != nil {
		slog.Error("Failed to queue paths", "error", err, "count", len(req.Paths))
		writeResponse(conn, Response{Status: "error", Message: err.Error()})

		return
	}

	slog.Debug("Queued paths", "count", len(req.Paths))
	writeResponse(conn, Response{Status: "ok"})
}

func writeResponse(conn net.Conn, resp Response) {
	if err := json.NewEncoder(conn).Encode(resp); err != nil {
		slog.Error("Failed to write response", "error", err)
	}
}

// GetListener acquires a net.Listener for the unix stream socket.
//
// It first checks for systemd socket activation (LISTEN_PID + LISTEN_FDS).
// If found, it uses fd 3 via net.FileListener, unsets the env vars, and returns (listener, true, nil).
// Otherwise, it removes any stale socket file and creates a new unix stream listener.
// The bool return value indicates whether the socket was acquired via socket activation;
// callers should not unlink the socket file on exit if true.
func GetListener(socketPath string) (net.Listener, bool, error) {
	ln, ok, err := trySocketActivation()
	if err != nil {
		return nil, false, err
	}

	if ok {
		return ln, true, nil
	}

	// No socket activation — create our own socket.
	// Remove stale socket file if it exists.
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, false, fmt.Errorf("removing stale socket %s: %w", socketPath, err)
	}

	lc := net.ListenConfig{}

	ln, err = lc.Listen(context.Background(), "unix", socketPath)
	if err != nil {
		return nil, false, fmt.Errorf("listening on %s: %w", socketPath, err)
	}

	return ln, false, nil
}

// trySocketActivation checks for systemd socket activation (LISTEN_PID + LISTEN_FDS).
func trySocketActivation() (net.Listener, bool, error) {
	listenPID := os.Getenv("LISTEN_PID")
	if listenPID == "" {
		return nil, false, nil
	}

	pid, err := strconv.Atoi(listenPID)
	if err != nil || pid != os.Getpid() {
		return nil, false, nil //nolint:nilerr // PID mismatch means activation isn't for us
	}

	nfds, err := strconv.Atoi(os.Getenv("LISTEN_FDS"))
	if err != nil || nfds < 1 {
		return nil, false, nil //nolint:nilerr // no fds means no activation
	}

	// fd 3 is the first socket activation fd.
	f := os.NewFile(3, "systemd-socket")
	if f == nil {
		return nil, false, errors.New("fd 3 is not valid")
	}

	ln, err := net.FileListener(f)
	_ = f.Close()

	if err != nil {
		return nil, false, fmt.Errorf("creating listener from fd 3: %w", err)
	}

	// Unset env vars so child processes don't try to use them.
	_ = os.Unsetenv("LISTEN_PID")
	_ = os.Unsetenv("LISTEN_FDS")

	return ln, true, nil
}
