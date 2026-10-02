package hook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultSocketPath is the default path for the niks3-hook upload socket.
// It can be overridden at build time via:
//
//	-ldflags "-X github.com/Mic92/niks3/hook.DefaultSocketPath=/custom/path"
var DefaultSocketPath = "/run/niks3/upload-to-cache.sock" //nolint:gochecknoglobals // ldflags override

// Response status values.
const (
	statusOK    = "ok"
	statusError = "error"
)

// QueueFunc is called by the server to persist paths. It must return nil on success.
type QueueFunc func(paths []string) error

// DefaultConnTimeout bounds one connection from accept to response. Clients
// give up after sendTimeout, so this only has to be long enough for a busy
// queue; without it a stalled client keeps a handler alive for as long as
// the connection exists, and Serve waits for it on shutdown.
const DefaultConnTimeout = 30 * time.Second

// DefaultMaxRequestBytes bounds one request. Nix hands the hook a build's
// output paths, a few hundred bytes each; a request larger than this is not
// one Nix sent. Without a bound the JSON decoder buffers whatever a client
// streams, and the socket is writable by the build users.
const DefaultMaxRequestBytes = 16 << 20

// Server listens on a unix stream socket and accepts path submissions.
type Server struct {
	listener  net.Listener
	queueFunc QueueFunc
	wg        sync.WaitGroup

	// ConnTimeout is the deadline for handling one connection.
	ConnTimeout time.Duration
	// MaxRequestBytes is the most a client may send in one request.
	MaxRequestBytes int64
	// StoreDir, if set, is the Nix store directory every queued path must
	// be a direct child of. Anything else is refused: the worker would stat
	// and try to push it every round, and the hook is not a way to make the
	// daemon read arbitrary files.
	StoreDir string
}

// NewServer creates a Server that accepts connections on listener and calls
// queueFunc for each batch of paths received.
func NewServer(listener net.Listener, queueFunc QueueFunc) *Server {
	return &Server{
		listener:        listener,
		queueFunc:       queueFunc,
		ConnTimeout:     DefaultConnTimeout,
		MaxRequestBytes: DefaultMaxRequestBytes,
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

	var (
		lastErr error
		delay   time.Duration
	)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}

			slog.Error("Accept failed", "error", err)
			lastErr = err

			// Back off like net/http does: a persistent error (EMFILE) would
			// otherwise spin, logging once per iteration.
			delay = min(max(2*delay, 5*time.Millisecond), time.Second)

			select {
			case <-ctx.Done():
			case <-time.After(delay):
			}

			continue
		}

		delay = 0

		s.wg.Go(func() {
			s.handleConn(conn)
		})
	}

	s.wg.Wait()

	return lastErr
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	if s.ConnTimeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(s.ConnTimeout))
	}

	var body io.Reader = conn
	if s.MaxRequestBytes > 0 {
		body = io.LimitReader(conn, s.MaxRequestBytes)
	}

	var req Request
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		slog.Error("Failed to decode request", "error", err)
		writeResponse(conn, Response{Status: statusError, Message: "invalid request"})

		return
	}

	if len(req.Paths) == 0 {
		writeResponse(conn, Response{Status: statusOK})

		return
	}

	for _, p := range req.Paths {
		if !s.validPath(p) {
			slog.Error("Refusing path outside the store", "path", p, "store", s.StoreDir)
			writeResponse(conn, Response{Status: statusError, Message: "not a store path: " + p})

			return
		}
	}

	if err := s.queueFunc(req.Paths); err != nil {
		slog.Error("Failed to queue paths", "error", err, "count", len(req.Paths))
		writeResponse(conn, Response{Status: statusError, Message: err.Error()})

		return
	}

	slog.Debug("Queued paths", "count", len(req.Paths))
	writeResponse(conn, Response{Status: statusOK})
}

// validPath reports whether p is a store path under s.StoreDir: absolute,
// clean, exactly one component below the store directory, and that component
// a store path name.
func (s *Server) validPath(p string) bool {
	if s.StoreDir == "" {
		return true
	}

	if !filepath.IsAbs(p) || filepath.Clean(p) != p {
		return false
	}

	rel, err := filepath.Rel(s.StoreDir, p)
	if err != nil {
		return false
	}

	return isStorePathName(rel)
}

// Store path names as Nix defines them (StorePath in libstore/path.cc): a
// hash part in Nix's base-32 alphabet, a dash, and a name of limited length
// drawn from a small set of characters.
const (
	storeHashLen     = 32
	storeMaxNameLen  = 211
	nixBase32Chars   = "0123456789abcdfghijklmnpqrsvwxyz"
	storeNameSymbols = "+-._?="
)

// isStorePathName reports whether base is a valid store path base name.
// Anything else directly below the store directory, such as .links, would
// be queued for good: it exists (or cannot even be stat'ed) and never
// becomes a valid path the push could upload.
func isStorePathName(base string) bool {
	if len(base) < storeHashLen+2 || len(base) > storeHashLen+1+storeMaxNameLen || base[storeHashLen] != '-' {
		return false
	}

	for _, c := range base[:storeHashLen] {
		if !strings.ContainsRune(nixBase32Chars, c) {
			return false
		}
	}

	for _, c := range base[storeHashLen+1:] {
		isAlnum := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if !isAlnum && !strings.ContainsRune(storeNameSymbols, c) {
			return false
		}
	}

	return true
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
