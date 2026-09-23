package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// listenFdsStart is the first file descriptor systemd passes for socket
// activation; see sd_listen_fds(3).
const listenFdsStart = 3

// namedListener carries the fd name from LISTEN_FDNAMES.
type namedListener struct {
	name string
	ln   net.Listener
}

// systemdListeners returns the sockets systemd passed (LISTEN_FDS), or nil when
// not socket activated. The activation variables are unset so children do not
// inherit them.
func systemdListeners() ([]namedListener, error) {
	names := os.Getenv("LISTEN_FDNAMES")

	defer func() {
		_ = os.Unsetenv("LISTEN_PID")
		_ = os.Unsetenv("LISTEN_FDS")
		_ = os.Unsetenv("LISTEN_FDNAMES")
	}()

	pid, err := strconv.Atoi(os.Getenv("LISTEN_PID"))
	if err != nil || pid != os.Getpid() {
		return nil, nil
	}

	count, err := strconv.Atoi(os.Getenv("LISTEN_FDS"))
	if err != nil || count == 0 {
		return nil, nil
	}

	fdNames := strings.Split(names, ":")
	out := make([]namedListener, 0, count)

	for i := range count {
		fd := listenFdsStart + i

		syscall.CloseOnExec(fd)

		file := os.NewFile(uintptr(fd), "LISTEN_FD")

		ln, err := net.FileListener(file)

		// FileListener dups the fd, so the original file is no longer needed.
		_ = file.Close()

		if err != nil {
			for _, l := range out {
				_ = l.ln.Close()
			}

			return nil, fmt.Errorf("failed to use socket-activated fd %d: %w", fd, err)
		}

		name := ""
		if i < len(fdNames) {
			name = fdNames[i]
		}

		out = append(out, namedListener{name: name, ln: ln})
	}

	return out, nil
}

// systemdListener returns the first socket-activated listener.
func systemdListener() (net.Listener, error) {
	lns, err := systemdListeners()
	if err != nil || len(lns) == 0 {
		return nil, err
	}

	for _, l := range lns[1:] {
		_ = l.ln.Close()
	}

	return lns[0].ln, nil
}

// sdNotify sends a status update to systemd via the NOTIFY_SOCKET. It is a
// no-op when the socket is not set (i.e. not running under systemd with
// Type=notify). See sd_notify(3).
func sdNotify(state string) error {
	socket := os.Getenv("NOTIFY_SOCKET")
	if socket == "" {
		return nil
	}

	addr := &net.UnixAddr{Net: "unixgram", Name: socket}

	// An abstract namespace socket is encoded with a leading '@'.
	if strings.HasPrefix(socket, "@") {
		addr.Name = "\x00" + socket[1:]
	}

	conn, err := net.DialUnix(addr.Net, nil, addr)
	if err != nil {
		return fmt.Errorf("failed to dial systemd notify socket: %w", err)
	}

	defer func() {
		_ = conn.Close()
	}()

	if _, err := conn.Write([]byte(state)); err != nil {
		return fmt.Errorf("failed to write to systemd notify socket: %w", err)
	}

	return nil
}

// notifySystemd sends a notify message best-effort, logging failures instead of
// propagating them: a missing or broken notify socket must not stop the server.
func notifySystemd(state string) {
	if err := sdNotify(state); err != nil {
		slog.Warn("failed to notify systemd", "state", state, "error", err)
	}
}

// watchdogInterval returns the heartbeat interval (half of WATCHDOG_USEC, per
// sd_watchdog_enabled(3)), or 0 when the watchdog is not enabled for us.
func watchdogInterval() time.Duration {
	if pid := os.Getenv("WATCHDOG_PID"); pid != "" {
		if p, err := strconv.Atoi(pid); err != nil || p != os.Getpid() {
			return 0
		}
	}

	usec, err := strconv.ParseInt(os.Getenv("WATCHDOG_USEC"), 10, 64)
	if err != nil || usec <= 0 {
		return 0
	}

	return time.Duration(usec) * time.Microsecond / 2
}

// runWatchdog sends WATCHDOG=1 to systemd every interval until ctx is done,
// skipping the beat when check fails so systemd restarts a wedged process.
func runWatchdog(ctx context.Context, interval time.Duration, check func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkCtx, cancel := context.WithTimeout(ctx, interval)
			err := check(checkCtx)

			cancel()

			if err != nil {
				slog.Warn("watchdog liveness check failed, skipping heartbeat", "error", err)

				continue
			}

			notifySystemd("WATCHDOG=1")
		}
	}
}
